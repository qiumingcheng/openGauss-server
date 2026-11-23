/* Restricted literal handling. The standard initial/current separation and
 * the one text-NULL current-default escape follow the pinned DuckLake
 * ColumnToSQLRecursive and DuckLakeFieldId implementation. No metadata string
 * is parsed as executable SQL, and no user expression is evaluated here. */
#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_types.h"
#include "lake_version.h"
#include "lake_column_default.h"
#include "lake_inline_private.h"
#include "lake_binary.h"
#include "lake_inline_sources.h"
#include "lake_json.h"
#include "lake_numeric.h"
#include "lake_temporal.h"
#include "lake_temporal_text.h"
#include "lake_uuid.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/readfuncs.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/float.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/shortest_dec.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"
#include <cctype>
#include <cmath>

bool LakeLiteralDefaultType(Oid type, int32 typmod)
{
    if (type == BOOLOID || type == INT2OID || type == INT4OID ||
        type == INT8OID || type == TEXTOID ||
        (type == BYTEAOID && LakeBinaryDefaultVersionEnabled()))
        return typmod == -1;
    if (!LakeExtendedDefaultsVersionEnabled())
        return false;
    if (type == FLOAT4OID || type == FLOAT8OID || type == DATEOID ||
        type == TIMESTAMPOID || type == TIMESTAMPTZOID ||
        type == UUIDOID || type == JSONOID)
        return typmod == -1;
    if (type == NUMERICOID)
        return LakeNumericTypmod(typmod);
    return false;
}

static void LakeLiteralUnsupported(void)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("native lake default requires one admitted typed literal")));
}

static Size LakeLiteralTextLength(const char* value)
{
    if(value==NULL)LakeLiteralUnsupported();
    Size length=strlen(value);
    if(length>LAKE_LITERAL_DEFAULT_TEXT_LIMIT)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("native lake literal default exceeds its text byte limit")));
    return length;
}

/* BLOB text is a bounded port of DuckDB Blob::ToString/TryGetBlobSize, not
 * PostgreSQL bytea_output. Pinned source identities are in the slice's
 * codec-provenance.json. Source: duckdb/src/common/types/blob.cpp
 * SHA256 ef0ca4a68195063603db03b80e5e12b3c520a3b9557269e7a441738d178d21b5
 * Keep the standard 1 MiB ENCODED text limit: fully
 * escaped content is at most 256 KiB, not arbitrary raw 1 MiB.
 *
 * Copyright 2018-2026 Stichting DuckDB Foundation
 * Copyright 2018-2025 Stichting DuckDB Foundation (DuckLake metadata spelling)
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
static bool LakeBinaryDefaultRegular(unsigned char value)
{ return value>=32&&value<=126&&value!='\\'&&value!='\''&&value!='"'; }

static int LakeBinaryDefaultHex(unsigned char value,bool uppercase)
{
    if(value>='0'&&value<='9')return value-'0';
    if(value>='A'&&value<='F')return value-'A'+10;
    if(!uppercase&&value>='a'&&value<='f')return value-'a'+10;
    return -1;
}

static void LakeBinaryDefaultInvalid(int sqlstate)
{ ereport(ERROR,(errcode(sqlstate),errmsg("invalid or noncanonical native lake BYTEA literal default"))); }

/* Validate and count before allocating. Unlike the general DuckDB input cast,
 * protected Native metadata admits only its exact canonical BLOB spelling. */
static Size LakeBinaryCanonicalSize(const char* canonical, Size maxEncodedBytes)
{
    if (canonical == NULL)
        LakeLiteralUnsupported();
    Size length = strlen(canonical), bytes = 0;
    if (length > maxEncodedBytes)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("native lake binary canonical text exceeds its byte limit")));
    for (Size i = 0; i < length;) {
        unsigned char value = (unsigned char)canonical[i];
        if (LakeBinaryDefaultRegular(value)) { ++i; ++bytes; continue; }
        if (value != '\\' || length - i < 4 || canonical[i + 1] != 'x')
            LakeBinaryDefaultInvalid(ERRCODE_DATA_CORRUPTED);
        int high = LakeBinaryDefaultHex((unsigned char)canonical[i + 2], true);
        int low = LakeBinaryDefaultHex((unsigned char)canonical[i + 3], true);
        if (high < 0 || low < 0 || LakeBinaryDefaultRegular((unsigned char)((high << 4) | low)))
            LakeBinaryDefaultInvalid(ERRCODE_DATA_CORRUPTED);
        i += 4; ++bytes;
    }
    return bytes;
}

static Size LakeBinaryDefaultSize(const char* canonical)
{
    Size length = LakeLiteralTextLength(canonical);
    (void)length;
    return LakeBinaryCanonicalSize(canonical, LAKE_LITERAL_DEFAULT_TEXT_LIMIT);
}

/* SQL input deliberately accepts only the PostgreSQL hex literal subset.
 * The SQL lexer already produced the A_Const string; no byteain/function or
 * expression lookup runs here. Preflight syntax and ENCODED bytes first. */
static Datum LakeParseBinaryDefault(const char* input)
{
    Size length=strlen(input);
    if(length<2||input[0]!='\\'||input[1]!='x'||(length-2)%2)
        LakeBinaryDefaultInvalid(ERRCODE_INVALID_TEXT_REPRESENTATION);
    Size bytes=(length-2)/2,encoded=0;
    if(bytes>LAKE_LITERAL_DEFAULT_TEXT_LIMIT)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake literal default exceeds its text byte limit")));
    for(Size i=2;i<length;i+=2){
        int high=LakeBinaryDefaultHex((unsigned char)input[i],false);
        int low=LakeBinaryDefaultHex((unsigned char)input[i+1],false);
        if(high<0||low<0)LakeBinaryDefaultInvalid(ERRCODE_INVALID_TEXT_REPRESENTATION);
        Size add=LakeBinaryDefaultRegular((unsigned char)((high<<4)|low))?1:4;
        if(add>LAKE_LITERAL_DEFAULT_TEXT_LIMIT-encoded)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake literal default exceeds its text byte limit")));
        encoded+=add;
    }
    bytea* value=(bytea*)palloc(VARHDRSZ+bytes);SET_VARSIZE(value,VARHDRSZ+bytes);
    unsigned char* target=(unsigned char*)VARDATA(value);
    for(Size i=0;i<bytes;++i)target[i]=(unsigned char)((LakeBinaryDefaultHex((unsigned char)input[2+i*2],false)<<4)|
        LakeBinaryDefaultHex((unsigned char)input[3+i*2],false));
    return PointerGetDatum(value);
}

char* LakeEncodeBinaryCanonical(Datum datum, Size maxEncodedBytes)
{
    Size raw=toast_raw_datum_size(datum);
    if(raw<VARHDRSZ)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native BYTEA default length")));
    if(raw-VARHDRSZ>maxEncodedBytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake literal default exceeds its text byte limit")));
    struct varlena* packed=PG_DETOAST_DATUM_PACKED(datum);
    Size length=VARSIZE_ANY_EXHDR(packed),encoded=0;
    if(length!=raw-VARHDRSZ)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native BYTEA default length changed")));
    const unsigned char* bytes=(const unsigned char*)VARDATA_ANY(packed);
    for(Size i=0;i<length;++i){
        Size add=LakeBinaryDefaultRegular(bytes[i])?1:4;
        if(add>maxEncodedBytes-encoded)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake literal default exceeds its text byte limit")));
        encoded+=add;
    }
    char* result=(char*)palloc(encoded+1);Size at=0;
    static const char hex[]="0123456789ABCDEF";
    for(Size i=0;i<length;++i){
        if(LakeBinaryDefaultRegular(bytes[i]))result[at++]=(char)bytes[i];
        else{result[at++]='\\';result[at++]='x';result[at++]=hex[bytes[i]>>4];result[at++]=hex[bytes[i]&15];}
    }
    result[at]='\0';
    if((Pointer)packed!=DatumGetPointer(datum))pfree(packed);
    return result;
}

Datum LakeDecodeBinaryCanonical(const char* canonical, Size maxEncodedBytes)
{
    Size bytes=LakeBinaryCanonicalSize(canonical, maxEncodedBytes);
    bytea* value=(bytea*)palloc(VARHDRSZ+bytes);SET_VARSIZE(value,VARHDRSZ+bytes);
    unsigned char* target=(unsigned char*)VARDATA(value);
    for(Size i=0,at=0;at<bytes;++at){
        if(canonical[i]=='\\'){
            target[at]=(unsigned char)((LakeBinaryDefaultHex((unsigned char)canonical[i+2],true)<<4)|
                LakeBinaryDefaultHex((unsigned char)canonical[i+3],true));i+=4;
        }else target[at]=(unsigned char)canonical[i++];
    }
    return PointerGetDatum(value);
}

static char* LakeEncodeBinaryDefault(Datum datum)
{ return LakeEncodeBinaryCanonical(datum, LAKE_LITERAL_DEFAULT_TEXT_LIMIT); }

static Datum LakeDecodeBinaryDefault(const char* canonical)
{ return LakeDecodeBinaryCanonical(canonical, LAKE_LITERAL_DEFAULT_TEXT_LIMIT); }

/* DuckLake src/storage/ducklake_metadata_manager.cpp
 * SHA256 474e16914719a6d566486668ee1f90b9227534391e6b1db30e293890d87b14e2
 * ColumnToSQLRecursive escapes a literal whose text is the sentinel
 * NULL as its typed SQL spelling. This is representation only, never eval.
 * Quotes in real BLOB content are \\x27, so bytes "'NULL'::BLOB" cannot collide. */
const char* LakeCurrentDefaultMetadata(Oid type,const char* canonical,bool* expression)
{
    bool escaped=canonical&&strcmp(canonical,"NULL")==0&&(type==TEXTOID||type==BYTEAOID);
    if(expression)*expression=escaped;
    return escaped?(type==BYTEAOID?"'NULL'::BLOB":"'NULL'"):canonical?canonical:"NULL";
}

static bool LakeCurrentDefaultEscapedNull(Oid type,const char* raw,const char* kind)
{
    return (type==TEXTOID||(type==BYTEAOID&&LakeBinaryDefaultVersionEnabled()))&&raw&&strcmp(kind,"expression")==0&&
        strcmp(raw,LakeCurrentDefaultMetadata(type,"NULL",NULL))==0;
}

/* DuckLake serializes non-UTC/typed TIMESTAMPTZ literal defaults using the
 * literal wrapper CAST('<iso>' AS TIMESTAMPTZ) with default_value_type='expression'.
 * Accept that exact bounded literal spelling on read without executing SQL. */
static const char* LakeCurrentDefaultTimestamptzCastLiteral(Oid type,const char* raw,const char* kind,char** owned)
{
    *owned=NULL;
    if(type!=TIMESTAMPTZOID||!LakeExtendedDefaultsVersionEnabled()||
       !raw||!kind||strcmp(kind,"expression")!=0)
        return NULL;
    static const char prefix[]="CAST('";
    static const char suffix[]="' AS TIMESTAMPTZ)";
    Size rawLen=strlen(raw);
    if(rawLen<=sizeof(prefix)-1+sizeof(suffix)-1||
       strncmp(raw,prefix,sizeof(prefix)-1)!=0||
       strcmp(raw+rawLen-(sizeof(suffix)-1),suffix)!=0)
        return NULL;
    Size innerLen=rawLen-(sizeof(prefix)-1)-(sizeof(suffix)-1);
    if(innerLen<10||innerLen>48||memchr(raw+sizeof(prefix)-1,'\'',innerLen)!=NULL)
        return NULL;
    *owned=pnstrdup(raw+sizeof(prefix)-1,innerLen);
    return *owned;
}

static bool LakeFloatValuesEqual(Oid type,const char* aText,const char* bText)
{
    if(type==FLOAT4OID){
        float4 a=DatumGetFloat4(DirectFunctionCall1(float4in,CStringGetDatum(aText)));
        float4 b=DatumGetFloat4(DirectFunctionCall1(float4in,CStringGetDatum(bText)));
        if(std::isnan(a)||std::isnan(b))return std::isnan(a)&&std::isnan(b);
        return a==b;
    }
    if(type==FLOAT8OID){
        float8 a=DatumGetFloat8(DirectFunctionCall1(float8in,CStringGetDatum(aText)));
        float8 b=DatumGetFloat8(DirectFunctionCall1(float8in,CStringGetDatum(bText)));
        if(std::isnan(a)||std::isnan(b))return std::isnan(a)&&std::isnan(b);
        return a==b;
    }
    return false;
}

/* No host input function/operator lookup: decimal magnitude is checked before
 * each multiply/add and before narrowing to the exact destination width. */
static int64 LakeLiteralInteger(const char* text, Oid type)
{
    const char* p=text;bool negative=false;
    if(*p=='-'||*p=='+'){negative=*p=='-';++p;}
    if(*p<'0'||*p>'9')LakeLiteralUnsupported();
    uint64 positive=type==INT2OID?32767UL:type==INT4OID?2147483647UL:(uint64)PG_INT64_MAX;
    uint64 limit=positive+(negative?1:0),magnitude=0;
    for(;*p;++p){
        if(*p<'0'||*p>'9')LakeLiteralUnsupported();
        unsigned digit=*p-'0';
        if(magnitude>(limit-digit)/10)
            ereport(ERROR,(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("native lake literal default is outside its integer type")));
        magnitude=magnitude*10+digit;
    }
    if(negative&&magnitude==(uint64)PG_INT64_MAX+1)return PG_INT64_MIN;
    return negative?-(int64)magnitude:(int64)magnitude;
}

char* LakeEncodeLiteralDefault(Oid type, int32 typmod, Datum value)
{
    if(!LakeLiteralDefaultType(type,typmod))LakeLiteralUnsupported();
    if(type==BYTEAOID)return LakeEncodeBinaryDefault(value);
    if(type==BOOLOID)return pstrdup(DatumGetBool(value)?"true":"false");
    if(type==INT2OID)return psprintf("%d",(int)DatumGetInt16(value));
    if(type==INT4OID)return psprintf("%d",DatumGetInt32(value));
    if(type==INT8OID)return psprintf(INT64_FORMAT,DatumGetInt64(value));
    if(type==TEXTOID){
        char* utf8=TextDatumGetCString(value);Size length=LakeLiteralTextLength(utf8);
        if(GetDatabaseEncoding()!=PG_UTF8)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake text defaults require UTF8")));
        (void)pg_verify_mbstr(PG_UTF8,utf8,(int)length,false);
        return utf8;
    }
    if(type==FLOAT4OID){
        float4 val=DatumGetFloat4(value);
        if(std::isnan(val))return pstrdup("nan");
        if(std::isinf(val))return pstrdup(val>0?"inf":"-inf");
        if(val==0.0f)return pstrdup("0.0");
        char buf[FLOAT_SHORTEST_DECIMAL_LEN];
        (void)float_to_shortest_decimal_buf(val,buf);
        return pstrdup(buf);
    }
    if(type==FLOAT8OID){
        float8 val=DatumGetFloat8(value);
        if(std::isnan(val))return pstrdup("nan");
        if(std::isinf(val))return pstrdup(val>0?"inf":"-inf");
        if(val==0.0)return pstrdup("0.0");
        char buf[DOUBLE_SHORTEST_DECIMAL_LEN];
        (void)double_to_shortest_decimal_buf(val,buf);
        return pstrdup(buf);
    }
    if(type==DATEOID||type==TIMESTAMPOID||type==TIMESTAMPTZOID)
        return TextDatumGetCString(LakeTemporalWriteText(value,type));
    if(type==UUIDOID)
        return DatumGetCString(DirectFunctionCall1(uuid_out,value));
    if(type==JSONOID){
        if(GetDatabaseEncoding()!=PG_UTF8)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake JSON defaults require UTF8")));
        Size raw=toast_raw_datum_size(value);
        if(raw<VARHDRSZ||raw-VARHDRSZ>LAKE_LITERAL_DEFAULT_TEXT_LIMIT)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake literal default exceeds its text byte limit")));
        struct varlena* packed=PG_DETOAST_DATUM_PACKED(value);
        Size len=VARSIZE_ANY_EXHDR(packed);
        const char* bytes=VARDATA_ANY(packed);
        Size tracked=0;
        Datum validated=LakeJsonFromUtf8Bytes(bytes,len,&tracked);
        char* out=(char*)palloc(len+1);
        if(len){errno_t rc=memcpy_s(out,len+1,bytes,len);securec_check(rc,"","");}
        out[len]='\0';
        if((void*)packed!=DatumGetPointer(value))pfree(packed);
        pfree(DatumGetPointer(validated));
        return out;
    }
    if(type==NUMERICOID){
        char buf[64];
        LakeNumericFormatCanonical(value,typmod,buf,sizeof(buf));
        return pstrdup(buf);
    }
    LakeLiteralUnsupported();
    return NULL;
}

Datum LakeDecodeLiteralDefault(Oid type, int32 typmod, const char* canonical)
{
    Size length=LakeLiteralTextLength(canonical);Datum result=(Datum)0;
    if(!LakeLiteralDefaultType(type,typmod))LakeLiteralUnsupported();
    if(type==BYTEAOID)return LakeDecodeBinaryDefault(canonical);
    if(type==TEXTOID){
        if(GetDatabaseEncoding()!=PG_UTF8)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake text defaults require UTF8")));
        (void)pg_verify_mbstr(PG_UTF8,canonical,(int)length,false);
        return CStringGetTextDatum(canonical);
    }
    if(type==BOOLOID){
        if(strcmp(canonical,"true")!=0&&strcmp(canonical,"false")!=0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("noncanonical native lake boolean default")));
        return BoolGetDatum(strcmp(canonical,"true")==0);
    }
    if(type==FLOAT4OID||type==FLOAT8OID){
        if(length==0||std::isspace((unsigned char)canonical[0])||std::isspace((unsigned char)canonical[length-1]))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("noncanonical native lake float default")));
        return DirectFunctionCall1(type==FLOAT4OID?float4in:float8in,CStringGetDatum(canonical));
    }
    if(type==DATEOID||type==TIMESTAMPOID||type==TIMESTAMPTZOID)
        return LakeTemporalParseText(canonical,(int)length,type);
    if(type==UUIDOID){
        pg_uuid_t parsed;
        if(!LakeUuidTryStatsBytes(canonical,&parsed))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("noncanonical native lake UUID default")));
        Size tracked=0;
        return LakeUuidFromBytes(parsed.data,&tracked);
    }
    if(type==JSONOID){
        if(GetDatabaseEncoding()!=PG_UTF8)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake JSON defaults require UTF8")));
        Size tracked=0;
        return LakeJsonFromUtf8Bytes(canonical,length,&tracked);
    }
    if(type==NUMERICOID){
        result=DirectFunctionCall3Coll(numeric_in,InvalidOid,CStringGetDatum(canonical),
            ObjectIdGetDatum(InvalidOid),Int32GetDatum(typmod),false);
        char buf[64];
        LakeNumericFormatCanonical(result,typmod,buf,sizeof(buf));
        if(strcmp(buf,canonical)!=0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("noncanonical native lake numeric default")));
        return result;
    }
    int64 value=LakeLiteralInteger(canonical,type);
    result=type==INT2OID?Int16GetDatum((int16)value):type==INT4OID?Int32GetDatum((int32)value):Int64GetDatum(value);
    char* encoded=LakeEncodeLiteralDefault(type,-1,result);bool exact=strcmp(encoded,canonical)==0;pfree(encoded);
    if(!exact)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("noncanonical native lake integer default")));
    return result;
}

Const* LakeParseNumericNullDefault(Node* raw,int32 typmod,Oid collation)
{
    if(!LakeNumericBusinessVersionEnabled()||!LakeNumericTypmod(typmod))LakeLiteralUnsupported();
    LakeNativeTypeRequireBusinessVersion(NUMERICOID);
    if(raw&&IsA(raw,TypeCast)){
        TypeCast* cast=(TypeCast*)raw;TypeName* name=cast->typname;
        if(!name||cast->fmt_str||cast->nls_fmt_str||cast->default_expr||name->arrayBounds||name->setof||
           name->pct_type||name->pct_rowtype||name->dependExtend||name->charset!=PG_INVALID_ENCODING)
            LakeLiteralUnsupported();
        Oid resolved;int32 declared;typenameTypeIdAndMod(NULL,name,&resolved,&declared);
        if(resolved!=NUMERICOID||(declared!=-1&&declared!=typmod))LakeLiteralUnsupported();
        raw=cast->arg;
    }
    if(!raw||!IsA(raw,A_Const)||nodeTag(&((A_Const*)raw)->val)!=T_Null)LakeLiteralUnsupported();
    return makeConst(NUMERICOID,typmod,collation,-1,(Datum)0,true,false);
}

/* SQL boolean keywords (true/false) arrive from gram.y makeBoolAConst as an
 * internal TypeCast(A_Const('t'/'f'), pg_catalog.bool), so an explicit single
 * boolean cast like false::boolean wraps two TypeCast nodes. */
static bool LakeIsParserBoolLiteralCast(Node* raw)
{
    if(!raw||!IsA(raw,TypeCast))return false;
    TypeCast* cast=(TypeCast*)raw;TypeName* name=cast->typname;
    if(!name||cast->fmt_str||cast->nls_fmt_str||cast->default_expr||name->typmods||
       name->arrayBounds||name->setof||name->pct_type||name->pct_rowtype||name->dependExtend||
       name->typemod!=-1||name->charset!=PG_INVALID_ENCODING||list_length(name->names)!=2||
       strcmp(strVal(linitial(name->names)),"pg_catalog")!=0||
       strcmp(strVal(lsecond(name->names)),"bool")!=0||
       !cast->arg||!IsA(cast->arg,A_Const))
        return false;
    A_Const* literal=(A_Const*)cast->arg;
    if(nodeTag(&literal->val)!=T_String)return false;
    const char* text=strVal(&literal->val);
    return strcmp(text,"t")==0||strcmp(text,"f")==0;
}

static Const* LakeParseLiteralDefaultInternal(Node* raw, Oid type, int32 typmod, Oid collation, bool allowNull)
{
    bool extended=LakeExtendedDefaultsVersionEnabled();
    bool legacyNull=allowNull&&!extended&&((type==UUIDOID&&LakeUuidBusinessVersionEnabled())||
        (type==JSONOID&&LakeJsonBusinessVersionEnabled()));
    if(!LakeLiteralDefaultType(type,typmod)&&!legacyNull)LakeLiteralUnsupported();
    if(legacyNull)LakeNativeTypeRequireBusinessVersion(type);
    bool unaryOp=false,unaryNeg=false;
    if(extended&&raw&&IsA(raw,A_Expr)&&
       (type==INT2OID||type==INT4OID||type==INT8OID||type==FLOAT4OID||type==FLOAT8OID||type==NUMERICOID)){
        A_Expr* expr=(A_Expr*)raw;
        if(expr->kind==AEXPR_OP&&expr->lexpr==NULL&&expr->rexpr!=NULL&&
           list_length(expr->name)==1&&IsA(linitial(expr->name),String)){
            const char* op=strVal(linitial(expr->name));
            if(strcmp(op,"-")==0||strcmp(op,"+")==0){
                unaryOp=true;unaryNeg=(op[0]=='-');raw=expr->rexpr;
            }
        }
    }
    if(raw&&IsA(raw,TypeCast)){
        TypeCast* cast=(TypeCast*)raw;TypeName* name=cast->typname;
        if(!name||cast->fmt_str||cast->nls_fmt_str||cast->default_expr||
           name->arrayBounds||name->setof||name->pct_type||name->pct_rowtype||
           name->dependExtend||name->charset!=PG_INVALID_ENCODING)
            LakeLiteralUnsupported();
        Oid resolved;int32 declared;typenameTypeIdAndMod(NULL,name,&resolved,&declared);
        if(resolved!=type)LakeLiteralUnsupported();
        if(type==NUMERICOID){
            if(declared!=-1&&declared!=typmod)LakeLiteralUnsupported();
        }else if(name->typmods||name->typemod!=-1||declared!=-1){
            LakeLiteralUnsupported();
        }
        raw=cast->arg; /* A second cast or expression fails the literal check. */
        if(type==BOOLOID&&LakeIsParserBoolLiteralCast(raw))raw=((TypeCast*)raw)->arg;
    }
    if(!raw||!IsA(raw,A_Const))LakeLiteralUnsupported();
    A_Const* literal=(A_Const*)raw;Datum value=(Datum)0;
    if(allowNull&&!unaryOp&&nodeTag(&literal->val)==T_Null){
        int16 length;bool byval;get_typlenbyval(type,&length,&byval);
        return makeConst(type,typmod,collation,length,(Datum)0,true,byval);
    }
    if(legacyNull||nodeTag(&literal->val)==T_Null)LakeLiteralUnsupported();
    if(type==BYTEAOID){
        if(nodeTag(&literal->val)!=T_String)LakeLiteralUnsupported();
        value=LakeParseBinaryDefault(strVal(&literal->val));
    }else if(type==TEXTOID){
        if(nodeTag(&literal->val)!=T_String)LakeLiteralUnsupported();
        const char* text=strVal(&literal->val);(void)LakeLiteralTextLength(text);
        value=LakeDecodeLiteralDefault(type,-1,text);
    }else if(type==BOOLOID){
        if(nodeTag(&literal->val)!=T_String)LakeLiteralUnsupported();
        const char* text=strVal(&literal->val);
        if(strcmp(text,"t")&&strcmp(text,"f")&&strcmp(text,"true")&&strcmp(text,"false"))LakeLiteralUnsupported();
        value=BoolGetDatum(strcmp(text,"t")==0||strcmp(text,"true")==0);
    }else if(type==DATEOID||type==TIMESTAMPOID||type==TIMESTAMPTZOID){
        if(nodeTag(&literal->val)!=T_String)LakeLiteralUnsupported();
        const char* text=strVal(&literal->val);(void)LakeLiteralTextLength(text);
        value=type==DATEOID?DirectFunctionCall1(date_in,CStringGetDatum(text)):
            type==TIMESTAMPOID?DirectFunctionCall3(timestamp_in,CStringGetDatum(text),ObjectIdGetDatum(InvalidOid),Int32GetDatum(-1)):
            DirectFunctionCall3(timestamptz_in,CStringGetDatum(text),ObjectIdGetDatum(InvalidOid),Int32GetDatum(-1));
        pfree(LakeEncodeLiteralDefault(type,-1,value));
    }else if(type==UUIDOID){
        if(nodeTag(&literal->val)!=T_String)LakeLiteralUnsupported();
        const char* text=strVal(&literal->val);(void)LakeLiteralTextLength(text);
        value=DirectFunctionCall1(uuid_in,CStringGetDatum(text));
    }else if(type==JSONOID){
        if(nodeTag(&literal->val)!=T_String)LakeLiteralUnsupported();
        const char* text=strVal(&literal->val);Size textLen=LakeLiteralTextLength(text);
        if(GetDatabaseEncoding()!=PG_UTF8)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake JSON defaults require UTF8")));
        Size tracked=0;
        value=LakeJsonFromUtf8Bytes(text,textLen,&tracked);
    }else if(type==FLOAT4OID||type==FLOAT8OID||type==NUMERICOID){
        const char* base=NULL;
        if(nodeTag(&literal->val)==T_Integer)base=psprintf("%ld",intVal(&literal->val));
        else if(nodeTag(&literal->val)==T_Float||nodeTag(&literal->val)==T_String)base=strVal(&literal->val);
        else LakeLiteralUnsupported();
        if(unaryOp&&(base[0]=='-'||base[0]=='+'))LakeLiteralUnsupported();
        const char* text=unaryNeg?psprintf("-%s",base):base;
        (void)LakeLiteralTextLength(text);
        value=type==FLOAT4OID?DirectFunctionCall1(float4in,CStringGetDatum(text)):
            type==FLOAT8OID?DirectFunctionCall1(float8in,CStringGetDatum(text)):
            DirectFunctionCall3Coll(numeric_in,InvalidOid,CStringGetDatum(text),ObjectIdGetDatum(InvalidOid),Int32GetDatum(typmod),false);
        pfree(LakeEncodeLiteralDefault(type,typmod,value));
    }else{
        const char* base=NULL;
        if(nodeTag(&literal->val)==T_Integer)base=psprintf("%ld",intVal(&literal->val));
        else if(nodeTag(&literal->val)==T_Float)base=strVal(&literal->val);
        else LakeLiteralUnsupported();
        if(unaryOp&&(base[0]=='-'||base[0]=='+'))LakeLiteralUnsupported();
        const char* text=unaryNeg?psprintf("-%s",base):base;
        int64 number=LakeLiteralInteger(text,type);
        value=type==INT2OID?Int16GetDatum((int16)number):type==INT4OID?Int32GetDatum((int32)number):Int64GetDatum(number);
    }
    int16 length;bool byval;get_typlenbyval(type,&length,&byval);
    return makeConst(type,typmod,collation,length,value,false,byval);
}

Const* LakeParseLiteralDefault(Node* raw, Oid type, int32 typmod, Oid collation)
{ return LakeParseLiteralDefaultInternal(raw,type,typmod,collation,false); }

Const* LakeParseCurrentLiteralDefault(Node* raw, Oid type, int32 typmod, Oid collation)
{ return LakeParseLiteralDefaultInternal(raw,type,typmod,collation,true); }

/* Called only by the exact native/server/CREATE/installed49 host bridge.
 * Authenticate DEFAULT/raw provenance before replacing either pointer. */
static void LakeChargeCreateLiteralBytes(Size* bytes,Size extra)
{
    if(bytes==NULL||extra>LAKE_CREATE_LITERAL_DEFAULT_MEMORY_LIMIT||
       *bytes>LAKE_CREATE_LITERAL_DEFAULT_MEMORY_LIMIT-extra)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native CREATE prehost default payload exceeds its byte limit")));
    *bytes+=extra;
}

void LakePrepareCreateColumnLiteral(ColumnDef* column,Oid type,int32 typmod,Oid collation,Size* statementBytes)
{
    if(!LakeCreateLiteralDefaultVersionEnabled()||!LakeNativeFlatTypmod(type,typmod)||column->is_from_type||
       column->is_serial||column->is_identity||column->cooked_default||column->update_default||
       column->generatedCol!='\0'||column->collClause||column->fdwoptions!=NIL||
       column->columnOptions!=NIL||column->clientLogicColumnRef||
       !LakePlainCreateColumnConstraints(column,true))LakeLiteralUnsupported();
    Constraint* declared=NULL;ListCell* cell=NULL;
    foreach(cell,column->constraints){
        Constraint* c=(Constraint*)lfirst(cell);
        if(c->contype==CONSTR_DEFAULT)declared=c;
    }
    if(!declared)LakeLiteralUnsupported();
    Const* literal=(type==NUMERICOID&&!LakeExtendedDefaultsVersionEnabled())?
        LakeParseNumericNullDefault(column->raw_default,typmod,collation):
        LakeParseCurrentLiteralDefault(column->raw_default,type,typmod,collation);
    if(literal->constisnull){
        /* Normalise BEFORE the host pre-sets atthasdef. Removing this one
         * already-authenticated DEFAULT does not discard any other node. */
        column->raw_default=NULL;
        column->constraints=list_delete_ptr(column->constraints,declared);
        pfree(literal);
    }else{
        /* cookDefault receives this exact already-typed Const. The host's
         * StoreAttrDefault serializes it with the same nodeToString. Count
         * that REAL length, which may exceed the canonical1MiB limit, before
         * the host cooks/stores any column defaults. No user expression runs.
         * One bounded per-column scratch precedes this aggregate check. */
        char* canonical=LakeEncodeLiteralDefault(type,typmod,literal->constvalue);
        char* adbin=nodeToString(literal);
        Size charge=sizeof(Const)+sizeof(AttrDefault);
        if(!literal->constbyval)
            LakeChargeCreateLiteralBytes(&charge,datumGetSize(literal->constvalue,false,literal->constlen));
        Size textBytes=strlen(canonical)+1;
        LakeChargeCreateLiteralBytes(&charge,textBytes);
        LakeChargeCreateLiteralBytes(&charge,textBytes);
        Size adbinBytes=strnlen(adbin,LAKE_CREATE_LITERAL_DEFAULT_MEMORY_LIMIT);
        if(adbinBytes==LAKE_CREATE_LITERAL_DEFAULT_MEMORY_LIMIT)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native CREATE prehost adbin exceeds its byte limit")));
        LakeChargeCreateLiteralBytes(&charge,adbinBytes+1);
        pfree(adbin);pfree(canonical);
        LakeChargeCreateLiteralBytes(statementBytes,charge);
        column->raw_default=(Node*)literal;
        declared->raw_expr=(Node*)literal;
    }
}

static bool LakeSameOptionalText(const char* a,const char* b)
{ return a&&b?strcmp(a,b)==0:a==b; }

bool LakeColumnDefaultsEqual(const LakeInlineColumn* a,const LakeInlineColumn* b)
{
    return a->not_null==b->not_null&&a->introduced_snapshot==b->introduced_snapshot&&
        a->defaults_validated==b->defaults_validated&&a->initial_value_proven==b->initial_value_proven&&
        a->initial_null_proven==b->initial_null_proven&&
        LakeSameOptionalText(a->initial_literal,b->initial_literal)&&
        LakeSameOptionalText(a->current_literal,b->current_literal);
}

Size LakeColumnDefaultBytes(const LakeInlineColumn* source)
{
    Size bytes=0;int16 length=0;bool byval=true;
    if(source->initial_literal)bytes+=LakeLiteralTextLength(source->initial_literal)+1;
    if(source->current_literal)bytes+=LakeLiteralTextLength(source->current_literal)+1;
    if(source->initial_literal){
        get_typlenbyval(source->type,&length,&byval);
        if(!byval)bytes+=datumGetSize(source->initial_value,false,length);
    }
    return bytes;
}

void LakeCopyColumnDefault(LakeInlineColumn* target,const LakeInlineColumn* source,
    LakeInlineBinding* allocationBudget)
{
    Size bytes=LakeColumnDefaultBytes(source);
    int16 length=0;bool byval=true;
    if(source->initial_literal)get_typlenbyval(source->type,&length,&byval);
    /* NULL budget is only for an exact independent clone of an already
     * bounded binding; its full cumulative counter is inherited unchanged. */
    if(allocationBudget)LakeReserveColumnMapBytes(allocationBudget,bytes);
    target->defaults_validated=source->defaults_validated;
    target->initial_value_proven=source->initial_value_proven;
    target->initial_null_proven=source->initial_null_proven;
    target->initial_value=source->initial_value;
    target->initial_literal=source->initial_literal?pstrdup(source->initial_literal):NULL;
    target->current_literal=source->current_literal?pstrdup(source->current_literal):NULL;
    if(source->initial_literal)target->initial_value=datumCopy(source->initial_value,byval,length);
}

/* Current defaults may change in SQL41. Each historical representation must
 * still be a canonical supported value. At most two bounded rows are resident;
 * consuming one row per query also detects duplicate version starts. No SQL
 * text is evaluated, and only the scalar cursor escapes each scratch reset. */
static void LakeCheckCurrentDefaultHistory(const LakeInlineBinding* binding,
    const LakeInlineColumn* field,Snapshot snapshot)
{
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(owner,"Native current default history",ALLOCSET_DEFAULT_SIZES);
    int64 previous=-1;bool seen=false;
    PG_TRY();{
        for(;;){
            CHECK_FOR_INTERRUPTS();MemoryContextReset(scratch);MemoryContextSwitchTo(scratch);
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(field->column_id),
                Int64GetDatum(binding->snapshot_id),Int64GetDatum(previous)};
            LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT begin_snapshot,end_snapshot,octet_length(default_value),"
                "CASE WHEN octet_length(default_value)<=1048576 THEN default_value::text END,"
                "CASE WHEN octet_length(default_value_type)<=16 THEN default_value_type::text END,"
                "CASE WHEN octet_length(default_value_dialect)<=16 THEN default_value_dialect::text END,"
                "column_type::text "
                "FROM %s WHERE table_id=$1 AND column_id=$2 AND begin_snapshot<=$3 AND begin_snapshot>$4 "
                "ORDER BY begin_snapshot LIMIT 2",LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_column")),
                4,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
            if(rows->count==0)break;
            int64 begin=DatumGetInt64(LakeMetadataValue(rows,0,1,INT8OID));
            bool endNull=false;Datum end=LakeMetadataValue(rows,0,2,INT8OID,true,&endNull);
            if(begin<0||begin<=previous||(!endNull&&DatumGetInt64(end)<=begin)||
               (rows->count==2&&DatumGetInt64(LakeMetadataValue(rows,1,1,INT8OID))==begin))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native current default version history")));
            bool null=false;Datum length=LakeMetadataValue(rows,0,3,INT4OID,true,&null);
            if(!null&&DatumGetInt32(length)>(int)LAKE_LITERAL_DEFAULT_TEXT_LIMIT)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake default metadata exceeds its byte limit")));
            const char* rowTypeStr=LakeMetadataText(rows,0,7);
            Oid rowType=LakeInlineType(rowTypeStr);
            int32 rowTypmod=rowType==NUMERICOID?LakeNumericDuckTypmod(rowTypeStr):-1;
            const char* raw=null?NULL:LakeMetadataText(rows,0,4);
            const char* kind=LakeMetadataText(rows,0,5);const char* dialect=LakeMetadataText(rows,0,6);
            bool none=raw==NULL||strcmp(raw,"NULL")==0;
            bool escaped=LakeCurrentDefaultEscapedNull(rowType,raw,kind);
            char* tzOwned=NULL;
            const char* tzLiteral=LakeCurrentDefaultTimestamptzCastLiteral(rowType,raw,kind,&tzOwned);
            bool vectorNone=rowType==VECTOROID&&none&&*kind=='\0';
            if(strcmp(dialect,"duckdb")||(!vectorNone&&!tzLiteral&&strcmp(kind,"literal")&&!escaped))LakeLiteralUnsupported();
            if(!none||escaped){
                if(!LakeLiteralDefaultType(rowType,rowTypmod))LakeLiteralUnsupported();
                (void)LakeDecodeLiteralDefault(rowType,rowTypmod,escaped?"NULL":tzLiteral?tzLiteral:raw);
            }
            if(tzOwned)pfree(tzOwned);
            previous=begin;seen=true;MemoryContextSwitchTo(owner);
        }
        MemoryContextSwitchTo(owner);
        if(!seen)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native current default history is empty")));
        MemoryContextDelete(scratch);
    }PG_CATCH();{
        MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);PG_RE_THROW();
    }PG_END_TRY();
}

/* Only the protected native CREATE loader calls this. Bytea/textsend equality
 * is independent of the metadata database's collation. Read the visible raw
 * representation once, then prove that ALL retained versions preserve it.
 * NULL initial_default is not the four-byte business string "NULL". */
static void LakeReadColumnDefaultInternal(LakeInlineBinding* binding,LakeInlineColumn* field,
    int64 tableStart,Snapshot snapshot,MemoryContext resultContext)
{
    Oid types[8]={INT8OID,INT8OID,INT8OID,TEXTOID,TEXTOID,TEXTOID,TEXTOID,TEXTOID};
    Datum args[8]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(field->column_id),
        Int64GetDatum(binding->snapshot_id),0,0,0,0,0};
    char* columns=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_column");
    LakeMetaRows* row=LakeMetadataQuery(psprintf("SELECT "
        "octet_length(initial_default),CASE WHEN octet_length(initial_default)<=1048576 THEN initial_default::text END,"
        "octet_length(default_value),CASE WHEN octet_length(default_value)<=1048576 THEN default_value::text END,"
        "CASE WHEN octet_length(default_value_type)<=16 THEN default_value_type::text END,"
        "CASE WHEN octet_length(default_value_dialect)<=16 THEN default_value_dialect::text END,"
        "%s,parent_column IS NULL,column_type::text FROM %s WHERE table_id=$1 AND column_id=$2 "
        "AND begin_snapshot<=$3 AND (end_snapshot IS NULL OR end_snapshot>$3)",
        binding->not_null_constraints?"nulls_allowed IS NOT NULL":"nulls_allowed IS TRUE",columns),
        3,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(row,1,"visible native default definition");
    bool initialNull=false,currentNull=false;
    Datum initialLength=LakeMetadataValue(row,0,1,INT4OID,true,&initialNull);
    Datum currentLength=LakeMetadataValue(row,0,3,INT4OID,true,&currentNull);
    if((!initialNull&&DatumGetInt32(initialLength)>(int)LAKE_LITERAL_DEFAULT_TEXT_LIMIT)||
       (!currentNull&&DatumGetInt32(currentLength)>(int)LAKE_LITERAL_DEFAULT_TEXT_LIMIT))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake default metadata exceeds its byte limit")));
    const char* initial=initialNull?NULL:LakeMetadataText(row,0,2);
    const char* current=currentNull?NULL:LakeMetadataText(row,0,4);
    const char* kind=LakeMetadataText(row,0,5);const char* dialect=LakeMetadataText(row,0,6);
    bool noCurrent=current==NULL||strcmp(current,"NULL")==0;
    bool literal=strcmp(kind,"literal")==0;
    bool vectorNone=field->type==VECTOROID&&initialNull&&noCurrent&&*kind=='\0';
    bool escapedNull=LakeCurrentDefaultEscapedNull(field->type,current,kind);
    char* tzCurrentOwned=NULL;
    const char* tzCurrent=LakeCurrentDefaultTimestamptzCastLiteral(field->type,current,kind,&tzCurrentOwned);
    if(!DatumGetBool(LakeMetadataValue(row,0,7,BOOLOID))||
       !DatumGetBool(LakeMetadataValue(row,0,8,BOOLOID))||strcmp(dialect,"duckdb")||
       (!literal&&!vectorNone&&!escapedNull&&!tzCurrent))LakeLiteralUnsupported();
    if(escapedNull)noCurrent=false;
    const char* decodedCurrent=noCurrent?NULL:escapedNull?"NULL":tzCurrent?tzCurrent:current;
    /* Old installed SQL keeps the original equality contract. SQL41 permits
     * different current values, but never changes the immutable initial one. */
    bool createdInitial=binding->column_evolution&&LakeCreateLiteralDefaultVersionEnabled()&&
        field->introduced_snapshot==tableStart;
    if((!binding->alter_defaults&&!LakeSameOptionalText(initial,decodedCurrent))||
       (!initialNull&&(!LakeLiteralDefaultType(field->type,field->typmod)||(field->introduced_snapshot<tableStart||(field->introduced_snapshot==tableStart&&!createdInitial)))))
        LakeLiteralUnsupported();
    if(binding->alter_defaults&&decodedCurrent){
        if(!LakeLiteralDefaultType(field->type,field->typmod))LakeLiteralUnsupported();
        (void)LakeDecodeLiteralDefault(field->type,field->typmod,decodedCurrent);
    }
    args[3]=initial?CStringGetTextDatum(initial):(Datum)0;
    args[4]=current?CStringGetTextDatum(current):(Datum)0;
    args[5]=CStringGetTextDatum(kind);args[6]=CStringGetTextDatum(dialect);
    args[7]=CStringGetTextDatum(LakeMetadataText(row,0,9));
    char nulls[9]={' ',' ',' ',initial?' ':'n',current?' ':'n',' ',' ',' ','\0'};
    LakeMetaRows* history=LakeMetadataQuery(psprintf("SELECT bool_and(COALESCE(parent_column IS NULL AND %s "
        "AND (initial_default IS NULL OR octet_length(initial_default)<=1048576) "
        "AND (default_value IS NULL OR octet_length(default_value)<=1048576) "
        "AND (CASE WHEN octet_length(initial_default)<=1048576 THEN pg_catalog.textsend(initial_default::text) END "
        "IS NOT DISTINCT FROM pg_catalog.textsend($4)) "
        "%s "
        "%s,false)) "
        "FROM %s WHERE table_id=$1 AND column_id=$2 AND begin_snapshot<=$3",
        binding->not_null_constraints?"nulls_allowed IS NOT NULL":"nulls_allowed IS TRUE",
        binding->alter_defaults?"":
        "AND (CASE WHEN octet_length(default_value)<=1048576 THEN pg_catalog.textsend(default_value::text) END "
        "IS NOT DISTINCT FROM pg_catalog.textsend($5)) "
        "AND pg_catalog.textsend(default_value_type::text)=pg_catalog.textsend($6) "
        "AND pg_catalog.textsend(default_value_dialect::text)=pg_catalog.textsend($7) ",
        binding->alter_type_evolution?"AND ($8 IS NOT NULL)":"AND pg_catalog.textsend(column_type::text)=pg_catalog.textsend($8)",
        columns),
        8,types,args,nulls,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(history,1,"immutable native initial/current defaults");
    bool proofNull=false;Datum proof=LakeMetadataValue(history,0,1,BOOLOID,true,&proofNull);
    if(proofNull||!DatumGetBool(proof))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake stable field default history changed")));
    Oid initialType=field->type;
    int32 initialTypmod=field->typmod;
    if(binding->alter_type_evolution){
        bool promoted=false;
        (void)LakeVerifyColumnTypeChain(binding->metadata_schema_oid,LakePermanentTableId(binding),
            field->column_id,binding->snapshot_id,field->type,field->typmod,
            &initialType,&initialTypmod,&promoted,snapshot);
    }
    if(binding->alter_defaults)LakeCheckCurrentDefaultHistory(binding,field,snapshot);
    Size bytes=(initial?strlen(initial)+1:0)+(decodedCurrent?strlen(decodedCurrent)+1:0);
    int16 length=0;bool byval=true;
    Datum decodedInitial=(Datum)0;
    if(initial){
        decodedInitial=LakeDecodeLiteralDefault(initialType,initialTypmod,initial);
        if(initialType!=field->type||initialTypmod!=field->typmod)
            decodedInitial=LakeWidenDatum(decodedInitial,initialType,initialTypmod,field->type,field->typmod);
        get_typlenbyval(field->type,&length,&byval);
        if(!byval){
            if(field->type==BYTEAOID)bytes+=VARHDRSZ+LakeBinaryDefaultSize(initial);
            else bytes+=datumGetSize(decodedInitial,byval,length);
        }
    }
    LakeReserveColumnMapBytes(binding,bytes);
    /* Only these checked, budgeted final values survive the per-column scratch. */
    MemoryContext scratch=MemoryContextSwitchTo(resultContext);
    field->initial_literal=initial?pstrdup(initial):NULL;
    field->current_literal=decodedCurrent?pstrdup(decodedCurrent):NULL;
    field->initial_value=initial?datumCopy(decodedInitial,byval,length):(Datum)0;
    field->defaults_validated=true;
    bool later=field->element_column_id<0&&field->introduced_snapshot>tableStart;
    field->initial_null_proven=later&&initialNull;
    field->initial_value_proven=later&&!initialNull;
    MemoryContextSwitchTo(scratch);
    if(tzCurrentOwned)pfree(tzCurrentOwned);
}

void LakeReadColumnDefault(LakeInlineBinding* binding,LakeInlineColumn* field,
    int64 tableStart,Snapshot snapshot)
{
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(owner,"Native lake default metadata scratch",ALLOCSET_DEFAULT_SIZES);
    PG_TRY();{
        MemoryContextSwitchTo(scratch);
        LakeReadColumnDefaultInternal(binding,field,tableStart,snapshot,owner);
        MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);
    }PG_CATCH();{
        MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);PG_RE_THROW();
    }PG_END_TRY();
}

void LakeMaterializeInitialDefault(const LakeInlineColumn* field,Datum* value,bool* isnull,Size* binaryRowBytes)
{
    *value=(Datum)0;*isnull=true;
    if(field->initial_literal){
        if(!field->initial_value_proven||!field->defaults_validated)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake initial value lacks its retained-history proof")));
        Oid tgtType=LakeColumnTargetType(field);
        int32 tgtTypmod=LakeColumnTargetTypmod(field);
        Datum src=field->initial_value;
        if(tgtType!=field->type||tgtTypmod!=field->typmod){
            src=LakeWidenDatum(src,field->type,field->typmod,tgtType,tgtTypmod);
        }
        int16 length;bool byval;get_typlenbyval(tgtType,&length,&byval);
        /* Copy once into the caller row context, charging before detoast
         * together with all physically present BLOB/UUID/JSON/NUMERIC fields. */
        *value=tgtType==BYTEAOID?LakeCopyBinaryDatum(src,binaryRowBytes):
            tgtType==UUIDOID?LakeCopyUuidDatum(src,binaryRowBytes):
            tgtType==JSONOID?LakeCopyJsonDatum(src,binaryRowBytes):
            tgtType==NUMERICOID?LakeCopyNumericDatum(src,tgtTypmod,binaryRowBytes):
            datumCopy(src,byval,length);*isnull=false;
    }else if(!field->initial_null_proven)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake initial NULL lacks its retained-history proof")));
}

/* adbin parsing and comparison are temporary validation, not binding data. */
static void LakeValidateHostDefaultValue(const LakeInlineColumn* field,
    Form_pg_attribute attr,const AttrDefault* host)
{
    if(host->generatedCol||host->has_on_update||(host->adbin_on_update&&*host->adbin_on_update)||!host->adbin)
        LakeLiteralUnsupported();
    Node* node=(Node*)stringToNode(host->adbin);
    if(!node||!IsA(node,Const))LakeLiteralUnsupported();
    Const* value=(Const*)node;int16 length;bool byval;get_typlenbyval(field->type,&length,&byval);
    if(value->consttype!=field->type||value->consttypmod!=field->typmod||value->constcollid!=attr->attcollation||
       value->constlen!=length||value->constbyval!=byval||value->constisnull)
        LakeLiteralUnsupported();
    char* canonical=LakeEncodeLiteralDefault(field->type,field->typmod,value->constvalue);
    if(strcmp(canonical,field->current_literal)!=0&&
       !LakeFloatValuesEqual(field->type,canonical,field->current_literal))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake host constant differs from its standard default")));
    pfree(canonical);
}

void LakeValidateHostDefaults(const LakeInlineBinding* binding,Relation relation)
{
    if(!binding->literal_defaults)return;
    TupleDesc desc=RelationGetDescr(relation);TupleConstr* constraints=desc->constr;
    int expected=0;
    for(int c=0;c<binding->column_count;++c){
        const LakeInlineColumn* field=&binding->columns[c];Form_pg_attribute attr=&desc->attrs[LakeLogicalToHost(binding,c)];
        if(!field->defaults_validated)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake default metadata was not validated")));
        AttrDefault* host=NULL;
        if(constraints)for(int i=0;i<constraints->num_defval;++i)
            if(constraints->defval[i].adnum==attr->attnum){
                if(host)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake host default")));
                host=&constraints->defval[i];
            }
        if((field->current_literal!=NULL)!=attr->atthasdef||((host!=NULL)!=attr->atthasdef))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake host/default metadata disagrees")));
        if(!host)continue;
        ++expected;
        MemoryContext owner=CurrentMemoryContext;
        MemoryContext scratch=AllocSetContextCreate(owner,"Native lake host default scratch",ALLOCSET_DEFAULT_SIZES);
        PG_TRY();{
            MemoryContextSwitchTo(scratch);
            LakeValidateHostDefaultValue(field,attr,host);
            MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);
        }PG_CATCH();{
            MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);PG_RE_THROW();
        }PG_END_TRY();
    }
    if(constraints&&constraints->num_defval!=expected)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake has an unmapped host default")));
}
