/* Adapted standard directory semantics from locked DuckLake 0aaa8e10:
 * GetNewPartitionKey/WriteNewPartitionKeys, and source-schema-bound flush.
 * DuckLake bucket hash is adapted with explicit persistent byte encoding.
 * No DuckDB executor, SDK extension or private persistent format. */
#include "postgres.h"
#include "lake_version.h"
#include "lake_inline_sources.h"
#include "lake_inline_private.h"
#include "lake_txn_private.h"
#include "lake_partition.h"
#include "lake_column_default.h"
#include "lake_types.h"
#include "lake_numeric.h"
#include "lake_temporal.h"
#include "lake_temporal_text.h"
#include "lake_uuid.h"
#include "utils/datetime.h"
#include "utils/fmgroids.h"
#include "vendor/ducklake_bucket_murmur3.h"
#include "access/tuptoaster.h"
#include "mb/pg_wchar.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include <cmath>
#include <errno.h>
#include <limits.h>

static void LakePartitionCorrupt(const char* message)
{ ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("%s",message))); }


/* The installed SQL capability is read in lake_catalog.cpp; cached plans
 * do not grant a newer capability before ALTER EXTENSION commits. */

static const Size LAKE_PARTITION_TUPLE_BYTES=4*1024*1024;
static bool LakePartitionBaseKeyType(Oid type)
{ return type==BOOLOID||type==INT2OID||type==INT4OID||type==INT8OID||type==TEXTOID; }

static bool LakePartitionExtendedIdentityType(Oid type)
{ return type==UUIDOID||type==NUMERICOID||type==DATEOID||type==TIMESTAMPOID||type==TIMESTAMPTZOID; }

static bool LakePartitionExtendedBucketType(Oid type)
{ return LakePartitionExtendedIdentityType(type)||type==BYTEAOID||type==FLOAT4OID||type==FLOAT8OID; }

bool LakePartitionIsTruncateTransform(const char* text)
{
    return text!=NULL&&(pg_strcasecmp(text,"truncate")==0||pg_strncasecmp(text,"truncate(",9)==0);
}

/* Installed bucket capability and reviewed inheriting SQL contracts are read in lake_catalog.cpp. */

/* Installed SQL32 time transforms and SQL33 inheritance are read in lake_catalog.cpp. */

bool LakePartitionIsTimeTransform(LakePartitionTransform transform)
{
    return transform>=LAKE_PARTITION_YEAR&&transform<=LAKE_PARTITION_EPOCH_HOUR;
}
static const char* const LakeTimeTransformNames[]={
    "year","month","day","hour","epoch_year","epoch_month","epoch_day","epoch_hour"
};
static bool LakeTimePartitionSource(Oid type)
{ return type==DATEOID||type==TIMESTAMPOID||type==TIMESTAMPTZOID; }

static bool LakePartitionTruncateType(Oid type)
{ return type==INT2OID||type==INT4OID||type==INT8OID||type==TEXTOID||type==BYTEAOID; }

Oid LakePartitionResultType(LakePartitionTransform transform,Oid type)
{
    if(LakePartitionIsTimeTransform(transform))return INT8OID;
    if(transform==LAKE_PARTITION_BUCKET)return INT4OID;
    if(transform==LAKE_PARTITION_TRUNCATE){
        if(type==INT2OID)return INT4OID;
        if(type==INT4OID)return INT8OID;
        return type;
    }
    return type;
}

bool LakePartitionFieldSupported(const LakePartitionField* field)
{
    if(!field)return false;
    if(LakePartitionIsTimeTransform(field->transform))
        return LakeTimePartitionVersionEnabled()&&LakeTimePartitionSource(field->type)&&
            field->parameter==0&&field->result_type==INT8OID;
    if(field->transform==LAKE_PARTITION_IDENTITY){
        bool typeOk=LakePartitionBaseKeyType(field->type)||
            (LakePartitionComboVersionEnabled()&&LakePartitionExtendedIdentityType(field->type));
        if(!typeOk)return false;
        if(field->type==NUMERICOID&&!LakeNumericTypmod(field->typmod))return false;
        return field->parameter==0&&field->result_type==field->type;
    }
    if(field->transform==LAKE_PARTITION_BUCKET&&LakeBucketPartitionVersionEnabled()){
        bool typeOk=LakePartitionBaseKeyType(field->type)||
            (LakePartitionComboVersionEnabled()&&LakePartitionExtendedBucketType(field->type));
        if(!typeOk)return false;
        if(field->type==NUMERICOID&&!LakeNumericTypmod(field->typmod))return false;
        return field->parameter>=1&&field->parameter<=INT_MAX&&field->result_type==INT4OID;
    }
    if(field->transform==LAKE_PARTITION_TRUNCATE&&LakeTruncatePartitionVersionEnabled()){
        return LakePartitionTruncateType(field->type)&&
            field->parameter>=1&&field->parameter<=INT_MAX&&
            field->result_type==LakePartitionResultType(LAKE_PARTITION_TRUNCATE,field->type);
    }
    return false;
}

/* Positive divisors only. C++ truncates toward zero; DuckLake epoch transforms
 * floor at hour/day boundaries, including the microsecond before Unix epoch. */
#ifdef HAVE_INT64_TIMESTAMP
static int64 LakeTimeFloorDivide(int64 value,int64 unit)
{
    Assert(unit>0);
    int64 result=value/unit;
    if(value%unit<0)--result;
    return result;
}
#endif
static int64 LakePartitionTimeValue(const LakePartitionField* field,Datum value)
{
    if(!LakePartitionFieldSupported(field))
        LakePartitionCorrupt("invalid native lake temporal partition descriptor");
    int year=0,month=0,day=0,hour=0;int64 unixDays=0,unixHours=0;
    if(field->type==DATEOID){
        DateADT date=DatumGetDateADT(value);
        unixDays=LakeTemporalDateToUnix(date);
        /* DATE supports a wider domain than Timestamp. Keep day arithmetic;
         * converting the whole date to microseconds would reject legal years. */
        unixHours=unixDays*INT64CONST(24); /* finite int32 days, cannot overflow */
        j2date((int)((int64)date+POSTGRES_EPOCH_JDATE),&year,&month,&day);
    }else{
#ifdef HAVE_INT64_TIMESTAMP
        Timestamp stamp=field->type==TIMESTAMPTZOID?DatumGetTimestampTz(value):DatumGetTimestamp(value);
        int64 unixMicros=LakeTemporalTimestampToUnix(stamp);
        unixDays=LakeTimeFloorDivide(unixMicros,INT64CONST(86400000000));
        unixHours=LakeTimeFloorDivide(unixMicros,INT64CONST(3600000000));
        struct pg_tm tm;fsec_t fsec;
        /* NULL tzp is deliberate: for TIMESTAMPTZ the stored instant is
         * decomposed in UTC, independent of the SQL session's timezone. */
        if(timestamp2tm(stamp,NULL,&tm,&fsec,NULL,NULL)!=0)
            ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake partition timestamp cannot be decomposed")));
        year=tm.tm_year;month=tm.tm_mon;day=tm.tm_mday;hour=tm.tm_hour;
#else
        (void)value;
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake timestamp transport requires integer datetimes")));
#endif
    }
    /* Native internal year zero is 1 BC, matching DuckDB's astronomical year;
     * SQL extract(year) would instead report -1 and is not used here. */
    switch(field->transform){
        case LAKE_PARTITION_YEAR:return (int64)year;
        case LAKE_PARTITION_MONTH:return month;
        case LAKE_PARTITION_DAY:return day;
        case LAKE_PARTITION_HOUR:return hour;
        case LAKE_PARTITION_EPOCH_YEAR:return (int64)year-1970;
        case LAKE_PARTITION_EPOCH_MONTH:return ((int64)year-1970)*12+(month-1);
        case LAKE_PARTITION_EPOCH_DAY:return unixDays;
        case LAKE_PARTITION_EPOCH_HOUR:return unixHours;
        default:LakePartitionCorrupt("unknown native lake temporal partition transform");
    }
    return 0;
}


static bool LakeParsePositiveParamTransform(const char* text,const char* prefix,Size prefixLen,int64* parameter)
{
    if(strncmp(text,prefix,prefixLen)!=0)return false;
    const char* p=text+prefixLen;if(*p<'1'||*p>'9')return false;
    int64 n=0;
    while(*p>='0'&&*p<='9'){
        int digit=*p++-'0';if(n>(INT_MAX-digit)/10)return false;n=n*10+digit;
    }
    if(*p!=')'||p[1]!='\0')return false;
    *parameter=n;return true;
}

bool LakePartitionTruncateInt64Floor(int64 value,int64 width,int64* output)
{
    if(width<1||width>INT_MAX)return false;
    __int128 x=(__int128)value,w=(__int128)width;
    __int128 q=x/w;
    if(x%w<0)--q;
    __int128 res=q*w;
    if(res<(__int128)PG_INT64_MIN||res>(__int128)PG_INT64_MAX)return false;
    if(output)*output=(int64)res;
    return true;
}

bool LakePartitionParseTransform(const char* text,LakePartitionTransform* transform,int64* parameter)
{
    if(!text||!transform||!parameter)return false;
    if(strcmp(text,"identity")==0){*transform=LAKE_PARTITION_IDENTITY;*parameter=0;return true;}
    for(Size i=0;i<lengthof(LakeTimeTransformNames);++i)
        if(strcmp(text,LakeTimeTransformNames[i])==0){
            *transform=(LakePartitionTransform)(LAKE_PARTITION_YEAR+i);*parameter=0;return true;
        }
    if(LakeParsePositiveParamTransform(text,"bucket(",7,parameter)){
        *transform=LAKE_PARTITION_BUCKET;return true;
    }
    if(LakeParsePositiveParamTransform(text,"truncate(",9,parameter)){
        *transform=LAKE_PARTITION_TRUNCATE;return true;
    }
    return false;
}
char* LakePartitionTransformText(const LakePartitionField* field)
{
    if(field&&field->transform==LAKE_PARTITION_IDENTITY&&field->parameter==0)return pstrdup("identity");
    if(field&&field->transform==LAKE_PARTITION_BUCKET&&field->parameter>=1&&field->parameter<=INT_MAX)
        return psprintf("bucket(%d)",(int)field->parameter);
    if(field&&field->transform==LAKE_PARTITION_TRUNCATE&&field->parameter>=1&&field->parameter<=INT_MAX)
        return psprintf("truncate(%d)",(int)field->parameter);
    if(field&&LakePartitionIsTimeTransform(field->transform)&&field->parameter==0)
        return pstrdup(LakeTimeTransformNames[field->transform-LAKE_PARTITION_YEAR]);
    LakePartitionCorrupt("invalid native lake partition transform");return NULL;
}
bool LakePartitionHasBucket(const LakePartitionSpec* spec)
{
    if(!spec||!spec->enabled)return false;
    if(spec->key_count<1||spec->key_count>MaxHeapAttributeNumber||!spec->keys)
        LakePartitionCorrupt("invalid native lake ordered partition descriptor");
    for(int k=0;k<spec->key_count;++k)if(spec->keys[k].transform==LAKE_PARTITION_BUCKET)return true;
    return false;
}
Datum LakePartitionApplyTransform(const LakePartitionField* field,Datum value)
{
    if(LakePartitionIsTimeTransform(field->transform))
        return Int64GetDatum(LakePartitionTimeValue(field,value));
    if(field->transform==LAKE_PARTITION_IDENTITY)return value;
    if(field->transform==LAKE_PARTITION_TRUNCATE){
        if(!LakeTruncatePartitionVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake truncate partitions are not enabled in this build")));
        if(!LakePartitionFieldSupported(field))
            LakePartitionCorrupt("invalid native lake truncate descriptor");
        switch(field->type){
            case INT2OID:{
                int64 res=0;
                if(!LakePartitionTruncateInt64Floor((int64)DatumGetInt16(value),field->parameter,&res)||
                   res<PG_INT32_MIN||res>PG_INT32_MAX)
                    LakePartitionCorrupt("smallint truncate overflowed int4");
                return Int32GetDatum((int32)res);
            }
            case INT4OID:{
                int64 res=0;
                if(!LakePartitionTruncateInt64Floor((int64)DatumGetInt32(value),field->parameter,&res))
                    LakePartitionCorrupt("integer truncate overflowed int8");
                return Int64GetDatum(res);
            }
            case INT8OID:{
                int64 res=0;
                if(!LakePartitionTruncateInt64Floor(DatumGetInt64(value),field->parameter,&res))
                    ereport(ERROR,(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        errmsg("bigint truncate(%lld) partition value out of range",(long long)field->parameter)));
                return Int64GetDatum(res);
            }
            case TEXTOID:{
                if(GetDatabaseEncoding()!=PG_UTF8)
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake text truncate keys require UTF8")));
                Size size=toast_raw_datum_size(value);
                if(size<VARHDRSZ||size>VARHDRSZ+4*1024*1024)
                    ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake truncate text exceeds its input byte budget")));
                text* input=DatumGetTextPP(value);int bytes=VARSIZE_ANY_EXHDR(input);
                const char* ptr=VARDATA_ANY(input);
                if(memchr(ptr,'\0',(size_t)bytes)!=NULL)
                    ereport(ERROR,(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),errmsg("native lake partition text contains a zero byte")));
                (void)pg_verify_mbstr(PG_UTF8,ptr,bytes,false);
                int clipped=pg_mbcharcliplen_orig(ptr,bytes,(int)field->parameter);
                text* out=cstring_to_text_with_len(ptr,clipped);
                if((Pointer)input!=DatumGetPointer(value))pfree(input);
                return PointerGetDatum(out);
            }
            case BYTEAOID:{
                Size size=toast_raw_datum_size(value);
                if(size<VARHDRSZ||size>VARHDRSZ+4*1024*1024)
                    ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake truncate bytea exceeds its input byte budget")));
                bytea* input=DatumGetByteaPP(value);int bytes=VARSIZE_ANY_EXHDR(input);
                int clipped=(int)Min((int64)bytes,field->parameter);
                bytea* out=(bytea*)palloc(VARHDRSZ+(Size)clipped);SET_VARSIZE(out,VARHDRSZ+(Size)clipped);
                if(clipped>0)memcpy(VARDATA(out),VARDATA_ANY(input),(Size)clipped);
                if((Pointer)input!=DatumGetPointer(value))pfree(input);
                return PointerGetDatum(out);
            }
            default:
                LakePartitionCorrupt("unsupported native lake truncate source type");
                return (Datum)0;
        }
    }
    if(field->transform!=LAKE_PARTITION_BUCKET||field->parameter<1||field->parameter>INT_MAX||field->result_type!=INT4OID)
        LakePartitionCorrupt("invalid native lake bucket descriptor");
    if(!LakeBucketPartitionVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake bucket partitions are not enabled in this build")));
    uint32 hash=0;
    switch(field->type){
        case BOOLOID:hash=OgLakeBucketMurmur3Int64(DatumGetBool(value)?1:0);break;
        case INT2OID:hash=OgLakeBucketMurmur3Int64((int64)DatumGetInt16(value));break;
        case INT4OID:hash=OgLakeBucketMurmur3Int64((int64)DatumGetInt32(value));break;
        case INT8OID:hash=OgLakeBucketMurmur3Int64(DatumGetInt64(value));break;
        case TEXTOID:{
            if(GetDatabaseEncoding()!=PG_UTF8)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake text bucket keys require UTF8")));
            /* Bound detoasting and one uncancellable pure hash invocation. */
            Size size=toast_raw_datum_size(value);
            if(size<VARHDRSZ||size>VARHDRSZ+4*1024*1024)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake bucket text exceeds its input byte budget")));
            text* input=DatumGetTextPP(value);int bytes=VARSIZE_ANY_EXHDR(input);
            (void)pg_verify_mbstr(PG_UTF8,VARDATA_ANY(input),bytes,false);
            hash=OgLakeBucketMurmur3((const uint8_t*)VARDATA_ANY(input),(size_t)bytes);
            if((Pointer)input!=DatumGetPointer(value))pfree(input);
            break;
        }
        case BYTEAOID:{
            if(!LakePartitionComboVersionEnabled())LakePartitionCorrupt("unsupported native lake bytea bucket transform");
            Size size=toast_raw_datum_size(value);
            if(size<VARHDRSZ||size>VARHDRSZ+4*1024*1024)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake bucket bytea exceeds its input byte budget")));
            bytea* input=DatumGetByteaPP(value);int bytes=VARSIZE_ANY_EXHDR(input);
            hash=OgLakeBucketMurmur3((const uint8_t*)VARDATA_ANY(input),(size_t)bytes);
            if((Pointer)input!=DatumGetPointer(value))pfree(input);
            break;
        }
        case UUIDOID:{
            if(!LakePartitionComboVersionEnabled())LakePartitionCorrupt("unsupported native lake uuid bucket transform");
            char* text=DatumGetCString(DirectFunctionCall1(uuid_out,value));
            hash=OgLakeBucketMurmur3((const uint8_t*)text,strlen(text));
            pfree(text);break;
        }
        case DATEOID:{
            if(!LakePartitionComboVersionEnabled())LakePartitionCorrupt("unsupported native lake date bucket transform");
            hash=OgLakeBucketMurmur3Int64((int64)LakeTemporalDateToUnix(DatumGetDateADT(value)));break;
        }
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:{
            if(!LakePartitionComboVersionEnabled())LakePartitionCorrupt("unsupported native lake timestamp bucket transform");
            Timestamp stamp=field->type==TIMESTAMPTZOID?DatumGetTimestampTz(value):DatumGetTimestamp(value);
            hash=OgLakeBucketMurmur3Int64(LakeTemporalTimestampToUnix(stamp));break;
        }
        case FLOAT4OID:
        case FLOAT8OID:{
            if(!LakePartitionComboVersionEnabled())LakePartitionCorrupt("unsupported native lake float bucket transform");
            double v=field->type==FLOAT4OID?(double)DatumGetFloat4(value):DatumGetFloat8(value);
            uint64 bits=0;
            if(std::isnan(v))bits=UINT64CONST(0x7ff8000000000000);
            else{if(v==0.0)v=0.0;memcpy(&bits,&v,sizeof(bits));}
            hash=OgLakeBucketMurmur3((const uint8_t*)&bits,sizeof(bits));break;
        }
        case NUMERICOID:{
            if(!LakePartitionComboVersionEnabled())LakePartitionCorrupt("unsupported native lake numeric bucket transform");
            int p=0,s=0;LakeNumericParameters(field->typmod,&p,&s);
            unsigned char coeff16[16];
            if(!LakeNumericTryCoefficient(value,field->typmod,coeff16,true))
                ereport(ERROR,(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),errmsg("native lake numeric bucket value is outside decimal(%d,%d)",p,s)));
            if(p<=18){
                __int128 signedVal=0;memcpy(&signedVal,coeff16,sizeof(signedVal));
                hash=OgLakeBucketMurmur3Int64((int64)signedVal);
            }else{
                char canonical[64];
                if(!LakeDecimal128Decode(coeff16,p,s,canonical,sizeof(canonical)))
                    LakePartitionCorrupt("native lake numeric bucket encoding failed");
                hash=OgLakeBucketMurmur3((const uint8_t*)canonical,strlen(canonical));
            }
            break;
        }
        default:LakePartitionCorrupt("unsupported native lake bucket source type");
    }
    return Int32GetDatum((int32)((hash&UINT32_C(0x7fffffff))%(uint32)field->parameter));
}

static void LakePartitionCheckSpec(const LakePartitionSpec* spec)
{
    if(!spec||!spec->enabled||spec->key_count<1||spec->key_count>MaxHeapAttributeNumber||!spec->keys)
        LakePartitionCorrupt("invalid native lake ordered partition descriptor");
    for(int k=0;k<spec->key_count;++k){
        const auto& key=spec->keys[k];
        if(key.column_id<0||key.column_index<0||key.column_index>=MaxHeapAttributeNumber||!LakePartitionFieldSupported(&key))
            LakePartitionCorrupt("invalid native lake identity key descriptor");
    }
}

bool LakePartitionNeedsSourceVersionMap(const LakePartitionSpec* spec)
{
    if(!spec||!spec->enabled)return false;
    LakePartitionCheckSpec(spec);
    for(int k=0;k<spec->key_count;++k)
        if(spec->keys[k].transform!=LAKE_PARTITION_IDENTITY||
           LakeNativeTypeIsTemporal(spec->keys[k].type)||
           spec->keys[k].type==NUMERICOID)return true;
    return false;
}

int LakePartitionFindIdentityKey(const LakePartitionSpec* spec,int64 columnId)
{
    if(!spec||!spec->enabled)return -1;
    if(spec->key_count<1||spec->key_count>MaxHeapAttributeNumber||!spec->keys)
        LakePartitionCorrupt("invalid native lake ordered partition descriptor");
    int result=-1;
    for(int k=0;k<spec->key_count;++k)if(spec->keys[k].column_id==columnId&&spec->keys[k].transform==LAKE_PARTITION_IDENTITY){
        if(result>=0||spec->keys[k].parameter!=0)
            LakePartitionCorrupt("invalid repeated native lake identity key lookup");
        result=k;
    }
    return result;
}

static bool LakePartitionDatumEqual(Oid type,Datum a,Datum b)
{
    switch(type){
        case BOOLOID:return DatumGetBool(a)==DatumGetBool(b);
        case INT2OID:return DatumGetInt16(a)==DatumGetInt16(b);
        case INT4OID:return DatumGetInt32(a)==DatumGetInt32(b);
        case INT8OID:return DatumGetInt64(a)==DatumGetInt64(b);
        case DATEOID:return DatumGetDateADT(a)==DatumGetDateADT(b);
        case TIMESTAMPOID:return DatumGetTimestamp(a)==DatumGetTimestamp(b);
        case TIMESTAMPTZOID:return DatumGetTimestampTz(a)==DatumGetTimestampTz(b);
        case UUIDOID:return memcmp(DatumGetUUIDP(a)->data,DatumGetUUIDP(b)->data,UUID_LEN)==0;
        case NUMERICOID:return DatumGetBool(DirectFunctionCall2(numeric_eq,a,b));
        case TEXTOID:{
            text* left=DatumGetTextPP(a);text* right=DatumGetTextPP(b);
            Size bytes=VARSIZE_ANY_EXHDR(left);
            bool same=bytes==(Size)VARSIZE_ANY_EXHDR(right)&&memcmp(VARDATA_ANY(left),VARDATA_ANY(right),bytes)==0;
            if((Pointer)left!=DatumGetPointer(a))pfree(left);
            if((Pointer)right!=DatumGetPointer(b))pfree(right);
            return same;
        }
        case BYTEAOID:{
            bytea* left=DatumGetByteaPP(a);bytea* right=DatumGetByteaPP(b);
            Size bytes=VARSIZE_ANY_EXHDR(left);
            bool same=bytes==(Size)VARSIZE_ANY_EXHDR(right)&&memcmp(VARDATA_ANY(left),VARDATA_ANY(right),bytes)==0;
            if((Pointer)left!=DatumGetPointer(a))pfree(left);
            if((Pointer)right!=DatumGetPointer(b))pfree(right);
            return same;
        }
        default:LakePartitionCorrupt("unsupported native lake identity value type");
    }
    return false;
}

/* The two owned encodings are intentionally bounded together. This budget is
 * separate from SDK writer memory and never permits an unbounded group key. */
static LakePartitionValue LakePartitionCopyValue(const LakePartitionField* field,Oid type,Datum raw,bool absent,MemoryContext target,Size* budget)
{
    LakePartitionValue result={};result.is_null=absent;
    if(absent)return result;
    if(type==TEXTOID){
        text* input=DatumGetTextPP(raw);Size bytes=VARSIZE_ANY_EXHDR(input);
        if(bytes>(LAKE_PARTITION_TUPLE_BYTES-VARHDRSZ-1)/2||
           *budget>LAKE_PARTITION_TUPLE_BYTES-(bytes*2+VARHDRSZ+1))
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake partition tuple exceeds its byte budget")));
        *budget+=bytes*2+VARHDRSZ+1;
        text* copied=(text*)MemoryContextAlloc(target,VARHDRSZ+bytes);SET_VARSIZE(copied,VARHDRSZ+bytes);
        memcpy(VARDATA(copied),VARDATA_ANY(input),bytes);
        result.value=PointerGetDatum(copied);
        result.canonical_text=(char*)MemoryContextAlloc(target,bytes+1);
        memcpy(result.canonical_text,VARDATA_ANY(input),bytes);result.canonical_text[bytes]='\0';
        if(memchr(result.canonical_text,'\0',bytes)!=NULL)
            LakePartitionCorrupt("native lake partition text contains a zero byte");
        if((Pointer)input!=DatumGetPointer(raw))pfree(input);
    }else if(type==BYTEAOID){
        char* encoded=LakeEncodeBinaryCanonical(raw,LAKE_PARTITION_TUPLE_BYTES);
        Size tlen=strlen(encoded);
        bytea* input=DatumGetByteaPP(raw);Size bytes=VARSIZE_ANY_EXHDR(input);
        if(bytes>LAKE_PARTITION_TUPLE_BYTES-VARHDRSZ||
           *budget>LAKE_PARTITION_TUPLE_BYTES-(bytes+VARHDRSZ+tlen+1)){
            if((Pointer)input!=DatumGetPointer(raw))pfree(input);
            pfree(encoded);
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake partition tuple exceeds its byte budget")));
        }
        *budget+=bytes+VARHDRSZ+tlen+1;
        bytea* copied=(bytea*)MemoryContextAlloc(target,VARHDRSZ+bytes);SET_VARSIZE(copied,VARHDRSZ+bytes);
        if(bytes>0)memcpy(VARDATA(copied),VARDATA_ANY(input),bytes);
        result.value=PointerGetDatum(copied);
        result.canonical_text=MemoryContextStrdup(target,encoded);
        pfree(encoded);
        if((Pointer)input!=DatumGetPointer(raw))pfree(input);
    }else if(type==UUIDOID){
        pg_uuid_t* in=DatumGetUUIDP(raw);
        if(!in)LakePartitionCorrupt("missing native lake UUID partition value");
        char* encoded=DatumGetCString(DirectFunctionCall1(uuid_out,raw));
        Size tlen=strlen(encoded);
        if(*budget>LAKE_PARTITION_TUPLE_BYTES-(sizeof(pg_uuid_t)+tlen+1))
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake partition tuple exceeds its byte budget")));
        *budget+=sizeof(pg_uuid_t)+tlen+1;
        pg_uuid_t* copy=(pg_uuid_t*)MemoryContextAlloc(target,sizeof(pg_uuid_t));
        memcpy(copy->data,in->data,UUID_LEN);
        result.value=UUIDPGetDatum(copy);
        result.canonical_text=MemoryContextStrdup(target,encoded);
        pfree(encoded);
    }else if(type==NUMERICOID){
        if(!field||!LakeNumericTypmod(field->typmod))
            LakePartitionCorrupt("missing native lake numeric partition parameters");
        char encoded[64];
        LakeNumericFormatCanonical(raw,field->typmod,encoded,sizeof(encoded));
        struct varlena* detoasted=PG_DETOAST_DATUM(raw);
        Size vlen=VARSIZE_ANY(detoasted);
        Size tlen=strlen(encoded);
        if(vlen>LAKE_PARTITION_TUPLE_BYTES||*budget>LAKE_PARTITION_TUPLE_BYTES-(vlen+tlen+1))
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake partition tuple exceeds its byte budget")));
        *budget+=vlen+tlen+1;
        void* copy=MemoryContextAlloc(target,vlen);
        memcpy(copy,detoasted,vlen);
        if((Pointer)detoasted!=DatumGetPointer(raw))pfree(detoasted);
        result.value=PointerGetDatum(copy);
        result.canonical_text=MemoryContextStrdup(target,encoded);
    }else if(type==DATEOID||type==TIMESTAMPOID||type==TIMESTAMPTZOID){
        Datum textDatum=LakeTemporalWriteText(raw,type);
        char* encoded=TextDatumGetCString(textDatum);
        pfree(DatumGetPointer(textDatum));
        Size tlen=strlen(encoded);
        if(*budget>LAKE_PARTITION_TUPLE_BYTES-tlen-1)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake partition tuple exceeds its byte budget")));
        *budget+=tlen+1;
        result.value=raw;
        result.canonical_text=MemoryContextStrdup(target,encoded);
        pfree(encoded);
    }else{
        char encoded[32];int length=-1;
        switch(type){
            case BOOLOID:length=snprintf(encoded,sizeof(encoded),"%s",DatumGetBool(raw)?"true":"false");break;
            case INT2OID:length=snprintf(encoded,sizeof(encoded),"%d",(int)DatumGetInt16(raw));break;
            case INT4OID:length=snprintf(encoded,sizeof(encoded),"%d",DatumGetInt32(raw));break;
            case INT8OID:length=snprintf(encoded,sizeof(encoded),"%lld",(long long)DatumGetInt64(raw));break;
            default:LakePartitionCorrupt("unsupported native lake identity value type");
        }
        if(length<0||length>=(int)sizeof(encoded))LakePartitionCorrupt("native lake partition value encoding failed");
        if(*budget>LAKE_PARTITION_TUPLE_BYTES-(Size)length-1)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake partition tuple exceeds its byte budget")));
        *budget+=(Size)length+1;result.value=raw;result.canonical_text=MemoryContextStrdup(target,encoded);
    }
    return result;
}

LakePartitionSpec LakePartitionCopySpec(const LakePartitionSpec* source,MemoryContext target)
{
    if(!source||!target)LakePartitionCorrupt("missing native lake partition copy owner");
    LakePartitionSpec result=*source;
    if(!source->enabled){result.keys=NULL;result.key_count=0;return result;}
    LakePartitionCheckSpec(source);
    result.keys=(LakePartitionField*)MemoryContextAlloc(target,sizeof(LakePartitionField)*source->key_count);
    memcpy(result.keys,source->keys,sizeof(LakePartitionField)*source->key_count);return result;
}

LakePartitionTuple LakePartitionCopyTuple(const LakePartitionSpec* spec,const LakePartitionTuple* source,MemoryContext target)
{
    LakePartitionCheckSpec(spec);
    if(!source||source->count!=spec->key_count||!source->values||!target)
        LakePartitionCorrupt("invalid native lake partition tuple copy");
    LakePartitionTuple result={};result.count=source->count;
    result.values=(LakePartitionValue*)MemoryContextAllocZero(target,sizeof(LakePartitionValue)*result.count);
    Size budget=sizeof(LakePartitionValue)*result.count;
    for(int k=0;k<result.count;++k)
        result.values[k]=LakePartitionCopyValue(&spec->keys[k],spec->keys[k].result_type,source->values[k].value,source->values[k].is_null,target,&budget);
    return result;
}

bool LakePartitionTupleEqual(const LakePartitionSpec* spec,const LakePartitionTuple* a,const LakePartitionTuple* b)
{
    LakePartitionCheckSpec(spec);
    if(!a||!b||a->count!=spec->key_count||b->count!=spec->key_count||!a->values||!b->values)
        LakePartitionCorrupt("invalid native lake partition tuple equality");
    for(int k=0;k<spec->key_count;++k){
        if(a->values[k].is_null!=b->values[k].is_null)return false;
        if(!a->values[k].is_null&&!LakePartitionDatumEqual(spec->keys[k].result_type,a->values[k].value,b->values[k].value))return false;
    }
    return true;
}

bool LakePartitionTupleMatches(const LakePartitionSpec* spec,const LakePartitionTuple* tuple,
    const Datum* values,const bool* nulls,const int* attrs)
{
    LakePartitionCheckSpec(spec);
    if(!tuple||tuple->count!=spec->key_count||!tuple->values||!values||!nulls||!attrs)
        LakePartitionCorrupt("invalid native lake partition tuple match");
    for(int k=0;k<spec->key_count;++k){
        int attr=attrs[spec->keys[k].column_index];
        if(attr<0)LakePartitionCorrupt("invalid native lake partition source attribute");
        if(tuple->values[k].is_null!=nulls[attr])return false;
        if(!nulls[attr]&&!LakePartitionDatumEqual(spec->keys[k].result_type,tuple->values[k].value,LakePartitionApplyTransform(&spec->keys[k],values[attr])))return false;
    }
    return true;
}

LakePartitionTuple LakePartitionFormTuple(const LakePartitionSpec* spec,
    const Datum* values,const bool* nulls,const int* attrs,MemoryContext target)
{
    LakePartitionCheckSpec(spec);
    if(!values||!nulls||!attrs||!target)LakePartitionCorrupt("invalid native lake partition tuple source");
    LakePartitionTuple result={};result.count=spec->key_count;
    result.values=(LakePartitionValue*)MemoryContextAllocZero(target,sizeof(LakePartitionValue)*result.count);
    Size budget=sizeof(LakePartitionValue)*result.count;
    for(int k=0;k<result.count;++k){
        int attr=attrs[spec->keys[k].column_index];
        if(attr<0)LakePartitionCorrupt("invalid native lake partition source attribute");
        result.values[k]=LakePartitionCopyValue(&spec->keys[k],spec->keys[k].result_type,nulls[attr]?(Datum)0:LakePartitionApplyTransform(&spec->keys[k],values[attr]),nulls[attr],target,&budget);
    }
    return result;
}

/* The standard tables deliberately have no uniqueness constraints. Validate
 * actual rows, rather than trusting a foreign metadata database's indexes. */
static LakeIdentityPartitionSpec LakeReadIdentityPartition(LakeInlineBinding* binding,
    Snapshot snapshot,int64 at,int64 exactId)
{
    LakeIdentityPartitionSpec result={};result.end_snapshot=-1;
    Oid types[3]={INT8OID,INT8OID,INT8OID};
    Datum args[3]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(at),Int64GetDatum(exactId)};
    char* info=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_partition_info");
    LakeMetaRows* rows=LakeMetadataQuery(psprintf(
        "SELECT partition_id,table_id,begin_snapshot,end_snapshot FROM %s WHERE "
        "%s ORDER BY partition_id LIMIT 2",info,exactId>=0?"partition_id=$3":
        "table_id=$1 AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2)"),
        exactId>=0?3:2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(rows->count==0){
        if(exactId>=0)LakePartitionCorrupt("native lake file references a missing partition definition");
        return result;
    }
    if(rows->count!=1)LakePartitionCorrupt("duplicate native lake partition definition");
    /* A backdated physical file names its own historical spec. Do not require
     * that spec to be the current setting at the queried logical snapshot.
     * Host MVCC still controls whether this metadata row is available. */
    result.present=true;
    result.partition_id=DatumGetInt64(LakeMetadataValue(rows,0,1,INT8OID));
    int64 table=DatumGetInt64(LakeMetadataValue(rows,0,2,INT8OID));
    result.begin_snapshot=DatumGetInt64(LakeMetadataValue(rows,0,3,INT8OID));
    bool absent=false;Datum end=LakeMetadataValue(rows,0,4,INT8OID,true,&absent);
    if(!absent)result.end_snapshot=DatumGetInt64(end);
    if(result.partition_id<0||result.partition_id>=LAKE_REFERENCE_LOCAL_ID_START||
       table!=LakePermanentTableId(binding)||result.begin_snapshot<0||
       (exactId<0&&result.begin_snapshot>binding->snapshot_id)||
       (!absent&&result.end_snapshot<result.begin_snapshot))
        LakePartitionCorrupt("invalid native lake partition definition interval or identity");
    char* columns=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_partition_column");
    args[2]=Int64GetDatum(result.partition_id);
    bool expanded=LakeIdentityPartitionKeysVersionEnabled();
    bool bucketEnabled=LakeBucketPartitionVersionEnabled();
    int keyLimit=bucketEnabled?MaxHeapAttributeNumber+1:expanded?binding->column_count+1:3;
    rows=LakeMetadataQuery(psprintf("SELECT table_id,partition_key_index,column_id,transform::text "
        "FROM %s WHERE partition_id=$3 ORDER BY partition_key_index LIMIT %d",columns,keyLimit),
        3,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(rows->count==0)return result; /* Valid empty RESET marker. */
    if(!expanded&&rows->count!=1){
        int64 previous=-1;
        for(uint32 i=0;i<rows->count;++i){
            int64 ordinal=DatumGetInt64(LakeMetadataValue(rows,i,2,INT8OID));
            if(DatumGetInt64(LakeMetadataValue(rows,i,1,INT8OID))!=table||ordinal<0||ordinal<=previous)
                LakePartitionCorrupt("duplicate or invalid native lake partition key ordinal");
            previous=ordinal;
        }
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake identity partition slice requires exactly one partition key")));
    }
    if(binding->column_count<1||binding->column_count>MaxHeapAttributeNumber||rows->count>(uint32)(bucketEnabled?MaxHeapAttributeNumber:binding->column_count))
        LakePartitionCorrupt("native lake partition key count exceeds its business columns");
    result.key_count=(int)rows->count;
    result.keys=(LakePartitionField*)palloc0(sizeof(LakePartitionField)*result.key_count);
    for(int k=0;k<result.key_count;++k){
        auto* key=&result.keys[k];key->column_index=-1;key->typmod=-1;
        int64 ordinal=DatumGetInt64(LakeMetadataValue(rows,k,2,INT8OID));
        key->column_id=DatumGetInt64(LakeMetadataValue(rows,k,3,INT8OID));
        if(DatumGetInt64(LakeMetadataValue(rows,k,1,INT8OID))!=table||ordinal!=k||key->column_id<0)
            LakePartitionCorrupt("invalid native lake partition column identity or ordinal");
        const char* transform=LakeMetadataText(rows,k,4);
        bool truncateEnabled=LakeTruncatePartitionVersionEnabled();
        if(LakePartitionIsTruncateTransform(transform)&&!truncateEnabled)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("DuckLake partition transform \"%s\" is not supported",transform)));
        if(!LakePartitionParseTransform(transform,&key->transform,&key->parameter)||
           (key->transform==LAKE_PARTITION_BUCKET&&!bucketEnabled)||
           (key->transform==LAKE_PARTITION_TRUNCATE&&!truncateEnabled)||
           (LakePartitionIsTimeTransform(key->transform)&&!LakeTimePartitionVersionEnabled()))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake partition transform requires identity")));
        for(int c=0;c<binding->column_count;++c)if(binding->columns[c].column_id==key->column_id){
            if(key->column_index>=0)LakePartitionCorrupt("ambiguous native lake partition business column");
            key->column_index=c;key->type=binding->columns[c].type;key->typmod=binding->columns[c].typmod;
        }
        if(key->column_index<0||(!expanded&&key->type!=INT4OID))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake identity partition slice requires a current top-level int4 column")));
        key->result_type=LakePartitionResultType(key->transform,key->type);
        if(!LakePartitionFieldSupported(key))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake partition transform is not supported for its source type")));
        for(int j=0;j<k;++j)if(result.keys[j].column_id==key->column_id&&
            result.keys[j].transform==key->transform&&result.keys[j].parameter==key->parameter)
            LakePartitionCorrupt("duplicate native lake identity partition column");
    }
    result.enabled=true;LakeRequireIdentityPartitionVersion();return result;
}

void LakeRequireIdentityPartitionVersion(void)
{
    if(!LakeIdentityPartitionVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake identity partitions require the coordinated partition SQL capability"),
            errhint("Install extension0.22 to use identity partitions; retained SQL contracts remain unchanged.")));
}

void LakeLoadCurrentIdentityPartition(LakeInlineBinding* binding,Snapshot snapshot)
{
    binding->partition=LakeReadIdentityPartition(binding,snapshot,binding->snapshot_id,-1);
    if(binding->partition.enabled&&!LakePartitionComboVersionEnabled()){
        LakeRejectUuidAcceleration(binding,"file partition rules");
        LakeRejectJsonAcceleration(binding,"file partition rules");
        LakeRejectNumericAcceleration(binding,"file partition rules");
    }
}

void LakeLoadInlineIdentityPartition(LakeInlineBinding* binding,int64 version,int64 origin,Snapshot snapshot)
{
    if(version<0||origin<0||origin>binding->snapshot_id)
        LakePartitionCorrupt("invalid native lake inline partition schema origin");
    binding->inline_schema_version=version;binding->inline_origin_snapshot=origin;
    binding->inline_partition=LakeReadIdentityPartition(binding,snapshot,origin,-1);
    if(binding->inline_partition.enabled&&!LakePartitionComboVersionEnabled()){
        LakeRejectUuidAcceleration(binding,"historical inline partition rules");
        LakeRejectJsonAcceleration(binding,"historical inline partition rules");
        LakeRejectNumericAcceleration(binding,"historical inline partition rules");
    }
}

void LakeLoadFileIdentityPartition(LakeInlineBinding* binding,LakeDataFile* file,int64 id,Snapshot snapshot)
{
    if(id>=0){
        file->partition.spec=LakeReadIdentityPartition(binding,snapshot,binding->snapshot_id,id);
        if(file->partition.spec.enabled&&!LakePartitionComboVersionEnabled()){
            LakeRejectUuidAcceleration(binding,"historical file partition rules");
            LakeRejectJsonAcceleration(binding,"historical file partition rules");
            LakeRejectNumericAcceleration(binding,"historical file partition rules");
        }
        if(!file->partition.spec.enabled)LakePartitionCorrupt("native lake file references an empty partition definition");
    }
    char* values=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_file_partition_value");
    Oid types[2]={INT8OID,INT8OID};Datum args[2]={Int64GetDatum(file->file_id),Int64GetDatum(LakePermanentTableId(binding))};
    int limit=id<0?2:file->partition.spec.key_count+1;
    LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT table_id,partition_key_index,partition_value::text "
        "FROM %s WHERE data_file_id=$1 ORDER BY partition_key_index LIMIT %d",values,limit),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(id<0){if(rows->count)LakePartitionCorrupt("unpartitioned native lake file has partition values");return;}
    const auto* spec=&file->partition.spec;
    if(rows->count!=(uint32)spec->key_count)LakePartitionCorrupt("missing or duplicate native lake file partition value");
    auto* tuple=&file->partition.tuple;tuple->count=spec->key_count;
    tuple->values=(LakePartitionValue*)palloc0(sizeof(LakePartitionValue)*tuple->count);
    Size budget=sizeof(LakePartitionValue)*tuple->count;
    for(int k=0;k<tuple->count;++k){
        if(DatumGetInt64(LakeMetadataValue(rows,k,1,INT8OID))!=LakePermanentTableId(binding)||
           DatumGetInt64(LakeMetadataValue(rows,k,2,INT8OID))!=k)
            LakePartitionCorrupt("invalid native lake file partition value identity or ordinal");
        bool absent=false;Datum raw=LakeMetadataValue(rows,k,3,TEXTOID,true,&absent);
        if(absent){tuple->values[k].is_null=true;continue;}
        Oid type=spec->keys[k].result_type;char* encoded=TextDatumGetCString(raw);Datum value=(Datum)0;
        if(type==TEXTOID)value=raw;
        else if(type==BYTEAOID)value=LakeDecodeBinaryCanonical(encoded,LAKE_PARTITION_TUPLE_BYTES);
        else if(type==BOOLOID){
            if(strcmp(encoded,"true")&&strcmp(encoded,"false"))LakePartitionCorrupt("noncanonical native lake boolean partition value");
            value=BoolGetDatum(strcmp(encoded,"true")==0);
        }else if(type==UUIDOID){
            if(strlen(encoded)!=36)LakePartitionCorrupt("noncanonical native lake UUID partition value");
            value=DirectFunctionCall1Coll(uuid_in,InvalidOid,CStringGetDatum(encoded),false);
        }else if(type==DATEOID||type==TIMESTAMPOID||type==TIMESTAMPTZOID){
            value=LakeTemporalParseText(encoded,(int)strlen(encoded),type);
        }else if(type==NUMERICOID){
            int p=0,s=0;LakeNumericParameters(spec->keys[k].typmod,&p,&s);
            unsigned char coeff[16];Size bytes=0;
            if(!LakeDecimal128Encode(encoded,strlen(encoded),p,s,coeff))
                LakePartitionCorrupt("noncanonical native lake numeric partition value");
            value=LakeNumericFromCoefficient(coeff,spec->keys[k].typmod,&bytes);
        }else{
            char* end=NULL;errno=0;long long parsed=strtoll(encoded,&end,10);
            if(errno||end==encoded||*end||(type==INT2OID&&(parsed<SHRT_MIN||parsed>SHRT_MAX))||
               (type==INT4OID&&(parsed<INT_MIN||parsed>INT_MAX)))
                LakePartitionCorrupt(type==INT4OID?"native lake identity partition value is outside int4":"native lake partition value is outside its integer type");
            value=type==INT2OID?Int16GetDatum((int16)parsed):type==INT4OID?Int32GetDatum((int32)parsed):Int64GetDatum((int64)parsed);
        }
        if(spec->keys[k].transform==LAKE_PARTITION_BUCKET&&
           (DatumGetInt32(value)<0||DatumGetInt32(value)>=spec->keys[k].parameter))
            LakePartitionCorrupt("native lake bucket partition value is outside its bucket count");
        if(LakePartitionIsTimeTransform(spec->keys[k].transform)){
            int64 v=DatumGetInt64(value);LakePartitionTransform t=spec->keys[k].transform;
            if((t==LAKE_PARTITION_MONTH&&(v<1||v>12))||
               (t==LAKE_PARTITION_DAY&&(v<1||v>31))||
               (t==LAKE_PARTITION_HOUR&&(v<0||v>23)))
                LakePartitionCorrupt("native lake calendar partition value is outside its component range");
        }
        tuple->values[k]=LakePartitionCopyValue(&spec->keys[k],type,value,false,CurrentMemoryContext,&budget);
        if(strcmp(encoded,tuple->values[k].canonical_text)!=0)
            LakePartitionCorrupt(type==INT4OID?"native lake identity partition value is not canonical int4":"noncanonical native lake partition value");
        pfree(encoded);
    }
}

static void LakeBindPartitionSpecHost(LakeInlineBinding* binding,Relation relation,LakePartitionSpec* spec)
{
    if(!spec->enabled)return;
    LakePartitionCheckSpec(spec);TupleDesc descriptor=RelationGetDescr(relation);
    for(int k=0;k<spec->key_count;++k){
        auto* key=&spec->keys[k];int c=key->column_index;
        if(c<0||c>=binding->column_count||binding->columns[c].column_id!=key->column_id)
            LakePartitionCorrupt("native lake partition column binding changed");
        AttrNumber att=binding->columns[c].output_attnum;
        if(att<=0||att>descriptor->natts)LakePartitionCorrupt("native lake partition host attribute is missing");
        Form_pg_attribute attr=&descriptor->attrs[att-1];
        if(attr->attisdropped||attr->atttypid!=key->type||attr->atttypmod!=binding->columns[c].typmod)
            ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native lake partition key differs from its host column")));
        key->typmod=attr->atttypmod;key->collation=attr->attcollation;key->host_bound=true;
    }
}

void LakeBindPartitionHostColumns(LakeInlineBinding* binding,Relation relation)
{
    LakeBindPartitionSpecHost(binding,relation,&binding->partition);
    LakeBindPartitionSpecHost(binding,relation,&binding->inline_partition);
    for(int i=0;i<binding->inline_source_count;++i){
        LakeInlineBinding physical=*binding;
        physical.columns=binding->inline_sources[i].columns;physical.column_count=binding->inline_sources[i].column_count;
        LakeBindPartitionSpecHost(&physical,relation,&binding->inline_sources[i].partition);
    }
    for(int f=0;f<binding->file_count;++f)LakeBindPartitionSpecHost(binding,relation,&binding->files[f].partition.spec);
}

bool LakeIdentityPartitionSpecEqual(const LakeIdentityPartitionSpec* a,const LakeIdentityPartitionSpec* b)
{
    if(a->present!=b->present||a->enabled!=b->enabled)return false;
    if(!a->present)return true;
    if(a->partition_id!=b->partition_id||a->begin_snapshot!=b->begin_snapshot||a->end_snapshot!=b->end_snapshot)return false;
    if(!a->enabled)return true;
    LakePartitionCheckSpec(a);LakePartitionCheckSpec(b);
    if(a->key_count!=b->key_count)return false;
    for(int k=0;k<a->key_count;++k){
        const auto& x=a->keys[k];const auto& y=b->keys[k];
        if(x.column_id!=y.column_id||x.column_index!=y.column_index||x.type!=y.type||x.result_type!=y.result_type||
           x.transform!=y.transform||x.parameter!=y.parameter)return false;
        if(x.host_bound&&y.host_bound&&(x.typmod!=y.typmod||x.collation!=y.collation))return false;
    }
    return true;
}
bool LakeIdentityPartitionFileEqual(const LakeIdentityPartitionFile* a,const LakeIdentityPartitionFile* b)
{
    return LakeIdentityPartitionSpecEqual(&a->spec,&b->spec)&&
        (!a->spec.enabled||LakePartitionTupleEqual(&a->spec,&a->tuple,&b->tuple));
}

bool LakeIdentityPartitioned(const LakeInlineBinding* binding)
{
    if(!binding)return false;
    if(binding->partition.enabled||binding->inline_partition.enabled)return true;
    for(int i=0;i<binding->inline_source_count;++i)if(binding->inline_sources[i].partition.enabled)return true;
    for(int f=0;f<binding->file_count;++f)if(binding->files[f].partition.spec.enabled)return true;
    return false;
}
void LakeRejectIdentityPartitionIntersection(const LakeInlineBinding* binding,const char* operation)
{
    if(LakeInlineHasHistoricalSources(binding))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake historical inline sources do not yet support %s",operation)));
    if(LakeIdentityPartitioned(binding))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake identity partitions do not yet support %s",operation)));
}
