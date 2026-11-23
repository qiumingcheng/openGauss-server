/* Exact finite numeric(p,s) ownership and decimal128 transport, SQL54 only. */
#ifndef OG_LAKE_NUMERIC_H
#define OG_LAKE_NUMERIC_H
#include "postgres.h"
#include "fmgr.h"
#include "nodes/primnodes.h"
#include "lake_binary.h"
#include "lake_decimal128_codec.h"
#include "utils/builtins.h"
#include "utils/numeric.h"

/* The signed low half matters: A-format negative or scale>precision typmods
 * remain unsupported even when the host accepts that DDL. */
static inline bool LakeNumericTypmod(int32 typmod,int* precision=NULL,int* scale=NULL)
{
    if(typmod<VARHDRSZ)return false;
    uint32 raw=(uint32)(typmod-VARHDRSZ);
    int p=(raw>>16)&0xffff,s=(int16)(raw&0xffff);
    if(!LakeDecimal128Parameters(p,s))return false;
    if(precision)*precision=p;
    if(scale)*scale=s;
    return true;
}
static inline void LakeNumericParameters(int32 typmod,int* precision,int* scale)
{
    if(!LakeNumericTypmod(typmod,precision,scale))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake requires finite numeric(p,s), 1<=p<=38 and 0<=s<=p")));
}
/* DuckLake's canonical parameterized type, not a private catalog extension. */
static inline int32 LakeNumericDuckTypmod(const char* text)
{
    if(!text||strncmp(text,"decimal(",8)!=0)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake decimal type is not canonical")));
    const char* q=text+8;int p=0,s=0;
    if(*q<'1'||*q>'9')goto invalid;
    while(*q>='0'&&*q<='9'){if(p>38)goto invalid;p=p*10+(*q++-'0');}
    if(*q++!=',')goto invalid;
    if(*q<'0'||*q>'9'||(*q=='0'&&q[1]!=')'))goto invalid;
    while(*q>='0'&&*q<='9'){if(s>38)goto invalid;s=s*10+(*q++-'0');}
    if(*q++!=')'||*q!='\0'||!LakeDecimal128Parameters(p,s))goto invalid;
    return (int32)(((uint32)p<<16)|(uint16)s)+VARHDRSZ;
invalid:
    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake decimal parameters are unsupported or noncanonical")));
    return -1;
}
static inline const char* LakeNumericDuckType(int32 typmod)
{
    int p,s;LakeNumericParameters(typmod,&p,&s);return psprintf("decimal(%d,%d)",p,s);
}
static inline const char* LakeNumericSqlType(int32 typmod)
{
    int p,s;LakeNumericParameters(typmod,&p,&s);return psprintf("pg_catalog.numeric(%d,%d)",p,s);
}
static inline const char* LakeNumericArrowFormat(int32 typmod)
{
    int p,s;LakeNumericParameters(typmod,&p,&s);return psprintf("d:%d,%d",p,s);
}
static inline bool LakeNumericTryCoefficient(Datum datum,int32 typmod,unsigned char* coefficient,bool trimTrailingZeros=false)
{
    int p,s;LakeNumericParameters(typmod,&p,&s);
    /* numeric_out_with_zero handles normal, short, BI64 and BI128 datums.
     * Its exact finite decimal text is never passed through floating point. */
    char* text=DatumGetCString(DirectFunctionCall1Coll(numeric_out_with_zero,
        InvalidOid,datum,false));
    size_t len=strnlen(text,81);
    if(trimTrailingZeros&&len>0&&len<=80){
        const char* dot=(const char*)memchr(text,'.',len);
        if(dot!=NULL){
            size_t fracStart=(size_t)(dot-text)+1;
            while(len>fracStart+(size_t)s&&text[len-1]=='0')--len;
            if(s==0&&len==fracStart)--len;
        }
    }
    bool valid=LakeDecimal128Encode(text,len,p,s,coefficient);
    pfree(text);
    return valid;
}
static inline void LakeNumericCoefficient(Datum datum,int32 typmod,unsigned char* coefficient)
{
    if(!LakeNumericTryCoefficient(datum,typmod,coefficient,false))
        ereport(ERROR,(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
            errmsg("native lake numeric value is nonfinite or outside its exact decimal parameters")));
}
static inline void LakeNumericFormatCanonical(Datum datum,int32 typmod,char* output,size_t capacity)
{
    int p,s;LakeNumericParameters(typmod,&p,&s);
    unsigned char coefficient[16];LakeNumericCoefficient(datum,typmod,coefficient);
    if(!LakeDecimal128Decode(coefficient,p,s,output,capacity))
        ereport(ERROR,(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
            errmsg("native lake decimal canonical formatting exceeded its exact precision")));
}
static inline Datum LakeCopyNumericDatum(Datum value,int32 typmod,Size* rowBytes)
{
    Size raw=toast_raw_datum_size(value);
    if(raw<VARHDRSZ||raw>(Size)MaxAllocSize)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake numeric datum size is invalid")));
    LakeReserveBinaryBytes(raw-VARHDRSZ,rowBytes);
    unsigned char coefficient[16];LakeNumericCoefficient(value,typmod,coefficient);
    return PointerGetDatum(PG_DETOAST_DATUM_COPY(value));
}
static inline Datum LakeNumericFromCoefficient(const unsigned char* bytes,int32 typmod,Size* rowBytes)
{
    int p,s;LakeNumericParameters(typmod,&p,&s);char text[48];
    if(!LakeDecimal128Decode(bytes,p,s,text,sizeof(text)))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake decimal coefficient exceeds its exact precision")));
    /* The coefficient is already at precisely this scale and within p. This
     * strict host input cannot perform a second lossy rescale or ignore error. */
    Datum result=DirectFunctionCall3Coll(numeric_in,InvalidOid,CStringGetDatum(text),
        ObjectIdGetDatum(InvalidOid),Int32GetDatum(typmod),false);
    LakeReserveBinaryBytes(toast_raw_datum_size(result)-VARHDRSZ,rowBytes);
    return result;
}
extern Const* LakeParseNumericNullDefault(Node* raw,int32 typmod,Oid collation);
#endif
