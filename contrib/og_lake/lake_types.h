/* Shared mapping. LIST -> vector requires the catalog tree and bound typmod. */
#ifndef OG_LAKE_TYPES_H
#define OG_LAKE_TYPES_H
#include "postgres.h"
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "lake_numeric.h"
#include "sdk/lake_parquet_sdk.h"

struct LakeNativeType {
    Oid oid;
    const char* duck_type;
    const char* sql_type;
    uint32 sdk_type;
    const char* arrow_format;
    int fixed_width; /* 0=boolean bits, -1=variable bytes (UTF8/BINARY), -2=nested LIST. */
    bool maintain_bounds;
    bool writable;
};
/* Registry lookup is pure; consumer admission below owns SQL/SDK checks. */
static inline bool LakeNativeTypeIsTemporal(Oid oid)
{
    return oid==DATEOID||oid==TIMESTAMPOID||oid==TIMESTAMPTZOID;
}
/* Exact installed SQL31/32/33 contract; SDK type capabilities are additionally
 * mandatory. Older installed SQL keeps rejecting temporal business fields. */
#define OG_LAKE_TEMPORAL_BUSINESS_SQL_VERSION "0.31"
bool LakeTemporalBusinessVersionEnabled(void);
bool LakeBinaryBusinessVersionEnabled(void);
bool LakeUuidBusinessVersionEnabled(void);
bool LakeJsonBusinessVersionEnabled(void);
bool LakeNumericBusinessVersionEnabled(void);
extern const OgLakeParquetDecimalApi* LakeFileDecimalProvider(void);
static const LakeNativeType lake_native_types[]={
    {BOOLOID,"boolean","pg_catalog.bool",OG_LAKE_SDK_BOOL,"b",0,true,true},
    {INT2OID,"int16","pg_catalog.int2",OG_LAKE_SDK_INT16,"s",2,true,true},
    {INT4OID,"int32","pg_catalog.int4",OG_LAKE_SDK_INT32,"i",4,true,true},
    {INT8OID,"int64","pg_catalog.int8",OG_LAKE_SDK_INT64,"l",8,true,true},
    {TEXTOID,"varchar","pg_catalog.text",OG_LAKE_SDK_UTF8,"u",-1,true,true},
    {BYTEAOID,"blob","pg_catalog.bytea",OG_LAKE_SDK_BINARY,"z",-1,false,true},
    {UUIDOID,"uuid","pg_catalog.uuid",OG_LAKE_SDK_UUID,"w:16",16,true,true},
    {JSONOID,"json","pg_catalog.bytea",OG_LAKE_SDK_JSON,"u",-1,false,true},
    {NUMERICOID,"decimal","pg_catalog.numeric",OG_LAKE_SDK_DECIMAL128,"d:",16,true,true},
    {FLOAT4OID,"float32","pg_catalog.float4",OG_LAKE_SDK_FLOAT32,"f",4,false,true},
    {FLOAT8OID,"float64","pg_catalog.float8",OG_LAKE_SDK_FLOAT64,"g",8,false,true},
    {VECTOROID,"list","pg_catalog.text",OG_LAKE_SDK_FLOAT32_LIST,"+l",-2,false,true},
    /* Executor/journal datums use the host epoch; PostgreSQL inline carriers
     * store standard VARCHAR. Formats below describe Arrow transport only.
     * Temporal text bounds are unknown until qualified. */
    {DATEOID,"date","pg_catalog.varchar",OG_LAKE_SDK_DATE32,"tdD",4,false,true},
    {TIMESTAMPOID,"timestamp","pg_catalog.varchar",OG_LAKE_SDK_TIMESTAMP_US,"tsu:",8,false,true},
    {TIMESTAMPTZOID,"timestamptz","pg_catalog.varchar",OG_LAKE_SDK_TIMESTAMPTZ_US,"tsu:UTC",8,false,true}
};
static inline const LakeNativeType* LakeNativeTypeByOid(Oid oid)
{
    for(Size i=0;i<lengthof(lake_native_types);++i)
        if(lake_native_types[i].oid==oid)return &lake_native_types[i];
    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake scalar type is not admitted")));
    return NULL;
}
static inline const LakeNativeType* LakeNativeTypeBySdk(uint32 type)
{
    for(Size i=0;i<lengthof(lake_native_types);++i)
        if(lake_native_types[i].sdk_type==type)return &lake_native_types[i];
    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake Arrow scalar type is not admitted")));
    return NULL;
}
static inline const LakeNativeType* LakeNativeTypeByDuck(const char* type)
{
    if(strncmp(type,"decimal(",8)==0){(void)LakeNumericDuckTypmod(type);return LakeNativeTypeByOid(NUMERICOID);}
    /* DuckLake's explicit microsecond alias; other units remain unsupported. */
    if(strcmp(type,"timestamp_us")==0)return LakeNativeTypeByOid(TIMESTAMPOID);
    for(Size i=0;i<lengthof(lake_native_types);++i)
        if(strcmp(lake_native_types[i].duck_type,type)==0)return &lake_native_types[i];
    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake DuckLake scalar type is not admitted")));
    return NULL;
}
static inline void LakeNativeTypeRequireSdk(Oid oid,const OgLakeParquetSdkApi* api)
{
    if(oid==NUMERICOID){
        if(!(api->capabilities&OG_LAKE_SDK_DECIMAL128_VALUES))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake numeric requires SDK decimal128 values")));
        (void)LakeFileDecimalProvider();
    }
    if(oid==JSONOID&&!(api->capabilities&OG_LAKE_SDK_JSON_VALUES))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake JSON requires an explicit SDK JSON values capability")));
    if(oid==UUIDOID&&!(api->capabilities&OG_LAKE_SDK_UUID_VALUES))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake UUID requires an explicit SDK UUID16 values capability")));
    if(oid==BYTEAOID&&!(api->capabilities&OG_LAKE_SDK_BINARY_VALUES))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake BYTEA requires an explicit SDK binary values capability")));
    uint64 temporal=oid==DATEOID?OG_LAKE_SDK_DATE32_VALUES:
        oid==TIMESTAMPOID?OG_LAKE_SDK_TIMESTAMP_US_VALUES:
        oid==TIMESTAMPTZOID?OG_LAKE_SDK_TIMESTAMPTZ_US_VALUES:0;
    if(temporal&&(api->capabilities&temporal)!=temporal)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake temporal file access requires an explicit SDK type capability")));
    if(oid==INT2OID&&!(api->capabilities&OG_LAKE_SDK_INT16_VALUES))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMALLINT file access requires an explicit SDK capability")));
    if((oid==FLOAT4OID||oid==FLOAT8OID)&&!(api->capabilities&OG_LAKE_SDK_FLOAT_VALUES))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake floating file access requires an explicit SDK capability")));
    const uint64 vector_caps=OG_LAKE_SDK_FLOAT32_LIST_VALUES|OG_LAKE_SDK_LIST_FIELD_IDS;
    if(oid==VECTOROID&&(api->capabilities&vector_caps)!=vector_caps)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake vector files require explicit LIST values and element identity capabilities")));
}
static inline const char* LakeNativeDuckType(Oid oid,int32 typmod)
{ return oid==NUMERICOID?LakeNumericDuckType(typmod):LakeNativeTypeByOid(oid)->duck_type; }
static inline const char* LakeNativeSqlType(Oid oid,int32 typmod)
{ return oid==NUMERICOID?LakeNumericSqlType(typmod):LakeNativeTypeByOid(oid)->sql_type; }
static inline bool LakeNativeFlatTypmod(Oid oid,int32 typmod)
{ return oid==NUMERICOID?LakeNumericTypmod(typmod):typmod==-1; }
/* Consumer-side admission only; never called by a Stream worker or pure
 * registry/Arrow value lookup. Retained SQL<=0.19 must keep rejecting INT2. */
bool LakeSmallintVersionEnabled(void);
extern const OgLakeParquetSdkApi* LakeFileProvider(void);
static inline void LakeNativeTypeRequireBusinessVersion(Oid oid)
{
    if(oid==NUMERICOID){
        if(!LakeNumericBusinessVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake numeric requires exact installed extension 0.54")));
        LakeNativeTypeRequireSdk(oid,LakeFileProvider());
    }
    if(oid==JSONOID){
        if(!LakeJsonBusinessVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake JSON requires installed extension 0.53 or 0.54")));
        /* The existing native Arrow builder borrows server text bytes. Keep
         * this first JSON slice UTF8-only rather than labelling another
         * database encoding as Arrow UTF8. */
        if(GetDatabaseEncoding()!=PG_UTF8)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake JSON requires a UTF8 database")));
        LakeNativeTypeRequireSdk(oid,LakeFileProvider());
    }
    if(oid==UUIDOID){
        if(!LakeUuidBusinessVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake UUID requires installed extension 0.52, 0.53 or 0.54")));
        /* Also reject old providers for empty and inline-only business input. */
        LakeNativeTypeRequireSdk(oid,LakeFileProvider());
    }
    if(oid==INT2OID&&!LakeSmallintVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMALLINT requires extension 0.20")));
    if(oid==BYTEAOID){
        if(!LakeBinaryBusinessVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake BYTEA requires extension 0.43")));
        LakeNativeTypeRequireSdk(oid,LakeFileProvider());
    }
    if(!LakeNativeTypeIsTemporal(oid))return;
    if(!LakeTemporalBusinessVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake temporal business types are not enabled in this build")));
#ifndef HAVE_INT64_TIMESTAMP
    if(oid==TIMESTAMPOID||oid==TIMESTAMPTZOID)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake timestamp transport requires integer datetimes")));
#endif
    /* Reject an old SDK even for empty/inline-only temporal CREATE or BIND.
     * Actual file consumers independently repeat the type capability check. */
    LakeNativeTypeRequireSdk(oid,LakeFileProvider());
}
static inline void LakeNativeTypeRequireWrite(Oid oid)
{
    if(!LakeNativeTypeByOid(oid)->writable)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake vector writes are not yet admitted")));
}
static inline Oid LakeInlinePhysicalType(Oid oid)
{
    // Match the fixed DuckLake PostgreSQL metadata backend carrier schema.
    return LakeNativeTypeIsTemporal(oid)?VARCHAROID:oid==VECTOROID?TEXTOID:
        oid==JSONOID?BYTEAOID:oid;
}
static inline bool LakeIsWideningPromotion(Oid fromType, int32 fromTypmod, Oid toType, int32 toTypmod)
{
    if (fromType == toType) {
        if (fromType == NUMERICOID) {
            int p1 = 0, s1 = 0, p2 = 0, s2 = 0;
            if (!LakeNumericTypmod(fromTypmod, &p1, &s1) || !LakeNumericTypmod(toTypmod, &p2, &s2))
                return false;
            return s2 >= s1 && (p2 - s2) >= (p1 - s1) && (p2 > p1 || s2 > s1);
        }
        return false;
    }
    if (fromType == INT2OID && (toType == INT4OID || toType == INT8OID))
        return true;
    if (fromType == INT4OID && toType == INT8OID)
        return true;
    if (fromType == FLOAT4OID && toType == FLOAT8OID)
        return true;
    return false;
}
static inline bool LakeIsTypeCompatibleOrPromoted(Oid oldType, int32 oldTypmod, Oid newType, int32 newTypmod)
{
    return (oldType == newType && oldTypmod == newTypmod) ||
           LakeIsWideningPromotion(oldType, oldTypmod, newType, newTypmod);
}
static inline Datum LakeWidenDatum(Datum val, Oid fromType, int32 fromTypmod, Oid toType, int32 toTypmod)
{
    if (fromType == toType && fromTypmod == toTypmod)
        return val;
    if (fromType == INT2OID && toType == INT4OID)
        return Int32GetDatum((int32)DatumGetInt16(val));
    if (fromType == INT2OID && toType == INT8OID)
        return Int64GetDatum((int64)DatumGetInt16(val));
    if (fromType == INT4OID && toType == INT8OID)
        return Int64GetDatum((int64)DatumGetInt32(val));
    if (fromType == FLOAT4OID && toType == FLOAT8OID)
        return Float8GetDatum((double)DatumGetFloat4(val));
    if (fromType == NUMERICOID && toType == NUMERICOID &&
        LakeIsWideningPromotion(fromType, fromTypmod, toType, toTypmod))
        return DirectFunctionCall2(numeric, val, Int32GetDatum(toTypmod));
    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
        errmsg("native lake datum conversion from oid %u(%d) to oid %u(%d) is not a widening promotion",
            fromType, fromTypmod, toType, toTypmod)));
    return (Datum)0;
}
#endif
