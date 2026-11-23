/* Ordered scalar keys for the existing signed-int64 Parquet index pages.
 * Floating encoding follows DuckDB's MIT-licensed common/radix.hpp
 * EncodeDouble, adapted to signed ordering; SQL comparisons stay native.
 * Copyright 2018-2026 Stichting DuckDB Foundation.
 * DuckDB permission notice: licenses/duckdb_radix_LICENSE. */
#ifndef OG_LAKE_SCALAR_KEY_H
#define OG_LAKE_SCALAR_KEY_H
#include "catalog/pg_type.h"
#include "lake_version.h"
#include "catalog/pg_opfamily.h"
#include <cmath>
#include <cstring>
#include <limits>

static inline bool LakeScalarFloating(Oid type)
{return type==FLOAT4OID||type==FLOAT8OID;}
static inline bool LakeScalarIndexedType(Oid type)
{return type==INT4OID||type==INT8OID||LakeScalarFloating(type)||
    (type==INT2OID&&LakeInt2ScalarIndexVersionEnabled());}
/* SQL46 admits only INT2 against INT2. Existing cross-int paths retain
 * their original RHS domains and never narrow their arguments. */
static inline bool LakeScalarArgumentType(Oid left,Oid right)
{return left==INT2OID?right==INT2OID:LakeScalarFloating(left)?LakeScalarFloating(right):
    ((left==INT4OID||left==INT8OID)&&(right==INT2OID||right==INT4OID||right==INT8OID));}
static inline Oid LakeScalarFamily(Oid type,bool ustore=false)
{
    /* Native pg_opfamily.h float_ops, B-tree1970 / UB-tree6970. */
    if(LakeScalarFloating(type))return ustore?6970:1970;
    return (type==INT2OID||type==INT4OID||type==INT8OID)?(ustore?INTEGER_UBTREE_FAM_OID:INTEGER_BTREE_FAM_OID):InvalidOid;
}
static inline const char* LakeScalarClassName(Oid type)
{return type==INT2OID?"int2_ops":type==INT4OID?"int4_ops":type==INT8OID?"int8_ops":type==FLOAT4OID?"float4_ops":"float8_ops";}
static inline int LakeScalarAlgorithm(Oid type)
{return LakeScalarFloating(type)?2:1;}
static inline int64 LakeScalarOrderKey(Datum input,Oid type)
{
    if(type==INT2OID)return DatumGetInt16(input);
    if(type==INT4OID)return DatumGetInt32(input);
    if(type==INT8OID)return DatumGetInt64(input);
    if(!LakeScalarFloating(type))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake scalar ordered key type")));
    static_assert(sizeof(double)==sizeof(uint64)&&std::numeric_limits<double>::is_iec559,
        "lake floating scalar keys require IEEE binary64");
    double value=type==FLOAT4OID?(double)DatumGetFloat4(input):DatumGetFloat8(input);
    /* PG treats all NaNs as equal and above +infinity, and both zeros equal. */
    if(std::isnan(value))return INT64_MAX;
    if(value==0)return 0;
    if(std::isinf(value))return value<0?INT64_MIN:INT64_MAX-1;
    uint64 bits;std::memcpy(&bits,&value,sizeof(bits));
    const uint64 sign=UINT64CONST(1)<<63;
    bits=(bits&sign)?(~bits)^sign:bits;
    int64 result;std::memcpy(&result,&bits,sizeof(result));return result;
}
#endif
