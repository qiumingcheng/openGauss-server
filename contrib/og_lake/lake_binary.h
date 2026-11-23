/* Native BYTEA ownership and row bounds. No text codec or SDK ABI change. */
#ifndef OG_LAKE_BINARY_H
#define OG_LAKE_BINARY_H
#include "postgres.h"
#include "executor/tuptable.h"
#include "access/tuptoaster.h"
#include "utils/memutils.h"

static const Size LAKE_BINARY_ROW_BYTES=64UL*1024UL*1024UL;
static inline void LakeReserveBinaryBytes(Size payload,Size* rowBytes)
{
    if(rowBytes==NULL||*rowBytes>LAKE_BINARY_ROW_BYTES||
       payload>LAKE_BINARY_ROW_BYTES-VARHDRSZ||
       payload+VARHDRSZ>LAKE_BINARY_ROW_BYTES-*rowBytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake BYTEA row exceeds its 64 MiB byte limit")));
    *rowBytes+=payload+VARHDRSZ;
}
static inline Datum LakeCopyBinaryDatum(Datum value,Size* rowBytes)
{
    /* toast_raw_datum_size reads the uncompressed length for compressed,
     * external, indirect and huge TOAST pointers; the pointer size is not a
     * payload budget. Check before the potentially large detoast allocation. */
    Size raw=toast_raw_datum_size(value);
    if(raw<VARHDRSZ)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake BYTEA raw length")));
    LakeReserveBinaryBytes(raw-VARHDRSZ,rowBytes);
    struct varlena* copy=PG_DETOAST_DATUM_COPY(value);
    if(VARSIZE(copy)!=raw)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake BYTEA detoast length changed")));
    return PointerGetDatum(copy);
}
#endif
