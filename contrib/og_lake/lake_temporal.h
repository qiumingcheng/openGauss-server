/* Unopened native temporal boundary. No SQL type is admitted by this header.
 * Executor/journal datums retain the host epoch; physical inline carriers use
 * standard VARCHAR. Only file/Arrow transport changes epoch.
 * Timestamp and TimestampTz share arithmetic, but their wire logical types and
 * timezone annotations remain distinct. Never interpret a plain int8 as time.
 */
#ifndef OG_LAKE_TEMPORAL_H
#define OG_LAKE_TEMPORAL_H
#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "sdk/lake_temporal_codec.h"

static_assert(POSTGRES_EPOCH_JDATE-UNIX_EPOCH_JDATE==OG_LAKE_TEMPORAL_EPOCH_DAYS,
    "native temporal day epoch differs from the shared SDK contract");
static_assert((int64)OG_LAKE_TEMPORAL_EPOCH_DAYS*INT64CONST(86400000000)==OG_LAKE_TEMPORAL_EPOCH_MICROS,
    "native temporal microsecond epoch differs from the shared SDK contract");

static inline bool LakeTemporalDateInRange(DateADT value)
{
    int64 julian=(int64)value+POSTGRES_EPOCH_JDATE;
    return !DATE_NOT_FINITE(value)&&julian>=DATETIME_MIN_JULIAN&&julian<DATE_END_JULIAN;
}
static inline int32 LakeTemporalDateToUnix(DateADT value)
{
    int32_t output=0;
    if(!LakeTemporalDateInRange(value)||!OgLakeTemporalDateToUnix(value,&output))
        ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake date is outside the finite temporal transport range")));
    return output;
}
static inline DateADT LakeTemporalDateFromUnix(int32 value)
{
    int32_t output=0;
    if(!OgLakeTemporalDateFromUnix(value,&output)||!LakeTemporalDateInRange(output))
        ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake file date is outside the finite host range")));
    return (DateADT)output;
}
static inline int64 LakeTemporalTimestampToUnix(Timestamp value)
{
#ifdef HAVE_INT64_TIMESTAMP
    int64_t output=0;
    if(TIMESTAMP_NOT_FINITE(value)||!IS_VALID_TIMESTAMP(value)||!OgLakeTemporalMicrosToUnix(value,&output))
        ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake timestamp is outside the finite temporal transport range")));
    return output;
#else
    (void)value;
    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake timestamp transport requires integer datetimes")));
    return 0;
#endif
}
static inline Timestamp LakeTemporalTimestampFromUnix(int64 value)
{
#ifdef HAVE_INT64_TIMESTAMP
    int64_t output=0;
    if(!OgLakeTemporalMicrosFromUnix(value,&output)||TIMESTAMP_NOT_FINITE(output)||!IS_VALID_TIMESTAMP(output))
        ereport(ERROR,(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),errmsg("native lake file timestamp is outside the finite host range")));
    return (Timestamp)output;
#else
    (void)value;
    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake timestamp transport requires integer datetimes")));
    return 0;
#endif
}
/* Validate before staging an inline after-image and after reading externally
 * supplied inline rows. Discard the converted value: inline stores the native
 * Datum, not Unix transport bits. NULL is handled by the caller. */
static inline void LakeTemporalValidateDatum(Oid type,Datum value)
{
    if(type==DATEOID)(void)LakeTemporalDateToUnix(DatumGetDateADT(value));
    else if(type==TIMESTAMPOID)(void)LakeTemporalTimestampToUnix(DatumGetTimestamp(value));
    else if(type==TIMESTAMPTZOID)(void)LakeTemporalTimestampToUnix(DatumGetTimestampTz(value));
}
#endif
