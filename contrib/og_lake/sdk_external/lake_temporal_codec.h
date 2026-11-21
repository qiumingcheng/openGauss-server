/* Internal common arithmetic for the finite temporal transport contract.
 * No host/Arrow objects, allocation, exceptions or callbacks cross this layer.
 * The caller must separately validate its host calendar/range and logical type.
 */
#ifndef OG_LAKE_TEMPORAL_CODEC_H
#define OG_LAKE_TEMPORAL_CODEC_H
#include <stdint.h>

#define OG_LAKE_TEMPORAL_EPOCH_DAYS INT32_C(10957)
#define OG_LAKE_TEMPORAL_EPOCH_MICROS INT64_C(946684800000000)

/* DuckLake/DuckDB's negative infinity is -MAX, unlike openGauss MIN.
 * The first codec slice excludes both sentinel conventions, not just MAX. */
static inline int OgLakeTemporalFiniteDate32(int32_t value)
{ return value != INT32_MIN && value != -INT32_MAX && value != INT32_MAX; }
static inline int OgLakeTemporalFiniteMicros(int64_t value)
{ return value != INT64_MIN && value != -INT64_MAX && value != INT64_MAX; }

/* On failure, output remains unchanged. The range check precedes arithmetic;
 * it must never rely on wrapping signed overflow or a floating conversion. */
static inline int OgLakeTemporalDateToUnix(int32_t host_days,int32_t* unix_days)
{
    if(!unix_days || host_days==INT32_MIN || host_days==INT32_MAX)return 0;
    int64_t value=(int64_t)host_days+OG_LAKE_TEMPORAL_EPOCH_DAYS;
    if(value<INT32_MIN || value>INT32_MAX || !OgLakeTemporalFiniteDate32((int32_t)value))return 0;
    *unix_days=(int32_t)value;return 1;
}
static inline int OgLakeTemporalDateFromUnix(int32_t unix_days,int32_t* host_days)
{
    if(!host_days || !OgLakeTemporalFiniteDate32(unix_days))return 0;
    int64_t value=(int64_t)unix_days-OG_LAKE_TEMPORAL_EPOCH_DAYS;
    if(value<=INT32_MIN || value>=INT32_MAX)return 0;
    *host_days=(int32_t)value;return 1;
}
static inline int OgLakeTemporalMicrosToUnix(int64_t host_micros,int64_t* unix_micros)
{
    if(!unix_micros || host_micros==INT64_MIN || host_micros==INT64_MAX ||
       host_micros>INT64_MAX-OG_LAKE_TEMPORAL_EPOCH_MICROS)return 0;
    int64_t value=host_micros+OG_LAKE_TEMPORAL_EPOCH_MICROS;
    if(!OgLakeTemporalFiniteMicros(value))return 0;
    *unix_micros=value;return 1;
}
static inline int OgLakeTemporalMicrosFromUnix(int64_t unix_micros,int64_t* host_micros)
{
    if(!host_micros || !OgLakeTemporalFiniteMicros(unix_micros) ||
       unix_micros<INT64_MIN+OG_LAKE_TEMPORAL_EPOCH_MICROS)return 0;
    int64_t value=unix_micros-OG_LAKE_TEMPORAL_EPOCH_MICROS;
    if(value==INT64_MIN || value==INT64_MAX)return 0;
    *host_micros=value;return 1;
}
#endif
