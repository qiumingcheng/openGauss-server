/* Experimental private codec; no native query or persistent index is wired. */
#ifndef OG_LAKE_BITMAP_CODEC_H
#define OG_LAKE_BITMAP_CODEC_H
#include "sdk/lake_parquet_sdk.h"
#ifdef __cplusplus
extern "C" {
#endif
#define OG_LAKE_BITMAP_CODEC_VERSION 1u
#define OG_LAKE_BITMAP_MAX_ROWS 1048576u
struct OgLakeBitmap;
typedef struct OgLakeBitmapDecodeSpec {
    uint32_t struct_size;
    uint32_t format_version;
    uint64_t physical_rows;
    uint64_t expected_cardinality;
    uint64_t memory_bytes;
} OgLakeBitmapDecodeSpec;
/* Portable CRoaring bytes have no source authentication. The caller must bind
 * this exact payload to the immutable source/version and key in its verified
 * Parquet page/root manifest before treating positions as query candidates.
 * The borrowed bytes must remain unchanged for this entire synchronous call.
 * Uses the qualified4.7.2 run-preallocation variant recorded by the build;
 * changing that dependency requires length/OOM behavior requalification. */
int og_lake_bitmap_decode(const OgLakeBitmapDecodeSpec*,const void* bytes,size_t length,
    struct OgLakeBitmap**,OgLakeSdkError*);
uint64_t og_lake_bitmap_count(const struct OgLakeBitmap*);
int og_lake_bitmap_contains(const struct OgLakeBitmap*,uint32_t position);
/* No allocation; writes exactly count positions, in increasing order. All
 * bounds are checked before writing. NULL is accepted only for an empty set.
 * Positions alone do not authenticate their immutable source file/version. */
int og_lake_bitmap_export(const struct OgLakeBitmap*,uint32_t* positions,
    uint64_t capacity,OgLakeSdkError*);
void og_lake_bitmap_close(struct OgLakeBitmap**);
#ifdef __cplusplus
}
#endif
#endif
