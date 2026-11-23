/* Experimental value -> CRoaring dictionary in standard Parquet. No native
 * catalog/query integration is implied. The host owns candidate publication. */
#ifndef OG_LAKE_BITMAP_FORMAT_H
#define OG_LAKE_BITMAP_FORMAT_H
#include "lake_scalar_format.h"
#include "lake_bitmap_codec.h"
#ifdef __cplusplus
extern "C" {
#endif
#define OG_LAKE_BITMAP_FORMAT_VERSION 1u
#define OG_LAKE_BITMAP_MAX_KEYS 65536u
#define OG_LAKE_BITMAP_DATA_FILE "bitmap_data.parquet"
typedef struct OgLakeBitmapFileSpec {
    uint32_t struct_size;
    uint32_t format_version;
    uint32_t key_type; /* INT32, or signed INT64 encoded key (also float keys). */
    int64_t file_id;
    uint64_t physical_rows;
    const char* context_hash; /* Canonical lowercase SHA256; copied on open. */
} OgLakeBitmapFileSpec;
typedef struct OgLakeBitmapWritten {
    uint32_t struct_size;
    uint64_t physical_source_rows;
    uint64_t dictionary_rows;
    char manifest_hash[65];
} OgLakeBitmapWritten;
typedef struct OgLakeBitmapWriter OgLakeBitmapWriter;
typedef struct OgLakeBitmapReader OgLakeBitmapReader;
typedef struct OgLakeBitmapResult OgLakeBitmapResult;
const OgLakeSdkField* og_lake_bitmap_fields(int32_t* count);
/* One immutable source per artifact. Input is key NULLS LAST, then position.
 * Every physical position [0,N) must occur exactly once, including NULL keys.
 * Prototype limits: N<=1048576, dictionary rows (including NULL)<=65536.
 * SDK writer is borrowed; caller finishes/fsyncs it and compares its Parquet
 * row count to dictionary_rows, NEVER to physical_source_rows. */
int og_lake_bitmap_writer_open(const OgLakeParquetSdkApi*,const OgLakeSdkLimits*,
    const OgLakeBitmapFileSpec*,OgLakeSdkWriter*,OgLakeBitmapWriter**,OgLakeSdkError*);
int og_lake_bitmap_writer_append(OgLakeBitmapWriter*,const OgLakeScalarEntry*,int64_t,OgLakeSdkError*);
int og_lake_bitmap_writer_finish(OgLakeBitmapWriter*,OgLakeBitmapWritten*,OgLakeSdkError*);
void og_lake_bitmap_writer_close(OgLakeBitmapWriter**);
/* Loads a bounded dictionary. Before returning a handle, checks the full
 * context-bound payload hash, schema/order, every Roaring payload, disjointness
 * and complete [0,N) coverage. No query result precedes this validation. */
int og_lake_bitmap_reader_open(const OgLakeParquetSdkApi*,const OgLakeSdkLimits*,
    const OgLakeBitmapFileSpec*,const char* manifest_hash,const char* path,
    OgLakeBitmapReader**,OgLakeSdkError*);
int og_lake_bitmap_select(OgLakeBitmapReader*,int predicate,int argument_is_null,
    const int64_t* values,int64_t count,OgLakeBitmapResult**,OgLakeSdkError*);
int64_t og_lake_bitmap_result_count(const OgLakeBitmapResult*);
const OgLakeScalarPosition* og_lake_bitmap_result_positions(const OgLakeBitmapResult*);
void og_lake_bitmap_result_close(OgLakeBitmapResult**);
void og_lake_bitmap_reader_close(OgLakeBitmapReader**);
#ifdef __cplusplus
}
#endif
#endif
