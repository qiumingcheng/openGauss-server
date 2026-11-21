/* Portable sorted-page adapter over the existing Parquet C ABI.
 * Catalogs, snapshots, authorization and candidate ownership stay in the host.
 * No database or C++ objects cross this interface. */
#ifndef OG_LAKE_SCALAR_FORMAT_H
#define OG_LAKE_SCALAR_FORMAT_H
#include "sdk/lake_parquet_sdk.h"
#ifdef __cplusplus
extern "C" {
#endif

#define OG_LAKE_SCALAR_FORMAT_VERSION 1u
#define OG_LAKE_SCALAR_PAGE_ROWS 4096u
#define OG_LAKE_SCALAR_DATA_FILE "page_data.parquet"
#define OG_LAKE_SCALAR_LOOKUP_FILE "page_lookup.parquet"

typedef struct OgLakeScalarEntry {
    int64_t key;
    int64_t file_id;
    int64_t row_position;
    int32_t is_null;
} OgLakeScalarEntry;
typedef struct OgLakeScalarPosition {
    int64_t file_id;
    int64_t row_position;
} OgLakeScalarPosition;
typedef struct OgLakeScalarWritten {
    uint32_t struct_size;
    int64_t num_rows;
    int64_t num_pages;
    char manifest_hash[65];
} OgLakeScalarWritten;
typedef struct OgLakeScalarWriter OgLakeScalarWriter;
typedef struct OgLakeScalarReader OgLakeScalarReader;
typedef struct OgLakeScalarResult OgLakeScalarResult;

/* Caller opens two session-owned candidates with these exact field layouts.
 * Each data append is one page; configure row_group_rows=PAGE_ROWS.
 * context_hash binds the format to host definition/source identities. */
const OgLakeSdkField* og_lake_scalar_fields(int lookup, int32_t* count);
int og_lake_scalar_writer_open(const OgLakeParquetSdkApi* api,
    const OgLakeSdkLimits* limits, uint32_t key_type, const char* context_hash,
    OgLakeSdkWriter* data, OgLakeSdkWriter* lookup, OgLakeScalarWriter** out,
    OgLakeSdkError* error);
/* Input is already ordered by native tuplesort: key NULLS LAST, file, position.
 * This component neither sorts business data nor owns the SDK writers. */
int og_lake_scalar_writer_append(OgLakeScalarWriter*, const OgLakeScalarEntry*,
    int64_t count, OgLakeSdkError*);
int og_lake_scalar_writer_finish(OgLakeScalarWriter*, OgLakeScalarWritten*,
    OgLakeSdkError*);
void og_lake_scalar_writer_close(OgLakeScalarWriter**);

int og_lake_scalar_reader_open(const OgLakeParquetSdkApi*,const OgLakeSdkLimits*,
    uint32_t key_type,const char* context_hash,const char* manifest_hash,
    const char* data_path,const char* lookup_path,OgLakeScalarReader**,OgLakeSdkError*);
/* Administrative validation of every leaf, without materializing all postings.
 * Bounded directory and one-page working memory; cancellation still applies. */
int og_lake_scalar_verify(OgLakeScalarReader*,OgLakeSdkError*);
/* Comparison values are lossless signed integers, with NULL handled explicitly.
 * IN values may repeat; NULL elements are removed by the host. */
enum OgLakeScalarPredicate {
    OG_LAKE_SCALAR_LT=1, OG_LAKE_SCALAR_LE=2, OG_LAKE_SCALAR_EQ=3,
    OG_LAKE_SCALAR_GE=4, OG_LAKE_SCALAR_GT=5,
    OG_LAKE_SCALAR_IS_NULL=6, OG_LAKE_SCALAR_NOT_NULL=7, OG_LAKE_SCALAR_IN=8
};
int og_lake_scalar_select(OgLakeScalarReader*,int predicate,int argument_is_null,
    const int64_t* values,int64_t count,OgLakeScalarResult**,OgLakeSdkError*);
int64_t og_lake_scalar_result_count(const OgLakeScalarResult*);
const OgLakeScalarPosition* og_lake_scalar_result_positions(const OgLakeScalarResult*);
void og_lake_scalar_result_close(OgLakeScalarResult**);
void og_lake_scalar_reader_close(OgLakeScalarReader**);

#ifdef __cplusplus
}
#endif
#endif
