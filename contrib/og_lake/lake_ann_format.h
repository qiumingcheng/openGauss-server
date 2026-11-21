/* Portable graph storage only. Graph construction/search remain DiskANN's.
 * No Relation, heap TID, native index page, pointer or C struct is persisted. */
#ifndef OG_LAKE_ANN_FORMAT_H
#define OG_LAKE_ANN_FORMAT_H
#include "sdk/lake_parquet_sdk.h"
#ifdef __cplusplus
extern "C" {
#endif

#define OG_LAKE_ANN_FORMAT_VERSION 1u
#define OG_LAKE_ANN_PAGE_ROWS 64u
#define OG_LAKE_ANN_MAX_PAGES 16384u
#define OG_LAKE_ANN_MAX_DIM 1536u
#define OG_LAKE_ANN_MAX_DEGREE 96u
#define OG_LAKE_ANN_MAX_POSTINGS 10u
#define OG_LAKE_ANN_NODES_FILE "ann_nodes.parquet"
#define OG_LAKE_ANN_LOOKUP_FILE "ann_lookup.parquet"

typedef struct OgLakeAnnAddress {
    int64_t file_id;
    int64_t row_position;
    int64_t row_id;
    int64_t birth_snapshot;
} OgLakeAnnAddress;
typedef struct OgLakeAnnNode {
    uint32_t node_id; /* Dense segment-local identity, never a lake row_id. */
    uint32_t entry;
    uint32_t deleted; /* Excluded as a result; still usable for navigation. */
    uint32_t neighbor_count;
    uint32_t posting_count;
    const float* vector;
    const uint32_t* neighbors;
    const OgLakeAnnAddress* postings;
} OgLakeAnnNode;
typedef struct OgLakeAnnSpec {
    uint32_t struct_size;
    uint32_t format_version;
    uint32_t dimension;
    uint32_t metric; /* 1=L2, 2=inner product, 3=cosine. */
    uint32_t node_count;
    uint32_t entry_count;
    int64_t posting_count;
    const char* context_hash; /* 64 lower-case hex, host definition/source hash. */
} OgLakeAnnSpec;
typedef struct OgLakeAnnWritten {
    uint32_t struct_size;
    uint32_t page_count;
    char manifest_hash[65];
} OgLakeAnnWritten;
typedef struct OgLakeAnnWriter OgLakeAnnWriter;
typedef struct OgLakeAnnReader OgLakeAnnReader;

/* Host owns both SDK candidate handles and finish/abort/publication. Set the
 * nodes writer row_group_rows=PAGE_ROWS. Inputs must include every physical
 * source position; an ANN approximation is not a coverage proof. */
const OgLakeSdkField* og_lake_ann_fields(int lookup,int32_t* count);
int og_lake_ann_writer_open(const OgLakeParquetSdkApi*,const OgLakeSdkLimits*,
    const OgLakeAnnSpec*,OgLakeSdkWriter* nodes,OgLakeSdkWriter* lookup,
    OgLakeAnnWriter**,OgLakeSdkError*);
/* Append in dense node_id order, with native DiskANN topology and vector
 * representation. This API neither builds the graph nor normalizes vectors. */
int og_lake_ann_writer_append(OgLakeAnnWriter*,const OgLakeAnnNode*,int64_t count,OgLakeSdkError*);
int og_lake_ann_writer_finish(OgLakeAnnWriter*,OgLakeAnnWritten*,OgLakeSdkError*);
void og_lake_ann_writer_close(OgLakeAnnWriter**);

/* Lookup root binds every page hash to spec/context. Multiple node pages are
 * cached in bounded LRU order; a miss reads one row group and verifies it before
 * returning a node. Returned pointers survive until that page is evicted or
 * reader_close. SDK and component working budgets are separate and host-owned. */
int og_lake_ann_reader_open(const OgLakeParquetSdkApi*,const OgLakeSdkLimits*,
    const OgLakeAnnSpec*,const char* manifest_hash,const char* nodes_path,
    const char* lookup_path,OgLakeAnnReader**,OgLakeSdkError*);
/* Inspect structural metadata and open an authenticated reader in one pass. */
int og_lake_ann_reader_open_inspect(const OgLakeParquetSdkApi*,const OgLakeSdkLimits*,
    uint32_t dimension,uint32_t metric,const char* context_hash,const char* manifest_hash,
    const char* nodes_path,const char* lookup_path,OgLakeAnnSpec*,OgLakeAnnReader**,OgLakeSdkError*);
/* Derive bounded node/entry/posting populations from ordinary Parquet metadata
 * and the small page directory. These counts are untrusted until reader_open
 * also verifies the catalog's manifest_hash. No extra catalog columns/file are
 * needed. The returned context_hash borrows the supplied context pointer. */
int og_lake_ann_inspect(const OgLakeParquetSdkApi*,const OgLakeSdkLimits*,
    uint32_t dimension,uint32_t metric,const char* context_hash,const char* nodes_path,
    const char* lookup_path,OgLakeAnnSpec*,OgLakeSdkError*);
int og_lake_ann_reader_node(OgLakeAnnReader*,uint32_t node_id,const OgLakeAnnNode**,OgLakeSdkError*);
const OgLakeAnnSpec* og_lake_ann_reader_spec(const OgLakeAnnReader*);
/* Entry identities are copied from verified entry-bearing pages, and remain
 * valid until close. Deleted/zero-posting vertices may still guide traversal. */
int og_lake_ann_reader_entries(OgLakeAnnReader*,const uint32_t**,uint32_t* count,OgLakeSdkError*);
int og_lake_ann_verify(OgLakeAnnReader*,OgLakeSdkError*);
void og_lake_ann_reader_close(OgLakeAnnReader**);

#ifdef __cplusplus
}
#endif
#endif
