/* Independent checked PQ sidecar component. No Native catalog/cache wiring. */
#ifndef OG_LAKE_PQ_PARQUET_FORMAT_H
#define OG_LAKE_PQ_PARQUET_FORMAT_H
#include "pq_cpu.h"
#include "sdk/lake_parquet_sdk.h"
#ifdef __cplusplus
extern "C" {
#endif
#define PQ_PARQUET_VERSION 1u
#define PQ_PARQUET_MANIFEST_ROWS 20u
enum PqParquetFile {PQ_PARQUET_MODEL=1,PQ_PARQUET_CODES=2,PQ_PARQUET_MANIFEST=3};
typedef struct PqParquetSpec {
    uint32_t struct_size,version,dimension,chunks,metric;
    uint64_t training_rows,node_count;
    int64_t source_file_id;
    const char* context_hash;
    const char* source_version_hash;
    const char* schema_hash;
} PqParquetSpec;
typedef struct PqParquetPaths {
    uint32_t struct_size;
    const char* model_path;
    const char* codes_path;
    const char* manifest_path;
} PqParquetPaths;
typedef struct PqParquetWritten {
    uint32_t struct_size;
    uint64_t model_rows,codes_rows,manifest_rows;
    char manifest_hash[65],model_hash[65],codebook_hash[65],codes_hash[65],backend_hash[65];
} PqParquetWritten;
typedef struct PqParquetWriter PqParquetWriter;
typedef struct PqParquetReader PqParquetReader;
typedef struct PqParquetQuery PqParquetQuery;
const OgLakeSdkField* pq_parquet_fields(uint32_t file,int32_t* count);
/* Returns reserved adapter bytes, CPU allowance and PER-HANDLE SDK pool
 * allowance. Total reserves three simultaneous SDK writer pools; readers
 * close each footer before opening its reader. This covers participating
 * handles/buffers/pools, not the host query or allocator bookkeeping. */
int pq_parquet_budget(const PqParquetSpec*,const OgLakeSdkLimits*,
    uint64_t* adapter_bytes,uint64_t* cpu_bytes,uint64_t* sdk_bytes,OgLakeSdkError*);
/* Writers are distinct, caller-owned SDK candidates opened with the exact
 * fields above and the per-handle pool returned by pq_parquet_budget. Caller
 * also owns SDK callback lifetime until SDK close. This
 * wrapper retains no callback pointers: every append/finish borrows limits.
 * Open copies the immutable model. Append rows1..1024 in dense node_id order;
 * node_id is graph node order, neither logical row_id nor physical position.
 * Finish emits the manifest and roots, but does not finish/fsync SDK files:
 * caller must finish all three, verify row counts, and atomically publish.
 * Any append/finish error poisons this wrapper; caller must abort candidates.
 * Current frozen CPU API has no ranged export: the first writer encodes each
 * batch twice using the original MS code (Parquet batch + full-root builder).
 * This deliberate bounded CPU overhead is not a new quantization algorithm. */
int pq_parquet_writer_open(const OgLakeParquetSdkApi*,const PqParquetSpec*,
    const OgLakeSdkLimits*,const PqCpuModel*,OgLakeSdkWriter* model_file,
    OgLakeSdkWriter* codes_file,OgLakeSdkWriter* manifest_file,PqParquetWriter**,OgLakeSdkError*);
int pq_parquet_writer_append(PqParquetWriter*,const OgLakeSdkLimits*,
    uint64_t first_node,uint64_t rows,const float*,uint64_t float_count,OgLakeSdkError*);
int pq_parquet_writer_finish(PqParquetWriter*,const OgLakeSdkLimits*,PqParquetWritten*,OgLakeSdkError*);
void pq_parquet_writer_close(PqParquetWriter**);
/* expected_manifest_hash must come from the trusted catalog/context, not
 * solely from the files being opened. No reader/row escapes before manifest,
 * schema, full primitive roots and exact dense node coverage all validate. */
int pq_parquet_reader_open(const OgLakeParquetSdkApi*,const PqParquetSpec*,
    const OgLakeSdkLimits*,const PqParquetPaths*,const char* expected_manifest_hash,
    PqParquetReader**,OgLakeSdkError*);
int pq_parquet_reader_info(const PqParquetReader*,PqParquetWritten*,PqCpuInfo*,PqCpuInfo*,OgLakeSdkError*);
int pq_parquet_centers(const PqParquetReader*,float*,uint64_t,OgLakeSdkError*);
int pq_parquet_codes(const PqParquetReader*,uint8_t*,uint64_t,OgLakeSdkError*);
int pq_parquet_query_open(const PqParquetReader*,const OgLakeSdkLimits*,const float*,uint64_t,PqParquetQuery**,OgLakeSdkError*);
int pq_parquet_distances(const PqParquetReader*,const PqParquetQuery*,const OgLakeSdkLimits*,uint64_t,uint64_t,float*,uint64_t,OgLakeSdkError*);
void pq_parquet_query_close(PqParquetQuery**);
void pq_parquet_reader_close(PqParquetReader**);
#ifdef __cplusplus
}
#endif
#endif
