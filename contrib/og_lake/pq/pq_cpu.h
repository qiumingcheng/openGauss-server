/* Experimental serial CPU PQ component. No Native/SDK integration or production
 * thread safety is qualified. Model/Codes/Query are immutable after open;
 * the explicit CodesBuilder has a serialized mutable construction phase.
 * Every output handle slot must be initialized to NULL. */
#ifndef OG_LAKE_PQ_CPU_H
#define OG_LAKE_PQ_CPU_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
#define PQ_CPU_VERSION 1u
#define PQ_CPU_MAX_DIM 1536u
#define PQ_CPU_MAX_NODES 1048576u
#define PQ_CPU_MIN_TRAIN 256u
#define PQ_CPU_MAX_TRAIN 1024u
#define PQ_CPU_PERSIST_VERSION 1u
#define PQ_CPU_ALGORITHM_VERSION 1u
#define PQ_CPU_MAX_BATCH_ROWS 1024u
enum PqCpuStatus {PQ_CPU_OK=0,PQ_CPU_INVALID=1,PQ_CPU_MEMORY=2,PQ_CPU_CANCELLED=3,PQ_CPU_UNSUPPORTED=4,PQ_CPU_STATE=5};
enum PqCpuMetric {PQ_CPU_L2=1,PQ_CPU_NEGATIVE_IP=2,PQ_CPU_COSINE=3};
typedef struct PqCpuError {int32_t code;char message[256];} PqCpuError;
typedef int (*PqCpuShouldCancel)(void*);
typedef struct PqCpuLimits {
    uint32_t struct_size;
    uint64_t memory_bytes;
    /* Poll only, no throwing, host ERROR/longjmp or callback re-entry. Borrowed
     * for this call; limits/callback pointers are never kept in handles. */
    PqCpuShouldCancel should_cancel;
    void* cancel_context;
} PqCpuLimits;
typedef struct PqCpuTrainSpec {
    uint32_t struct_size,version,dimension,chunks,metric;
    uint64_t training_rows;
    const char* context_hash; /* Canonical64hex graph/source/definition context. */
} PqCpuTrainSpec;
typedef struct PqCpuEncodeSpec {
    uint32_t struct_size,version;
    int64_t source_file_id;
    uint64_t node_count; /* Dense immutable node_id range [0,node_count). */
    const char* context_hash;
    const char* model_hash;
} PqCpuEncodeSpec;
typedef struct PqCpuInfo {
    uint32_t struct_size,version,dimension,chunks,metric;
    uint64_t rows,resident_bytes;
    int64_t source_file_id;
    char context_hash[65],model_hash[65],payload_hash[65];
} PqCpuInfo;
typedef struct PqCpuModel PqCpuModel;
typedef struct PqCpuCodes PqCpuCodes;
typedef struct PqCpuQuery PqCpuQuery;
typedef struct PqCpuCodesBuilder PqCpuCodesBuilder;
/* K=256, plain fixed chunks, zero_mean=false, OPQ=false. d1..1536,
 * M>0, M<=d, d%M==0; training rows256..1024. Data are contiguous finite
 * float32 in a safe accumulation range |x|<=sqrt(FLT_MAX/(16384*d)). Cosine
 * inputs must already be nonzero unit vectors; this module never normalizes.
 * Budget includes participating resident handles and conservative component
 * workspace (including the original N*256 distance matrix). Borrowed input,
 * allocator bookkeeping and internal BLAS allocations are separate. This is
 * not a whole-native-query memory limit. Calls must remain serialized in an
 * isolated process until BLAS locking/concurrency qualification is complete. */
int pq_cpu_train(const PqCpuTrainSpec*,const PqCpuLimits*,const float* data,
    uint64_t float_count,PqCpuModel**,PqCpuError*);
int pq_cpu_encode(const PqCpuModel*,const PqCpuEncodeSpec*,const PqCpuLimits*,
    const float* data,uint64_t float_count,PqCpuCodes**,PqCpuError*);
int pq_cpu_query_open(const PqCpuModel*,const PqCpuLimits*,const float* vector,
    uint64_t float_count,PqCpuQuery**,PqCpuError*);
/* Returns PQ distance to reconstructed centroid vectors, NOT SQL exact
 * distance to original vectors. Cosine uses normalized inputs and negative-IP
 * frontier scores. Host rechecks/reranks with its full-vector SQL kernels.
 * Computes through temporary storage. On failure/cancellation the caller's
 * output is unchanged. Codes and LUT must share the exact model/context. */
int pq_cpu_distances(const PqCpuCodes*,const PqCpuQuery*,const PqCpuLimits*,
    uint64_t first_node,uint64_t count,float* output,uint64_t capacity,PqCpuError*);
int pq_cpu_model_info(const PqCpuModel*,PqCpuInfo*,PqCpuError*);
int pq_cpu_codes_info(const PqCpuCodes*,PqCpuInfo*,PqCpuError*);
/* Copies preserve portable identity; these are primitive values, not a wire
 * file format. No borrowed pointers into opaque C++ representations escape. */
int pq_cpu_model_centers(const PqCpuModel*,float* output,uint64_t capacity,PqCpuError*);
int pq_cpu_codes_export(const PqCpuCodes*,uint8_t* output,uint64_t capacity,PqCpuError*);
void pq_cpu_model_close(PqCpuModel**);
void pq_cpu_codes_close(PqCpuCodes**);
void pq_cpu_query_close(PqCpuQuery**);

/* Additive primitive persistence/range API. Existing v1 ABI fields and numeric
 * algorithms are unchanged. These structs are NOT a disk/wire layout: callers
 * must serialize explicit primitive fields in their checked file protocol.
 * Backend identity binds the locked numerical source and exact BLAS config;
 * the host must separately pin its actual dependency binary/build manifest. */
typedef struct PqCpuBackendInfo {
    uint32_t struct_size,component_version,persist_version,algorithm_version,centroids;
    char source_commit[41],algorithm_hash[65],backend_hash[65],blas_config[256];
} PqCpuBackendInfo;
typedef struct PqCpuModelRestoreSpec {
    uint32_t struct_size,version,component_version,algorithm_version;
    uint32_t dimension,chunks,centroids,metric;
    uint64_t training_rows;
    const char* context_hash;
    const char* codebook_hash;
    const char* model_hash;
    const char* backend_hash;
} PqCpuModelRestoreSpec;
enum PqCpuCodesInput {PQ_CPU_INPUT_FLOATS=1,PQ_CPU_INPUT_CODES=2};
typedef struct PqCpuCodesBuilderSpec {
    uint32_t struct_size,version,component_version,algorithm_version;
    uint32_t dimension,chunks,centroids,metric,input_kind;
    int64_t source_file_id;
    uint64_t node_count;
    const char* context_hash;
    const char* model_hash;
    const char* backend_hash;
    /* Required for restored raw codes, optional for newly encoded floats. */
    const char* expected_payload_hash;
} PqCpuCodesBuilderSpec;
enum PqCpuBuilderState {PQ_CPU_BUILDER_ACTIVE=1,PQ_CPU_BUILDER_FAILED=2,PQ_CPU_BUILDER_FINISHED=3};
typedef struct PqCpuCodesBuilderInfo {
    uint32_t struct_size,input_kind,state,max_batch_rows;
    uint64_t next_node,node_count,resident_bytes;
} PqCpuCodesBuilderInfo;
int pq_cpu_backend_info(PqCpuBackendInfo*,PqCpuError*);
/* Restores an immutable model from exactly 256*d original-order float32
 * centers. Recomputes codebook and model hashes; reconstructs all private
 * transposed/offset/zero-mean fields. Centers need not be unit cosine vectors. */
int pq_cpu_model_restore(const PqCpuModelRestoreSpec*,const PqCpuLimits*,
    const float* centers,uint64_t float_count,PqCpuModel**,PqCpuError*);
/* Open privately copies the model and allocates the final N*M code storage,
 * all charged to this operation's budget. The original model may be closed
 * after open. Builder calls must be serialized; close must not race any call.
 * Append requires first_node==next_node and 1..1024 rows. Input and limits are
 * borrowed for that call only. FLOATS and raw CODES modes cannot be mixed.
 * Every append/finish failure permanently fails the builder; close/reopen to
 * retry. No Codes handle/hash is published until a complete successful finish.
 * A finished builder is close-only (info remains available). Empty N is valid
 * and finishes without append. A cancelled/failed builder cannot resume. */
int pq_cpu_codes_builder_open(const PqCpuModel*,const PqCpuCodesBuilderSpec*,
    const PqCpuLimits*,PqCpuCodesBuilder**,PqCpuError*);
int pq_cpu_codes_builder_append(PqCpuCodesBuilder*,const PqCpuLimits*,
    uint64_t first_node,uint64_t rows,const float* data,uint64_t float_count,PqCpuError*);
int pq_cpu_codes_builder_append_raw(PqCpuCodesBuilder*,const PqCpuLimits*,
    uint64_t first_node,uint64_t rows,const uint8_t* codes,uint64_t byte_count,PqCpuError*);
int pq_cpu_codes_builder_finish(PqCpuCodesBuilder*,const PqCpuLimits*,PqCpuCodes**,PqCpuError*);
int pq_cpu_codes_builder_info(const PqCpuCodesBuilder*,PqCpuCodesBuilderInfo*,PqCpuError*);
void pq_cpu_codes_builder_close(PqCpuCodesBuilder**);
#ifdef __cplusplus
}
#endif
#endif
