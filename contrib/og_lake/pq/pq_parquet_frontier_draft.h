/* ISOLATED DRAFT: not linked into Native, not a ResourceOwner implementation.
 * Reuses qualified PQ sidecars for the existing DiskAnnSearchGraph distance
 * callback. It introduces no process/session/global cache. */
#ifndef OG_LAKE_PQ_PARQUET_FRONTIER_DRAFT_H
#define OG_LAKE_PQ_PARQUET_FRONTIER_DRAFT_H
#include "pq_parquet_format.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct PqParquetFrontier PqParquetFrontier;
enum PqParquetFallback {PQ_PARQUET_CONTINUE=0,PQ_PARQUET_RESTART_FULL_GRAPH=1,PQ_PARQUET_ABORT_QUERY=2};
/* Source and exact verified GRAPH root together identify dense node order.
 * This derived context is used during model training and sidecar writing too.
 * Host must authenticate both graph/PQ roots through a versioned descriptor;
 * current Native ANN format1 catalog has only a graph root and cannot simply
 * trust an unregistered sidecar's self-declared PQ root. */
int pq_parquet_graph_context(const PqParquetSpec*,const char* graph_context_hash,
    const char* verified_graph_manifest_hash,char output[65],OgLakeSdkError*);
/* Host prerequisite BEFORE calling open:
 * 1. Register a null cleanup slot under CurrentResourceOwner AND the owning
 *    query/candidate MemoryContext. Remember/Forget/SubCommit/SubAbort and
 *    BEFORE_LOCKS cleanup must all be real host operations.
 * 2. Pin the existing index/catalog/file lifetime for this verified graph.
 * 3. Hold interrupts only around this C boundary, then check host interrupts.
 * The host's registered slot receives *out only on success. The facade owns
 * its immutable Model/Codes and one query LUT; close is nonthrowing/idempotent.
 * No BLAS call, SQL ERROR/longjmp or borrowed callback survives a C call.
 * Lifetime is one active file/search. Release before moving to the next file;
 * no new unbounded cache or reuse via Relation.rd_indexcxt is introduced. */
int pq_parquet_frontier_open(const OgLakeParquetSdkApi*,const PqParquetSpec*,
    const OgLakeSdkLimits*,const PqParquetPaths*,const char* trusted_pq_root,
    const char* graph_context_hash,const char* verified_graph_manifest_hash,
    const float* normalized_or_original_query,uint64_t float_count,
    PqParquetFrontier**,OgLakeSdkError*);
/* Node IDs come from the SAME verified graph. Approximate distance is only
 * frontier state. It must not replace returned SQL original-vector distance. */
int pq_parquet_frontier_distance(PqParquetFrontier*,const OgLakeSdkLimits*,
    uint64_t node_id,float* distance,OgLakeSdkError*);
/* If a recoverable PQ error occurs after some frontier distances were used,
 * discard all those candidates and restart DiskAnnSearchGraph from entries:
 * queue.clear(), visited reset, visited_nodes/limit reset, PQ disabled. Never
 * return the mixed partial frontier. Underlying graph error falls through the
 * existing whole-source fallback; source/delete/permission errors propagate.
 * Cancellation always aborts: retrying as a full scan would ignore cancel.
 * C-returned MEMORY may fall back after freeing PQ resources; a host palloc
 * ERROR is NOT converted into an unconditional transparent retry. */
int pq_parquet_frontier_fallback(int status);
void pq_parquet_frontier_disable(PqParquetFrontier*);
void pq_parquet_frontier_close(PqParquetFrontier**);
#ifdef __cplusplus
}
#endif
#endif
