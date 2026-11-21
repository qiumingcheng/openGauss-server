/* Internal ANALYZE sampler. No SDK ABI or persistent catalog additions. */
#ifndef OG_LAKE_SAMPLE_H
#define OG_LAKE_SAMPLE_H
#include "postgres.h"
#include "utils/relcache.h"
#include "storage/buf/block.h"
struct LakeTxnTable;
struct LakeInlineBinding;
extern int LakeAcquireFileSampleRows(Relation relation,LakeTxnTable* table,CommandId commandLimit,
    HeapTuple* rows,int target,double* totalrows,double* totaldeadrows,int elevel);
/* Borrowed views created with one pinned host statement snapshot and CID.
 * Up to two distinct same-catalog table domains share one physical sampling
 * probability and all per-call budgets. This is internal, not SQL admission. */
extern int LakeAcquireTableSampleRows(Relation relation,LakeTxnTable* const* tables,int tableCount,CommandId commandLimit,
    HeapTuple* rows,int target,double* totalrows,double* totaldeadrows,int elevel);
extern BlockNumber LakeAnalyzePhysicalPages(const LakeInlineBinding* binding);
#endif
