/* Bounded first Stream read slice; not a second executor or a thread pool. */
#ifndef OG_LAKE_STREAM_H
#define OG_LAKE_STREAM_H
#include "lake_txn_private.h"
#include "nodes/relation.h"
struct ExplainState;
struct LakeStreamWorker;
extern void LakeCheckStreamConstraintSnapshot(PlannedStmt*,Snapshot);
struct LakeScalarFileSources;
struct LakeAnnFileSources;
extern const LakeAnnFileSources* LakeStreamWorkerAnnSources(const LakeStreamWorker*);
extern int LakeStreamAnnPlanDop(PlannerInfo*,RelOptInfo*,const LakeInlineBinding*,int metric,List* filter,bool pq);
extern const LakeScalarFileSources* LakeStreamWorkerScalarSources(const LakeStreamWorker*);
extern bool LakeStreamWorkerVectorSmpQualified(const LakeStreamWorker*);
/* Only the plain full-scan caller may opt into the separate INT2 row
 * contract. Existing scalar/ANN callers keep their old INT2 refusal. */
extern int LakeStreamPlanDop(PlannerInfo*,RelOptInfo*,const LakeInlineBinding*,bool plainInt2Row=false);
extern LakeStreamWorker* LakeStreamOpenWorker(ForeignScanState*,int,LakeTxnTable**,char***);
extern void LakeStreamWorkerReady(LakeStreamWorker*);
extern void LakeStreamWorkerIndexReady(LakeStreamWorker*,int verified,int bitmap,int fallback);
extern void LakeStreamWorkerAnnReady(LakeStreamWorker*,int verified,int pq,int fallback,int64 candidates);
extern void LakeStreamWorkerFirstRow(LakeStreamWorker*);
extern void LakeStreamWorkerRow(LakeStreamWorker*);
extern void LakeStreamWorkerAddRows(LakeStreamWorker*,uint64 count);
extern void LakeStreamWorkerEnd(LakeStreamWorker*);
extern void LakeStreamExplain(const Plan*,ExplainState*);
#endif
