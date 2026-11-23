/* Native DiskANN over a private inline carrier, with lake visibility recheck. */
#ifndef OG_LAKE_INLINE_ANN_H
#define OG_LAKE_INLINE_ANN_H
#include "lake_index_private.h"

extern int LakeAnnMetric(const char* name);
extern Oid LakeAnnOperator(int metric);
extern void LakeAnnCheckMethod(void);
extern bool LakeIsAnnInlineIndex(Oid indexOid);
extern Relation LakeOpenTrustedAnnInlineIndex(Oid indexOid, Oid owner);
extern Relation LakeOpenInlineAnnIndex(Oid indexOid, const LakeInlineBinding* binding,
    AttrNumber column, int metric);
extern Oid LakeBuildInlineAnnIndex(LakeInlineBinding* binding, int column,
    const char* uuid, int metric);
extern bool LakeInlineAnnAvailable(const LakeInlineBinding* binding, const char* uuid,
    Oid indexOid, AttrNumber column, int metric, Snapshot businessSnapshot);
extern void LakeAddInlineAnnPaths(PlannerInfo* root, RelOptInfo* relation,
    Oid relationOid, double startup, double total, const struct LakeCostLedger* ledger=NULL);
#endif
