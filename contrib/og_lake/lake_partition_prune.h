/* Query-owned, conservative file selection; never changes the source binding. */
#ifndef OG_LAKE_PARTITION_PRUNE_H
#define OG_LAKE_PARTITION_PRUNE_H
#include "postgres.h"
#include "foreign/fdwapi.h"
struct LakeInlineBinding;
struct LakePartitionPrune;
/* Installed SQL29 ordinary serial SELECT capability. */
extern bool LakePartitionPruneVersionEnabled(void);
extern LakePartitionPrune* LakePartitionPruneCreate(ForeignScanState* node,
    const LakeInlineBinding* binding);
/* Invoke at first Iterate, including after ReScan, when correlated parameters
 * have been assigned. Return mask uses original binding->files indexes.
 * true means proven irrelevant. Caller copies the mask before next refresh. */
extern const bool* LakePartitionPruneRefresh(LakePartitionPrune*, ExprContext*);
extern int LakePartitionPruneSkipped(const LakePartitionPrune*);
#endif
