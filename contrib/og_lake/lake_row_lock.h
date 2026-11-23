/* Stable host heap row heads. These are synchronization, never lake data. */
#ifndef OG_LAKE_ROW_LOCK_H
#define OG_LAKE_ROW_LOCK_H
#include "lake_inline_private.h"

struct LakeTxnTable;
struct LakeNativePartitionIdentity;
/* Exact installed SQL version gate; old SQL retains its optimistic contract. */
extern bool LakeRowLocksEnabled(void);
/* Reclamation schema/worker is enabled only by explicit SQL0.18 migration. */
extern bool LakeRowHeadReclaimEnabled(void);
/* DDL binding preparation/removal; caller retains the host relation DDL lock.
 * Preparation follows publication of the canonical host binding. Removal
 * precedes table_binding removal; INTERNAL dependency removes the head heap. */
extern void LakePrepareRowLockBinding(const LakeInlineBinding* binding, Oid relationOid, Snapshot snapshot);
/* Private parent promotion only: same transaction, parent AEL and exact typed
 * binding are required. No SQL version/activation is assigned here. */
extern void LakePrepareNativePartitionRowLockBinding(const LakeInlineBinding* binding,
    const LakeNativePartitionIdentity* identity,Snapshot snapshot);
extern void LakeDropRowLockBinding(Oid relationOid, Snapshot snapshot);
/* Holds the native tuple-header lock until the owning (sub)transaction ends.
 * Performs no lake-version lookup, EPQ, dummy UPDATE or independent commit. */
extern void LakeLockLogicalRow(LakeTxnTable* table, int64 rowId);
/* Copied host identity: survives worker COMMIT and native VACUUM's internal
 * transactions. The row_id itself belongs to THIS one head, never the parent
 * alone. No relation/partition pointer or SPI Datum crosses a transaction. */
struct LakeRowHeadMaintenanceTarget {
    Oid relation_oid;
    Oid leaf_partition_oid;
    Oid head_oid;
    int partition_no;
    char binding_id[37];
    Oid server_oid;
    int64 table_id;
    char table_uuid[37];
};
/* Private future capability remains closed until a coordinated SQL contract
 * explicitly admits native-parent maintenance. Does not open SQL0.25. */
extern bool LakeNativeRowHeadMaintenanceEnabled(void);
/* Worker-only bounded metadata reclamation, never remote-file GC. */
struct LakeRowHeadReclaimResult {
    Oid head_oid;
    uint32 inspected;
    uint32 deleted;
    bool soft_budget_exceeded;
    bool pressure_raised;
    LakeRowHeadMaintenanceTarget native_target;
};
extern LakeRowHeadReclaimResult LakeReclaimLogicalRowHeads(Oid relationOid, int budgetMs);
extern LakeRowHeadReclaimResult LakeReclaimNativePartitionRowHeads(Oid parentOid,int budgetMs);
/* Top-level worker helper; native vacuum owns its transaction lifecycle. */
extern bool LakeVacuumLogicalRowHeads(Oid relationOid, Oid expectedHeadOid);
extern bool LakeVacuumNativePartitionRowHeads(const LakeRowHeadMaintenanceTarget* target);
/* Trusted ACL adapters: exact registry/canonical identity, not a name prefix. */
extern bool LakeIsRegisteredRowLockHead(Relation relation, Oid owner);
extern List* LakeRowLockHeadRelations(Oid owner);
/* Extension-script-only atomic handover before normal 0.17 owner validation. */
extern void LakeAdoptRowLockCatalogForUpgrade(void);
extern "C" Datum og_lake_row_lock_catalog_upgrade(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_reclaim_row_heads(PG_FUNCTION_ARGS);
#endif
