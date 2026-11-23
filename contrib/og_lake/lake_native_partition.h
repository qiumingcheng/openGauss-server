/* Native OG parent/pg_partition identities; no DuckLake durable format here.
 * Foundation only. No SQL binder, root activation or FDW dispatch is exposed. */
#ifndef OG_LAKE_NATIVE_PARTITION_H
#define OG_LAKE_NATIVE_PARTITION_H
#include "postgres.h"
#include "utils/relcache.h"
#include "executor/tuptable.h"
#include "utils/snapshot.h"
#include "foreign/fdwapi.h"

/* SQL23: bounded two-leaf native RANGE read and positive-inline INSERT. */
#define OG_LAKE_NATIVE_PARTITION_SQL_VERSION "0.23"
/* This gate grants INSERT only. UPDATE/DELETE/COPY remain unavailable. */
#define OG_LAKE_NATIVE_PARTITION_WRITE_SQL_VERSION "0.23"
/* SQL24 inherits SQL23 and adds bounded UPDATE/DELETE, COPY and table proofs. */
#define OG_LAKE_NATIVE_PARTITION_MUTATION_SQL_VERSION "0.24"
/* COPY is the same SQL24 contract; SQL23 does not inherit it. */
#define OG_LAKE_NATIVE_PARTITION_COPY_SQL_VERSION "0.24"
/* SQL25 inherits the complete SQL24 contract and adds whole-parent manual ANALYZE. */
#define OG_LAKE_NATIVE_PARTITION_ANALYZE_SQL_VERSION "0.25"
/* SQL26 inherits SQL25 and adds typed native-parent row-head maintenance. */
#define OG_LAKE_NATIVE_PARTITION_ROW_HEAD_SQL_VERSION "0.26"
/* One exact installed-SQL contract table. Ordinary inherited gates use the
 * same pure known-future predicate; no numeric >= or runtime-version shortcut.
 * Resource/worker descriptors consume copied booleans, never SPI here. */
enum LakeNativeParentCapability {
    LAKE_NATIVE_PARENT_READ=1U,
    LAKE_NATIVE_PARENT_INSERT=2U,
    LAKE_NATIVE_PARENT_MUTATION=4U,
    LAKE_NATIVE_PARENT_COPY=8U,
    LAKE_NATIVE_PARENT_ANALYZE=16U,
    LAKE_NATIVE_PARENT_ROW_HEAD_MAINTENANCE=32U,
    LAKE_NATIVE_PARENT_EXPANDED=64U
};
extern uint32 LakeNativePartitionContractCapabilities(const char* installedVersion);
extern bool LakeNativeFutureContractEnabled(const char* installedVersion);
struct LakeInlineBinding;
struct LakeTxnTable;
struct LakeTxnState;
struct ModifyTableState;
struct ResultRelInfo;
struct EState;
struct EPQState;


static const int LAKE_NATIVE_RANGE_LEAF_LIMIT = 64;
extern int LakeNativePartitionLeafLimit(void);
struct LakeNativePartitionIdentity {
    Oid parent_relid;
    Oid leaf_partition_oid;
    int partition_no; /* Re-resolution fence, not a lake row ID or catalog ID. */
};
struct LakeNativeRangeLayout {
    Oid parent_relid;
    char strategy;
    AttrNumber key_attribute;
    int leaf_count;
    LakeNativePartitionIdentity leaves[LAKE_NATIVE_RANGE_LEAF_LIMIT];
};
/* Callers own the parent Relation/normal ACL and relation locks. Return only
 * copied POD; neither rd_att, partMap, Partition nor a dummy Relation escapes. */
extern LakeNativeRangeLayout LakeNativeRangeDescribe(Relation parent);
/* Activation preflight only: caller holds parent AccessExclusiveLock.
 * SnapshotAny is deliberately stricter than current-visible-empty: this first
 * slice refuses physical historic/dead tuples, never trusts reltuples=0. */
extern void LakeNativeRangeRequirePhysicallyEmpty(Relation parent, const LakeNativeRangeLayout* layout);
/* Reuses OG heapTupleGetPartitionOid and PartitionOpenWithPartitionno. Caller
 * invokes after host BEFORE/generated/constraint processing, before leaf FDW.
 * It does not append/publish, and never obtains a leaf from a PARAM_EXEC. */
extern LakeNativePartitionIdentity LakeNativeRangeRoute(Relation parent,
    const LakeNativeRangeLayout* layout, TupleTableSlot* slot);
extern bool LakeNativePartitionIdentityEqual(const LakeNativePartitionIdentity*,const LakeNativePartitionIdentity*);
struct LakeNativeLeafReadBinding {
    LakeNativePartitionIdentity host;
    LakeInlineBinding* binding;
    int64 certified_snapshot;
};
extern bool LakeNativePartitionVersionEnabled(void);
extern bool LakeNativePartitionWriteVersionEnabled(void);
extern bool LakeNativePartitionMutationVersionEnabled(void);
extern void LakeNativePartitionBeginMutation(ModifyTableState*,ResultRelInfo*,int,int);
extern bool LakeNativePartitionRecheckMutation(EState*,ResultRelInfo*,TupleTableSlot**,TupleTableSlot**,EPQState*,ModifyTableState*);
extern TupleTableSlot* LakeNativePartitionExecMutation(EState*,ResultRelInfo*,TupleTableSlot*,TupleTableSlot*,CmdType);
extern bool LakeNativePartitionEndMutation(ResultRelInfo*);
extern bool LakeNativePartitionCopyFromVersionEnabled(void);
extern bool LakeNativePartitionAnalyzeVersionEnabled(void);
/* Bind/controlled activation only, parent AX required; never called by ANALYZE. */
extern void LakeNativeParentInvalidateStatistics(Relation);
/* Extension upgrade only: all existing real parents are validated and reset
 * in one host transaction before a future ANALYZE capability is published. */
extern "C" Datum og_lake_native_parent_statistics_upgrade(PG_FUNCTION_ARGS);
extern void LakeNativePartitionCheckInsertTarget(Relation,Snapshot,bool requireWritable=true);
extern "C" Datum og_lake_enable_native_partition_insert(PG_FUNCTION_ARGS);
/* Internal publisher-only entry: exact state phase and typed canonical journal
 * are checked; no public callback guard/session flag is weakened. */
extern LakeInlineBinding* LakeNativePartitionPublicationBinding(LakeTxnState*,const LakeTxnTable*,Snapshot,int64 claimedSnapshot=-1);
struct LakeNativePublicationCertificates;
/* Metadata-only, attempt-owned complete sibling tokens. NULL retains SQL23. */
extern LakeNativePublicationCertificates* LakeNativePreparePublicationCertificates(LakeTxnState*,LakeTxnTable**,int,Snapshot);
/* After winning the snapshot PK: compare fresh private control tokens.
 * false rolls back that attempt; final exact CAS closes any later race. */
extern bool LakeNativePublicationCertificatesStillCurrent(LakeTxnState*,const LakeNativePublicationCertificates*,Snapshot);
extern void LakeNativePartitionRefreshPublished(LakeTxnState*,LakeTxnTable**,int,Snapshot,int64,
    const LakeNativePublicationCertificates* certificates=NULL);
extern void LakeRequireNativePartitionVersion(void);
extern LakeInlineBinding* LakeReadNativePartitionSource(Oid server,int64 tableId,Snapshot snapshot,
    Relation parent,const char* bindingId=NULL,int64 admittedSnapshot=-1);
extern LakeNativeLeafReadBinding LakeLoadNativePartitionLeaf(Relation parent,
    const LakeNativePartitionIdentity* leaf,Snapshot snapshot);
extern Oid LakeNativePartitionServer(Relation parent,Snapshot snapshot);
extern FdwRoutine* LakeNativePartitionRoutine(Relation parent);
extern bool LakeIsNativePartitionRoutine(const FdwRoutine* routine);
extern int LakeNativePartitionScanCount(ForeignScanState* state);
extern void LakeNativePartitionIteratorSelect(ForeignScanState* state,int selected);
extern LakeTxnTable* LakeTxnCreateQueryReadView(LakeInlineBinding*,Snapshot,TupleDesc);
/* Independent per-leaf canonical journal, same catalog, ordinary statement RC. */
extern LakeTxnTable* LakeTxnReadNativePartitionTable(Relation,const LakeNativePartitionIdentity*,Snapshot);
extern LakeTxnTable* LakeTxnReadStatementPartitionTable(Relation,const LakeNativePartitionIdentity*,Snapshot);
/* Caller has run native generated/constraints processing; append owns no
 * independent transaction. UPDATE caller MUST lock+latest-refetch+EPQ first. */
extern void LakeNativePartitionRouteAppend(Relation,TupleTableSlot*,Snapshot,CommandId,uint64);
extern void LakeNativePartitionRouteUpdated(Relation,LakeTxnTable*,const struct LakeRowLocator*,
    TupleTableSlot*,CommandId,uint64);
extern "C" Datum og_lake_bind_native_partition_readonly(PG_FUNCTION_ARGS);
#endif
