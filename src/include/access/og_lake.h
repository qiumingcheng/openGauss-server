/*
 * Native lake host boundary.  The implementation is statically linked into
 * gaussdb; SQL objects are installed separately by CREATE EXTENSION og_lake.
 */
#ifndef ACCESS_OG_LAKE_H
#define ACCESS_OG_LAKE_H

#include "postgres.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "storage/buf/block.h"

struct PlannedStmt;
struct Plan;
struct Query;
struct RelationData;
struct PlannerInfo;
/* No-op for ordinary queries outside the lake metadata scope. */
extern void LakeCheckQueryPermissions(Query* query);
/* Exact native FDW identity, used only by the narrow ANALYZE bridge. */
extern bool LakeIsNativeForeignRelation(RelationData* relation);
extern void LakeCheckMetadataParseRelation(RelationData* relation);
extern void LakeCheckMetadataExecutor(PlannedStmt* plan, struct SnapshotData* snapshot);
extern void LakeCheckNotNullPlanSnapshot(PlannedStmt* plan, struct SnapshotData* snapshot);
/* Closed native-parent bridge foundation; ordinary unmarked relations are no-op.
 * No user activation exists until the complete binding/dispatch protocol lands. */
extern bool LakeHasNativePartitionMarker(RelationData* relation);
/* Bounded manual parent ANALYZE only. Host retains native utility ACL/locks.
 * Opaque source state owns one captured host snapshot and two borrowed views. */
struct LakeNativeAnalyzeState;
struct HeapTupleData;
extern bool LakeNativePartitionAnalyzeVersionEnabled(void);
extern LakeNativeAnalyzeState* LakeNativeParentAnalyzeBegin(RelationData*,BlockNumber*);
extern int LakeNativeParentAnalyzeSample(LakeNativeAnalyzeState*,RelationData*,int,HeapTupleData**,int,double*,double*);
extern void LakeNativeParentAnalyzePublishSize(LakeNativeAnalyzeState*,RelationData*,BlockNumber,double);
extern void LakeNativeParentAnalyzeEnd(LakeNativeAnalyzeState*);

extern void LakeCheckNativePartitionExecutor(PlannedStmt* plan);
/* Runtime cached-opfusion fence; marked parent has no heap bypass path. */
extern void LakeRejectNativePartitionFusion(PlannedStmt* plan);
extern void LakeCheckNativePartitionExtensionDrop(void);
extern void LakeRejectNativePartitionRelOptions(RelationData* relation, List* options);
extern void LakeRejectNativePartitionDDL(RelationData* relation, const char* operation);
struct EPQState;
struct ModifyTableState;
struct EState;
struct ResultRelInfo;
struct TupleTableSlot;
extern void LakeNativePartitionExecInsert(EState*,ResultRelInfo*,TupleTableSlot*);
extern void LakeNativePartitionEndModify(ResultRelInfo*);
struct RangeTblEntry;
extern bool LakeNativePartitionAddUpdateTargets(Query*,RangeTblEntry*,RelationData*);
extern void LakeNativePartitionBeginMutation(ModifyTableState*,ResultRelInfo*,int,int);
extern TupleTableSlot* LakeNativePartitionExecMutation(EState*,ResultRelInfo*,TupleTableSlot*,TupleTableSlot*,CmdType);

/* Closed parent COPY FROM route. Gate checks do not perform SPI. The caller
 * captures CID/snapshot before metadata admission and keeps the native parser. */
extern void LakeNativePartitionRequireCopyFrom(RelationData*);
extern void LakeNativePartitionBeginCopyFrom(EState*,ResultRelInfo*,TupleTableSlot*,CommandId);

struct FdwRoutine;
struct ForeignScanState;
extern FdwRoutine* LakeNativePartitionRoutine(RelationData* relation);
extern bool LakeIsNativePartitionRoutine(const FdwRoutine* routine);
extern Oid LakeNativePartitionServer(RelationData* relation, struct SnapshotData* snapshot);
extern int LakeNativePartitionScanCount(ForeignScanState* state);
extern void LakeNativePartitionIteratorSelect(ForeignScanState* state,int selected);
class StreamNodeGroup;

/* Exact, closed symbol resolver used by both CREATE FUNCTION and fmgr. */
extern PGFunction LakeLookupBuiltinFunction(const char* name);

/* Session-owned host state; these APIs perform no SDK or catalog publication. */
extern bool LakeTryMaintenanceClaim(void);
extern bool LakeReleaseMaintenanceClaim(void);
extern bool LakeTryCatalogGuard(Oid serverOid, bool exclusive);
extern void LakeAcquireCatalogGuard(Oid serverOid);
extern bool LakeReleaseCatalogGuard(Oid serverOid);
extern char* LakeHostStateJson(void);
/* After native BEFORE_LOCKS/invalidations, before ordinary lock release.
 * No allocation, publication, SDK I/O or generic callback dispatch here.
 */
extern void LakeHostTransactionEnd(bool committed);
extern void LakeHostSubTransactionAbort(SubTransactionId subid);
/* Explicit, non-throwing candidate close/abort after ordinary resource and
 * lock release; no catalog publication or transaction-memory access. */
extern void LakeHostTransactionAfterLocks(bool committed);
extern void LakeHostSubTransactionAfterLocks(SubTransactionId subid);
extern bool LakeSetExtensionDropScope(bool active);
extern bool LakeInExtensionDropScope(void);
struct CreateForeignTableStmt;
struct ColumnDef;
/* Exact native CREATE/SQL49 only; returns an authenticated typed Const to the
 * unchanged host cookDefault path, normalizing SQL NULL before atthasdef. */
extern bool LakeNativePrepareCreateColumnDefault(CreateForeignTableStmt*,ColumnDef*,Oid,int32,Oid,Size*);
extern void LakeNativeBindForeignTable(Oid relationOid, CreateForeignTableStmt* statement);
extern bool LakeIsExternalFileRelation(RelationData* relation);
extern void LakeExternalBindForeignTable(Oid relationOid, CreateForeignTableStmt* statement);
extern void LakeNativeForgetForeignTable(Oid relationOid);
extern void LakeNativeForgetForeignServer(Oid serverOid);
/* Caller holds the host relation's AccessExclusiveLock; other FDWs are no-op. */
extern void LakeNativeRenameForeignTable(RelationData* relation, const char* newName, const char* newSchema);
/* Same host AccessExclusiveLock and utility transaction as pg_attribute. */
extern void LakeNativeRenameForeignColumn(RelationData* relation, const char* oldName, const char* newName,
    bool recursing, int expectedParents);

/* Native COPY keeps the host parser/ACLs/tuple slot. Initialize one captured
 * command ID/token; existing INSERT/EndForeignModify append and release the
 * same host journal/read view. This entry does not publish or commit. */
extern void LakeBeginCopyFrom(EState* estate, ResultRelInfo* info, CommandId commandId);

/* Native DML planner integration; Stream remains unavailable. */
extern void LakePrepareDmlLocatorPlaceholders(PlannerInfo* root, List* tlist);
extern void LakeValidateDmlLocatorPlan(PlannedStmt* plan);
/* Native lake only: lock the stable logical row, refresh the target and run
 * the host EPQ subplan before BEFORE triggers/generated columns consume it.
 * Ordinary relations and retained pre-0.17 lake contracts are unchanged. */
extern bool LakeRecheckModifyTarget(EState* estate, ResultRelInfo* info,
    TupleTableSlot** valueSlot, TupleTableSlot** planSlot,
    EPQState* epqstate, ModifyTableState* node);
extern void LakePrepareStreamBindings(PlannedStmt* plan, EState* estate, StreamNodeGroup* group);
/* Called by the top consumer only, after every Stream participant exited. */
extern void LakeStreamThreadStarted(StreamNodeGroup*,const Plan*,unsigned int);
extern void LakeReleaseStreamBindings(StreamNodeGroup* group);

/* Restricted ordinary native CREATE ADD COLUMN; opaque statement-owned plan. */
extern void* LakeNativePrepareAddColumn(RelationData* relation, struct List* commands);
extern void LakeNativeFinishAddColumn(void* plan);
extern void* LakeNativePrepareNullability(RelationData* relation, struct List* commands);
extern void LakeNativeFinishNullability(void* plan);
extern void* LakeNativePrepareDropColumn(RelationData* relation, struct List* commands);
extern void LakeNativeFinishDropColumn(void* plan);
extern void* LakeNativePrepareCurrentDefault(RelationData* relation, struct List* commands);
extern void LakeNativeFinishCurrentDefault(void* plan);
extern void* LakeNativePrepareAlterColumnType(RelationData* relation, struct List* commands);
extern void LakeNativeFinishAlterColumnType(void* plan);

struct CopyStmt;
struct tupleDesc;
struct TupleTableSlot;
extern void LakeCopyNormalizePath(CopyStmt* stmt);
extern bool LakeCopyOptionsHaveParquet(struct List* options);
extern int LakeCopyParseCompressionOption(const char* value);
extern bool LakeCopyAllowForeignTo(RelationData* rel);
extern bool LakeCopyAllowLakeFileFrom(void);
extern Node* LakeCopyBuildRelationSelectQuery(RelationData* rel, struct List* attnums);
extern FILE* LakeCopyOpenAuthorizedCsvFile(const char* filename, bool isCsv);
extern void* LakeCopyBeginParquetIn(RelationData* rel, const char* filename, struct List* attnumlist,
    MemoryContext copyContext);
extern bool LakeCopyNextParquetRow(void* opaque, Datum* values, bool* nulls);
extern void* LakeCopyBeginParquetOut(const char* filename, int compression, MemoryContext copyContext);
extern void LakeCopyStartParquetOut(void* opaque, struct tupleDesc* tupDesc);
extern void LakeCopyReceiveParquetSlot(void* opaque, struct TupleTableSlot* slot);
extern void LakeCopyFinishParquetOut(void* opaque);
extern void LakeCopyEndParquetState(void* opaque);
extern bool LakeCopyAllowJsonFormat(void);
extern bool LakeCopyOptionsHaveJson(struct List* options);
extern void* LakeCopyBeginJsonIn(RelationData* rel, const char* filename, struct List* attnumlist,
    MemoryContext copyContext);
extern void* LakeCopyBeginJsonInFd(RelationData* rel, int fd, struct List* attnumlist,
    MemoryContext copyContext, bool enforceNotNull);
extern bool LakeCopyNextJsonRow(void* opaque, Datum* values, bool* nulls);
extern void* LakeCopyBeginJsonOut(const char* filename, MemoryContext copyContext);
extern void LakeCopyStartJsonOut(void* opaque, struct tupleDesc* tupDesc);
extern void LakeCopyReceiveJsonSlot(void* opaque, struct TupleTableSlot* slot);
extern void LakeCopyFinishJsonOut(void* opaque);
extern void LakeCopyEndJsonState(void* opaque, bool isFrom);

#endif
