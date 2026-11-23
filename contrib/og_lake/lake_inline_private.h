/* Internal native-inline ABI, not the SDK ABI. */
#ifndef OG_LAKE_INLINE_PRIVATE_H
#define OG_LAKE_INLINE_PRIVATE_H
#include "postgres.h"
#include "lake_identity.h"
#include "lake_partition.h"
#include "fmgr.h"
#include "access/htup.h"
#include "access/tableam.h"
#include "foreign/fdwapi.h"
#include "lib/stringinfo.h"
#include "utils/snapshot.h"
#include "utils/uuid.h"

struct LakeMetaRows {
    uint32 count;
    TupleDesc descriptor;
    HeapTuple* rows;
};
struct LakeInlineColumn {
    int64 column_id;
    char* name; /* Current host/logical name, bound by stable column_id/attnum. */
    char* inline_name; /* Historical physical source name; NULL for a new carrier. */
    Oid type;
    AttrNumber output_attnum;
    int32 typmod;
    int64 element_column_id; /* -1 for flat scalars; explicit standard LIST child. */
    int64 introduced_snapshot; /* Earliest retained version of this stable field ID. */
    bool not_null; /* Logical constraint at this field descriptor's own schema snapshot. */
    bool initial_null_proven; /* Later-added immutable initial NULL, independent of later SET/DROP. */
    bool initial_value_proven; /* SQL39: the same introduction/birth proof, non-NULL initial value. */
    bool defaults_validated; /* Complete standard history decoded without evaluating SQL. */
    char* initial_literal; /* Canonical data; NULL pointer means initial SQL NULL. */
    Datum initial_value; /* Binding-owned typed initial value, distinct from current default. */
    char* current_literal; /* Canonical current data; NULL pointer means no host default. */
    Oid target_type; /* InvalidOid unless bound_snapshot adapts an older field type to host slot. */
    int32 target_typmod;
};
static inline Oid LakeColumnTargetType(const LakeInlineColumn* col)
{ return OidIsValid(col->target_type) ? col->target_type : col->type; }
static inline int32 LakeColumnTargetTypmod(const LakeInlineColumn* col)
{ return OidIsValid(col->target_type) ? col->target_typmod : col->typmod; }
static inline const char* LakeInlinePhysicalName(const LakeInlineColumn* column)
{ return column->inline_name == NULL ? column->name : column->inline_name; }

struct LakeDeleteFile {
    int64 file_id;
    int64 begin_snapshot;
    int64 record_count;
    int64 file_bytes;
    int64 partial_max; /* -1 when absent. */
    char* path;
};
struct LakeRetiredColumn {
    LakeInlineColumn column;
    int64 retired_snapshot; /* final end; no subsequent version of this stable ID */
    Oid collation; /* protected historical host mapping */
};

struct LakeDataFile {
    int64 file_id;
    int64 begin_snapshot;
    int64 row_id_start;
    int64 record_count;
    int64 file_bytes;
    int64 row_group_count; /* -1 if the standard catalog has no value. */
    char* path;
    bool row_id_start_null;
    int64 partial_max; /* -1 when absent. */
    LakeDeleteFile* deletes;
    int inline_delete_count;
    int64* inline_delete_positions;
    LakeIdentityPartitionFile partition; /* Immutable, normalized historical spec/value. */
};
/* One format-1.1 inline registration. Partition-only evolution leaves business
 * field IDs/types/order intact; each registration still owns its historical
 * physical names and partition specification. Never reinterpret an old source
 * using the newest table setting. All pointer storage belongs to the binding. */
struct LakeInlineSource {
    Oid relation_oid;
    char* table_name;
    int64 schema_version;
    int64 origin_snapshot;
    int column_count;
    char** column_names; /* Physical source order, not current host order. */
    LakeInlineColumn* columns; /* Complete physical schema, owned by this binding. */
    int* logical_to_source; /* Current compact index -> physical index; -1 proven initial value. */
    LakeIdentityPartitionSpec partition;
};
struct LakeInlineBinding {
    Oid relation_oid;
    Oid server_oid;
    Oid metadata_schema_oid;
    char* metadata_schema;
    char* binding_id;
    char* table_uuid;
    LakeTableIdentity identity;
    int64 snapshot_id;
    int64 schema_version;
    int column_count;
    LakeInlineColumn* columns;
    bool column_evolution; /* Protected CREATE admission; never inferred from source count. */
    bool rewrite_birth_bound; /* SQL0.60 capability captured with the protected column binding. */
    bool literal_defaults; /* Protected CREATE plus admitted literal-default capability. */
    bool alter_defaults; /* SQL41 current default may differ; initial remains immutable. */
    bool not_null_constraints; /* Protected CREATE, exact installed constraint capability. */
    bool has_added_columns;
    bool drop_columns;
    bool has_dropped_columns;
    bool alter_type_evolution;
    bool has_altered_types;
    int host_natts; /* includes dropped host slots; 0 only for dense transient definitions */
    int retired_count;
    LakeRetiredColumn* retired_columns; /* Includes old files even when only one current carrier exists. */
    bool historical_read; /* Immutable time_travel read; never overlays own journal. */
    bool bound_snapshot_read; /* Transaction-bound read-only snapshot mapped to host TupleDesc. */
    int64 visible_head_snapshot; /* Committed head under caller statement snapshot. */
    int future_count;
    LakeInlineColumn* future_columns; /* Fields introduced after historical snapshot_id in later-flushed files. */
    Size column_map_bytes; /* Checked aggregate descriptor allocation budget. */
    bool read_only;
    char* data_root;
    uint8 catalog_id[UUID_LEN];
    int64 binding_generation;
    bool has_catalog_identity;
    int file_count;
    LakeDataFile* files;
    Oid inline_oid; /* InvalidOid means a legitimately empty pure-inline table. */
    LakeIdentityPartitionSpec partition; /* Current table setting at binding.snapshot_id. */
    LakeIdentityPartitionSpec inline_partition; /* Setting at the source schema origin. */
    int64 inline_schema_version;
    int64 inline_origin_snapshot;
    bool inline_sources_loaded;
    int inline_source_count;
    LakeInlineSource* inline_sources;
    int inline_write_index; /* -1: latest per-table schema has no carrier yet. */
    int64 inline_write_schema_version;
    int64 inline_write_origin_snapshot;
};

static inline int64 LakePermanentTableId(const LakeInlineBinding* binding)
{
    if (binding->identity.kind != LAKE_TABLE_PERMANENT || binding->identity.key.permanent_table_id < 0)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("local lake object identity cannot be used as a permanent table ID")));
    return binding->identity.key.permanent_table_id;
}

extern bool LakeBindingHasBinary(const LakeInlineBinding* binding);
extern void LakeRejectBinaryAcceleration(const LakeInlineBinding* binding,const char* operation);
extern bool LakeBindingHasUuid(const LakeInlineBinding* binding);
extern bool LakeBindingHasJson(const LakeInlineBinding* binding);
extern bool LakeBindingHasNumeric(const LakeInlineBinding* binding);
extern bool LakeBindingHasTemporal(const LakeInlineBinding* binding);
extern bool LakeVectorType(Oid type, bool extended = false);
extern bool LakeVectorBindingSupported(const LakeInlineBinding* binding, bool extended);
extern void LakeRejectNumericAcceleration(const LakeInlineBinding* binding,const char* operation);
extern void LakeRejectJsonAcceleration(const LakeInlineBinding* binding,const char* operation);
extern void LakeRejectUuidAcceleration(const LakeInlineBinding* binding,const char* operation);
extern void LakeInlineRequireAdministrator(void);
extern Oid LakeMetadataOwner(void);
extern void LakeRequireAccessPolicy(void);
extern void LakeCheckMaskingAdmission(void);
extern void LakeValidatePrivateNamespace(Oid namespaceOid, Oid owner);
extern void LakeValidatePrivateRelation(Relation relation, Oid owner, bool extensionControl);
extern bool LakeTrustedForeignServer(Oid serverOid);
extern void LakeCheckExecutorPermissions(EState* estate);
struct LakeMetadataScope { Oid namespace_oid; Oid owner_oid; bool require_oid_binding; };
extern void LakeCheckMetadataObjectIdentity(Oid namespaceOid, Oid relationOid, Oid owner);
extern void LakeRegisterMetadataObjects(Oid namespaceOid);
/* Admin registration/owner handover: catalog_binding lock precedes policy/registry. */
extern void LakeLockCatalogRegistration(void);
/* ADD already owns host AX: fail 40001 instead of reversing the waiting order. */
extern void LakeTryLockCatalogRegistration(void);
extern bool LakeColumnEvolutionVersionEnabled(void);
extern bool LakeNativeProtectedCreate(Oid relationOid);
extern LakeMetadataScope* LakeGetMetadataScope(void);
extern LakeMetadataScope* LakeSetMetadataScope(LakeMetadataScope* scope);
extern void LakeRestoreMetadataScope(LakeMetadataScope* scope);
extern int LakeEnterFastValidationScope(void);
extern void LakeLeaveFastValidationScope(int previous_depth);
extern void LakeInvalidateFastValidationCache(void);
extern bool LakeFastVersionLookup(char** version_out);
extern void LakeFastVersionStore(const char* version);
extern bool LakeFastOwnerLookup(Oid* owner_out);
extern void LakeFastOwnerStore(Oid owner);
extern "C" Datum og_lake_configure_internal_owner(PG_FUNCTION_ARGS);
extern LakeMetaRows* LakeMetadataQuery(const char* sql, int nargs, Oid* argtypes,
    Datum* args, const char* nulls, Snapshot snapshot, bool readOnly, int expectedStatus,
    Oid expectedNamespace = InvalidOid);
extern bool LakeVerifyColumnTypeChain(Oid ns, int64 tableId, int64 columnId, int64 maxSnapshot,
    Oid expectedFinalType, int32 expectedFinalTypmod, Oid* earliestTypeOut, int32* earliestTypmodOut,
    bool* promotedOut, Snapshot snapshot);
extern Datum LakeMetadataValue(const LakeMetaRows* rows, uint32 row, int column,
    Oid expectedType, bool nullable = false, bool* isnull = NULL);
extern char* LakeMetadataText(const LakeMetaRows* rows, uint32 row, int column);
extern void LakeMetadataRequireCount(const LakeMetaRows* rows, uint32 count, const char* object);
extern char* LakeMetadataRelation(Oid schemaOid, const char* table);
extern void LakeCleanupAppendPart(StringInfo target, const char* part);
extern int LakeOpenAnchoredDirectory(const char* absDir);
extern Oid LakeInlineType(const char* name);
extern void LakeRequireWriteVersion(CmdType operation = CMD_INSERT);
/* COPY has an installed-SQL capability gate; retained SQL stays closed. */
extern void LakeRequireCopyVersion(void);
extern bool LakeVectorBatchVersionEnabled(void);
extern bool LakeDirectInsertVersionEnabled(void);
extern bool LakeRewriteVersionEnabled(void);
extern LakeInlineBinding* LakeLoadInlineBinding(Oid relationOid, Snapshot snapshot);
struct CreateForeignTableStmt;
struct LakeTxnTable;
extern void LakeNativeBindForeignTable(Oid relationOid, CreateForeignTableStmt* statement);
extern void LakeRequireBusinessCreateVersion(void);
extern void LakeRequireFlushVersion(void);
extern bool LakeHasFlushVersion(void);
extern void LakeRequireTableRenameVersion(void);
extern bool LakeHasColumnRenameVersion(void);
extern void LakeRequireColumnRenameVersion(void);
extern void LakeRequireFileReadVersion(void);
extern bool LakeFileVersionedReadsEnabled(void);
extern bool LakeFileModificationsEnabled(void);
/* Fixed primitive DATA or FILE_DELETE carrier, without a host dependency.
 * Caller publishes any standard registration in the same SQL transaction. */
extern Oid LakeCreatePrivateCarrier(LakeInlineBinding* binding, const char* name, Oid owner, bool fileDeletes);
extern void LakeLoadDataFiles(LakeInlineBinding* binding, Snapshot snapshot);
/* Bounded physical index input, not a business-scan binding. One visible
 * source at most; no delete marks or inline rows. Paths are lexically checked;
 * the index I/O phase must validate realpath outside publication locks. */
extern void LakeLoadIndexDataFile(LakeInlineBinding* binding,Snapshot snapshot,int64 fileId);
extern LakeInlineBinding* LakeLoadCatalogHeader(Oid serverOid, Snapshot snapshot);
extern LakeInlineBinding* LakeReloadAddColumnDefinition(const LakeInlineBinding* original,Snapshot snapshot);
extern LakeInlineBinding* LakeLoadCommittedInlineBinding(Oid relationOid, Snapshot snapshot);
extern LakeInlineBinding* LakeLoadHistoricalInlineBinding(Oid relationOid, int64 targetSnapshot,
    int64 targetSchemaVersion, int64 visibleHeadSnapshot, Snapshot snapshot, TupleDesc* historicalDescriptorOut);
extern LakeInlineBinding* LakeLoadBoundSnapshotInlineBinding(Oid relationOid, int64 targetSnapshot,
    int64 targetSchemaVersion, int64 visibleHeadSnapshot, Snapshot snapshot, bool definitionOnly = false);
struct Query;
struct PlannedStmt;
struct ForeignScanState;
extern void LakeCheckBoundSnapshotQuery(Query* query);
extern void LakeCheckBoundSnapshotPlan(PlannedStmt* plan);
extern void LakeCheckBoundSnapshotScanVars(ForeignScanState* node, const LakeInlineBinding* binding);
extern const LakeInlineColumn* LakeFindFutureColumn(const LakeInlineBinding* binding, int64 columnId);
extern LakeInlineBinding* LakeLoadIndexSourceBinding(Oid relationOid,int64 sourceFile,Snapshot snapshot);
extern void LakePublishHostBinding(LakeInlineBinding* binding, Oid relationOid, Snapshot snapshot);
extern void LakeNativeCreateForeignTable(Oid relationOid, CreateForeignTableStmt* statement, const char* lakeSchema);
extern void LakeNativeDropCreatedCarrier(LakeTxnTable* table);
extern void LakeRegisterCreatedCarrier(Oid namespaceOid, Oid relationOid);
extern void LakeNativeForgetForeignTable(Oid relationOid);
extern void LakeNativeForgetForeignServer(Oid serverOid);
extern void LakeNativeValidateOptions(List* options, Oid catalog);
extern void LakeInstallInlineCallbacks(FdwRoutine* routine);
/* Blocking ShareLock using lake_host.cpp's owned guard. */
extern void LakeAcquireCatalogGuard(Oid serverOid);
extern bool LakeTryCatalogGuard(Oid serverOid, bool exclusive);
extern bool LakeReleaseCatalogGuard(Oid serverOid);
extern "C" Datum og_lake_attach_catalog(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_create_catalog(PG_FUNCTION_ARGS);
/* Session-local committed snapshot tracking keyed by immutable catalog binding. */
struct LakeCommittedCatalogKey {
    Oid database_oid;
    Oid server_oid;
    Oid metadata_schema_oid;
    uint8 catalog_id[UUID_LEN];
    int64 binding_generation;
    const char* data_root;
};
extern bool LakeCommittedKeyFromBinding(const LakeInlineBinding* binding, LakeCommittedCatalogKey* key);
extern void* LakeHostPrepareImmediatePublish(const LakeInlineBinding* binding, SubTransactionId ownerSubid);
extern void LakeHostActivateImmediatePublish(void* slot, int64 snapshotId);
extern void LakeHostReserveDeferredPublish(const LakeInlineBinding* binding, SubTransactionId ownerSubid);
extern void LakeHostActivateDeferredPublish(const LakeInlineBinding* binding, int64 snapshotId);
extern void LakeHostStageCatalogReset(const LakeCommittedCatalogKey* key, SubTransactionId ownerSubid);
extern void LakeHostStageResetAll(SubTransactionId ownerSubid);
extern bool LakeHostLookupCommittedSnapshot(const LakeCommittedCatalogKey* key, int64* snapshotId);
#endif
