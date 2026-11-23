/* Native host transaction ABI; never persisted in DuckLake metadata. */
#ifndef OG_LAKE_TXN_PRIVATE_H
#define OG_LAKE_TXN_PRIVATE_H
#include "lake_inline_private.h"
#include "lake_locator.h"
#include "utils/hsearch.h"
#include "access/xact.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

/* Fixed reference common/index.hpp treats IDs >= 10^18 as transaction-local.
 * This is a reference compatibility restriction, not a format-wide limit.
 * An exclusive end of 10^18 is valid; no emitted permanent ID reaches it. */
static const int64 LAKE_REFERENCE_LOCAL_ID_START = INT64CONST(1000000000000000000);
static const Size LAKE_INLINE_TXN_MEMORY_LIMIT = 64 * 1024 * 1024;

struct LakeTxnIdentity;
struct LakeOriginKey { uint64 row_id; uint64 begin_snapshot; };
struct LakeOriginEntry { LakeOriginKey key; LakeTxnIdentity* identity; };
struct LakeLogicalOriginEntry { uint64 row_id; LakeTxnIdentity* identity; };
struct LakeTokenEntry { uint64 token; struct LakeTxnRow* row; };

enum LakeTxnPhase { LAKE_TXN_ACTIVE, LAKE_TXN_PUBLISHING, LAKE_TXN_CATALOG_READY };
struct LakeTxnPayloadStore;
enum LakeTxnPayloadKind { LAKE_PAYLOAD_TOMBSTONE, LAKE_PAYLOAD_RESIDENT, LAKE_PAYLOAD_SPILLED };
struct LakeTxnRow {
    HeapTuple tuple; /* Only RESIDENT owns this pointer; NULL is not a tombstone test. */
    LakeTxnPayloadKind payload_kind;
    uint32 payload_length;
    LakeTxnPayloadStore* payload_store;
    int payload_file;
    off_t payload_offset;
    LakeTxnIdentity* identity;
    struct LakeTxnRow* previous_version;
    uint64 statement_token;
    Size charged_bytes;
    CommandId command_id;
    SubTransactionId owner_subid;
    uint64 local_token;
    bool active;
    LakeTxnRow* next;
};
struct LakeTxnIdentity {
    bool committed_origin;
    LakeRowLocator origin;
    SubTransactionId owner_subid;
    LakeTxnRow* head;
    LakeTxnIdentity* next;
};
struct LakeTxnCatalog;
struct LakeCreateDefinition;
enum LakePendingTableState { LAKE_OBJECT_ACTIVE, LAKE_OBJECT_DROPPED, LAKE_OBJECT_ABORTED };
struct LakePendingTable {
    uint64 object_token;
    Oid relation_oid;
    SubTransactionId owner_subid;
    CommandId create_command_id;
    LakePendingTableState state;
    LakeCreateDefinition* definition;
    Size charged_bytes;
    struct LakeTxnTable* table;
};
struct LakeObjectEvent {
    LakePendingTable* object;
    LakePendingTableState previous_state;
    SubTransactionId owner_subid;
    LakeObjectEvent* next;
};
struct LakeObjectOidEntry { Oid relation_oid; LakePendingTable* object; };
struct LakeObjectTokenEntry { uint64 token; LakePendingTable* object; };
/* An attempted UPDATE/DELETE can conflict even when no old row matched.
 * This is transaction-local conflict state, never a durable changeset entry. */
struct LakeDeleteAttempt { SubTransactionId owner_subid; LakeDeleteAttempt* next; };
/* A first modification has its own per-table conflict frontier. This does not
 * move the catalog frontier or any already-created statement read view. */
struct LakeFirstModifyBaseline {
    MemoryContext context;
    LakeInlineBinding* previous_binding;
    int64 snapshot_id;
    SubTransactionId owner_subid;
};
struct LakeTxnTable {
    MemoryContext context;
    LakePendingTable* pending; /* NULL for a permanently bound object. */
    LakeInlineBinding* binding;
    TupleDesc descriptor;
    LakeTxnCatalog* catalog;
    SubTransactionId owner_subid;
    bool active;
    int64 active_rows;
    LakeTxnIdentity* identities;
    LakeDeleteAttempt* delete_attempts;
    LakeFirstModifyBaseline* first_modify;
    HTAB* origin_index;
    HTAB* token_index;
    LakeTxnRow* rows;
    LakeTxnRow* tail;
    LakeTxnTable* next;
    /* Statement views never enter catalog->tables or own a journal. Their
     * immutable binding/snapshot can outlive subsequent statements/cursors. */
    LakeTxnTable* journal;
    Snapshot statement_snapshot;
    bool statement_rc;
    bool stream_readonly;
    /* Native parent is the real pg_class object. Leaf identity is pg_partition,
     * never a substitute relation_oid or a parent-global row-ID namespace. */
    Oid native_leaf_partition_oid;
    int native_partition_no;
    ResourceOwner stream_snapshot_owner;
    HTAB* logical_origin_index;
    LakeTxnTable* read_next;
};
struct LakeTxnCatalog {
    MemoryContext context;
    Oid server_oid;
    Snapshot business_snapshot;
    int64 read_snapshot;
    bool snapshot_bound;
    int64 bound_snapshot_id;
    int64 bound_schema_version;
    int64 bound_visible_head;
    SubTransactionId owner_subid;
    bool active;
    LakeTxnTable* tables;
    LakeTxnCatalog* next;
};
struct LakeBindingDdl { SubTransactionId owner_subid; LakeBindingDdl* next; };
/* First partition slice admits one definition operation per host xact. */
struct LakePartitionDdl { SubTransactionId owner_subid; LakePartitionDdl* next; bool column_definition; };
struct LakeExpiryEvent { SubTransactionId owner_subid; LakeExpiryEvent* next; };
struct LakeFlushOperation;
struct LakeRewriteOperation;
struct LakeRenameEvent;
struct LakeDropEvent;
struct LakeTxnState {
    MemoryContext context;
    LakeTxnPhase phase;
    LakeTxnCatalog* catalogs;
    Oid writable_server;
    uint64 next_token;
    uint64 next_object_token;
    HTAB* objects_by_oid;
    HTAB* objects_by_token;
    LakeObjectEvent* object_events;
    uint64 next_statement_token;
    uint64 transaction_nonce;
    uint64 journal_generation;
    LakeBindingDdl* binding_ddl;
    LakePartitionDdl* partition_ddl;
    LakeExpiryEvent* expiry_events;
    LakeFlushOperation* flush;
    LakeRewriteOperation* rewrite;
    LakeRenameEvent* renames;
    LakeDropEvent* drops;
    Size staged_bytes;
    Size resident_payload_bytes;
    LakeTxnPayloadStore* payload_store; /* Top transaction lifetime, including poison and abandoned extents. */
    bool publication_subtransaction;
    LakeTxnTable* read_views;
};

/* Session state owns this pointer; never use a thread-local singleton. */
extern void** LakeHostTransactionStateAddress(void);
/* Existing objects only; this accessor never admits or initializes new work. */
extern LakeTxnState* LakeHostExistingTransactionState(void);
extern bool LakeJournalPayloadSpillVersionEnabled(void);
extern bool LakeTxnRowHasPayload(const LakeTxnRow* row);
extern void LakeTxnPayloadCheck(const LakeTxnState* state);
/* Caller owns every returned tuple in CurrentMemoryContext, for either storage kind. */
extern HeapTuple LakeTxnMaterializeRow(const LakeTxnRow* row);
extern Size LakeTxnStoreRowPayload(LakeTxnState* state,LakeTxnRow* row,HeapTuple tuple,
    bool spillEnabled,Size journalCharge);
extern void LakeTxnPayloadVerify(LakeTxnState* state);
extern void LakeTxnPayloadClose(LakeTxnState* state);
/* Private following slice: no SQL contract opens this capability yet. */
extern bool LakeReadOnlyRootRefreshEnabled(void);
extern bool LakeTxnPristineModifyRoot(const LakeTxnTable*);
extern bool LakeTxnFirstModifyHistoryEnabled(const LakeTxnState*);
extern bool LakeTxnStreamReadEligible(void);
/* No catalog lookup or session journal is created in a Stream participant. */
extern LakeTxnTable* LakeTxnCreateStreamReadView(LakeInlineBinding*,Snapshot,TupleDesc);
extern void LakeTxnReleaseStreamReadView(LakeTxnTable*);
extern LakeTxnTable* LakeTxnReadTable(Oid relationOid, Snapshot snapshot);
extern LakeTxnTable* LakeTxnReadStatementTable(Oid relationOid, Snapshot snapshot);
/* Only after all readers using the view have closed. Shared journals remain. */
extern void LakeTxnReleaseReadView(LakeTxnTable* table);
extern LakeTxnTable* LakeTxnJournalTable(LakeTxnTable* table);
/* Caller owns output Datums/tuple in its current context. Resolver creates a
 * separate fresh view; it never advances the input statement's binding. */
extern bool LakeTxnResolveLatestRow(LakeTxnTable* table, int64 rowId, CommandId commandLimit,
    TupleTableSlot* output, LakeRowLocator* locator, LakeTxnTable** resolvedView = NULL);
extern LakeTxnCatalog* LakeTxnBeginCreate(Oid serverOid, Snapshot snapshot);
extern LakeTxnTable* LakeTxnAdoptCreatedTable(LakeTxnCatalog* catalog,
    const LakeInlineBinding* binding, const LakeCreateDefinition* definition);
extern LakeTxnTable* LakeTxnFindCreatedTable(Oid relationOid, bool requireActive = true);
extern bool LakeTxnDropCreatedTable(Oid relationOid);
extern void LakeTxnRequireActiveTable(const LakeTxnTable* table);
extern Snapshot LakeTxnBusinessSnapshot(const LakeTxnTable* table);
extern void LakeTxnCheckModify(LakeTxnTable* table, Relation hostRelation, CmdType operation);
extern void LakeTxnAppend(LakeTxnTable* table, TupleTableSlot* slot, CommandId commandId, uint64 statementToken);
extern void LakeTxnModifyRow(LakeTxnTable* table, const LakeRowLocator* locator,
    TupleTableSlot* newValue, CommandId commandId, uint64 statementToken);
extern LakeTxnRow* LakeTxnResolvePrivateVersion(LakeTxnTable* table, const LakeRowLocator* locator, CommandId commandId);
extern uint64 LakeTxnNonce(void);
extern uint64 LakeTxnNextStatementToken(void);
extern LakeTxnRow* LakeTxnVisibleVersion(LakeTxnIdentity* identity, CommandId commandLimit);
extern bool LakeTxnHidesInlineRow(LakeTxnTable* table, int64 rowId, int64 begin, CommandId commandLimit);
extern bool LakeTxnTableHasChanges(const LakeTxnTable* table);
extern LakeTxnRow* LakeTxnNextRow(LakeTxnTable* table, LakeTxnRow** cursor,
    uint64* lastToken, uint64* generation, CommandId commandLimit);
extern void LakeTxnPreCommit(void* state);
extern void LakeTxnSubCommit(void* state, SubTransactionId child, SubTransactionId parent);
extern void LakeTxnSubAbort(void* state, SubTransactionId child);
extern void LakeTxnReleaseSnapshots(void* state);
extern void LakeTxnRejectBindingDdl(void);
extern void LakeTxnMarkSnapshotExpiry(void);
extern bool LakeTxnHasSnapshotExpiry(void);
extern LakeTxnCatalog* LakeTxnGetBoundCatalog(Oid serverOid);
extern bool LakeTxnHasBoundSnapshot(void);
extern void LakeTxnRejectBoundSnapshotWrite(Oid serverOid);
extern void LakeTxnBindCatalogSnapshot(Oid serverOid, const char* serverName, int64 snapshotId,
    int64 schemaVersion, int64 visibleHead, Snapshot snapshot);
extern void LakeTxnEnd(void** state);
extern void LakePublishInlineChanges(LakeTxnState* state);
extern void LakePublishInlineFlush(LakeTxnState* state);
extern void LakePublishFileRewrite(LakeTxnState* state);
extern MemoryContext LakeTxnBeginRewrite(LakeTxnTable* table);
extern void LakeTxnAdoptRewrite(LakeRewriteOperation* operation);
extern MemoryContext LakeTxnBeginFlush(LakeTxnTable* table);
extern void LakeTxnAdoptFlush(LakeFlushOperation* operation);
extern void LakeDetachPublishedCarrier(const LakeCreateDefinition* definition);
extern void LakeValidateCreateOptions(const LakeCreateDefinition* definition, Snapshot snapshot);
extern void LakeValidateInlineWriteTarget(LakeTxnTable* table, Relation hostRelation, Snapshot snapshot);
extern void LakeInstallModifyCallbacks(FdwRoutine* routine);
extern LakeTxnState* LakeTxnMarkPartitionDefinition(void);
extern LakeTxnState* LakeTxnBeginColumnDefinition(void);
extern void LakeTxnCheckHostConstraintCopy(const LakeInlineBinding* binding,TupleDesc descriptor);
#endif
