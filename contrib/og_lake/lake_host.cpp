/* Session-owned single-openGauss host adapter and late candidate resolution. */
#include "postgres.h"
#include "lake_version.h"
#include "knl/knl_variable.h"
#include "access/og_lake.h"
#include "access/og_lake_maintenance.h"
#include "lake_txn_private.h"
#include "lake_file_sdk.h"
#include "lake_ann_rescan.h"
#include "lake_index_maintenance.h"
#include "access/xact.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "storage/lock/lock.h"
#include "storage/proc.h"
#include "threadpool/threadpool.h"
#include "utils/memutils.h"

/* SQL advisory keys use discriminator 1 (int8) or 2 (int4,int4).
 * Separate internal discriminators keep application keys disjoint, including
 * applications that happen to use the same numeric namespace/object ID.
 */
static const uint32 LAKE_LOCK_NAMESPACE = 0x4f474c4b;  /* OGLK */
static const uint32 LAKE_MAINTENANCE_LOCK = 0x4f474c01;
static const uint32 LAKE_CATALOG_LOCK = 0x4f474c02;

struct LakeCatalogGuard {
    Oid server_oid;
    LOCKMODE mode;
    SubTransactionId owner_subid;
    LakeCatalogGuard* next;
};

struct LakeSessionCommittedNode {
    Oid database_oid;
    Oid server_oid;
    Oid metadata_schema_oid;
    uint8 catalog_id[UUID_LEN];
    int64 binding_generation;
    char* data_root; /* Owned in session MEMORY_CONTEXT_STORAGE; key fields are immutable. */
    bool valid;
    int64 snapshot_id;
    LakeSessionCommittedNode* next;
};

enum LakeHostEventKind {
    LAKE_HOST_EVENT_PUBLISH = 0,
    LAKE_HOST_EVENT_RESET_CATALOG = 1,
    LAKE_HOST_EVENT_RESET_ALL = 2
};

struct LakeHostCommittedEvent {
    LakeSessionCommittedNode* node;
    LakeHostEventKind kind;
    int64 snapshot_id;
    bool deferred;
    bool active;
    SubTransactionId owner_subid;
    LakeHostCommittedEvent* next;
};

struct LakeHostSessionState {
    bool xact_callback_registered;
    bool subxact_callback_registered;
    bool maintenance_held;
    bool extension_drop_scope;
    LakeCatalogGuard* guards;
    void* transaction_state;
    LakeFileCandidate* file_candidates;
    LakeMetadataScope* metadata_scope;
    int fast_scope_depth;
    bool fast_version_valid;
    bool fast_version_is_null;
    char fast_version[64];
    bool fast_owner_valid;
    Oid fast_owner;
    LakeSessionCommittedNode* committed_catalogs;
    LakeHostCommittedEvent* pending_events;
    LakeHostCommittedEvent* pending_events_tail;
    LakeHostCommittedEvent* free_pending_events;
    uint64 xact_commits;
    uint64 xact_aborts;
    uint64 subxact_commits;
    uint64 subxact_aborts;
    LakeAnnRescanStats ann_rescan;
};

static void LakeXactCallback(XactEvent event, void* argument);
static void LakeSubXactCallback(SubXactEvent event, SubTransactionId subid,
                               SubTransactionId parentSubid, void* argument);

static void LakeLockTag(LOCKTAG* tag, Oid objectOid, uint32 discriminator)
{
    SET_LOCKTAG_ADVISORY(*tag, u_sess->proc_cxt.MyDatabaseId,
                        LAKE_LOCK_NAMESPACE, objectOid, discriminator);
}

static LakeHostSessionState* LakeGetHostState(void)
{
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    if (state == NULL) {
        state = (LakeHostSessionState*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(LakeHostSessionState));
        u_sess->og_lake_state = state;
    }
    /* Individual flags also make initialization safe if the second allocation
     * fails after RegisterXactCallback has already succeeded.
     */
    if (!state->xact_callback_registered) {
        RegisterXactCallback(LakeXactCallback, state);
        state->xact_callback_registered = true;
    }
    if (!state->subxact_callback_registered) {
        RegisterSubXactCallback(LakeSubXactCallback, state);
        state->subxact_callback_registered = true;
    }
    return state;
}

LakeMetadataScope* LakeGetMetadataScope(void)
{
    LakeHostSessionState* state=(LakeHostSessionState*)u_sess->og_lake_state;
    return state==NULL?NULL:state->metadata_scope;
}
LakeMetadataScope* LakeSetMetadataScope(LakeMetadataScope* scope)
{
    LakeHostSessionState* state=LakeGetHostState();
    LakeMetadataScope* previous=state->metadata_scope;
    state->metadata_scope=scope;
    return previous;
}

void LakeRestoreMetadataScope(LakeMetadataScope* scope)
{
    /* Only paired with LakeSetMetadataScope; this path allocates nothing. */
    LakeHostSessionState* state=(LakeHostSessionState*)u_sess->og_lake_state;
    Assert(state!=NULL);
    state->metadata_scope=scope;
}

int LakeEnterFastValidationScope(void)
{
    if (u_sess->og_lake_state == NULL && u_sess->og_lake_precommit_callbacks)
        return -1;
    LakeHostSessionState* state = LakeGetHostState();
    int previous = state->fast_scope_depth;
    if (previous == 0) {
        state->fast_version_valid = false;
        state->fast_version_is_null = false;
        state->fast_owner_valid = false;
        state->fast_owner = InvalidOid;
    }
    ++state->fast_scope_depth;
    return previous;
}

void LakeLeaveFastValidationScope(int previous_depth)
{
    if (previous_depth < 0)
        return;
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    if (state == NULL)
        return;
    state->fast_scope_depth = previous_depth;
    if (previous_depth == 0) {
        state->fast_version_valid = false;
        state->fast_version_is_null = false;
        state->fast_owner_valid = false;
        state->fast_owner = InvalidOid;
    }
}

void LakeInvalidateFastValidationCache(void)
{
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    if (state == NULL)
        return;
    state->fast_version_valid = false;
    state->fast_version_is_null = false;
    state->fast_owner_valid = false;
    state->fast_owner = InvalidOid;
}

bool LakeFastVersionLookup(char** version_out)
{
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    if (state == NULL || state->fast_scope_depth <= 0 || !state->fast_version_valid)
        return false;
    *version_out = state->fast_version_is_null ? NULL : pstrdup(state->fast_version);
    return true;
}

void LakeFastVersionStore(const char* version)
{
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    if (state == NULL || state->fast_scope_depth <= 0)
        return;
    if (version == NULL) {
        state->fast_version_is_null = true;
        state->fast_version[0] = '\0';
        state->fast_version_valid = true;
        return;
    }
    Size len = strlen(version);
    if (len >= sizeof(state->fast_version)) {
        state->fast_version_valid = false;
        return;
    }
    errno_t rc = memcpy_s(state->fast_version, sizeof(state->fast_version), version, len + 1);
    securec_check(rc, "", "");
    state->fast_version_is_null = false;
    state->fast_version_valid = true;
}

bool LakeFastOwnerLookup(Oid* owner_out)
{
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    if (state == NULL || state->fast_scope_depth <= 0 || !state->fast_owner_valid)
        return false;
    *owner_out = state->fast_owner;
    return true;
}

void LakeFastOwnerStore(Oid owner)
{
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    if (state == NULL || state->fast_scope_depth <= 0)
        return;
    state->fast_owner = owner;
    state->fast_owner_valid = true;
}

LakeTxnState* LakeHostExistingTransactionState(void)
{
    /* Read an already-owned journal for validation, payload reads or cleanup.
     * Do not allocate session/transaction state, register callbacks or return
     * an address that could admit new work from PRE_COMMIT/PRE_PREPARE. */
    LakeHostSessionState* host = (LakeHostSessionState*)u_sess->og_lake_state;
    return host == NULL ? NULL : (LakeTxnState*)host->transaction_state;
}

void** LakeHostTransactionStateAddress(void)
{
    if (u_sess->og_lake_precommit_callbacks)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("new native lake operations are not allowed inside pre-commit or pre-prepare callbacks")));
    return &LakeGetHostState()->transaction_state;
}

LakeFileCandidate** LakeHostFileCandidatesAddress(void)
{
    /* SQL19 prepares pure-new-identity INSERT payloads in its own publisher,
     * before opening any snapshot-claim subtransaction. Reuse only that
     * already-owned transaction registry; never admit a new business
     * operation or allocate transaction state from another xact callback.
     * Other callbacks see ACTIVE/CATALOG_READY, and internal claim callbacks
     * have publication_subtransaction set. Public work keeps its guard below. */
    LakeHostSessionState* host=(LakeHostSessionState*)u_sess->og_lake_state;
    LakeTxnState* transaction=host?(LakeTxnState*)host->transaction_state:NULL;
    if(u_sess->og_lake_precommit_callbacks && transaction!=NULL &&
       transaction->phase==LAKE_TXN_PUBLISHING && OidIsValid(transaction->writable_server) &&
       transaction->flush==NULL && transaction->rewrite==NULL && transaction->renames==NULL &&
       !transaction->publication_subtransaction && !IsSubTransaction() &&
       LakeDirectInsertVersionEnabled())
        return &host->file_candidates;
    (void)LakeHostTransactionStateAddress();
    return &LakeGetHostState()->file_candidates;
}

static LakeCatalogGuard** LakeFindGuard(LakeHostSessionState* state, Oid serverOid)
{
    LakeCatalogGuard** link = &state->guards;
    while (*link != NULL && (*link)->server_oid != serverOid)
        link = &(*link)->next;
    return link;
}

static bool LakeValidCommittedKey(const LakeCommittedCatalogKey* key)
{
    return key != NULL && OidIsValid(key->database_oid) && OidIsValid(key->server_oid) &&
           OidIsValid(key->metadata_schema_oid) && key->binding_generation > 0 &&
           key->data_root != NULL && key->data_root[0] != '\0';
}

bool LakeCommittedKeyFromBinding(const LakeInlineBinding* binding, LakeCommittedCatalogKey* key)
{
    if (binding == NULL || !binding->has_catalog_identity || key == NULL)
        return false;
    key->database_oid = u_sess->proc_cxt.MyDatabaseId;
    key->server_oid = binding->server_oid;
    key->metadata_schema_oid = binding->metadata_schema_oid;
    errno_t rc = memcpy_s(key->catalog_id, UUID_LEN, binding->catalog_id, UUID_LEN);
    securec_check(rc, "", "");
    key->binding_generation = binding->binding_generation;
    key->data_root = binding->data_root;
    return LakeValidCommittedKey(key);
}

static bool LakeCommittedNodeMatches(const LakeSessionCommittedNode* node, const LakeCommittedCatalogKey* key)
{
    return node != NULL && key != NULL &&
           node->database_oid == key->database_oid &&
           node->server_oid == key->server_oid &&
           node->metadata_schema_oid == key->metadata_schema_oid &&
           node->binding_generation == key->binding_generation &&
           memcmp(node->catalog_id, key->catalog_id, UUID_LEN) == 0 &&
           strcmp(node->data_root, key->data_root) == 0;
}

static LakeSessionCommittedNode* LakeFindCommittedNode(LakeHostSessionState* state, const LakeCommittedCatalogKey* key)
{
    if (state == NULL || !LakeValidCommittedKey(key))
        return NULL;
    for (LakeSessionCommittedNode* node = state->committed_catalogs; node != NULL; node = node->next) {
        if (LakeCommittedNodeMatches(node, key))
            return node;
    }
    return NULL;
}

static LakeSessionCommittedNode* LakeEnsureCommittedNode(LakeHostSessionState* state, const LakeCommittedCatalogKey* key)
{
    if (!LakeValidCommittedKey(key))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("invalid native lake catalog session identity")));
    LakeSessionCommittedNode* found = LakeFindCommittedNode(state, key);
    if (found != NULL)
        return found;
    MemoryContext storage = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    char* rootCopy = MemoryContextStrdup(storage, key->data_root);
    LakeSessionCommittedNode* node = (LakeSessionCommittedNode*)MemoryContextAllocZero(
        storage, sizeof(LakeSessionCommittedNode));
    node->database_oid = key->database_oid;
    node->server_oid = key->server_oid;
    node->metadata_schema_oid = key->metadata_schema_oid;
    errno_t rc = memcpy_s(node->catalog_id, UUID_LEN, key->catalog_id, UUID_LEN);
    securec_check(rc, "", "");
    node->binding_generation = key->binding_generation;
    node->data_root = rootCopy;
    node->valid = false;
    node->snapshot_id = 0;
    node->next = state->committed_catalogs;
    state->committed_catalogs = node;
    return node;
}

static LakeHostCommittedEvent* LakeAllocPendingEvent(LakeHostSessionState* state)
{
    LakeHostCommittedEvent* event = state->free_pending_events;
    if (event != NULL) {
        state->free_pending_events = event->next;
        errno_t rc = memset_s(event, sizeof(*event), 0, sizeof(*event));
        securec_check(rc, "", "");
    } else {
        event = (LakeHostCommittedEvent*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(LakeHostCommittedEvent));
    }
    event->next = NULL;
    if (state->pending_events_tail != NULL)
        state->pending_events_tail->next = event;
    else
        state->pending_events = event;
    state->pending_events_tail = event;
    return event;
}

static void LakeMovePendingEventToTail(LakeHostSessionState* state, LakeHostCommittedEvent* target)
{
    if (state->pending_events_tail == target)
        return;
    LakeHostCommittedEvent** link = &state->pending_events;
    LakeHostCommittedEvent* prev = NULL;
    while (*link != NULL && *link != target) {
        prev = *link;
        link = &(*link)->next;
    }
    if (*link != target)
        return;
    *link = target->next;
    if (state->pending_events_tail == target)
        state->pending_events_tail = prev;
    target->next = NULL;
    if (state->pending_events_tail != NULL)
        state->pending_events_tail->next = target;
    else
        state->pending_events = target;
    state->pending_events_tail = target;
}

static void LakeRecyclePendingEvents(LakeHostSessionState* state)
{
    while (state->pending_events != NULL) {
        LakeHostCommittedEvent* event = state->pending_events;
        state->pending_events = event->next;
        event->active = false;
        event->deferred = false;
        event->node = NULL;
        event->snapshot_id = 0;
        event->owner_subid = InvalidSubTransactionId;
        event->next = state->free_pending_events;
        state->free_pending_events = event;
    }
    state->pending_events_tail = NULL;
}

void* LakeHostPrepareImmediatePublish(const LakeInlineBinding* binding, SubTransactionId ownerSubid)
{
    if (u_sess->og_lake_precommit_callbacks)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("new native lake session publication state cannot be created in pre-commit callbacks")));
    LakeCommittedCatalogKey key;
    if (!LakeCommittedKeyFromBinding(binding, &key))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake publisher lacks a complete catalog binding identity")));
    LakeHostSessionState* state = LakeGetHostState();
    LakeSessionCommittedNode* node = LakeEnsureCommittedNode(state, &key);
    LakeHostCommittedEvent* event = LakeAllocPendingEvent(state);
    event->node = node;
    event->kind = LAKE_HOST_EVENT_PUBLISH;
    event->deferred = false;
    event->active = false;
    event->owner_subid = ownerSubid;
    return event;
}

void LakeHostActivateImmediatePublish(void* slot, int64 snapshotId)
{
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    LakeHostCommittedEvent* event = (LakeHostCommittedEvent*)slot;
    Assert(state != NULL && event != NULL && event->node != NULL && !event->deferred && snapshotId >= 0);
    event->snapshot_id = snapshotId;
    event->active = true;
    LakeMovePendingEventToTail(state, event);
}

void LakeHostReserveDeferredPublish(const LakeInlineBinding* binding, SubTransactionId ownerSubid)
{
    if (u_sess->og_lake_precommit_callbacks)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("new native lake session publication state cannot be reserved in pre-commit callbacks")));
    LakeCommittedCatalogKey key;
    if (!LakeCommittedKeyFromBinding(binding, &key))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake deferred publisher lacks a complete catalog binding identity")));
    LakeHostSessionState* state = LakeGetHostState();
    LakeSessionCommittedNode* node = LakeEnsureCommittedNode(state, &key);
    for (LakeHostCommittedEvent* existing = state->pending_events; existing != NULL; existing = existing->next) {
        if (existing->deferred && !existing->active && existing->node == node &&
            existing->owner_subid == ownerSubid)
            return;
    }
    LakeHostCommittedEvent* event = LakeAllocPendingEvent(state);
    event->node = node;
    event->kind = LAKE_HOST_EVENT_PUBLISH;
    event->deferred = true;
    event->active = false;
    event->owner_subid = ownerSubid;
}

void LakeHostActivateDeferredPublish(const LakeInlineBinding* binding, int64 snapshotId)
{
    LakeCommittedCatalogKey key;
    bool validKey = LakeCommittedKeyFromBinding(binding, &key);
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    (void)validKey;
    Assert(validKey && state != NULL && snapshotId >= 0);
    LakeSessionCommittedNode* node = LakeFindCommittedNode(state, &key);
    Assert(node != NULL);
    LakeHostCommittedEvent* selected = NULL;
    for (LakeHostCommittedEvent* event = state->pending_events; event != NULL; event = event->next) {
        if (event->deferred && !event->active && event->node == node) {
            selected = event;
            break;
        }
    }
    if (selected == NULL) {
        for (LakeHostCommittedEvent* event = state->pending_events; event != NULL; event = event->next) {
            if (event->deferred && event->active && event->node == node)
                selected = event;
        }
    }
    Assert(selected != NULL);
    selected->snapshot_id = snapshotId;
    selected->active = true;
    selected->owner_subid = TopSubTransactionId;
    LakeMovePendingEventToTail(state, selected);
}

void LakeHostStageCatalogReset(const LakeCommittedCatalogKey* key, SubTransactionId ownerSubid)
{
    if (!LakeValidCommittedKey(key))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake catalog reset requires a complete binding identity")));
    LakeHostSessionState* state = LakeGetHostState();
    LakeSessionCommittedNode* node = LakeEnsureCommittedNode(state, key);
    LakeHostCommittedEvent* event = LakeAllocPendingEvent(state);
    event->node = node;
    event->kind = LAKE_HOST_EVENT_RESET_CATALOG;
    event->deferred = false;
    event->active = true;
    event->owner_subid = ownerSubid;
}

void LakeHostStageResetAll(SubTransactionId ownerSubid)
{
    LakeHostSessionState* state = LakeGetHostState();
    LakeHostCommittedEvent* event = LakeAllocPendingEvent(state);
    event->node = NULL;
    event->kind = LAKE_HOST_EVENT_RESET_ALL;
    event->deferred = false;
    event->active = true;
    event->owner_subid = ownerSubid;
}

bool LakeHostLookupCommittedSnapshot(const LakeCommittedCatalogKey* key, int64* snapshotId)
{
    if (snapshotId == NULL || !LakeValidCommittedKey(key))
        return false;
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    LakeSessionCommittedNode* node = LakeFindCommittedNode(state, key);
    if (node == NULL || !node->valid)
        return false;
    *snapshotId = node->snapshot_id;
    return true;
}

bool LakeSetExtensionDropScope(bool active)
{
    if (active) LakeCheckNativePartitionExtensionDrop();
    LakeHostSessionState* state = LakeGetHostState();
    bool previous = state->extension_drop_scope;
    if (!previous && active)
        LakeHostStageResetAll(GetCurrentSubTransactionId());
    state->extension_drop_scope = active;
    if (previous != active)
        LakeInvalidateFastValidationCache();
    return previous;
}

bool LakeInExtensionDropScope(void)
{
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    return state != NULL && state->extension_drop_scope;
}

static void LakeUnlockGuard(LakeCatalogGuard** link)
{
    LakeCatalogGuard* guard = *link;
    LOCKTAG tag;
    LakeLockTag(&tag, guard->server_oid, LAKE_CATALOG_LOCK);
    (void)LockRelease(&tag, guard->mode, true);
    *link = guard->next;
    pfree(guard);
}

static void LakeXactCallback(XactEvent event, void* argument)
{
    LakeHostSessionState* state = (LakeHostSessionState*)argument;
    if (event == XACT_EVENT_PRE_COMMIT)
        LakeTxnPreCommit(state->transaction_state);
    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT)
        LakeTxnReleaseSnapshots(state->transaction_state);
    if (event == XACT_EVENT_PRE_PREPARE && (state->guards != NULL || state->maintenance_held ||
        state->transaction_state != NULL || state->file_candidates != NULL ||
        state->pending_events != NULL))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot prepare a transaction using native lake operations")));

    /* Both COMMIT and ABORT callbacks precede resource cleanup in this host.
     * Actual guard release uses the dedicated late transaction/subtransaction
     * hooks. Maintenance claims deliberately survive commit and rollback.
     */
}

void LakeHostTransactionEnd(bool committed)
{
    u_sess->og_lake_precommit_callbacks = false;
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    if (state == NULL)
        return;
    /* FATAL enters proc_exit without unwinding the SPI PG_CATCH frame.
     * Native abort has already restored the transaction's user/security
     * context. Its abandoned metadata scope can never be resumed. */
    if (!committed && t_thrd.proc_cxt.proc_exit_inprogress)
        state->metadata_scope=NULL;
    Assert(state->metadata_scope==NULL);
    state->fast_scope_depth = 0;
    state->fast_version_valid = false;
    state->fast_version_is_null = false;
    state->fast_owner_valid = false;
    state->fast_owner = InvalidOid;
    LakeTxnState* completed=(LakeTxnState*)state->transaction_state;
    bool notify=committed&&completed!=NULL&&OidIsValid(completed->writable_server);
    if (committed) {
        for (LakeHostCommittedEvent* event = state->pending_events; event != NULL; event = event->next) {
            if (!event->active) continue;
            if (event->kind == LAKE_HOST_EVENT_PUBLISH && event->node != NULL) {
                event->node->snapshot_id = event->snapshot_id;
                event->node->valid = true;
            } else if (event->kind == LAKE_HOST_EVENT_RESET_CATALOG && event->node != NULL) {
                event->node->valid = false;
                event->node->snapshot_id = 0;
            } else if (event->kind == LAKE_HOST_EVENT_RESET_ALL) {
                for (LakeSessionCommittedNode* n = state->committed_catalogs; n != NULL; n = n->next) {
                    n->valid = false;
                    n->snapshot_id = 0;
                }
            }
        }
    }
    LakeRecyclePendingEvents(state);
    LakeTxnEnd(&state->transaction_state);
    /* Bounded, allocation-free hint only; committed directory remains authoritative. */
    if(notify)LakeMaintenanceNoteCommit(u_sess->proc_cxt.MyDatabaseId);
    /* Keep session protection through the late SDK close/abort boundary. */
    if (committed)
        ++state->xact_commits;
    else
        ++state->xact_aborts;
}

static void LakeSubXactCallback(SubXactEvent event, SubTransactionId subid,
                               SubTransactionId parentSubid, void* argument)
{
    LakeHostSessionState* state = (LakeHostSessionState*)argument;
    if (event != SUBXACT_EVENT_COMMIT_SUB) return;
    LakeTxnState* txn = (LakeTxnState*)state->transaction_state;
    if (txn == NULL || !txn->publication_subtransaction) {
        for (LakeHostCommittedEvent* pending = state->pending_events; pending != NULL; pending = pending->next) {
            if (pending->owner_subid == subid)
                pending->owner_subid = parentSubid;
        }
    }
    LakeTxnSubCommit(state->transaction_state, subid, parentSubid);
    LakeFileCandidatesSubCommit(state->file_candidates,subid,parentSubid);
    /* Adoption is safe before cleanup: it does not release any protection. */
    for (LakeCatalogGuard* guard = state->guards; guard != NULL; guard = guard->next) {
        if (guard->owner_subid == subid)
            guard->owner_subid = parentSubid;
    }
    ++state->subxact_commits;
}

void LakeHostSubTransactionAbort(SubTransactionId subid)
{
    LakeHostSessionState* state = (LakeHostSessionState*)u_sess->og_lake_state;
    if (state == NULL)
        return;
    state->fast_version_valid = false;
    state->fast_version_is_null = false;
    state->fast_owner_valid = false;
    state->fast_owner = InvalidOid;
    LakeTxnState* txn = (LakeTxnState*)state->transaction_state;
    if (txn == NULL || !txn->publication_subtransaction) {
        LakeHostCommittedEvent** link = &state->pending_events;
        LakeHostCommittedEvent* prev = NULL;
        while (*link != NULL) {
            LakeHostCommittedEvent* pending = *link;
            if (pending->owner_subid == subid) {
                *link = pending->next;
                pending->active = false;
                pending->deferred = false;
                pending->node = NULL;
                pending->snapshot_id = 0;
                pending->owner_subid = InvalidSubTransactionId;
                pending->next = state->free_pending_events;
                state->free_pending_events = pending;
            } else {
                prev = pending;
                link = &pending->next;
            }
        }
        state->pending_events_tail = prev;
    }
    LakeTxnSubAbort(state->transaction_state, subid);
    /* Child-owned SDK candidates still need this protection after ordinary
     * child locks are released. Parent protection remains independently held. */
    ++state->subxact_aborts;
}

void LakeHostTransactionAfterLocks(bool committed)
{
    LakeSnapshotPinReleaseSession();
    LakeHostSessionState* state=(LakeHostSessionState*)u_sess->og_lake_state;
    if(state==NULL)return;
    LakeFileCandidatesEnd(&state->file_candidates,committed);
    while(state->guards!=NULL)LakeUnlockGuard(&state->guards);
}

void LakeHostSubTransactionAfterLocks(SubTransactionId subid)
{
    LakeHostSessionState* state=(LakeHostSessionState*)u_sess->og_lake_state;
    if(state==NULL)return;
    LakeFileCandidatesEnd(&state->file_candidates,false,subid);
    LakeCatalogGuard** link=&state->guards;
    while(*link!=NULL){
        if((*link)->owner_subid==subid){LakeUnlockGuard(link);continue;}
        link=&(*link)->next;
    }
}

bool LakeTryMaintenanceClaim(void)
{
    LakeHostSessionState* state = LakeGetHostState();
    if (state->maintenance_held)
        return true;

    LOCKTAG tag;
    LakeLockTag(&tag, InvalidOid, LAKE_MAINTENANCE_LOCK);
    LockAcquireResult result = LockAcquire(&tag, ExclusiveLock, true, true);
    if (result == LOCKACQUIRE_NOT_AVAIL)
        return false;
    state->maintenance_held = true;
    return true;
}

bool LakeReleaseMaintenanceClaim(void)
{
    LakeHostSessionState* state = LakeGetHostState();
    if (!state->maintenance_held)
        return false;
    LOCKTAG tag;
    LakeLockTag(&tag, InvalidOid, LAKE_MAINTENANCE_LOCK);
    bool released = LockRelease(&tag, ExclusiveLock, true);
    state->maintenance_held = false;
    return released;
}

static bool LakeAcquireCatalogGuardInternal(Oid serverOid, bool exclusive, bool dontWait)
{
    /* This prototype uses an existing og_lake server as its canonical identity.
     * Alias resolution/real DuckLake catalog bindings are not implemented yet.
     */
    ForeignServer* server = GetForeignServer(serverOid);
    ForeignDataWrapper* fdw = GetForeignDataWrapper(server->fdwid);
    if (strcmp(fdw->fdwname, "og_lake_fdw") != 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("native lake diagnostic guard requires an og_lake_fdw server")));

    LakeHostSessionState* state = LakeGetHostState();
    LOCKMODE mode = exclusive ? ExclusiveLock : ShareLock;
    LakeCatalogGuard** found = LakeFindGuard(state, serverOid);
    if (*found != NULL) {
        if ((*found)->mode != mode) {
            /* Internal readers (dontWait == false) requesting at-least-ShareLock are
             * already protected when this session holds an ExclusiveLock guard.
             */
            if (!exclusive && !dontWait && (*found)->mode == ExclusiveLock)
                return true;
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot change the mode of an acquired native lake catalog guard"),
                errhint("Release the guard outside a nested transaction before changing its mode.")));
        }
        return true;
    }

    /* Allocate before locking so allocation failure cannot leak a session lock. */
    LakeCatalogGuard* guard = (LakeCatalogGuard*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(LakeCatalogGuard));
    guard->server_oid = serverOid;
    guard->mode = mode;
    guard->owner_subid = GetCurrentSubTransactionId();
    LOCKTAG tag;
    LakeLockTag(&tag, serverOid, LAKE_CATALOG_LOCK);
    LockAcquireResult result;
    PG_TRY();
    {
        result = LockAcquire(&tag, mode, true, dontWait);
    }
    PG_CATCH();
    {
        /* A cancelled waiter can already have been granted its session lock.
         * Reconcile the wait queue before freeing the unlinked guard, then
         * release any just-granted lock. Generic transaction abort deliberately
         * retains USER_LOCKMETHOD session locks and cannot do this for us.
         */
        LockErrorCleanup();
        if (CheckLock(&tag, mode, true))
            (void)LockRelease(&tag, mode, true);
        pfree(guard);
        PG_RE_THROW();
    }
    PG_END_TRY();
    if (result == LOCKACQUIRE_NOT_AVAIL) {
        pfree(guard);
        return false;
    }
    guard->next = state->guards;
    state->guards = guard;
    return true;
}

bool LakeTryCatalogGuard(Oid serverOid, bool exclusive)
{
    return LakeAcquireCatalogGuardInternal(serverOid, exclusive, true);
}

void LakeAcquireCatalogGuard(Oid serverOid)
{
    /* Normal readers wait using the host lock manager, with cancel/timeouts. */
    if (!LakeAcquireCatalogGuardInternal(serverOid, false, false))
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("could not acquire native lake catalog guard")));
}

bool LakeReleaseCatalogGuard(Oid serverOid)
{
    LakeHostSessionState* state = LakeGetHostState();
    LakeCatalogGuard** found = LakeFindGuard(state, serverOid);
    if (*found == NULL)
        return false;
    if ((*found)->owner_subid != GetCurrentSubTransactionId())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("a subtransaction cannot release its parent's native lake catalog guard")));
    LakeUnlockGuard(found);
    return true;
}

void LakeHostRecordAnnRescan(uint64 hits, uint64 misses, uint64 loads,
    uint64 evictions, uint64 invalidations, uint64 queries, uint64 peakBytes)
{
    LakeHostSessionState* state = LakeGetHostState();
    state->ann_rescan.hits += hits;
    state->ann_rescan.misses += misses;
    state->ann_rescan.loads += loads;
    state->ann_rescan.evictions += evictions;
    state->ann_rescan.invalidations += invalidations;
    state->ann_rescan.queries += queries;
    if (peakBytes > state->ann_rescan.peak)
        state->ann_rescan.peak = peakBytes;
}

char* LakeHostAnnRescanJson(void)
{
    LakeHostSessionState* state = LakeGetHostState();
    return psprintf(
        "{\"hits\":%llu,\"misses\":%llu,\"loads\":%llu,\"evictions\":%llu,"
        "\"invalidations\":%llu,\"queries\":%llu,\"peak_bytes\":%llu}",
        (unsigned long long)state->ann_rescan.hits,
        (unsigned long long)state->ann_rescan.misses,
        (unsigned long long)state->ann_rescan.loads,
        (unsigned long long)state->ann_rescan.evictions,
        (unsigned long long)state->ann_rescan.invalidations,
        (unsigned long long)state->ann_rescan.queries,
        (unsigned long long)state->ann_rescan.peak);
}

char* LakeHostStateJson(void)
{
    LakeHostSessionState* state = LakeGetHostState();
    uint32 shared = 0;
    uint32 exclusive = 0;
    for (LakeCatalogGuard* guard = state->guards; guard != NULL; guard = guard->next) {
        if (guard->mode == ShareLock)
            ++shared;
        else
            ++exclusive;
    }
    LakeTxnState* txn = (LakeTxnState*)state->transaction_state;
    uint32 catalogCount = 0, tableCount = 0;
    uint64 rowCount = 0;
    if (txn != NULL) {
        for (LakeTxnCatalog* catalog = txn->catalogs; catalog != NULL; catalog = catalog->next) {
            ++catalogCount;
            for (LakeTxnTable* table = catalog->tables; table != NULL; table = table->next) {
                ++tableCount;
                rowCount += table->active_rows;
            }
        }
    }
    StringInfoData buffer;
    initStringInfo(&buffer);
    appendStringInfo(&buffer,
        "{\"host_abi\":1,\"module_version\":\"" OG_LAKE_MODULE_VERSION "\",\"provider\":\"single-opengauss\","
        "\"data_scan\":false,\"writes\":false,\"sdk\":false,\"physical_gc\":false,"
        "\"native_partition_expanded\":true,\"native_partition_expanded_sql_version\":\"0.74\","
        "\"vector_smp\":true,\"vector_smp_sql_version\":\"0.72\","
        "\"json_format_io\":true,\"json_format_sql_version\":\"0.71\","
        "\"alter_column_type_evolution\":true,\"alter_type_ops_sql_version\":\"0.70\",\"ops1_backup_integrity_retention\":true,"
        "\"physical_cleanup\":true,\"physical_cleanup_sql_version\":\"0.69\",\"n9b_local_cleanup\":true,"
        "\"snapshot_pin\":true,\"snapshot_pin_sql_version\":\"0.68\",\"pgl5_snapshot_pin\":true,"
        "\"ix_observability\":true,\"ix_obs_sql_version\":\"0.67\",\"pgl4_local_observability\":true,"
        "\"extended_defaults\":true,\"extended_defaults_sql_version\":\"0.66\",\"add_column_not_null_default\":true,\"relcache_plan_invalidation\":true,"
        "\"stats_extended\":true,\"stats_extended_sql_version\":\"0.65\",\"numeric_minmax_stats\":true,\"file_column_stats_bounds\":true,"
        "\"ix_smp_extended\":true,\"ix_smp_extended_sql_version\":\"0.64\",\"pq_ann_smp\":true,\"extended_payload_indexes\":true,"
        "\"file_partition_combinations\":true,\"file_partition_combinations_sql_version\":\"0.63\",\"truncate_partition_supported\":true,\"truncate_partition_sql_version\":\"0.73\","
        "\"copy_io\":true,\"copy_io_sql_version\":\"0.62\",\"copy_formats\":[\"parquet\",\"csv\",\"text\",\"json\"],"
        "\"external_file_fdw\":true,\"external_file_sql_version\":\"0.61\",\"external_file_formats\":[\"parquet\",\"csv\",\"text\",\"json\"],"
        "\"merge_adjacent_files\":true,\"merge_adjacent_sql_version\":\"0.60\",\"cross_evolution_rewrite\":true,\"target_file_size_option\":true,\"auto_compact_option\":true,"
        "\"bind_snapshot\":true,\"bind_snapshot_sql_version\":\"0.59\",\"management_status_views\":true,\"rebuild_lake_index\":true,\"bind_snapshot_requires\":\"exact installed SQL0.59/0.60; explicit READ COMMITTED transaction block before first catalog access; authorized caller; nonnegative visible non-expired snapshot; serial row read over mapped columns with missing-column rejection and strict read-only transaction enforcement\","
        "\"table_changes\":true,\"table_changes_sql_version\":\"0.58\",\"table_changes_requires\":\"exact installed SQL0.58/0.59; extant authorized lake regclass with schema USAGE and whole-table or complete live-column SELECT ACL; READ COMMITTED active statement snapshot in serial backend; explicit FROM AS column definitions matching end_snapshot historical schema (with (snapshot_id bigint, rowid bigint, change_type text) prefix on table_changes and optional (snapshot_id bigint, rowid bigint) prefix on table_insertions/table_deletions); journal-free inline, Parquet, delete-file, inline-delete and rewrite_delete aware CDC scan\","
        "\"time_travel\":true,\"time_travel_sql_version\":\"0.57\",\"time_travel_requires\":\"exact installed SQL0.57/0.58; extant authorized lake regclass with schema USAGE and whole-table or complete live-column SELECT ACL; READ COMMITTED active statement snapshot in serial backend; explicit FROM AS column definitions strictly matching historical column count, order, type OID, typmod and collation; journal-free read view over inline, Parquet and position deletes; nonnegative/non-future visible non-expired snapshot or timestamp\","
        "\"last_committed_snapshot\":true,\"last_committed_snapshot_sql_version\":\"0.56\",\"last_committed_snapshot_requires\":\"exact installed SQL0.56/0.57/0.58; superuser; READ COMMITTED active statement snapshot in serial backend; u_sess logical-session committed marker across all10 business and DDL publishers; NULL before first business commit or across attach, reset and rollback\","
        "\"snapshot_metadata\":true,\"snapshot_metadata_sql_version\":\"0.55\",\"snapshot_metadata_requires\":\"exact installed SQL0.55/0.56/0.57/0.58; superuser; READ COMMITTED active statement snapshot in serial backend; snapshots/current_snapshot\","
        "\"numeric_business_type\":true,\"numeric_sdk_type\":15,\"numeric_optional_sdk_abi\":5,\"numeric_business_type_requires\":\"exact installed SQL0.54/0.55, explicit decimal128 values plus optional ABI5 ordinal precision/scale; finite numeric(p,s),1<=p<=38,0<=s<=p; exact host NUMERIC inline and owned varlena; Arrow native-endian coefficient16 with no floating conversion or second rounding; empty and NULL schema checks; only NULL defaults; serial row read/write/flush/rewrite, original R/birth/P/E and journal unchanged; no numeric index, ANN, Vec, SMP, partition combinations or decimal minmax maintenance; new full SDK and PQ qualification separately pending\","
        "\"json_business_type\":true,\"json_sdk_type\":14,\"json_business_type_requires\":\"exact installed SQL0.53/0.54/0.55 and explicit physical JSON SDK capability; UTF8 database JSONOID114 validated text with raw BYTEA inline; SQL NULL distinct from JSON null; serial row transport and NULL-only defaults; no JSONB, JSON keys, index, SMP, Vec or partition combinations\","
        "\"uuid_business_type\":true,\"uuid_sdk_type\":13,\"uuid_bytes\":16,\"uuid_business_type_requires\":\"exact installed SQL0.52/0.53/0.54/0.55 and explicit SDK UUID16 capability; ordinary serial row UUIDOID2950/native pg_uuid_t16; nullable ADD, NULL-only default declarations, logical NOT NULL, DML/COPY text or CSV/flush/same-schema rewrite; original R/birth/P/E and RC journal unchanged; no non-NULL or volatile UUID defaults, UUID keys, ANN/index/SMP/Vec or partition combinations; typed statistics with malformed bounds unknown; sdk qualification separate\","
        "\"bounded_multi_file_rewrite\":true,\"bounded_multi_file_rewrite_max_sources\":16,\"bounded_multi_file_rewrite_requires\":\"exact installed SQL0.51/0.52/0.53/0.54/0.55; explicit1..16 complete current files;3..16 require authenticated ordinary protected CREATE without ADD/DROP; one supported schema and same historical partition_id/complete ordered typed tuple including NULL; stable original R/birth; all source/output, deletion and publication rechecks retained; no auto selection, mixed schema, sort, encryption, mapping, rebucket, physical GC or SDK ABI expansion; old SQL retains1..2\","
        "\"logical_snapshot_expiry\":true,\"logical_snapshot_expiry_requires\":\"exact installed SQL0.50/0.51/0.52/0.53/0.54/0.55; administrator, trusted catalog, READ COMMITTED; at most512 old snapshots per batch and65536 distinct active anchors per catalog; preserve current MAX and active native partition certificate anchors; dry_run defaults true; caller transaction owns locks to commit; all changes, inline, other metadata and files retained; no physical GC or public time-travel API\","
        "\"capability_scope\":\"runtime_binary\",\"write_requires_extension_version\":\"0.3\",\"update_delete_requires_extension_version\":\"0.4\","
        "\"inline_scan\":true,\"inline_read_only\":false,\"inline_insert\":true,\"inline_update\":true,\"inline_delete\":true,\"file_scan\":false,\"ordinary_user_access\":true,\"ordinary_access_requires\":\"extension-0.5-and-configured-private-owner\","
        "\"local_file_scan\":true,\"local_file_scan_requires\":\"extension-0.8-and-installed-provider; SQL0.8 excludes deletes and partial versions\","
        "\"local_file_versions\":true,\"local_file_position_deletes\":true,\"file_versions_require\":\"extension-0.9; inline and Parquet positions only; no Puffin/mapping/encryption\","
        "\"file_inline_dml\":true,\"file_inline_dml_requires\":\"extension-0.10, configured private owner, READ COMMITTED; changes publish as inline versions and file positions\","
        "\"bounded_file_delete_flush\":true,\"bounded_file_delete_flush_requires\":\"extension-0.11; local provider; up to16 outputs per call; remaining file populations retained; existing deletion position budget; physical GC deferred\","
        "\"partial_inline_flush\":true,\"partial_file_delete_flush\":true,"
        "\"native_vector_inline\":true,\"native_vector_file\":true,\"native_vector_write\":true,\"native_vector_flush\":true,"
        "\"native_vector_requires\":\"explicit vector(n), standard list-float32; files require LIST values/element IDs and flush requires nested ABI4 plus lifecycle ABI2\",\"lake_diskann\":false,\"lake_vector_scalar_fusion\":false,"
        "\"bounded_inline_flush\":true,\"bounded_inline_flush_requires\":\"extension-0.11, local lifecycle provider, READ COMMITTED, administrator; up to262144 versions per call; exact identity prefix; bounded file-delete consolidation\","
        "\"lake_index_definitions\":true,\"lake_index_definitions_requires\":\"extension-0.14; private owner; scalar int4/int8/float4/float8 definitions\",\"lake_index_query\":true,\"lake_index_build\":true,\"lake_index_requires\":\"extension-0.15; scalar heap B-tree or Ustore UB-tree; complete uncovered input fallback\",\"lake_index_file_build\":true,\"lake_index_file_query\":true,"
        "\"lake_index_file_requires\":\"extension-0.16; committed independent index_root; immutable source-version provider; int4/int8/float4/float8 physical candidates; floating ordered-key adapter2; runtime content/coverage validation; complete uncovered fallback\","
        "\"persistent_index_root\":true,\"persistent_index_root_requires\":\"extension-0.16; catalog administrator; independent canonical provisioned local directory; retained segments pin the root\","
        "\"transactional_table_rename\":true,\"transactional_table_rename_requires\":\"extension-0.13; permanent native table; same host/lake schema; separate lake DDL transaction\","
        "\"persistent_inline_options\":true,\"persistent_inline_options_requires\":\"extension-0.12 for positive data_inlining_row_limit; zero requires extension-0.19 bounded direct INSERT; table owner or catalog administrator; separate lake DDL transaction\","
        "\"business_table_create\":true,\"business_table_create_requires\":\"extension-0.7-and-configured-private-owner\",\"locator_wire_version\":2,"
        "\"catalog_create\":true,\"catalog_create_requires\":\"extension-0.6-and-configured-private-owner\","
        "\"catalog_create_storage\":\"local-absolute-root-without-credentials\","
        "\"inline_analyze\":true,\"inline_optimizer_statistics\":true,\"file_analyze\":true,\"autoanalyze\":true,"
        "\"file_analyze_requires\":\"explicit single-node local files; bounded equal-probability physical-position sample; complete selected row groups; default all-column projection; insufficient survivors/budget rejects without publishing stats; independent footer I/O unobserved\","
        "\"autoanalyze_requires\":\"extension-0.19; validated control-table ownership; configured private owner or administrator-owned catalog; existing maintenance launcher; enable_lake_autoanalyze defaults off; bounded native ANALYZE and committed row-change counters\","
        "\"transactional_column_rename\":true,\"transactional_column_rename_requires\":\"extension-0.18/0.19/0.20/0.21; one column of a committed permanent table; separate lake DDL transaction; stable field IDs and retained physical inline names\","
        "\"bounded_direct_file_insert\":true,\"bounded_direct_file_insert_requires\":\"extension-0.19; committed permanent table; pure new logical identities; 64MiB tracked resident journal; local provider; one immutable file per unpartitioned table before metadata claim; SQL35/36/37/38/39/40/41/42/43/44/45/46/47/48/49 additionally supports partitioned pure-new INSERT with at most sixteen candidates across the whole transaction; large COPY tuple spill requires exact ordinary-table SQL38/39/40/41/42/43/44/45/46/47/48/49; old SQL has no large COPY spill; direct committed-origin UPDATE remains unsupported\","
        "\"create_literal_defaults\":true,\"create_literal_defaults_requires\":\"exact installed SQL0.49; protected ordinary CREATE with bool/int2/int4/int8/text/BYTEA typed literals, original host cookDefault and complete pending ownership/journal budget; standard immutable initial and separate current values; created columns never gain later-ADD missing proof; no float/time/functions/expressions/ATTACH/SDK ABI expansion\","
        "\"partitioned_file_rewrite\":true,\"partitioned_file_rewrite_requires\":\"exact installed SQL0.48/0.49/0.50/0.51/0.52; protected CREATE stable business schema, serial RC, one or two current files (up to16 only under SQL0.51/0.52) with same historical partition_id and complete ordered typed tuple including NULL; same-batch full physical key proof, original R/birth, source metadata recheck and old spec/value registration; no ADD/DROP, sort history, encryption, mapping, different keys/specs, rebucket, GC or SDK ABI expansion; local-stat identity is not a content checksum or source lease\","
        "\"smallint_values\":true,\"smallint_values_requires\":\"extension-0.20; signed16 inline values; explicit INT16 SDK capability for files; serial INT2 indexes require exact installed SQL46/47/48/49; INT2 Vec and ANN scalar fusion remain disabled; plain INT2 row SMP requires separate exact installed SQL47/48/49\","
        "\"partition_evolution\":true,\"partition_evolution_requires\":\"extension-0.34/0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; ordinary native CREATE only; stable business column IDs/type/order; bounded historical source descriptors and lazy current writer; source-specific fixed-cutoff flush and exact late deletion positions; row_id stable across carriers; serial full scan fallback for historical-source indexes/SMP; positive bounded inline DML; no arbitrary column evolution or native-parent expansion; stale transaction root across structural DDL may require40001 retry\","
        "\"journal_tuple_spill\":true,\"journal_tuple_spill_requires\":\"exact installed extension-0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; ordinary native lake rows only; 8 MiB tuple resident soft limit and unchanged64 MiB tracked resident identity/version budget; transaction-owned append-only BufFile up to64 GiB including abandoned payload; strict I/O poisoning survives savepoint rollback; no unlimited row count or single-value SDK expansion; old SQL and native-parent keep resident refusal\","
        "\"binary_business_type\":true,\"binary_business_type_requires\":\"exact installed extension-0.43/0.44/0.45/0.46/0.47/0.48/0.49 and explicit SDK binary capability; BYTEA/blob on protected ordinary CREATE; raw zero and non-UTF8 bytes; serial row/COPY text or CSV/inline/direct/flush and existing narrow rewrite; 64MiB row budget and unchanged journal/spill limits; no binary keys; NOT NULL requires exact installed SQL44/45/46/47/48/49 and non-NULL defaults require exact SQL45/46/47/48/49; external ATTACH/native parent and binary table index/SMP/Vec acceleration remain closed\","
        "\"binary_not_null_constraints\":true,\"binary_not_null_constraints_requires\":\"exact installed extension-0.44/0.45/0.46/0.47/0.48/0.49; ordinary protected CREATE BYTEA or one committed-table SET/DROP NOT NULL; existing logical validity checks after EPQ/default/COPY, snapshot-specific schema proof and visible-row SET validation; empty BLOB is non-NULL; physical history stays nullable; BYTEA non-NULL defaults require separate exact SQL45/46/47/48/49; no ADD NOT NULL, native parent or binary index/SMP/Vec expansion\","
        "\"int2_row_smp\":true,\"int2_row_smp_requires\":\"exact installed extension-0.47/0.48/0.49; plain complete row ForeignScan only; ordinary protected read-only single base table with INT2 and admitted flat scalar payload; existing local2/4 whole-file tasks and inline owner0; leader query-bound qualification and snapshot; no worker SQL re-read, INT2 parallel index/ANN/PQ/Vec, temporal/binary/vector payload, partition/history or rescan expansion\","
        "\"int2_scalar_indexes\":true,\"int2_scalar_indexes_requires\":\"exact installed extension-0.46/0.47/0.48/0.49; serial same-type int2 five comparisons and NULL tests; native int2_ops plus signed INT64 file keys with distinct INT16 manifest tag; existing complete source coverage, visibility and corruption fallback; no INT2 arrays, cross-type, ANN scalar fusion, Vec or SMP expansion\","
        "\"binary_literal_defaults\":true,\"binary_literal_defaults_requires\":\"exact installed extension-0.45/0.46/0.47/0.48/0.49; nullable ADD non-NULL BYTEA literal or committed-table SET/DROP current; PostgreSQL hex literal input only, standard DuckDB BLOB encoded text; immutable initial/birth/id history;1MiB ENCODED text, fully escaped content at most256KiB, not arbitrary raw1MiB;64MiB mapping and shared binary row budgets; existing binary SDK capability; no expression evaluation, CREATE default, ADD NOT NULL or index expansion\","
        "\"drop_scalar_column\":true,\"drop_scalar_column_requires\":\"exact installed extension-0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; one ordinary protected CREATE scalar RESTRICT target; protected host holes and complete retired field history; no last field, target index, retained partition-key or sort history; affected accelerated scans conservatively replan serially; no physical GC or parent expansion\","
        "\"alter_literal_default\":true,\"alter_literal_default_requires\":\"exact installed extension-0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; one committed ordinary protected CREATE bool/int2/int4/int8/UTF8 text column; SET admitted typed constant or DROP; initial/default history, stable ID, first introduction and nullability preserved; same host transaction and savepoint rollback; independent1MiB values and64MiB mapping budget; no expressions, parent or mixed lake DML/DDL; delayed old-snapshot readers with incompatible host current default are rejected\","
        "\"not_null_constraints\":true,\"not_null_constraints_requires\":\"exact installed extension-0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; ordinary protected CREATE top-level admitted scalar or one committed-table SET/DROP; full logical visible stream validates SET; final after-image check after EPQ/default/COPY and before tuple spill; old physical NULL and initial defaults preserved; preplanner and actual executor/Fusion snapshots must match current host constraints; no ADD NOT NULL, nested/vector constraints, parent expansion or mixed lake DML/DDL\","
        "\"add_literal_default\":true,\"add_literal_default_requires\":\"exact installed extension-0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; one nullable bool/int2/int4/int8/UTF8 text literal on ordinary protected CREATE; separate immutable initial and host current defaults; stable-ID history and per-row birth proof; text1MiB and mapping64MiB limits; no functions, non-NULL float/time/vector defaults, ADD NOT NULL or mixed lake DML; ALTER current DEFAULT requires separate SQL41 gate; skipped-column-statistics policy remains unsupported\","
        "\"add_nullable_column\":true,\"add_nullable_column_requires\":\"extension-0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; one appended nullable admitted flat scalar on ordinary protected CREATE; no default or literal NULL (nonnull literal requires separateSQL39 gate); stable field-ID and per-source physical maps; missing initial NULL requires historical introduction and birth proof; same-statement host and standard metadata; no mixed lake writes or active cursor; index/rewrite/row-SMP combinations conservatively fall back or reject; DROP COLUMN requires the separate SQL42 gate; no native-parent expansion\","
        "\"logical_drop\":true,\"logical_drop_requires\":\"extension-0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; ordinary committed native CREATE; administrator and trusted private catalog; ATTACH/old SQL detach only; up to64 same-catalog drops in one host commit; no lake DML/CREATE/rename/SET/maintenance mix; DROP SERVER CASCADE with managed CREATE rejected; historical carriers and physical files retained; no native-parent expansion\","
        "\"rc_first_modify_refresh\":true,\"rc_first_modify_refresh_requires\":\"extension-0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; READ COMMITTED ordinary native CREATE; pristine per-table root at first modifying statement; original cursor and catalog baseline retained; no pending binding DDL or maintenance\","
        "\"partitioned_direct_insert\":true,\"partitioned_direct_insert_requires\":\"extension-0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; final private new rows only; current complete partition tuple; sixteen candidates across entire host transaction; single total row-ID allocation; all files one commit; no committed-origin/native-parent/vector expansion\","
        "\"time_epoch_range_pruning\":true,\"time_epoch_range_pruning_requires\":\"extension-0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; same-type epoch_year/month/day/hour monotone source range; weakened inclusive synthetic bounds and original residual filter; shared32keys/1024nodes per-file and4MiB per-refresh; periodic calendar and bucket ranges do not prove exclusion\","
        "\"time_file_partition_pruning\":true,\"time_file_partition_pruning_requires\":\"extension-0.33/0.34/0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49 and separately qualified query-proof gate; finite same-type temporal equality, ready Params and NULL tests over the file historical calendar/epoch key; original row quals retained; temporal ranges require SQL35/36/37/38/39/40/41/42/43/44/45/46/47/48/49 epoch proof; no arrays, cross-type proof or file min/max bounds; shared transformed-key32/node1024 per-file and4MiB per-refresh budget\","
        "\"time_file_partition\":true,\"time_file_partition_requires\":\"extension-0.32/0.33/0.34/0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; ordered year/month/day/hour and epoch_year/epoch_month/epoch_day/epoch_hour over finite DATE and integer-microsecond TIMESTAMP/TIMESTAMPTZ; exact DuckLake BIGINT keys; UTC and astronomical BC years; full source-version to physical-position mapping; native CREATE only; no time identity/bucket; time query proof uses its independent SQL33 gate; SET/RESET require no registered inline carrier through SQL33; SQL34/35/36/37/38/39/40/41/42/43/44/45/46/47/48/49 use versioned carriers\","
        "\"temporal_business_types\":true,\"temporal_business_types_requires\":\"extension-0.31/0.32/0.33/0.34/0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49 and explicit SDK capabilities; finite date and microsecond timestamp/timestamptz; integer datetimes; standard Gregorian UTC VARCHAR carrier; time partition transforms require extension-0.32/0.33/0.34/0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; no temporal min/max pruning; native CREATE binding only; external attach and native parent temporal fields remain closed\","
        "\"bucket_file_partition\":true,\"bucket_file_partition_requires\":\"extension-0.30/0.31/0.32/0.33/0.34/0.35/0.36/0.37/0.38/0.39/0.40/0.41/0.42/0.43/0.44/0.45/0.46/0.47/0.48/0.49; ordered identity/bucket(N) over bool/int2/int4/int8/UTF8 text; canonical DuckLake Murmur3; bounded grouped flush and late-version position mapping; serial SELECT same-type equality, ready Params and NULL pruning; text equality requires C/POSIX; original row quals retained; cross-int, ANY and ranges have no bucket proof; SET/RESET require no registered inline carrier through SQL33; SQL34/35/36/37/38/39/40/41/42/43/44/45/46/47/48/49 use versioned carriers\","
        "\"identity_file_partition\":true,\"identity_file_partition_requires\":\"extension-0.22 retains one int4 identity key; extension-0.29/0.30/0.31/0.32/0.33/0.34/0.35/0.36 add ordered bool/int2/int4/int8/text identity keys and serial SELECT file pruning; bounded grouped inline flush; SET/RESET require no registered inline carrier through SQL33; SQL34/35/36/37/38/39/40/41/42/43/44/45/46/47/48/49 use versioned carriers; partitioned direct file INSERT requires SQL35/36/37/38/39/40/41/42/43/44/45/46/47/48/49; partitioned indexes, Vec and SMP remain disabled; partitioned rewrite requires exact SQL0.48/0.49 with the same historical spec and complete ordered tuple; native parent/leaf routing is separate\","
        "\"transactional_file_rewrite\":true,\"transactional_file_rewrite_requires\":\"extension-0.21; table owner; one or two complete current files (SQL0.51/0.52 allows1..16 with its stable-schema gate); stable row/birth identity; historical intervals retained; concurrent selected-file changes abort; no physical GC\","
        "\"schema_evolution\":false,\"smp\":false,"
        "\"planner_bridge\":false,\"stream_bridge\":false,"
        "\"session_id\":%llu,\"logical_session_id\":%llu,\"thread_id\":%llu,\"session_mode\":\"%s\","
        "\"staged_bytes\":%llu,\"transaction_memory_bytes\":%llu,"
        "\"transaction_catalogs\":%u,\"transaction_tables\":%u,\"transaction_rows\":%llu,"
        "\"database_oid\":%u,\"maintenance_held\":%s,"
        "\"catalog_guards\":%u,\"catalog_shared_guards\":%u,\"catalog_exclusive_guards\":%u,"
        "\"xact_commits\":%llu,\"xact_aborts\":%llu,"
        "\"subxact_commits\":%llu,\"subxact_aborts\":%llu,\"callbacks_registered\":%s}",
        (unsigned long long)(IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid),
        (unsigned long long)(IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid),
        (unsigned long long)t_thrd.proc_cxt.MyProcPid, IS_THREAD_POOL_WORKER ? "thread_pool" : "dedicated",
        (unsigned long long)(txn == NULL ? 0 : txn->staged_bytes),
        (unsigned long long)(txn == NULL ? 0 : GetMemoryTotalSpace(txn->context, true)),
        catalogCount, tableCount, (unsigned long long)rowCount,
        u_sess->proc_cxt.MyDatabaseId,
        state->maintenance_held ? "true" : "false", shared + exclusive, shared, exclusive,
        (unsigned long long)state->xact_commits, (unsigned long long)state->xact_aborts,
        (unsigned long long)state->subxact_commits, (unsigned long long)state->subxact_aborts,
        (state->xact_callback_registered && state->subxact_callback_registered) ? "true" : "false");
    if(buffer.len>0&&buffer.data[buffer.len-1]=='}') {
        --buffer.len;buffer.data[buffer.len]='\0';
        char* rescanJson = LakeHostAnnRescanJson();
        char* maintJson = LakeMaintenanceTelemetryJson();
        appendStringInfo(&buffer,",\"registered_snapshots\":%u,\"automatic_flush\":%s,"
            "\"ann_rescan_cache\":%s,\"maintenance\":%s}",
            (unsigned)u_sess->utils_cxt.RegisteredSnapshots,LakeMaintenanceStateJson(),
            rescanJson, maintJson);
        pfree(rescanJson);
        pfree(maintJson);
    }
    return buffer.data;
}
