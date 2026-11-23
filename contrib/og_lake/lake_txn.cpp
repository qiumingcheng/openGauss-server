/* Transaction-private inline versions: no durable provisional row IDs. */
#include "postgres.h"
#include "lake_uuid.h"
#include "lake_column_layout.h"
#include "lake_column_default.h"
#include "lake_nullability.h"
#include "lake_version.h"
#include "lake_inline_sources.h"
#include "lake_temporal.h"
#include "lake_native_partition.h"
#include "access/og_lake.h"
#include "lake_types.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_create_publish.h"
#include "lake_flush.h"
#include "lake_rewrite.h"
#include "lake_rename.h"
#include "lake_drop.h"
#include "lake_row_lock.h"
#include "lake_inline_reader.h"
#include "access/heapam.h"
#include "access/tuptoaster.h"
#include "catalog/pg_type.h"
#include "utils/datum.h"
#include "foreign/foreign.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/plancache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Ordinary roots participate in strict host-descriptor equality. Copy the
 * complete descriptor only for the already-admitted logical constraint/default
 * paths. Other legacy bindings keep their original row-only behavior. */
static bool LakeTxnNeedsHostConstraints(const LakeInlineBinding* binding)
{ return binding->column_evolution||binding->not_null_constraints||binding->literal_defaults; }

static void LakeTxnAddConstraintBytes(Size* bytes,Size extra)
{
    if(extra>LAKE_INLINE_TXN_MEMORY_LIMIT||*bytes>LAKE_INLINE_TXN_MEMORY_LIMIT-extra)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake host constraint copy exceeds its byte limit")));
    *bytes+=extra;
}
static void LakeTxnAddConstraintText(Size* bytes,const char* value)
{
    if(!value)return;
    Size length=strnlen(value,LAKE_INLINE_TXN_MEMORY_LIMIT);
    if(length==LAKE_INLINE_TXN_MEMORY_LIMIT)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake host constraint text exceeds its byte limit")));
    LakeTxnAddConstraintBytes(bytes,length+1);
}
static Size LakeTxnHostConstraintBytes(const LakeInlineBinding* binding,TupleDesc source)
{
    if(!LakeTxnNeedsHostConstraints(binding)||!source->constr)return 0;
    const TupleConstr* constraints=source->constr;Size bytes=sizeof(TupleConstr);
    /* Exactly the additional allocations made by the host TupleConstrCopy.
     * Attributes and initdefvals were already copied by the row-only helper. */
    if(constraints->num_defval){
        LakeTxnAddConstraintBytes(&bytes,sizeof(AttrDefault)*(Size)constraints->num_defval);
        LakeTxnAddConstraintBytes(&bytes,(sizeof(char)+sizeof(bool))*(Size)source->natts);
        for(int i=0;i<constraints->num_defval;++i){
            LakeTxnAddConstraintText(&bytes,constraints->defval[i].adbin);
            LakeTxnAddConstraintText(&bytes,constraints->defval[i].adbin_on_update);
        }
    }
    if(constraints->num_check){
        LakeTxnAddConstraintBytes(&bytes,sizeof(ConstrCheck)*(Size)constraints->num_check);
        for(int i=0;i<constraints->num_check;++i){
            LakeTxnAddConstraintText(&bytes,constraints->check[i].ccname);
            LakeTxnAddConstraintText(&bytes,constraints->check[i].ccbin);
        }
    }
    if(constraints->cons_autoinc)LakeTxnAddConstraintBytes(&bytes,sizeof(ConstrAutoInc));
    return bytes;
}
/* Committed bindings must fit the complete descriptor their ordinary read
 * retains. Use the real post-cook host text and the freshly loaded mapping;
 * canonical literal length alone cannot bound serialized Const/adbin text. */
void LakeTxnCheckHostConstraintCopy(const LakeInlineBinding* binding,TupleDesc descriptor)
{
    LakeInlineBinding budget=*binding;
    LakeReserveColumnMapBytes(&budget,LakeTxnHostConstraintBytes(binding,descriptor));
}
static TupleDesc LakeTxnCopyHostDescriptor(const LakeInlineBinding* binding,TupleDesc source)
{
    if(!LakeTxnNeedsHostConstraints(binding))return CreateTupleDescCopy(source);
    TupleDesc result=CreateTupleDescCopyConstr(source);
    /* This host helper omits tdhasuids; retain the original row-header layout,
     * as the existing native-leaf full-copy path already does. */
    result->tdhasuids=source->tdhasuids;
    return result;
}

static LakeTxnState* LakeGetTransactionState(void)
{
    void** address = LakeHostTransactionStateAddress();
    if (*address != NULL) {
        LakeTxnPayloadCheck((LakeTxnState*)*address);
        return (LakeTxnState*)*address;
    }
    MemoryContext context = AllocSetContextCreate(u_sess->top_transaction_mem_cxt,
        "Native lake transaction", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    LakeTxnState* state = (LakeTxnState*)MemoryContextAllocZero(context, sizeof(LakeTxnState));
    state->context = context;
    state->phase = LAKE_TXN_ACTIVE;
    state->next_token = 1;
    state->next_object_token = 1;
    HASHCTL objectHash;
    errno_t rc = memset_s(&objectHash, sizeof(objectHash), 0, sizeof(objectHash));
    securec_check(rc, "", "");
    objectHash.hcxt = context;
    objectHash.keysize = sizeof(Oid); objectHash.entrysize = sizeof(LakeObjectOidEntry);
    state->objects_by_oid = hash_create("Native lake local objects by OID", 16,
        &objectHash, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    objectHash.keysize = sizeof(uint64); objectHash.entrysize = sizeof(LakeObjectTokenEntry);
    state->objects_by_token = hash_create("Native lake local objects by token", 16,
        &objectHash, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    state->next_statement_token = 1;
    *address = state;
    return state;
}

void LakeTxnRequireActiveTable(const LakeTxnTable* table)
{
    LakeTxnPayloadCheck(LakeHostExistingTransactionState());
    if(table!=NULL&&table->journal!=NULL) {
        if(!table->active||table->statement_snapshot==NULL)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake statement view is no longer active")));
        LakeTxnRequireActiveTable(table->journal);return;
    }
    if (table == NULL || !table->active || !table->catalog->active ||
        (table->pending != NULL && (table->pending->state != LAKE_OBJECT_ACTIVE ||
         table->pending->table != table || table->binding->identity.kind != LAKE_TABLE_LOCAL ||
         table->binding->identity.key.local_object_token != table->pending->object_token)))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake transaction-local object is no longer active")));
}

LakeTxnTable* LakeTxnJournalTable(LakeTxnTable* table)
{
    LakeTxnRequireActiveTable(table);
    return table->journal==NULL?table:table->journal;
}

LakeTxnTable* LakeTxnFindCreatedTable(Oid relationOid, bool requireActive)
{
    LakeTxnState* state = (LakeTxnState*)*LakeHostTransactionStateAddress();
    if (state == NULL) return NULL;
    LakeObjectOidEntry* entry = (LakeObjectOidEntry*)hash_search(state->objects_by_oid, &relationOid, HASH_FIND, NULL);
    if (entry == NULL) return NULL;
    LakePendingTable* object = entry->object;
    LakeObjectTokenEntry* token = (LakeObjectTokenEntry*)hash_search(state->objects_by_token,
        &object->object_token, HASH_FIND, NULL);
    if (token == NULL || token->object != object || object->relation_oid != relationOid)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native lake local object indexes disagree")));
    if (requireActive) LakeTxnRequireActiveTable(object->table);
    return object->table;
}

LakeTxnCatalog* LakeTxnGetBoundCatalog(Oid serverOid)
{
    if (!OidIsValid(serverOid))
        return NULL;
    LakeTxnState* state = LakeHostExistingTransactionState();
    if (state == NULL)
        return NULL;
    for (LakeTxnCatalog* catalog = state->catalogs; catalog != NULL; catalog = catalog->next) {
        if (catalog->active && catalog->snapshot_bound && catalog->server_oid == serverOid)
            return catalog;
    }
    return NULL;
}

bool LakeTxnHasBoundSnapshot(void)
{
    LakeTxnState* state = LakeHostExistingTransactionState();
    if (state == NULL)
        return false;
    for (LakeTxnCatalog* catalog = state->catalogs; catalog != NULL; catalog = catalog->next) {
        if (catalog->active && catalog->snapshot_bound)
            return true;
    }
    return false;
}

void LakeTxnRejectBoundSnapshotWrite(Oid serverOid)
{
    LakeTxnState* state = LakeHostExistingTransactionState();
    if (state == NULL)
        return;
    for (LakeTxnCatalog* catalog = state->catalogs; catalog != NULL; catalog = catalog->next) {
        if (!catalog->active || !catalog->snapshot_bound)
            continue;
        if (!OidIsValid(serverOid) || catalog->server_oid == serverOid)
            ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                errmsg("cannot modify or run DDL/maintenance on a native lake catalog with a bound read-only snapshot")));
    }
}

static inline void LakeTxnPinCatalogRead(LakeTxnCatalog* catalog)
{
    if (catalog != NULL && catalog->active && !catalog->snapshot_bound && catalog->read_snapshot >= 0)
        LakeSnapshotPinRegister(catalog->server_oid, catalog->read_snapshot, "TXN_READ", (uint64)catalog);
}

void LakeTxnBindCatalogSnapshot(Oid serverOid, const char* serverName, int64 snapshotId,
    int64 schemaVersion, int64 visibleHead, Snapshot snapshot)
{
    LakeTxnState* state = LakeGetTransactionState();
    if (state->phase != LAKE_TXN_ACTIVE || state->binding_ddl != NULL || state->partition_ddl != NULL ||
        state->flush != NULL || state->rewrite != NULL || state->renames != NULL || state->drops != NULL ||
        state->object_events != NULL ||
        (OidIsValid(state->writable_server) && state->writable_server == serverOid))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("bind_snapshot must be called before any lake access, write, DDL or maintenance on catalog \"%s\" in the transaction",
                serverName)));
    for (LakeTxnCatalog* catalog = state->catalogs; catalog != NULL; catalog = catalog->next) {
        if (catalog->active && catalog->server_oid == serverOid)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("bind_snapshot must be called before the first lake access on catalog \"%s\" in the transaction",
                    serverName)));
    }
    for (LakeTxnTable* view = state->read_views; view != NULL; view = view->read_next) {
        if (view->active && view->binding != NULL && view->binding->server_oid == serverOid)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("bind_snapshot must be called before the first lake access on catalog \"%s\" in the transaction",
                    serverName)));
    }
    MemoryContext caller = CurrentMemoryContext;
    MemoryContext context = AllocSetContextCreate(state->context, "Native lake bound catalog",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    Snapshot volatile registered = NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(context);
        registered = RegisterSnapshotOnOwner(snapshot, t_thrd.utils_cxt.TopTransactionResourceOwner);
        LakeTxnCatalog* catalog = (LakeTxnCatalog*)palloc0(sizeof(LakeTxnCatalog));
        catalog->context = context;
        catalog->server_oid = serverOid;
        catalog->business_snapshot = registered;
        catalog->read_snapshot = snapshotId;
        catalog->snapshot_bound = true;
        catalog->bound_snapshot_id = snapshotId;
        catalog->bound_schema_version = schemaVersion;
        catalog->bound_visible_head = visibleHead;
        catalog->owner_subid = GetCurrentSubTransactionId();
        catalog->active = true;
        catalog->next = state->catalogs;
        state->catalogs = catalog;
        LakeSnapshotPinRegister(serverOid, snapshotId, "BOUND_SNAPSHOT", (uint64)catalog);
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if (registered != NULL)
            UnregisterSnapshotFromOwner(registered, t_thrd.utils_cxt.TopTransactionResourceOwner);
        MemoryContextDelete(context);
        PG_RE_THROW();
    }
    PG_END_TRY();
    ResetPlanCache();
}

LakeTxnCatalog* LakeTxnBeginCreate(Oid serverOid, Snapshot snapshot)
{
    LakeRequireAccessPolicy();
    LakeTxnRejectBoundSnapshotWrite(serverOid);
    LakeTxnState* state = LakeGetTransactionState();
    if (state->phase != LAKE_TXN_ACTIVE || state->binding_ddl != NULL || state->flush != NULL || state->rewrite != NULL || state->renames != NULL || state->drops != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake business CREATE cannot mix with catalog binding DDL or publication")));
    if (u_sess->utils_cxt.XactIsoLevel != XACT_READ_COMMITTED)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake business CREATE requires READ COMMITTED")));
    if (u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION), errmsg("cannot create a lake table in a read-only transaction")));
    if (OidIsValid(state->writable_server) && state->writable_server != serverOid)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("one transaction cannot write two native lake catalogs")));
    for (LakeTxnCatalog* catalog = state->catalogs; catalog != NULL; catalog = catalog->next)
        if (catalog->active && catalog->server_oid == serverOid) return catalog;
    MemoryContext caller = CurrentMemoryContext;
    MemoryContext context = AllocSetContextCreate(state->context, "Native lake catalog",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    Snapshot volatile registered = NULL;
    LakeTxnCatalog* result = NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(context);
        result = (LakeTxnCatalog*)palloc0(sizeof(LakeTxnCatalog));
        result->context = context;
        result->server_oid = serverOid;
        result->owner_subid = GetCurrentSubTransactionId();
        /* Pending definitions own copied metadata. CREATE preparation and
         * subsequent consumers each own explicit snapshots, so a child CREATE
         * cannot attach a rollback-unsafe pin to an ancestor readonly catalog. */
        if(!LakeRowLocksEnabled())
            registered = RegisterSnapshotOnOwner(snapshot, t_thrd.utils_cxt.TopTransactionResourceOwner);
        result->business_snapshot = registered;
        result->read_snapshot = -1;
        result->active = true;
        result->next = state->catalogs;
        state->catalogs = result;
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if (registered != NULL) UnregisterSnapshotFromOwner(registered, t_thrd.utils_cxt.TopTransactionResourceOwner);
        MemoryContextDelete(context);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return result;
}

LakeTxnTable* LakeTxnAdoptCreatedTable(LakeTxnCatalog* catalog,
    const LakeInlineBinding* input, const LakeCreateDefinition* definition)
{
    LakeTxnState* state = LakeGetTransactionState();
    if (!catalog->active || catalog->server_oid != input->server_oid ||
        (!LakeRowLocksEnabled() && catalog->read_snapshot >= 0 && catalog->read_snapshot != input->snapshot_id))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native lake CREATE catalog snapshot changed")));
    if (state->next_object_token == PG_UINT64_MAX)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native lake local object token space is exhausted")));
    const uint64 token = state->next_object_token++; /* Never rewound by errors or savepoints. */
    Size charge = 128 * 1024 + sizeof(LakePendingTable) + sizeof(LakeTxnTable) + sizeof(LakeInlineBinding) +
        sizeof(LakeCreateDefinition) + sizeof(LakeInlineColumn) * input->column_count;
    const char* strings[] = {input->metadata_schema,input->binding_id,input->table_uuid,input->data_root,definition->schema_uuid,
        definition->schema_name,definition->table_name,definition->relative_path,definition->inline_name};
    for (Size i=0;i<lengthof(strings);++i) if(strings[i]!=NULL) charge += strlen(strings[i])+1;
    for(int i=0;i<input->column_count;++i) {charge += strlen(input->columns[i].name)+1;
        if(input->columns[i].inline_name)charge+=strlen(input->columns[i].inline_name)+1;}
    /* Preflight every owned default and the REAL serialized host adbin,
     * before creating the persistent table context or copying any payload. */
    for(int i=0;i<input->column_count;++i)
        LakeTxnAddConstraintBytes(&charge,LakeColumnDefaultBytes(&input->columns[i]));
    Relation relation=heap_open(input->relation_oid,AccessShareLock);
    LakeTxnAddConstraintBytes(&charge,LakeTxnHostConstraintBytes(input,RelationGetDescr(relation)));
    if (charge > LAKE_INLINE_TXN_MEMORY_LIMIT || state->staged_bytes > LAKE_INLINE_TXN_MEMORY_LIMIT-charge)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native lake transaction object journal exceeds the memory limit")));
    MemoryContext caller = CurrentMemoryContext;
    MemoryContext context = AllocSetContextCreate(catalog->context, "Native lake local table",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    volatile bool oidIndexed=false, tokenIndexed=false;
    LakeTxnTable* table=NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(context);
        table=(LakeTxnTable*)palloc0(sizeof(LakeTxnTable));
        table->context=context; table->catalog=catalog; table->owner_subid=GetCurrentSubTransactionId(); table->active=true;
        table->binding=(LakeInlineBinding*)palloc(sizeof(LakeInlineBinding));
        *table->binding=*input;
        LakeInlineBinding* binding=table->binding;
        binding->identity=LakeLocalIdentity(token);
        binding->schema_version=0; /* No permanent schema version assigned yet. */
        binding->metadata_schema=pstrdup(input->metadata_schema);
        binding->binding_id=pstrdup(input->binding_id);
        binding->table_uuid=pstrdup(input->table_uuid);
        binding->data_root=input->data_root==NULL?NULL:pstrdup(input->data_root);
        binding->columns=(LakeInlineColumn*)palloc(sizeof(LakeInlineColumn)*input->column_count);
        for(int i=0;i<input->column_count;++i) { binding->columns[i]=input->columns[i];binding->columns[i].name=pstrdup(input->columns[i].name);
            binding->columns[i].inline_name=input->columns[i].inline_name?pstrdup(input->columns[i].inline_name):NULL;
            /* Already charged into this pending object's staged_bytes. */
            LakeCopyColumnDefault(&binding->columns[i],&input->columns[i],NULL); }
        table->descriptor=LakeTxnCopyHostDescriptor(binding,RelationGetDescr(relation)); heap_close(relation,NoLock);
        HASHCTL control; errno_t rc=memset_s(&control,sizeof(control),0,sizeof(control));securec_check(rc,"","");
        control.hcxt=context;control.keysize=sizeof(LakeOriginKey);control.entrysize=sizeof(LakeOriginEntry);
        table->origin_index=hash_create("Native lake origin versions",16,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        control.keysize=sizeof(uint64);control.entrysize=sizeof(LakeLogicalOriginEntry);
        table->logical_origin_index=hash_create("Native lake logical origins",16,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        control.keysize=sizeof(uint64);control.entrysize=sizeof(LakeTokenEntry);
        table->token_index=hash_create("Native lake local versions",128,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        LakePendingTable* pending=(LakePendingTable*)palloc0(sizeof(LakePendingTable));
        pending->object_token=token;pending->relation_oid=input->relation_oid;
        pending->owner_subid=table->owner_subid;pending->create_command_id=GetCurrentCommandId(false);
        pending->state=LAKE_OBJECT_ACTIVE;pending->charged_bytes=charge;pending->table=table;
        pending->definition=(LakeCreateDefinition*)palloc(sizeof(LakeCreateDefinition));
        *pending->definition=*definition;
        pending->definition->binding=binding;
        pending->definition->schema_uuid=pstrdup(definition->schema_uuid);
        pending->definition->schema_name=pstrdup(definition->schema_name);
        pending->definition->table_name=pstrdup(definition->table_name);
        pending->definition->relative_path=pstrdup(definition->relative_path);
        pending->definition->inline_name=definition->inline_name==NULL?NULL:pstrdup(definition->inline_name);
        table->pending=pending;
        bool found=false;
        LakeObjectOidEntry* byOid=(LakeObjectOidEntry*)hash_search(state->objects_by_oid,&input->relation_oid,HASH_ENTER,&found);
        if(found) ereport(ERROR,(errcode(ERRCODE_DUPLICATE_OBJECT),errmsg("native lake relation already has a local object")));
        oidIndexed=true;byOid->object=pending;
        LakeObjectTokenEntry* byToken=(LakeObjectTokenEntry*)hash_search(state->objects_by_token,&token,HASH_ENTER,&found);
        if(found) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake object token was reused")));
        tokenIndexed=true;byToken->object=pending;
        table->next=catalog->tables;catalog->tables=table;
        catalog->read_snapshot=catalog->read_snapshot<0?input->snapshot_id:
            (LakeRowLocksEnabled()?Min(catalog->read_snapshot,input->snapshot_id):input->snapshot_id);
        LakeTxnPinCatalogRead(catalog);
        LakeHostReserveDeferredPublish(input, GetCurrentSubTransactionId());
        state->staged_bytes+=charge;state->writable_server=input->server_oid;
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if(oidIndexed) (void)hash_search(state->objects_by_oid,&input->relation_oid,HASH_REMOVE,NULL);
        if(tokenIndexed) (void)hash_search(state->objects_by_token,&token,HASH_REMOVE,NULL);
        MemoryContextDelete(context);PG_RE_THROW();
    }
    PG_END_TRY();
    return table;
}

bool LakeTxnDropCreatedTable(Oid relationOid)
{
    LakeTxnTable* table=LakeTxnFindCreatedTable(relationOid,false);
    if(table==NULL) return false;
    LakeTxnRequireActiveTable(table);
    LakeTxnState* state=LakeGetTransactionState();
    if(state->phase!=LAKE_TXN_ACTIVE)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("cannot drop a local lake object during publication")));
    if(state->staged_bytes>LAKE_INLINE_TXN_MEMORY_LIMIT-sizeof(LakeObjectEvent))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake object journal exceeds the memory limit")));
    LakeObjectEvent* event=(LakeObjectEvent*)MemoryContextAlloc(state->context,sizeof(LakeObjectEvent));
    event->object=table->pending;event->previous_state=table->pending->state;
    event->owner_subid=GetCurrentSubTransactionId();event->next=state->object_events;state->object_events=event;
    state->staged_bytes+=sizeof(LakeObjectEvent);
    table->pending->state=LAKE_OBJECT_DROPPED;table->active=false;
    ++state->journal_generation;
    return true;
}

LakeTxnTable* LakeTxnReadTable(Oid relationOid, Snapshot snapshot)
{
    Relation managedTarget=heap_open(relationOid,AccessShareLock);
    LakeRejectNativePartitionDDL(managedTarget,"ordinary lake write or maintenance entry");heap_close(managedTarget,NoLock);
    LakeRequireAccessPolicy();
    LakeTxnState* state = LakeGetTransactionState();
    if (state->phase != LAKE_TXN_ACTIVE)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake cannot begin another operation after publication has started")));
    ForeignTable* foreign = GetForeignTable(relationOid);
    LakeTxnRejectBoundSnapshotWrite(foreign->serverid);
    LakeTxnCatalog* catalog = state->catalogs;
    while (catalog != NULL && (!catalog->active || catalog->server_oid != foreign->serverid))
        catalog = catalog->next;
    if (catalog != NULL) {
        for (LakeTxnTable* table = catalog->tables; table != NULL; table = table->next)
            if (table->active && table->binding->relation_oid == relationOid && !OidIsValid(table->native_leaf_partition_oid)) {
                LakeTxnRequireActiveTable(table);
                /* Binding cache is data state, never an authorization cache. */
                Oid owner=LakeMetadataOwner();
                if(OidIsValid(owner)) {
                    LakeValidatePrivateNamespace(table->binding->metadata_schema_oid,owner);
                    for(int source=0;source<LakeInlineReadSourceCount(table->binding);++source) {
                        Oid sourceOid=LakeInlineReadSourceOid(table->binding,source);
                        Relation carrier=heap_open(sourceOid,AccessShareLock);
                        if(RelationGetNamespace(carrier)!=table->binding->metadata_schema_oid)
                            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake cached carrier changed its namespace")));
                        LakeValidatePrivateRelation(carrier,owner,false);
                        LakeCheckMetadataObjectIdentity(table->binding->metadata_schema_oid,sourceOid,owner);
                        heap_close(carrier,NoLock);
                    }
                }
                return table;
            }
    }
    MemoryContext previous = CurrentMemoryContext;
    MemoryContext volatile newCatalogContext = NULL;
    MemoryContext volatile tableContext = NULL;
    Snapshot volatile newSnapshot = NULL;
    Snapshot volatile tableSnapshot = NULL;
    const bool statementRc=LakeRowLocksEnabled();
    LakeTxnTable* result = NULL;
    PG_TRY();
    {
        if (catalog == NULL) {
            newCatalogContext = AllocSetContextCreate(state->context, "Native lake catalog",
                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
            MemoryContextSwitchTo(newCatalogContext);
            catalog = (LakeTxnCatalog*)palloc0(sizeof(LakeTxnCatalog));
            catalog->context = newCatalogContext;
            catalog->server_oid = foreign->serverid;
            catalog->owner_subid = GetCurrentSubTransactionId();
            if(!statementRc)
                newSnapshot = RegisterSnapshotOnOwner(snapshot, t_thrd.utils_cxt.TopTransactionResourceOwner);
            catalog->business_snapshot = newSnapshot;
            catalog->active = true;
        }
        tableContext = AllocSetContextCreate(catalog->context, "Native lake table",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        MemoryContextSwitchTo(tableContext);
        /* Metadata is copied into the journal while the caller's snapshot is
         * valid. Only a consumer read-view registers a persistent snapshot;
         * root journals must not keep the first RC command's xmin alive. */
        LakeInlineBinding* binding = LakeLoadInlineBinding(relationOid, statementRc?snapshot:catalog->business_snapshot);
        if (catalog->tables == NULL && (newCatalogContext != NULL || catalog->read_snapshot < 0))
            catalog->read_snapshot = binding->snapshot_id;
        else if (!statementRc && binding->snapshot_id != catalog->read_snapshot)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native lake read snapshot changed within a transaction")));
        Relation host = heap_open(relationOid, AccessShareLock);
        result = (LakeTxnTable*)palloc0(sizeof(LakeTxnTable));
        result->context = tableContext;
        result->binding = binding;
        /* The binding's existing aggregate schema budget also covers the
         * new independent persistent copy of host default expression text. */
        LakeReserveColumnMapBytes(binding,LakeTxnHostConstraintBytes(binding,RelationGetDescr(host)));
        result->descriptor = LakeTxnCopyHostDescriptor(binding,RelationGetDescr(host));
        HASHCTL hashControl;
        errno_t rc = memset_s(&hashControl, sizeof(hashControl), 0, sizeof(hashControl));
        securec_check(rc, "", "");
        hashControl.hcxt = tableContext;
        hashControl.keysize = sizeof(LakeOriginKey);
        hashControl.entrysize = sizeof(LakeOriginEntry);
        result->origin_index = hash_create("Native lake origin versions", 128, &hashControl,
            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
        hashControl.keysize=sizeof(uint64);hashControl.entrysize=sizeof(LakeLogicalOriginEntry);
        result->logical_origin_index=hash_create("Native lake logical origins",128,&hashControl,
            HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        hashControl.keysize = sizeof(uint64);
        hashControl.entrysize = sizeof(LakeTokenEntry);
        result->token_index = hash_create("Native lake local versions", 128, &hashControl,
            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
        result->catalog = catalog;
        result->owner_subid = GetCurrentSubTransactionId();
        result->active = true;
        result->statement_snapshot=tableSnapshot;result->statement_rc=statementRc;
        if(statementRc)catalog->read_snapshot=Min(catalog->read_snapshot,binding->snapshot_id);
        heap_close(host, NoLock);
        /* Link only after construction succeeds; error cleanup owns all else. */
        result->next = catalog->tables;
        catalog->tables = result;
        if (newCatalogContext != NULL) {
            catalog->next = state->catalogs;
            state->catalogs = catalog;
        }
        LakeTxnPinCatalogRead(catalog);
        MemoryContextSwitchTo(previous);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(previous);
        if (newSnapshot != NULL)
            UnregisterSnapshotFromOwner(newSnapshot, t_thrd.utils_cxt.TopTransactionResourceOwner);
        if(tableSnapshot!=NULL)UnregisterSnapshotFromOwner(tableSnapshot,t_thrd.utils_cxt.TopTransactionResourceOwner);
        if (newCatalogContext != NULL) MemoryContextDelete(newCatalogContext);
        else if (tableContext != NULL) MemoryContextDelete(tableContext);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return result;
}

/* The existing relation_oid cache is deliberately not reused for two native
 * leaves: row_id zero in table A and row_id zero in table B are distinct. The
 * canonical key is the trusted binding UUID plus exact parent/leaf identity.
 * Root journals copy metadata but own no statement xmin, just like SQL17 RC. */
struct LakeNativeJournalProbe {
    Oid server;
    LakeTableIdentity table;
    int64 snapshot;
    char binding_uuid[37];
    char table_uuid[37];
};
static LakeNativeJournalProbe LakeTxnProbeNativeLeaf(Relation parent,
    const LakeNativePartitionIdentity* identity,Snapshot snapshot)
{
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(caller,"Native leaf metadata probe",ALLOCSET_SMALL_SIZES);
    LakeNativeJournalProbe result={};
    PG_TRY(); {
        MemoryContextSwitchTo(scratch);
        LakeNativeLeafReadBinding leaf=LakeLoadNativePartitionLeaf(parent,identity,snapshot);
        if(!leaf.binding->binding_id||strlen(leaf.binding->binding_id)!=36||strlen(leaf.binding->table_uuid)!=36)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native leaf UUID length changed")));
        result.server=leaf.binding->server_oid;result.table=leaf.binding->identity;result.snapshot=leaf.binding->snapshot_id;
        errno_t rc=memcpy_s(result.binding_uuid,sizeof(result.binding_uuid),leaf.binding->binding_id,37);securec_check(rc,"","");
        rc=memcpy_s(result.table_uuid,sizeof(result.table_uuid),leaf.binding->table_uuid,37);securec_check(rc,"","");
        MemoryContextSwitchTo(caller);MemoryContextDelete(scratch);
    } PG_CATCH(); {MemoryContextSwitchTo(caller);MemoryContextDelete(scratch);PG_RE_THROW();} PG_END_TRY();
    return result; /* Only copied POD escapes; all file/path/column rows freed. */
}

LakeTxnTable* LakeTxnReadNativePartitionTable(Relation parent,
    const LakeNativePartitionIdentity* identity,Snapshot snapshot)
{
    LakeRequireNativePartitionVersion();LakeRequireAccessPolicy();
    if(!parent||!identity||!snapshot||identity->parent_relid!=RelationGetRelid(parent)||
       !LakeHasNativePartitionMarker(parent)||!LakeRowLocksEnabled()||
       u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native leaf journals require a coordinated READ COMMITTED parent binding")));
    LakeTxnState* state=LakeGetTransactionState();
    if(state->phase!=LAKE_TXN_ACTIVE)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native leaf journal cannot start during publication")));
    LakeNativeJournalProbe input=LakeTxnProbeNativeLeaf(parent,identity,snapshot);
    LakeTxnRejectBoundSnapshotWrite(input.server);
    LakeTxnCatalog* catalog=state->catalogs;
    while(catalog&&(!catalog->active||catalog->server_oid!=input.server))catalog=catalog->next;
    if(catalog)for(LakeTxnTable* table=catalog->tables;table;table=table->next) {
        if(table->active&&table->binding->relation_oid==identity->parent_relid&&
           table->native_leaf_partition_oid==identity->leaf_partition_oid&&
           (!table->binding->binding_id||strcmp(table->binding->binding_id,input.binding_uuid)))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native leaf canonical binding changed within the transaction")));
        if(!table->active||!table->binding->binding_id||strcmp(table->binding->binding_id,input.binding_uuid))continue;
        if(table->binding->relation_oid!=identity->parent_relid||table->native_leaf_partition_oid!=identity->leaf_partition_oid||
           table->native_partition_no!=identity->partition_no||
           !LakeSameIdentity(table->binding->identity,input.table)||
           strcmp(table->binding->table_uuid,input.table_uuid))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native leaf journal binding alias or replacement detected")));
        LakeTxnRequireActiveTable(table);return table;
    }
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext volatile newCatalogContext=NULL,tableContext=NULL;
    LakeTxnTable* result=NULL;
    PG_TRY(); {
        if(!catalog) {
            newCatalogContext=AllocSetContextCreate(state->context,"Native partition lake catalog",ALLOCSET_DEFAULT_SIZES);
            MemoryContextSwitchTo(newCatalogContext);catalog=(LakeTxnCatalog*)palloc0(sizeof(LakeTxnCatalog));
            catalog->context=newCatalogContext;catalog->server_oid=input.server;
            catalog->owner_subid=GetCurrentSubTransactionId();catalog->active=true;catalog->read_snapshot=input.snapshot;
        }
        tableContext=AllocSetContextCreate(catalog->context,"Native leaf canonical journal",ALLOCSET_DEFAULT_SIZES);
        MemoryContextSwitchTo(tableContext);
        LakeNativeLeafReadBinding own=LakeLoadNativePartitionLeaf(parent,identity,snapshot);
        if(own.binding->server_oid!=input.server||!LakeSameIdentity(own.binding->identity,input.table)||
           strcmp(own.binding->binding_id,input.binding_uuid)||strcmp(own.binding->table_uuid,input.table_uuid))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native canonical leaf changed while copying its binding")));
        result=(LakeTxnTable*)palloc0(sizeof(LakeTxnTable));result->context=tableContext;result->binding=own.binding;
        /* The later statement guard compares the complete host descriptor.
         * Native heap relcache keeps even an empty TupleConstr; a row-only
         * copy drops it and would fail strict equality without any DDL. */
        result->descriptor=CreateTupleDescCopyConstr(RelationGetDescr(parent));
        /* This host helper does not copy tdhasuids (the row-only helper does).
         * Keep the source tuple header layout as well as its constraints. */
        result->descriptor->tdhasuids=RelationGetDescr(parent)->tdhasuids;
        result->catalog=catalog;
        result->owner_subid=GetCurrentSubTransactionId();result->active=true;result->statement_rc=true;
        result->native_leaf_partition_oid=identity->leaf_partition_oid;result->native_partition_no=identity->partition_no;
        HASHCTL control={};control.hcxt=tableContext;control.keysize=sizeof(LakeOriginKey);control.entrysize=sizeof(LakeOriginEntry);
        result->origin_index=hash_create("Native leaf origin versions",128,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        control.keysize=sizeof(uint64);control.entrysize=sizeof(LakeLogicalOriginEntry);
        result->logical_origin_index=hash_create("Native leaf logical origins",128,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        control.entrysize=sizeof(LakeTokenEntry);
        result->token_index=hash_create("Native leaf local versions",128,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        catalog->read_snapshot=catalog->read_snapshot<0?own.binding->snapshot_id:Min(catalog->read_snapshot,own.binding->snapshot_id);
        result->next=catalog->tables;catalog->tables=result;
        if(newCatalogContext){catalog->next=state->catalogs;state->catalogs=catalog;}
        LakeTxnPinCatalogRead(catalog);
        MemoryContextSwitchTo(caller);
    } PG_CATCH(); {
        MemoryContextSwitchTo(caller);
        if(newCatalogContext)MemoryContextDelete(newCatalogContext);else if(tableContext)MemoryContextDelete(tableContext);
        PG_RE_THROW();
    } PG_END_TRY();return result;
}

static LakeInlineBinding* LakeTxnStatementBinding(LakeTxnTable* root,Oid relationOid,Snapshot snapshot)
{
    if(!OidIsValid(root->native_leaf_partition_oid))return LakeLoadInlineBinding(relationOid,snapshot);
    Relation parent=heap_open(relationOid,AccessShareLock);
    LakeNativePartitionIdentity identity={relationOid,root->native_leaf_partition_oid,root->native_partition_no};
    LakeNativeLeafReadBinding leaf=LakeLoadNativePartitionLeaf(parent,&identity,snapshot);heap_close(parent,NoLock);return leaf.binding;
}

Snapshot LakeTxnBusinessSnapshot(const LakeTxnTable* table)
{
    Snapshot snapshot=table->statement_snapshot!=NULL?table->statement_snapshot:table->catalog->business_snapshot;
    if(snapshot==NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake scanning requires an explicit active statement view")));
    return snapshot;
}

static LakeTxnTable* LakeTxnCreateStatementView(LakeTxnTable* root,Oid relationOid,Snapshot snapshot)
{
    if(!LakeRowLocksEnabled())return root;
    LakeTxnState* state=LakeGetTransactionState();
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(state->context,"Native lake statement view",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    Snapshot volatile registered=NULL;
    LakeTxnTable* view=NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(context);
        registered=RegisterSnapshotOnOwner(snapshot,t_thrd.utils_cxt.TopTransactionResourceOwner);
        LakeInlineBinding* binding=LakeTxnStatementBinding(root,relationOid,registered);
        const LakeInlineBinding* old=root->binding;
        if(!LakeSameIdentity(binding->identity,old->identity)||binding->server_oid!=old->server_oid||
            binding->column_count!=old->column_count||binding->metadata_schema_oid!=old->metadata_schema_oid||
            strcmp(binding->table_uuid,old->table_uuid)!=0)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake statement binding changed structurally")));
        for(int i=0;i<old->column_count;++i)
            if(binding->columns[i].column_id!=old->columns[i].column_id||binding->columns[i].type!=old->columns[i].type||
                binding->columns[i].typmod!=old->columns[i].typmod||binding->columns[i].element_column_id!=old->columns[i].element_column_id||
                strcmp(binding->columns[i].name,old->columns[i].name)!=0)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake statement columns changed")));
        /* schema_version belongs to the whole lake catalog: another table's
         * CREATE must not invalidate this table's otherwise identical schema. */
        Relation host=heap_open(relationOid,AccessShareLock);
        bool sameDescriptor=equalTupleDescs(root->descriptor,RelationGetDescr(host));heap_close(host,NoLock);
        if(!sameDescriptor)ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake host row descriptor changed between statements")));
        view=(LakeTxnTable*)palloc0(sizeof(LakeTxnTable));
        view->context=context;view->binding=binding;view->descriptor=root->descriptor;view->catalog=root->catalog;
        /* Pending CREATE metadata remains journal-owned. A reader of that
         * object owns this view/snapshot until its reader closes or subaborts. */
        view->pending=root->pending;
        view->native_leaf_partition_oid=root->native_leaf_partition_oid;view->native_partition_no=root->native_partition_no;
        view->owner_subid=GetCurrentSubTransactionId();view->active=true;view->journal=root;
        view->statement_snapshot=registered;view->statement_rc=true;root->statement_rc=true;
        view->read_next=state->read_views;state->read_views=view;
        LakeSnapshotPinRegister(view->catalog->server_oid,view->binding->snapshot_id,"STATEMENT_VIEW",(uint64)view);
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if(registered!=NULL)UnregisterSnapshotFromOwner(registered,t_thrd.utils_cxt.TopTransactionResourceOwner);
        MemoryContextDelete(context);PG_RE_THROW();
    }
    PG_END_TRY();
    return view;
}

static LakeTxnTable* LakeTxnCreateBoundSnapshotReadView(LakeTxnCatalog* bound, Oid relationOid)
{
    LakeTxnState* state = LakeGetTransactionState();
    MemoryContext caller = CurrentMemoryContext;
    MemoryContext context = AllocSetContextCreate(state->context, "Native lake bound snapshot view",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    Snapshot volatile registered = NULL;
    LakeTxnTable* view = NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(context);
        registered = RegisterSnapshotOnOwner(bound->business_snapshot, t_thrd.utils_cxt.TopTransactionResourceOwner);
        LakeInlineBinding* binding = LakeLoadBoundSnapshotInlineBinding(relationOid, bound->bound_snapshot_id,
            bound->bound_schema_version, bound->bound_visible_head, registered, false);
        Relation host = heap_open(relationOid, AccessShareLock);
        TupleDesc hostDesc = CreateTupleDescCopy(RelationGetDescr(host));
        heap_close(host, NoLock);
        view = (LakeTxnTable*)palloc0(sizeof(LakeTxnTable));
        view->context = context;
        view->binding = binding;
        view->descriptor = hostDesc;
        view->catalog = bound;
        view->owner_subid = GetCurrentSubTransactionId();
        view->active = true;
        view->journal = NULL;
        view->statement_snapshot = registered;
        view->statement_rc = true;
        view->stream_readonly = true;
        view->read_next = state->read_views;
        state->read_views = view;
        LakeSnapshotPinRegister(bound->server_oid, bound->bound_snapshot_id, "STATEMENT_VIEW", (uint64)view);
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if (registered != NULL)
            UnregisterSnapshotFromOwner(registered, t_thrd.utils_cxt.TopTransactionResourceOwner);
        MemoryContextDelete(context);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return view;
}

LakeTxnTable* LakeTxnReadStatementTable(Oid relationOid,Snapshot snapshot)
{
    ForeignTable* foreign = GetForeignTable(relationOid);
    LakeTxnCatalog* bound = LakeTxnGetBoundCatalog(foreign->serverid);
    if (bound != NULL)
        return LakeTxnCreateBoundSnapshotReadView(bound, relationOid);
    return LakeTxnCreateStatementView(LakeTxnReadTable(relationOid,snapshot),relationOid,snapshot);
}

LakeTxnTable* LakeTxnReadStatementPartitionTable(Relation parent,const LakeNativePartitionIdentity* identity,Snapshot snapshot)
{
    /* GetLatestSnapshot may return the mutable static SecondarySnapshot. Pin
     * it before any nested SPI/syscache work; the returned view owns its own
     * reference, and this short admission reference is always released. */
    if(!snapshot)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native statement snapshot missing")));
    Snapshot pinned=RegisterSnapshotOnOwner(snapshot,t_thrd.utils_cxt.TopTransactionResourceOwner);
    LakeTxnTable* view=NULL;
    PG_TRY(); {
        LakeTxnTable* root=LakeTxnReadNativePartitionTable(parent,identity,pinned);
        view=LakeTxnCreateStatementView(root,RelationGetRelid(parent),pinned);
        UnregisterSnapshotFromOwner(pinned,t_thrd.utils_cxt.TopTransactionResourceOwner);
    } PG_CATCH(); {UnregisterSnapshotFromOwner(pinned,t_thrd.utils_cxt.TopTransactionResourceOwner);PG_RE_THROW();} PG_END_TRY();
    return view;
}

void LakeTxnReleaseReadView(LakeTxnTable* table)
{
    LakeTxnState* state=LakeHostExistingTransactionState();
    if(state==NULL||table==NULL)return;
    /* Find by address before dereferencing: End callbacks may see a view
     * already removed by an earlier error cleanup. Root journals aren't listed. */
    LakeTxnTable** link=&state->read_views;
    while(*link!=NULL&&*link!=table)link=&(*link)->read_next;
    if(*link==NULL)return;
    *link=table->read_next;
    LakeSnapshotPinUnregisterHandle((uint64)table);
    if(table->statement_snapshot!=NULL)
        UnregisterSnapshotFromOwner(table->statement_snapshot,t_thrd.utils_cxt.TopTransactionResourceOwner);
    MemoryContextDelete(table->context);
}

static LakeRowLocator LakeResolvedLocator(LakeTxnTable* view,const LakeInlineRowIdentity* row,CommandId limit)
{
    LakeRowLocator locator;
    errno_t rc=memset_s(&locator,sizeof(locator),0,sizeof(locator));securec_check(rc,"","");
    const LakeInlineBinding* binding=view->binding;
    locator.source=row->source;locator.database_oid=u_sess->proc_cxt.MyDatabaseId;
    locator.relation_oid=binding->relation_oid;locator.server_oid=binding->server_oid;
    locator.leaf_oid=view->native_leaf_partition_oid;
    locator.schema_version=binding->schema_version;locator.identity=binding->identity;
    locator.read_snapshot=binding->snapshot_id;locator.transaction_nonce=LakeTxnNonce();locator.producer_command_id=limit;
    if(row->source==LAKE_LOCATOR_TXN)locator.local_version_token=row->local_token;
    else {
        locator.row_id=row->row_id;locator.begin_snapshot=row->begin_snapshot;
        if(row->source==LAKE_LOCATOR_INLINE)locator.inline_oid=row->inline_oid;
        else{locator.file_id=row->file_id;locator.file_row_position=row->file_position;}
    }
    return locator;
}

bool LakeTxnResolveLatestRow(LakeTxnTable* table,int64 rowId,CommandId limit,
    TupleTableSlot* output,LakeRowLocator* locator,LakeTxnTable** resolvedView)
{
    LakeTxnTable* root=LakeTxnJournalTable(table);
    if(!LakeRowLocksEnabled()||rowId<0||output==NULL||locator==NULL||limit==InvalidCommandId)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native lake latest-row request")));
    if(output->tts_tupleDescriptor==NULL||output->tts_tupleDescriptor->natts!=root->descriptor->natts)
        ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native lake latest-row output requires the business row descriptor")));
    ExecClearTuple(output);
    if(resolvedView!=NULL)*resolvedView=table;
    uint64 key=(uint64)rowId;
    LakeLogicalOriginEntry* own=(LakeLogicalOriginEntry*)hash_search(root->logical_origin_index,&key,HASH_FIND,NULL);
    if(own!=NULL) {
        LakeTxnRow* row=LakeTxnVisibleVersion(own->identity,limit);
        if(row!=NULL) {
            if(!LakeTxnRowHasPayload(row))return false;
            LakeInlineRowIdentity identity;errno_t rc=memset_s(&identity,sizeof(identity),0,sizeof(identity));securec_check(rc,"","");
            identity.source=LAKE_LOCATOR_TXN;identity.local_token=row->local_token;
            *locator=LakeResolvedLocator(table,&identity,limit);
            ExecStoreTuple(LakeTxnMaterializeRow(row),output,InvalidBuffer,true);return true;
        }
    }
    /* Only this target gets a fresh view. The original statement's binding,
     * other scans and catalog publication baseline are never advanced here. */
    LakeTxnTable* fresh=NULL;
    if(OidIsValid(root->native_leaf_partition_oid)) {
        Relation parent=heap_open(root->binding->relation_oid,AccessShareLock);
        LakeNativePartitionIdentity identity={root->binding->relation_oid,root->native_leaf_partition_oid,root->native_partition_no};
        fresh=LakeTxnReadStatementPartitionTable(parent,&identity,GetLatestSnapshot());heap_close(parent,NoLock);
    } else fresh=LakeTxnReadStatementTable(root->binding->relation_oid,GetLatestSnapshot());
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(caller,"Native lake latest row scratch",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    LakeInlineReader* volatile reader=NULL;
    HeapTuple volatile found=NULL;
    LakeRowLocator selected;
    PG_TRY();
    {
        MemoryContextSwitchTo(scratch);
        reader=LakeInlineReaderOpen(fresh,limit);
        MemoryContext rows=AllocSetContextCreate(scratch,"Native lake latest candidate",
            ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        Datum* values=(Datum*)palloc0(sizeof(Datum)*fresh->binding->column_count);
        bool* nulls=(bool*)palloc0(sizeof(bool)*fresh->binding->column_count);
        for(;;) {
            CHECK_FOR_INTERRUPTS();MemoryContextReset(rows);MemoryContextSwitchTo(rows);
            LakeInlineRowIdentity identity;
            if(!LakeInlineReaderNext(reader,values,nulls,&identity))break;
            if(identity.source==LAKE_LOCATOR_TXN||identity.row_id!=rowId)continue;
            if(found!=NULL)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake latest logical row has multiple visible versions")));
            selected=LakeResolvedLocator(fresh,&identity,limit);
            MemoryContextSwitchTo(caller);found=LakeFormHostTuple(fresh->binding,root->descriptor,values,nulls);
        }
        MemoryContextSwitchTo(scratch);LakeInlineReaderClose(reader);reader=NULL;
        MemoryContextSwitchTo(caller);
        if(found!=NULL) {
            *locator=selected;if(resolvedView!=NULL)*resolvedView=fresh;
            ExecStoreTuple(found,output,InvalidBuffer,true);found=NULL;
        }
        if(TupIsNull(output)||resolvedView==NULL)LakeTxnReleaseReadView(fresh);
        MemoryContextDelete(scratch);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if(reader!=NULL)LakeInlineReaderClose(reader);
        if(found!=NULL)heap_freetuple(found);
        MemoryContextDelete(scratch);LakeTxnReleaseReadView(fresh);PG_RE_THROW();
    }
    PG_END_TRY();
    return !TupIsNull(output);
}

/* Read-only roots refresh at first modification only under installed SQL35. */
bool LakeReadOnlyRootRefreshEnabled(void) { return LakeCoreFollowupsVersionEnabled(); }

bool LakeTxnPristineModifyRoot(const LakeTxnTable* root)
{
    return root!=NULL&&root->journal==NULL&&root->active&&root->catalog->active&&
        root->binding!=NULL&&root->binding->inline_sources_loaded&&
        root->binding->identity.kind==LAKE_TABLE_PERMANENT&&!root->stream_readonly&&
        !OidIsValid(root->native_leaf_partition_oid)&&root->pending==NULL&&
        root->first_modify==NULL&&root->active_rows==0&&root->identities==NULL&&
        root->delete_attempts==NULL&&root->rows==NULL&&root->tail==NULL&&
        root->origin_index!=NULL&&root->logical_origin_index!=NULL&&root->token_index!=NULL&&
        hash_get_num_entries(root->origin_index)==0&&hash_get_num_entries(root->logical_origin_index)==0&&
        hash_get_num_entries(root->token_index)==0;
}

bool LakeTxnFirstModifyHistoryEnabled(const LakeTxnState* state)
{
    if(!LakeReadOnlyRootRefreshEnabled()||!LakeRowLocksEnabled()||!LakePartitionEvolutionVersionEnabled()||
       u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED||state==NULL)return false;
    if(state->binding_ddl!=NULL||state->partition_ddl!=NULL||state->object_events!=NULL||
       state->flush!=NULL||state->rewrite!=NULL||state->renames!=NULL||state->drops!=NULL||
       hash_get_num_entries(state->objects_by_oid)!=0||hash_get_num_entries(state->objects_by_token)!=0)return false;
    for(LakeTxnCatalog* c=state->catalogs;c!=NULL;c=c->next)
        for(LakeTxnTable* t=c->tables;t!=NULL;t=t->next)
            if(t->pending!=NULL)return false;
    return true;
}

static void LakeTxnCaptureFirstModify(LakeTxnState* state,LakeTxnTable* view,Relation host)
{
    if(!LakeTxnFirstModifyHistoryEnabled(state)||view->journal==NULL||
       view->statement_snapshot==NULL||!view->statement_rc)return;
    LakeTxnTable* root=view->journal;
    if(!LakeTxnPristineModifyRoot(root)||state->phase!=LAKE_TXN_ACTIVE)return;
    /* Other tables may already contain writes. Their bindings, conflict
     * frontiers and the catalog's numeric read_snapshot are not touched. */
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(root->context,"Native lake first modification",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    PG_TRY();
    {
        MemoryContextSwitchTo(context);
        /* Reload exactly the already-registered statement snapshot once, in
         * root-owned storage. Never attach view-owned arrays to the journal. */
        LakeInlineBinding* binding=LakeLoadInlineBinding(RelationGetRelid(host),view->statement_snapshot);
        const LakeInlineBinding* expected=view->binding;
        if(!LakeColumnLayoutsEqual(binding,expected)||!binding->inline_sources_loaded||binding->read_only||
           !LakeSameIdentity(binding->identity,expected->identity)||binding->server_oid!=expected->server_oid||
           binding->metadata_schema_oid!=expected->metadata_schema_oid||
           strcmp(binding->binding_id,expected->binding_id)||strcmp(binding->table_uuid,expected->table_uuid)||
           binding->snapshot_id!=expected->snapshot_id||binding->schema_version!=expected->schema_version||
           binding->inline_oid!=expected->inline_oid||binding->column_count!=expected->column_count||
           binding->inline_source_count!=expected->inline_source_count||
           binding->inline_write_schema_version!=expected->inline_write_schema_version||
           binding->inline_write_origin_snapshot!=expected->inline_write_origin_snapshot||
           binding->snapshot_id<root->catalog->read_snapshot||
           !equalTupleDescs(root->descriptor,RelationGetDescr(host)))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake first modification binding differs from its statement")));
        for(int c=0;c<binding->column_count;++c)
            if(binding->columns[c].column_id!=expected->columns[c].column_id||
               binding->columns[c].type!=expected->columns[c].type||binding->columns[c].typmod!=expected->columns[c].typmod||
               binding->columns[c].output_attnum!=expected->columns[c].output_attnum||
               binding->columns[c].element_column_id!=expected->columns[c].element_column_id||
               strcmp(binding->columns[c].name,expected->columns[c].name))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake first modification columns differ from its statement")));
        LakeFirstModifyBaseline* baseline=(LakeFirstModifyBaseline*)palloc0(sizeof(LakeFirstModifyBaseline));
        baseline->context=context;baseline->previous_binding=root->binding;
        baseline->snapshot_id=binding->snapshot_id;baseline->owner_subid=GetCurrentSubTransactionId();
        MemoryContextSwitchTo(caller);
        /* No operation that can ERROR follows these two assignments. Existing
         * cursors retain independent immutable bindings and snapshots. */
        root->binding=binding;root->first_modify=baseline;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);MemoryContextDelete(context);PG_RE_THROW();
    }
    PG_END_TRY();
}

void LakeTxnCheckModify(LakeTxnTable* table, Relation hostRelation, CmdType operation)
{
    if(table&&table->stream_readonly)ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("native lake read-only or bound-snapshot views cannot be modified")));
    LakeTxnRequireActiveTable(table);
    LakeTxnRejectBoundSnapshotWrite(table->binding->server_oid);
    for(int i=0;i<table->binding->column_count;++i)
        LakeNativeTypeRequireWrite(table->binding->columns[i].type);
    if(table->binding->file_count!=0 && (!LakeFileModificationsEnabled()||!OidIsValid(LakeMetadataOwner())))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake file-table DML requires extension 0.10 and a configured private owner")));
    LakeTxnState* state = LakeGetTransactionState();
    if(state->flush!=NULL||state->rewrite!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake DML cannot follow flush in the same transaction")));
    if (state->phase != LAKE_TXN_ACTIVE)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake publication has already started")));
    if (u_sess->utils_cxt.XactIsoLevel != XACT_READ_COMMITTED)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake inline writes currently require READ COMMITTED")));
    if (state->binding_ddl != NULL || state->renames != NULL || state->drops != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake writes cannot follow binding DDL in the same transaction")));
    LakeRequireWriteVersion(operation);
    if (u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION), errmsg("cannot modify a native lake table in a read-only transaction")));
    if (table->binding->read_only)
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION), errmsg("native lake catalog or table binding is read-only")));
    if (OidIsValid(state->writable_server) && state->writable_server != table->binding->server_oid)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("one transaction cannot write two native lake catalogs")));
    if(OidIsValid(table->binding->inline_oid)&&LakeIdentityPartitioned(table->binding)&&
       !LakeIdentityPartitionSpecEqual(&table->binding->partition,&table->binding->inline_partition))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake DML requires the inline source partition schema adapter after SET/RESET")));
    LakeValidateInlineWriteTarget(table, hostRelation, LakeTxnBusinessSnapshot(table));
    LakeTxnCaptureFirstModify(state,table,hostRelation);
    LakeHostReserveDeferredPublish(table->binding, GetCurrentSubTransactionId());
    state->writable_server = table->binding->server_oid;
    table=LakeTxnJournalTable(table);
    if (operation == CMD_UPDATE || operation == CMD_DELETE) {
        /* BeginForeignModify calls this before fetching candidate rows, so
         * WHERE false and zero-match attempts enter the same conflict set.
         * One marker per live subtransaction bounds repeated statements. */
        SubTransactionId subid = GetCurrentSubTransactionId();
        for (LakeDeleteAttempt* attempt = table->delete_attempts; attempt != NULL; attempt = attempt->next)
            if (attempt->owner_subid == subid) return;
        if (state->staged_bytes > LAKE_INLINE_TXN_MEMORY_LIMIT - sizeof(LakeDeleteAttempt))
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("native lake inline transaction exceeds the write-set limit")));
        LakeDeleteAttempt* attempt = (LakeDeleteAttempt*)MemoryContextAlloc(table->context, sizeof(LakeDeleteAttempt));
        attempt->owner_subid = subid;
        attempt->next = table->delete_attempts;
        table->delete_attempts = attempt;
        state->staged_bytes += sizeof(LakeDeleteAttempt);
    }
}

uint64 LakeTxnNonce(void)
{
    LakeTxnState* state = LakeGetTransactionState();
    if (state->transaction_nonce == 0) state->transaction_nonce = (uint64)GetTopTransactionId();
    return state->transaction_nonce;
}

uint64 LakeTxnNextStatementToken(void)
{
    LakeTxnState* state = LakeGetTransactionState();
    if (state->next_statement_token == PG_UINT64_MAX)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native lake statement token space is exhausted")));
    return state->next_statement_token++;
}

LakeTxnRow* LakeTxnVisibleVersion(LakeTxnIdentity* identity, CommandId commandLimit)
{
    LakeTxnRow* version = identity->head;
    while (version != NULL && version->command_id >= commandLimit) version = version->previous_version;
    return version;
}

bool LakeTxnHidesInlineRow(LakeTxnTable* table, int64 rowId, int64 begin, CommandId commandLimit)
{
    table=LakeTxnJournalTable(table);
    if(table->stream_readonly)return false; /* admission excludes own writes */
    if(table->statement_rc) {
        uint64 key=(uint64)rowId;
        LakeLogicalOriginEntry* logical=(LakeLogicalOriginEntry*)hash_search(table->logical_origin_index,&key,HASH_FIND,NULL);
        return logical!=NULL&&LakeTxnVisibleVersion(logical->identity,commandLimit)!=NULL;
    }
    LakeOriginKey key = {(uint64)rowId, (uint64)begin};
    LakeOriginEntry* found = (LakeOriginEntry*)hash_search(table->origin_index, &key, HASH_FIND, NULL);
    return found != NULL && LakeTxnVisibleVersion(found->identity, commandLimit) != NULL;
}

bool LakeTxnTableHasChanges(const LakeTxnTable* table)
{
    if(table->journal!=NULL)table=table->journal;
    for (LakeTxnIdentity* identity = table->identities; identity != NULL; identity = identity->next)
        if (identity->head != NULL && (identity->committed_origin || LakeTxnRowHasPayload(identity->head))) return true;
    return false;
}

/* Append an immutable after-image or tombstone. Construct everything before
 * linking it into the live journal, so an allocation error has no half-entry. */
static void LakeAppendVersion(LakeTxnTable* table, LakeTxnIdentity* identity,
    const LakeRowLocator* newOrigin, TupleTableSlot* slot, CommandId commandId, uint64 statementToken)
{
    LakeTxnState* state = LakeGetTransactionState();
    if (state->phase != LAKE_TXN_ACTIVE || !table->active)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake modification state is no longer active")));
    int columns = table->binding->column_count;
    int hostColumns=LakeHostColumnCount(table->binding);
    bool spillEnabled=!OidIsValid(table->native_leaf_partition_oid)&&LakeJournalPayloadSpillVersionEnabled();
    Size rawBytes = sizeof(HeapTupleData) + sizeof(LakeTxnRow) + sizeof(LakeTxnIdentity) + (Size)hostColumns * sizeof(Datum);
    if (slot != NULL) {
        tableam_tslot_getallattrs(slot);
        if(slot->tts_tupleDescriptor->natts!=hostColumns)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake write host descriptor changed")));
        LakeCheckNotNullSlot(table->binding,slot);
        for (int i = 0; i < columns; ++i) {
            int h=LakeLogicalToHost(table->binding,i);
            if(!slot->tts_isnull[h])
                LakeTemporalValidateDatum(table->binding->columns[i].type,slot->tts_values[h]);
            if(!slot->tts_isnull[h]&&table->binding->columns[i].type==UUIDOID){
                if(!DatumGetUUIDP(slot->tts_values[h]))
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake UUID input datum is missing")));
                if(rawBytes>LAKE_INLINE_TXN_MEMORY_LIMIT-UUID_LEN)
                    ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake UUID row exceeds the write-set limit")));
                rawBytes+=UUID_LEN;
            }
            if (!slot->tts_isnull[h] && (LakeNativeTypeByOid(table->binding->columns[i].type)->fixed_width<0||
                table->binding->columns[i].type==NUMERICOID)) {
                Size size = toast_raw_datum_size(slot->tts_values[h]);
                if (size > LAKE_INLINE_TXN_MEMORY_LIMIT || rawBytes > LAKE_INLINE_TXN_MEMORY_LIMIT - size)
                    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native lake inline row exceeds the write-set limit")));
                rawBytes += size;
                if(table->binding->columns[i].type==NUMERICOID){
                    unsigned char coefficient[16];LakeNumericCoefficient(slot->tts_values[h],table->binding->columns[i].typmod,coefficient);
                }
            }
        }
    }
    if (rawBytes > LAKE_INLINE_TXN_MEMORY_LIMIT ||
        (!spillEnabled&&state->staged_bytes > LAKE_INLINE_TXN_MEMORY_LIMIT - rawBytes))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native lake inline transaction exceeds the write-set limit")));
    if (state->next_token == PG_UINT64_MAX || table->active_rows == PG_INT64_MAX)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native lake transaction identity space is exhausted")));
    MemoryContext previous = CurrentMemoryContext;
    MemoryContext scratch = AllocSetContextCreate(state->context, "Native lake input row",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    LakeTxnRow* volatile ownedRow = NULL;
    LakeTxnIdentity* volatile ownedIdentity = NULL;
    volatile bool tokenIndexed = false, originIndexed = false,logicalIndexed=false;
    uint64 token = state->next_token;
    LakeOriginKey key = {newOrigin == NULL ? 0 : (uint64)newOrigin->row_id,
        newOrigin == NULL ? 0 : newOrigin->begin_snapshot};
    PG_TRY();
    {
        MemoryContextSwitchTo(scratch);
        HeapTuple flat = NULL;
        if (slot != NULL) {
            Datum* values = (Datum*)palloc0(sizeof(Datum) * hostColumns);
            bool* nulls=(bool*)palloc(sizeof(bool)*hostColumns);
            for(int h=0;h<hostColumns;++h)nulls[h]=true;
            for (int i = 0; i < columns; ++i) {
                int h=LakeLogicalToHost(table->binding,i);
                nulls[h]=slot->tts_isnull[h];values[h]=slot->tts_values[h];
                if (!nulls[h] && (LakeNativeTypeByOid(table->binding->columns[i].type)->fixed_width<0||
                    table->binding->columns[i].type==NUMERICOID))
                    values[h] = PointerGetDatum(PG_DETOAST_DATUM_COPY(values[h]));
            }
            flat = heap_form_tuple(table->descriptor, values, nulls);
        }
        Size journalBytes = sizeof(LakeTxnRow) + sizeof(LakeTokenEntry) +
            (identity == NULL ? sizeof(LakeTxnIdentity) + sizeof(LakeOriginEntry) + sizeof(LakeLogicalOriginEntry) : 0);
        Size bytes=journalBytes+(!spillEnabled&&flat!=NULL?(Size)flat->t_len+sizeof(HeapTupleData):0);
        if (bytes > LAKE_INLINE_TXN_MEMORY_LIMIT || state->staged_bytes > LAKE_INLINE_TXN_MEMORY_LIMIT - bytes)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("native lake inline transaction exceeds the prototype's 64 MiB write-set limit")));
        MemoryContextSwitchTo(table->context);
        ownedRow = (LakeTxnRow*)palloc0(sizeof(LakeTxnRow));
        ownedRow->local_token=token;
        Size residentBytes=LakeTxnStoreRowPayload(state,ownedRow,flat,spillEnabled,journalBytes);
        bytes=journalBytes+residentBytes;
        if (identity == NULL) {
            ownedIdentity = (LakeTxnIdentity*)palloc0(sizeof(LakeTxnIdentity));
            ownedIdentity->owner_subid = GetCurrentSubTransactionId();
            if (newOrigin != NULL) { ownedIdentity->committed_origin = true; ownedIdentity->origin = *newOrigin; }
        }
        LakeTxnIdentity* target = identity == NULL ? ownedIdentity : identity;
        ownedRow->identity = target;
        ownedRow->previous_version = target->head;
        ownedRow->charged_bytes = bytes;
        ownedRow->command_id = commandId;
        ownedRow->owner_subid = GetCurrentSubTransactionId();
        ownedRow->local_token = token;
        ownedRow->statement_token = statementToken;
        ownedRow->active = true;
        bool existed = false;
        LakeTokenEntry* tokenEntry = (LakeTokenEntry*)hash_search(table->token_index, &token, HASH_ENTER, &existed);
        if (existed) ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate native lake local version token")));
        tokenIndexed = true;
        tokenEntry->row = ownedRow;
        if (newOrigin != NULL && identity == NULL) {
            LakeOriginEntry* originEntry = (LakeOriginEntry*)hash_search(table->origin_index, &key, HASH_ENTER, &existed);
            if (existed) ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate native lake origin version")));
            originIndexed = true;
            originEntry->identity = target;
            if(table->statement_rc) {
                uint64 rowid=(uint64)newOrigin->row_id;
                LakeLogicalOriginEntry* logical=(LakeLogicalOriginEntry*)hash_search(table->logical_origin_index,&rowid,HASH_ENTER,&existed);
                if(existed)ereport(ERROR,(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),errmsg("native lake logical row was already modified")));
                logicalIndexed=true;logical->identity=target;
            }
        }
        if (ownedIdentity != NULL) { ownedIdentity->next = table->identities; table->identities = ownedIdentity; }
        if (LakeTxnRowHasPayload(target->head)) --table->active_rows;
        target->head = ownedRow;
        if (LakeTxnRowHasPayload(ownedRow)) ++table->active_rows;
        if (table->tail != NULL) table->tail->next = ownedRow; else table->rows = ownedRow;
        table->tail = ownedRow;
        ++state->next_token;
        state->staged_bytes += bytes;
        state->resident_payload_bytes+=residentBytes;
        MemoryContextSwitchTo(previous);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(previous);
        if (tokenIndexed) (void)hash_search(table->token_index, &token, HASH_REMOVE, NULL);
        if (originIndexed) (void)hash_search(table->origin_index, &key, HASH_REMOVE, NULL);
        if(logicalIndexed){uint64 rowid=(uint64)newOrigin->row_id;(void)hash_search(table->logical_origin_index,&rowid,HASH_REMOVE,NULL);}
        if (ownedRow != NULL&&ownedRow->payload_kind==LAKE_PAYLOAD_RESIDENT&&ownedRow->tuple!=NULL)
            heap_freetuple(ownedRow->tuple);
        if (ownedRow != NULL) pfree(ownedRow);
        if (ownedIdentity != NULL) pfree(ownedIdentity);
        MemoryContextDelete(scratch);
        PG_RE_THROW();
    }
    PG_END_TRY();
    MemoryContextDelete(scratch);
}

void LakeTxnAppend(LakeTxnTable* table, TupleTableSlot* slot, CommandId commandId, uint64 statementToken)
{
    table=LakeTxnJournalTable(table);
    LakeAppendVersion(table, NULL, NULL, slot, commandId, statementToken);
}

LakeTxnRow* LakeTxnResolvePrivateVersion(LakeTxnTable* table, const LakeRowLocator* locator, CommandId commandId)
{
    LakeValidateInlineLocator(locator,table,LakeTxnNonce(),commandId);
    table=LakeTxnJournalTable(table);
    if(locator->source!=LAKE_LOCATOR_TXN)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("expected a transaction-private lake locator")));
    uint64 token=locator->local_version_token;
    LakeTokenEntry* entry=(LakeTokenEntry*)hash_search(table->token_index,&token,HASH_FIND,NULL);
    if(entry==NULL || !LakeTxnRowHasPayload(entry->row))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake private version is no longer available")));
    LakeTxnRow* row=entry->row;
    if(row->command_id>=locator->producer_command_id || LakeTxnVisibleVersion(row->identity,locator->producer_command_id)!=row)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake private locator does not identify its scan-visible version")));
    if(row->identity->head!=row)
        ereport(ERROR,(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),errmsg("native lake private version was already modified by another operation")));
    return row;
}

void LakeTxnModifyRow(LakeTxnTable* table, const LakeRowLocator* locator,
    TupleTableSlot* newValue, CommandId commandId, uint64 statementToken)
{
    LakeValidateInlineLocator(locator, table, LakeTxnNonce(), commandId);
    if(locator->source==LAKE_LOCATOR_TXN) {
        LakeTxnIdentity* identity=LakeTxnResolvePrivateVersion(table,locator,commandId)->identity;
        LakeAppendVersion(LakeTxnJournalTable(table),identity,NULL,newValue,commandId,statementToken);return;
    }
    table=LakeTxnJournalTable(table);
    LakeTxnIdentity* identity = NULL;
    if (locator->source == LAKE_LOCATOR_INLINE || locator->source == LAKE_LOCATOR_FILE) {
        if (locator->row_id >= LAKE_REFERENCE_LOCAL_ID_START)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake committed row ID overlaps the fixed reference transaction-local range")));
        LakeOriginKey key = {(uint64)locator->row_id, locator->begin_snapshot};
        LakeOriginEntry* entry = (LakeOriginEntry*)hash_search(table->origin_index, &key, HASH_FIND, NULL);
        if (entry != NULL) identity = entry->identity;
        if(identity!=NULL && (identity->origin.source!=locator->source ||
            (locator->source==LAKE_LOCATOR_FILE && (identity->origin.file_id!=locator->file_id ||
                identity->origin.file_row_position!=locator->file_row_position))))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("one native lake logical version has multiple physical origins")));
        if (identity != NULL && identity->head != NULL)
            ereport(ERROR, (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                errmsg("native lake old version was already modified by another operation")));
    } else {
        identity=LakeTxnResolvePrivateVersion(table,locator,commandId)->identity;
    }
    LakeAppendVersion(table, identity, locator->source != LAKE_LOCATOR_TXN ? locator : NULL,
        newValue, commandId, statementToken);
}

LakeTxnRow* LakeTxnNextRow(LakeTxnTable* table, LakeTxnRow** cursor,
    uint64* lastToken, uint64* generation, CommandId commandLimit)
{
    table=LakeTxnJournalTable(table);
    /* A pure committed publication reader has no private rows. Do not ask
     * the public new-work entry for transaction state during PRE_COMMIT. */
    if (table->rows == NULL) return NULL;
    LakeTxnState* state = LakeHostExistingTransactionState();
    if (state == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake private rows have no owning transaction")));
    LakeTxnPayloadCheck(state);
    if (*generation != state->journal_generation) { *cursor = table->rows; *generation = state->journal_generation; }
    while (*cursor != NULL) {
        CHECK_FOR_INTERRUPTS();
        LakeTxnRow* row = *cursor;
        *cursor = row->next;
        if (row->local_token <= *lastToken || !LakeTxnRowHasPayload(row) ||
            LakeTxnVisibleVersion(row->identity, commandLimit) != row) continue;
        *lastToken = row->local_token;
        return row;
    }
    return NULL;
}

static void LakeTxnPublishAtPreCommit(void* pointer)
{
    LakeTxnState* state = (LakeTxnState*)pointer;
    if (state == NULL) return;
    if (state->phase == LAKE_TXN_CATALOG_READY) return;
    if (!OidIsValid(state->writable_server)) { state->phase = LAKE_TXN_CATALOG_READY; return; }
    if (state->phase != LAKE_TXN_ACTIVE)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("reentrant native lake publication")));
    if(state->drops!=NULL){
        state->phase=LAKE_TXN_PUBLISHING;LakePublishDrops(state);state->phase=LAKE_TXN_CATALOG_READY;return;
    }
    if(state->renames!=NULL){
        state->phase=LAKE_TXN_PUBLISHING;LakePublishRenames(state);state->phase=LAKE_TXN_CATALOG_READY;return;
    }
    if(state->rewrite!=NULL){
        state->phase=LAKE_TXN_PUBLISHING;LakePublishFileRewrite(state);state->phase=LAKE_TXN_CATALOG_READY;return;
    }
    if(state->flush!=NULL){
        state->phase=LAKE_TXN_PUBLISHING;LakePublishInlineFlush(state);state->phase=LAKE_TXN_CATALOG_READY;return;
    }
    bool hasRows = false;
    for (LakeTxnCatalog* catalog = state->catalogs; catalog != NULL; catalog = catalog->next)
        if (catalog->active)
            for (LakeTxnTable* table = catalog->tables; table != NULL; table = table->next)
                hasRows |= table->active && (table->pending != NULL || LakeTxnTableHasChanges(table));
    if (!hasRows) { state->phase = LAKE_TXN_CATALOG_READY; return; }
    state->phase = LAKE_TXN_PUBLISHING;
    LakePublishInlineChanges(state);
    state->phase = LAKE_TXN_CATALOG_READY;
}

void LakeTxnPreCommit(void* pointer)
{
    LakeTxnState* state=(LakeTxnState*)pointer;
    if(state==NULL)return;
    if(state->phase==LAKE_TXN_CATALOG_READY){LakeTxnPayloadCheck(state);LakeTxnPayloadClose(state);return;}
    /* Poison survives user subabort even if no writable rows remain. Verify
     * all surviving payload versions, not merely the final identity heads. */
    LakeTxnPayloadVerify(state);
    LakeTxnPublishAtPreCommit(state);
    LakeTxnPayloadClose(state); /* Before commit, never in transaction-end cleanup. */
}

void LakeTxnSubCommit(void* pointer, SubTransactionId child, SubTransactionId parent)
{
    LakeTxnState* state = (LakeTxnState*)pointer;
    if (state == NULL || state->publication_subtransaction) return;
    for(LakeTxnTable* view=state->read_views;view!=NULL;view=view->read_next)
        if(view->owner_subid==child)view->owner_subid=parent;
    for(LakeRenameEvent* event=state->renames;event!=NULL;event=event->next)
        if(event->owner_subid==child) event->owner_subid=parent;
    for(LakeDropEvent* event=state->drops;event!=NULL;event=event->next)
        if(event->owner_subid==child) event->owner_subid=parent;
    if(state->flush!=NULL&&state->flush->owner_subid==child)state->flush->owner_subid=parent;
    if(state->rewrite!=NULL&&state->rewrite->owner_subid==child)state->rewrite->owner_subid=parent;
    for (LakeObjectEvent* event = state->object_events; event != NULL; event = event->next)
        if (event->owner_subid == child) event->owner_subid = parent;
    for(LakePartitionDdl* marker=state->partition_ddl;marker!=NULL;marker=marker->next)
        if(marker->owner_subid==child)marker->owner_subid=parent;
    for (LakeBindingDdl* ddl = state->binding_ddl; ddl != NULL; ddl = ddl->next)
        if (ddl->owner_subid == child) ddl->owner_subid = parent;
    for (LakeExpiryEvent* ev = state->expiry_events; ev != NULL; ev = ev->next)
        if (ev->owner_subid == child) ev->owner_subid = parent;
    for (LakeTxnCatalog* catalog = state->catalogs; catalog != NULL; catalog = catalog->next) {
        if (catalog->owner_subid == child) catalog->owner_subid = parent;
        for (LakeTxnTable* table = catalog->tables; table != NULL; table = table->next) {
            if (table->owner_subid == child) table->owner_subid = parent;
            if(table->first_modify!=NULL&&table->first_modify->owner_subid==child)
                table->first_modify->owner_subid=parent;
            if (table->pending != NULL && table->pending->owner_subid == child)
                table->pending->owner_subid = parent;
            bool parentAttempt = false;
            for (LakeDeleteAttempt* attempt = table->delete_attempts; attempt != NULL; attempt = attempt->next)
                parentAttempt |= attempt->owner_subid == parent;
            LakeDeleteAttempt** attemptLink = &table->delete_attempts;
            while (*attemptLink != NULL) {
                LakeDeleteAttempt* attempt = *attemptLink;
                if (attempt->owner_subid == child) {
                    if (parentAttempt) {
                        *attemptLink = attempt->next;
                        Assert(state->staged_bytes >= sizeof(LakeDeleteAttempt));
                        state->staged_bytes -= sizeof(LakeDeleteAttempt);
                        pfree(attempt);
                        continue;
                    }
                    attempt->owner_subid = parent;
                    parentAttempt = true;
                }
                attemptLink = &attempt->next;
            }
            for (LakeTxnIdentity* identity = table->identities; identity != NULL; identity = identity->next)
                if (identity->owner_subid == child) identity->owner_subid = parent;
            for (LakeTxnRow* row = table->rows; row != NULL; row = row->next)
                if (row->owner_subid == child) row->owner_subid = parent;
        }
    }
}

void LakeTxnSubAbort(void* pointer, SubTransactionId child)
{
    LakeTxnState* state = (LakeTxnState*)pointer;
    if (state == NULL || state->publication_subtransaction) return;
    /* The late abort hook runs after aborted portals/scans release resources.
     * Drop dependent views before their shared journal/catalog is reclaimed. */
    LakeTxnTable** viewLink=&state->read_views;
    while(*viewLink!=NULL) {
        LakeTxnTable* view=*viewLink;
        if(view->owner_subid==child||(view->journal!=NULL&&view->journal->owner_subid==child)||view->catalog->owner_subid==child) {
            *viewLink=view->read_next;
            LakeSnapshotPinUnregisterHandle((uint64)view);
            if(view->statement_snapshot!=NULL)UnregisterSnapshotFromOwner(view->statement_snapshot,t_thrd.utils_cxt.TopTransactionResourceOwner);
            MemoryContextDelete(view->context);
        }else viewLink=&view->read_next;
    }
    LakeDropEvent** dropLink=&state->drops;
    while(*dropLink!=NULL) {
        LakeDropEvent* event=*dropLink;
        if(event->owner_subid==child) {
            *dropLink=event->next;
            Assert(state->staged_bytes>=event->charged_bytes);
            state->staged_bytes-=event->charged_bytes;
            MemoryContextDelete(event->context);
        } else dropLink=&event->next;
    }
    LakeRenameEvent** renameLink=&state->renames;
    while(*renameLink!=NULL) {
        LakeRenameEvent* event=*renameLink;
        if(event->owner_subid==child) {
            *renameLink=event->next;
            Assert(state->staged_bytes>=event->charged_bytes);
            state->staged_bytes-=event->charged_bytes;
            MemoryContextDelete(event->context);
        } else renameLink=&event->next;
    }
    /* Called by the late host hook after aborted portals and BEFORE_LOCKS
     * resources are closed. Ancestor tables survive; child rows are unlinked. */
    if(state->flush!=NULL&&state->flush->owner_subid==child){
        MemoryContextDelete(state->flush->context);state->flush=NULL;
    }
    if(state->rewrite!=NULL&&state->rewrite->owner_subid==child){
        MemoryContextDelete(state->rewrite->context);state->rewrite=NULL;
    }
    ++state->journal_generation;
    LakeObjectEvent** eventLink = &state->object_events;
    while (*eventLink != NULL) {
        LakeObjectEvent* event = *eventLink;
        if (event->owner_subid == child) {
            event->object->state = event->previous_state;
            event->object->table->active = event->previous_state == LAKE_OBJECT_ACTIVE;
            *eventLink = event->next;
            Assert(state->staged_bytes >= sizeof(LakeObjectEvent));
            state->staged_bytes -= sizeof(LakeObjectEvent);
            pfree(event);
        } else eventLink = &event->next;
    }
    LakePartitionDdl** partitionLink=&state->partition_ddl;
    while(*partitionLink!=NULL){
        LakePartitionDdl* marker=*partitionLink;
        if(marker->owner_subid==child){*partitionLink=marker->next;pfree(marker);}
        else partitionLink=&marker->next;
    }
    LakeBindingDdl** ddlLink = &state->binding_ddl;
    while (*ddlLink != NULL) {
        LakeBindingDdl* ddl = *ddlLink;
        if (ddl->owner_subid == child) { *ddlLink = ddl->next; pfree(ddl); }
        else ddlLink = &ddl->next;
    }
    LakeExpiryEvent** expLink = &state->expiry_events;
    while (*expLink != NULL) {
        LakeExpiryEvent* ev = *expLink;
        if (ev->owner_subid == child) { *expLink = ev->next; pfree(ev); }
        else expLink = &ev->next;
    }
    bool remainingWrites = false;
    LakeTxnCatalog** catalogLink = &state->catalogs;
    while (*catalogLink != NULL) {
        LakeTxnCatalog* catalog = *catalogLink;
        LakeTxnTable** tableLink = &catalog->tables;
        while (*tableLink != NULL) {
            LakeTxnTable* table = *tableLink;
            const bool removeTable = table->owner_subid == child || catalog->owner_subid == child;
            LakeDeleteAttempt** attemptLink = &table->delete_attempts;
            while (*attemptLink != NULL) {
                LakeDeleteAttempt* attempt = *attemptLink;
                if (removeTable || attempt->owner_subid == child) {
                    *attemptLink = attempt->next;
                    Assert(state->staged_bytes >= sizeof(LakeDeleteAttempt));
                    state->staged_bytes -= sizeof(LakeDeleteAttempt);
                    pfree(attempt);
                } else attemptLink = &attempt->next;
            }
            LakeTxnIdentity** identityLink = &table->identities;
            while (*identityLink != NULL) {
                LakeTxnIdentity* identity = *identityLink;
                while (identity->head != NULL && (removeTable || identity->head->owner_subid == child))
                    identity->head = identity->head->previous_version;
                if (identity->head == NULL) {
                    *identityLink = identity->next;
                    if (identity->committed_origin) {
                        LakeOriginKey key = {(uint64)identity->origin.row_id, identity->origin.begin_snapshot};
                        (void)hash_search(table->origin_index, &key, HASH_REMOVE, NULL);
                        uint64 rowid=(uint64)identity->origin.row_id;
                        (void)hash_search(table->logical_origin_index,&rowid,HASH_REMOVE,NULL);
                    }
                    pfree(identity);
                } else identityLink = &identity->next;
            }
            LakeTxnRow** rowLink = &table->rows;
            table->tail = NULL;
            table->active_rows = 0;
            while (*rowLink != NULL) {
                LakeTxnRow* row = *rowLink;
                if (row->owner_subid == child || removeTable) {
                    *rowLink = row->next;
                    (void)hash_search(table->token_index, &row->local_token, HASH_REMOVE, NULL);
                    Assert(state->staged_bytes >= row->charged_bytes);
                    state->staged_bytes -= row->charged_bytes;
                    if (row->payload_kind==LAKE_PAYLOAD_RESIDENT) {
                        Size payloadBytes=sizeof(HeapTupleData)+(Size)row->payload_length;
                        Assert(state->resident_payload_bytes>=payloadBytes);
                        state->resident_payload_bytes-=payloadBytes;
                        heap_freetuple(row->tuple);
                    }
                    pfree(row);
                } else {
                    if (row->identity->head == row && LakeTxnRowHasPayload(row)) ++table->active_rows;
                    table->tail = row; rowLink = &row->next;
                }
            }
            if(!removeTable&&table->first_modify!=NULL&&table->first_modify->owner_subid==child) {
                /* First admission required a pristine root. All child rows,
                 * identities and attempts have now been removed above, and
                 * aborted read views were released before walking catalogs.
                 * A child-owned root instead deletes its whole context once. */
                LakeFirstModifyBaseline* baseline=table->first_modify;
                table->binding=baseline->previous_binding;table->first_modify=NULL;
                Assert(LakeTxnPristineModifyRoot(table));
                MemoryContextDelete(baseline->context);
            }
            if (removeTable && table->pending != NULL) {
                /* Retain a bounded inactive descriptor for an ancestor portal.
                 * Tokens are removed, rows already reclaimed, active is checked
                 * before any reader touches a carrier or catalog snapshot. */
                LakePendingTable* pending = table->pending;
                pending->state = LAKE_OBJECT_ABORTED;
                table->active = false;
                (void)hash_search(state->objects_by_oid, &pending->relation_oid, HASH_REMOVE, NULL);
                (void)hash_search(state->objects_by_token, &pending->object_token, HASH_REMOVE, NULL);
                table->owner_subid = InvalidSubTransactionId;
                pending->owner_subid = InvalidSubTransactionId;
                tableLink = &table->next;
            } else if (removeTable) {
                *tableLink = table->next;
                LakeSnapshotPinUnregisterHandle((uint64)table);
                if(table->statement_snapshot!=NULL)UnregisterSnapshotFromOwner(table->statement_snapshot,t_thrd.utils_cxt.TopTransactionResourceOwner);
                MemoryContextDelete(table->context);
            } else {
                remainingWrites |= table->active && (table->pending != NULL ||
                    table->rows != NULL || table->delete_attempts != NULL);
                tableLink = &table->next;
            }
        }
        if (catalog->owner_subid == child) {
            const bool wasBound = catalog->snapshot_bound;
            LakeSnapshotPinUnregisterHandle((uint64)catalog);
            UnregisterSnapshotFromOwner(catalog->business_snapshot, t_thrd.utils_cxt.TopTransactionResourceOwner);
            catalog->business_snapshot = NULL;
            if (catalog->tables != NULL) {
                /* Only bounded inactive LOCAL sentinels remain here. */
                catalog->active = false;
                catalog->owner_subid = InvalidSubTransactionId;
                catalogLink = &catalog->next;
            } else {
                *catalogLink = catalog->next;
                MemoryContextDelete(catalog->context);
            }
            if (wasBound && !t_thrd.proc_cxt.proc_exit_inprogress)
                ResetPlanCache();
        } else catalogLink = &catalog->next;
    }
    if (!remainingWrites && state->flush==NULL && state->rewrite==NULL && state->renames==NULL && state->drops==NULL) state->writable_server = InvalidOid;
}

void LakeTxnForgetReadOnlyTableForRename(Oid relationOid)
{
    LakeTxnState* state=LakeGetTransactionState();
    for(LakeTxnTable* view=state->read_views;view!=NULL;view=view->read_next)
        if(view->binding->relation_oid==relationOid)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_IN_USE),errmsg("cannot rename a lake column while this transaction has an active reader")));
    for(LakeTxnCatalog* catalog=state->catalogs;catalog!=NULL;catalog=catalog->next) {
        LakeTxnTable** link=&catalog->tables;
        while(*link!=NULL) {
            LakeTxnTable* table=*link;
            if(table->binding->relation_oid!=relationOid){link=&table->next;continue;}
            if(table->pending!=NULL || table->rows!=NULL || table->delete_attempts!=NULL || table->identities!=NULL ||
               table->active_rows!=0 || hash_get_num_entries(table->origin_index)!=0 ||
               hash_get_num_entries(table->logical_origin_index)!=0 || hash_get_num_entries(table->token_index)!=0)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake column RENAME cannot forget a writable or local table")));
            *link=table->next;
            if(table->statement_snapshot!=NULL)
                UnregisterSnapshotFromOwner(table->statement_snapshot,t_thrd.utils_cxt.TopTransactionResourceOwner);
            MemoryContextDelete(table->context);
            /* Keep the catalog and its numeric read/publication baseline. */
        }
    }
}

LakeTxnState* LakeTxnBeginDrop(Oid serverOid)
{
    LakeTxnRejectBoundSnapshotWrite(serverOid);
    LakeTxnState* state=LakeGetTransactionState();
    if(state->phase!=LAKE_TXN_ACTIVE||state->binding_ddl||state->partition_ddl||state->flush||state->rewrite||state->renames||
       (OidIsValid(state->writable_server)&&state->drops==NULL))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake logical DROP cannot mix with lake DML, CREATE, binding or definition DDL, or maintenance")));
    if(OidIsValid(state->writable_server)&&state->writable_server!=serverOid)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("one transaction cannot drop tables in two lake catalogs")));
    int count=0;for(LakeDropEvent* event=state->drops;event;event=event->next)++count;
    if(count>=64)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake DROP journal exceeds64 operations")));
    return state;
}

void LakeTxnAdoptDrop(LakeDropEvent* event)
{
    LakeTxnState* state=LakeTxnBeginDrop(event->binding->server_oid);
    for(LakeDropEvent* prior=state->drops;prior;prior=prior->next)
        if(prior->binding->relation_oid==event->binding->relation_oid||
           LakePermanentTableId(prior->binding)==LakePermanentTableId(event->binding))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake DROP event")));
    if(event->charged_bytes>LAKE_INLINE_TXN_MEMORY_LIMIT||
       state->staged_bytes>LAKE_INLINE_TXN_MEMORY_LIMIT-event->charged_bytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake DROP journal exceeds the transaction memory limit")));
    LakeHostReserveDeferredPublish(event->binding, event->owner_subid);
    event->next=state->drops;state->drops=event;
    state->staged_bytes+=event->charged_bytes;
    state->writable_server=event->binding->server_oid;
}

LakeTxnState* LakeTxnBeginRename(Oid serverOid)
{
    LakeTxnRejectBoundSnapshotWrite(serverOid);
    LakeTxnState* state=LakeGetTransactionState();
    if(state->phase!=LAKE_TXN_ACTIVE || state->binding_ddl!=NULL || state->flush!=NULL || state->rewrite!=NULL || state->drops!=NULL ||
       (OidIsValid(state->writable_server) && state->renames==NULL))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake RENAME cannot mix with lake DML, CREATE, binding or configuration DDL, or flush")));
    if(OidIsValid(state->writable_server) && state->writable_server!=serverOid)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("one transaction cannot rename tables in two lake catalogs")));
    int count=0;
    for(LakeRenameEvent* event=state->renames;event!=NULL;event=event->next) ++count;
    if(count>=64)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake RENAME journal exceeds64 operations")));
    return state;
}

void LakeTxnAdoptRename(LakeRenameEvent* event)
{
    LakeTxnState* state=LakeTxnBeginRename(event->binding->server_oid);
    if(event->charged_bytes>LAKE_INLINE_TXN_MEMORY_LIMIT ||
       state->staged_bytes>LAKE_INLINE_TXN_MEMORY_LIMIT-event->charged_bytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake RENAME journal exceeds the transaction memory limit")));
    LakeHostReserveDeferredPublish(event->binding, event->owner_subid);
    event->next=state->renames;
    state->renames=event;
    state->staged_bytes+=event->charged_bytes;
    state->writable_server=event->binding->server_oid;
}

void LakeTxnReleaseSnapshots(void* pointer)
{
    LakeTxnState* state = (LakeTxnState*)pointer;
    if (state == NULL) return;
    bool resetPlans = false;
    for(LakeTxnTable* view=state->read_views;view!=NULL;view=view->read_next)
        if(view->statement_snapshot!=NULL) {
            UnregisterSnapshotFromOwner(view->statement_snapshot,t_thrd.utils_cxt.TopTransactionResourceOwner);
            view->statement_snapshot=NULL;
        }
    for (LakeTxnCatalog* catalog = state->catalogs; catalog != NULL; catalog = catalog->next) {
        if (catalog->snapshot_bound)
            resetPlans = true;
        for(LakeTxnTable* table=catalog->tables;table!=NULL;table=table->next)
            if(table->statement_snapshot!=NULL) {
                UnregisterSnapshotFromOwner(table->statement_snapshot,t_thrd.utils_cxt.TopTransactionResourceOwner);
                table->statement_snapshot=NULL;
            }
        if (catalog->business_snapshot != NULL) {
            UnregisterSnapshotFromOwner(catalog->business_snapshot, t_thrd.utils_cxt.TopTransactionResourceOwner);
            catalog->business_snapshot = NULL;
        }
    }
    if (resetPlans && !t_thrd.proc_cxt.proc_exit_inprogress)
        ResetPlanCache();
}

LakeTxnState* LakeTxnMarkPartitionDefinition(void)
{
    LakeTxnRejectBoundSnapshotWrite(InvalidOid);
    LakeTxnState* state=LakeGetTransactionState();
    if(state->partition_ddl!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("one identity partition SET/RESET is supported per host transaction"),
            errhint("Commit or roll back before a second partition operation; this slice does not split one host commit into multiple lake snapshots.")));
    LakePartitionDdl* marker=(LakePartitionDdl*)MemoryContextAllocZero(state->context,sizeof(LakePartitionDdl));
    marker->owner_subid=GetCurrentSubTransactionId();marker->next=state->partition_ddl;state->partition_ddl=marker;
    return state;
}

LakeTxnState* LakeTxnBeginColumnDefinition(void)
{
    LakeTxnState* state=LakeGetTransactionState();
    if(state->phase!=LAKE_TXN_ACTIVE||state->binding_ddl||state->partition_ddl||state->flush||state->rewrite||
       state->renames||state->drops||state->object_events||OidIsValid(state->writable_server))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ADD COLUMN cannot mix with lake writes, maintenance or other lake DDL")));
    /* Both markers belong to the USER subtransaction, never a claim attempt. */
    LakeTxnRejectBindingDdl();
    state=LakeTxnMarkPartitionDefinition();
    state->partition_ddl->column_definition=true;
    return state;
}

void LakeTxnRejectBindingDdl(void)
{
    LakeTxnRejectBoundSnapshotWrite(InvalidOid);
    LakeTxnState* state = LakeGetTransactionState();
    for(LakePartitionDdl* marker=state->partition_ddl;marker!=NULL;marker=marker->next)
        if(marker->column_definition)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake binding DDL cannot follow ADD COLUMN in the same transaction")));
    if (OidIsValid(state->writable_server))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake binding DDL cannot follow a lake write in the same transaction")));
    SubTransactionId subid = GetCurrentSubTransactionId();
    for (LakeBindingDdl* ddl = state->binding_ddl; ddl != NULL; ddl = ddl->next)
        if (ddl->owner_subid == subid) return;
    LakeBindingDdl* ddl = (LakeBindingDdl*)MemoryContextAllocZero(state->context, sizeof(LakeBindingDdl));
    ddl->owner_subid = subid;
    ddl->next = state->binding_ddl;
    state->binding_ddl = ddl;
}

void LakeTxnMarkSnapshotExpiry(void)
{
    LakeTxnState* state = LakeGetTransactionState();
    SubTransactionId subid = GetCurrentSubTransactionId();
    for (LakeExpiryEvent* ev = state->expiry_events; ev != NULL; ev = ev->next)
        if (ev->owner_subid == subid) return;
    LakeExpiryEvent* ev = (LakeExpiryEvent*)MemoryContextAllocZero(state->context, sizeof(LakeExpiryEvent));
    ev->owner_subid = subid;
    ev->next = state->expiry_events;
    state->expiry_events = ev;
}

bool LakeTxnHasSnapshotExpiry(void)
{
    LakeTxnState* state = LakeHostExistingTransactionState();
    return state != NULL && state->expiry_events != NULL;
}

void LakeTxnEnd(void** address)
{
    LakeTxnState* state = (LakeTxnState*)*address;
    if (state == NULL) return;
    *address = NULL;
    /* COMMIT/ABORT released our snapshot references; BEFORE_LOCKS drained other readers. */
    MemoryContextDelete(state->context);
}

MemoryContext LakeTxnBeginFlush(LakeTxnTable* table)
{
    LakeTxnRequireActiveTable(table);
    LakeTxnRejectBoundSnapshotWrite(table->binding->server_oid);
    LakeTxnState* state=LakeGetTransactionState();
    for(int i=0;i<table->binding->column_count;++i){
        Oid type=table->binding->columns[i].type;LakeNativeTypeRequireWrite(type);
        if(type==VECTOROID){
            LakeNativeTypeRequireSdk(type,LakeFileProvider());(void)LakeFileNestedProvider();
        }
    }
    if(state->phase!=LAKE_TXN_ACTIVE||OidIsValid(state->writable_server)||state->binding_ddl!=NULL||state->flush!=NULL||state->rewrite!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake flush requires its own lake maintenance transaction")));
    if(table->pending!=NULL||table->binding->read_only||!OidIsValid(LakeMetadataOwner()))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake flush requires a writable permanent binding and private owner")));
    if(u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake flush requires READ COMMITTED")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot flush native lake in a read-only transaction")));
    LakeHostReserveDeferredPublish(table->binding, GetCurrentSubTransactionId());
    state->writable_server=table->binding->server_oid;
    return state->context;
}

void LakeTxnAdoptFlush(LakeFlushOperation* operation)
{
    LakeTxnState* state=LakeGetTransactionState();
    if(operation==NULL||state->flush!=NULL||state->rewrite!=NULL||state->phase!=LAKE_TXN_ACTIVE||
        state->writable_server!=operation->table->binding->server_oid)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("invalid native flush preparation adoption")));
    state->flush=operation;
}

bool LakeTxnStreamReadEligible(void)
{
    auto* state=(LakeTxnState*)*LakeHostTransactionStateAddress();
    LakeTxnPayloadCheck(state);
    if(!state)return true;
    if(state->phase!=LAKE_TXN_ACTIVE||OidIsValid(state->writable_server)||state->binding_ddl||state->flush||state->rewrite||state->renames||state->drops||state->object_events)return false;
    for(LakeTxnCatalog* c=state->catalogs;c;c=c->next){
        if(c->active&&c->snapshot_bound)return false;
        for(LakeTxnTable* t=c->tables;t;t=t->next)
            if(t->pending||LakeTxnTableHasChanges(t)||t->delete_attempts)return false;
    }
    return true;
}
LakeTxnTable* LakeTxnCreateStreamReadView(LakeInlineBinding* binding,Snapshot sealed,TupleDesc descriptor)
{
    if(!binding||!sealed||binding->identity.kind!=LAKE_TABLE_PERMANENT)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid native lake Stream read binding")));
    auto* table=(LakeTxnTable*)palloc0(sizeof(LakeTxnTable));auto* catalog=(LakeTxnCatalog*)palloc0(sizeof(LakeTxnCatalog));
    table->context=CurrentMemoryContext;table->binding=binding;table->descriptor=CreateTupleDescCopy(descriptor);
    table->catalog=catalog;table->active=true;table->stream_readonly=true;
    catalog->active=true;catalog->server_oid=binding->server_oid;catalog->read_snapshot=binding->snapshot_id;
    /* Register copies in the worker's transaction context, never increment
     * counters in a shared SnapshotData or allocate a registered snapshot in
     * the shorter reader context. The main sealed arrays remain pinned until
     * every worker has released transaction resources and exited. */
    SnapshotData seed=*sealed;seed.copied=false;seed.active_count=seed.regd_count=0;
    table->stream_snapshot_owner=t_thrd.utils_cxt.CurrentResourceOwner;
    table->statement_snapshot=RegisterSnapshotOnOwner(&seed,table->stream_snapshot_owner);
    catalog->business_snapshot=table->statement_snapshot;
    LakeSnapshotPinRegister(binding->server_oid,binding->snapshot_id,"STATEMENT_VIEW",(uint64)table);
    return table;
}

void LakeTxnReleaseStreamReadView(LakeTxnTable* table)
{
    if(!table||!table->stream_readonly||!table->active)return;
    table->active=false;
    LakeSnapshotPinUnregisterHandle((uint64)table);
    Snapshot snapshot=table->statement_snapshot;table->statement_snapshot=NULL;
    if(snapshot)UnregisterSnapshotFromOwner(snapshot,table->stream_snapshot_owner);
}

MemoryContext LakeTxnBeginRewrite(LakeTxnTable* table)
{
    /* Shares only the mature exclusive MAINTENANCE admission, not the flush
     * descriptor or inline-prefix publication/cleanup semantics. */
    return LakeTxnBeginFlush(table);
}
void LakeTxnAdoptRewrite(LakeRewriteOperation* operation)
{
    LakeTxnState* state=LakeGetTransactionState();
    if(!operation||state->flush||state->rewrite||state->renames||state->phase!=LAKE_TXN_ACTIVE||
       state->writable_server!=operation->table->binding->server_oid)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("invalid native rewrite adoption")));
    state->rewrite=operation;
}

/* Parent readonly scans are ordinary executor consumers, not Stream workers.
 * es_snapshot is owned by the QueryDesc/portal and stays valid until End;
 * borrowing avoids a second unregister path during ResourceOwner subabort.
 * The leaf cannot have own business writes: parent DML and alternate host
 * binding are rejected. No relation_oid-keyed root journal is created. */
LakeTxnTable* LakeTxnCreateQueryReadView(LakeInlineBinding* binding,Snapshot snapshot,TupleDesc descriptor)
{
    (void)LakeHostTransactionStateAddress();
    if(!binding || !snapshot || !descriptor || binding->identity.kind!=LAKE_TABLE_PERMANENT || !binding->read_only)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid native partition readonly view")));
    LakeTxnTable* table=(LakeTxnTable*)palloc0(sizeof(LakeTxnTable));
    LakeTxnCatalog* catalog=(LakeTxnCatalog*)palloc0(sizeof(LakeTxnCatalog));
    table->context=CurrentMemoryContext;table->binding=binding;table->descriptor=CreateTupleDescCopy(descriptor);
    table->catalog=catalog;table->active=true;table->stream_readonly=true;
    table->statement_snapshot=snapshot;
    catalog->active=true;catalog->server_oid=binding->server_oid;catalog->read_snapshot=binding->snapshot_id;
    catalog->business_snapshot=snapshot;
    LakeSnapshotPinRegister(binding->server_oid,binding->snapshot_id,"STATEMENT_VIEW",(uint64)table);
    return table;
}
