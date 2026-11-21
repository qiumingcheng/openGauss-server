/* Controlled diagnostics for the native lake callback-order acceptance tests. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_pq_test.h"
#include "lake_row_lock.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "storage/lock/lock.h"
#include "pgxc/pgxc.h"
#include "threadpool/threadpool.h"

enum LakeAnalyzeProbeState {
    LAKE_ANALYZE_PROBE_OFF,
    LAKE_ANALYZE_PROBE_ARMING,
    LAKE_ANALYZE_PROBE_READY
};

enum LakeRowHeadProbeState {
    LAKE_ROWHEAD_OFF,LAKE_ROWHEAD_ARMING,LAKE_ROWHEAD_READY,LAKE_ROWHEAD_RUNNING
};

struct LakeCommitProbe {
    LakeRowHeadProbeState rowhead_state;
    Oid rowhead_database,rowhead_user,rowhead_outer,rowhead_relation;
    uint64 rowhead_session;
    int64 rowhead_row,rowhead_gate;
    char rowhead_binding[37];
    uint64 rowhead_attempts,rowhead_ensures,rowhead_inserted,rowhead_zero,rowhead_success,rowhead_exhausts,rowhead_aborts;
    uint64 rowhead_before_gates,rowhead_after_gates;
    int pq_mode;
    int64 pq_source,pq_gate;
    Oid pq_database,pq_user,pq_outer;
    uint64 pq_active,pq_opened,pq_closed,pq_destroyed,pq_owner,pq_context,pq_explicit,pq_reparent,pq_restart,pq_subcommit,pq_violations,pq_distances;
    uint32 pq_distance_countdown;
    uint64 ann_cache_live,ann_cache_opened,ann_cache_closed,ann_cache_loads,ann_cache_hits,ann_cache_evictions,ann_cache_queries,ann_cache_bytes,ann_cache_peak,ann_cache_reparent,ann_cache_budget;
    bool xact_registered;
    bool subxact_registered;
    bool active;
    SubTransactionId owner_subid;
    char* sql;
    LakeAnalyzeProbeState analyze_state;
    Oid analyze_database;
    Oid analyze_session_user;
    Oid analyze_outer_user;
    Oid analyze_relation;
    uint64 analyze_after_rows;
    int64 analyze_lock_key;
};

static void LakeDisarmAnalyzeProbe(LakeCommitProbe* probe)
{
    probe->analyze_state = LAKE_ANALYZE_PROBE_OFF;
    probe->analyze_database = InvalidOid;
    probe->analyze_session_user = InvalidOid;
    probe->analyze_outer_user = InvalidOid;
    probe->analyze_relation = InvalidOid;
    probe->analyze_after_rows = 0;
    probe->analyze_lock_key = 0;
}

static void LakeCommitProbeCallback(XactEvent event, void* argument)
{
    LakeCommitProbe* probe=(LakeCommitProbe*)argument;
    if(event==XACT_EVENT_COMMIT) {
        if(probe->rowhead_state==LAKE_ROWHEAD_ARMING) probe->rowhead_state=LAKE_ROWHEAD_READY;
        else probe->rowhead_state=LAKE_ROWHEAD_OFF;
        probe->active=false;
        /* Only the arming statement's successful commit crosses a transaction
         * boundary. The immediately following transaction consumes or expires
         * READY, including an unrelated SELECT or an empty ANALYZE. */
        if(probe->analyze_state==LAKE_ANALYZE_PROBE_ARMING)
            probe->analyze_state=LAKE_ANALYZE_PROBE_READY;
        else
            LakeDisarmAnalyzeProbe(probe);
        return;
    }
    if(event==XACT_EVENT_ABORT || event==XACT_EVENT_PREPARE) {
        if(event==XACT_EVENT_ABORT && probe->rowhead_state==LAKE_ROWHEAD_RUNNING) ++probe->rowhead_aborts;
        probe->rowhead_state=LAKE_ROWHEAD_OFF;
        probe->pq_mode=0;probe->pq_distance_countdown=0;
        probe->active=false;
        LakeDisarmAnalyzeProbe(probe);
        return;
    }
    if(!probe->active || (event!=XACT_EVENT_PRE_COMMIT && event!=XACT_EVENT_PRE_PREPARE)) return;
    probe->active=false;
    volatile bool connected=false;
    PG_TRY();
    {
        if(SPI_connect()!=SPI_OK_CONNECT)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("cannot connect native lake callback probe to SPI")));
        connected=true;
        /* The transaction phase guard must reject this real INSERT with 0A000.
         * Do not manufacture an error here: success would expose a failed guard. */
        int status=SPI_execute(probe->sql,false,0);
        if(status!=SPI_OK_INSERT)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("unexpected native lake callback probe status")));
        SPI_finish(); connected=false;
    }
    PG_CATCH();
    {
        if(connected) SPI_finish();
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void LakeCommitProbeSubCallback(SubXactEvent event, SubTransactionId child,
    SubTransactionId parent, void* argument)
{
    LakeCommitProbe* probe=(LakeCommitProbe*)argument;
    if(probe->owner_subid==child) {
        if(event==SUBXACT_EVENT_ABORT_SUB) probe->active=false;
        else if(event==SUBXACT_EVENT_COMMIT_SUB) probe->owner_subid=parent;
    }
    /* Arming in a subtransaction is forbidden. Any later subabort expires a
     * pending diagnostic instead of allowing caught errors to carry it on. */
    if(event==SUBXACT_EVENT_ABORT_SUB) LakeDisarmAnalyzeProbe(probe);
    if(event==SUBXACT_EVENT_ABORT_SUB) {
        if(probe->rowhead_state==LAKE_ROWHEAD_RUNNING) ++probe->rowhead_aborts;
        probe->rowhead_state=LAKE_ROWHEAD_OFF;
    }
}

extern "C" Datum og_lake_test_precommit_insert(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_test_precommit_insert);
Datum og_lake_test_precommit_insert(PG_FUNCTION_ARGS)
{
    if(!superuser())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native lake callback diagnostics require superuser privileges")));
    int mode=PG_GETARG_INT32(1);
    if(mode<0 || mode>2)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native lake callback probe mode must be 0, 1 or 2")));
    if(mode!=1 && u_sess->og_lake_state!=NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("first-access and after-lake probes require a fresh session")));
    Oid target=PG_GETARG_OID(0);
    Relation relation=heap_open(target,AccessShareLock);
    if(relation->rd_rel->relkind!=RELKIND_FOREIGN_TABLE || !IsSpecifiedFDWFromRelid(target,"og_lake_fdw"))
        ereport(ERROR,(errcode(ERRCODE_WRONG_OBJECT_TYPE),errmsg("native lake callback probe requires an og_lake foreign table")));
    char* sql=psprintf("INSERT INTO %s DEFAULT VALUES",quote_qualified_identifier(
        get_namespace_name(RelationGetNamespace(relation)),RelationGetRelationName(relation)));
    heap_close(relation,NoLock);
    LakeCommitProbe* probe=(LakeCommitProbe*)u_sess->og_lake_test_probe;
    if(probe==NULL) {
        probe=(LakeCommitProbe*)MemoryContextAllocZero(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),sizeof(LakeCommitProbe));
        u_sess->og_lake_test_probe=probe;
    }
    if(probe->active || probe->analyze_state!=LAKE_ANALYZE_PROBE_OFF)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake callback probe is already armed")));
    /* Safe here outside callback iteration; never unregister from itself. */
    if(probe->xact_registered) {
        UnregisterXactCallback(LakeCommitProbeCallback,probe);
        probe->xact_registered=false;
    }
    if(probe->subxact_registered) {
        UnregisterSubXactCallback(LakeCommitProbeSubCallback,probe);
        probe->subxact_registered=false;
    }
    if(probe->sql!=NULL) pfree(probe->sql);
    probe->sql=MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),sql);
    probe->owner_subid=GetCurrentSubTransactionId();
    if(mode==1) (void)LakeHostStateJson(); /* Register lake first: probe runs first. */
    RegisterXactCallback(LakeCommitProbeCallback,probe);
    probe->xact_registered=true;
    RegisterSubXactCallback(LakeCommitProbeSubCallback,probe);
    probe->subxact_registered=true;
    if(mode==2) (void)LakeHostStateJson(); /* Register lake last: probe runs last. */
    probe->active=true;
    PG_RETURN_VOID();
}

/* Test-only SQL registration is created inside the owned acceptance database.
 * This uses the same codec and binding validator as ForeignModify. It does not
 * resolve old-row ownership, stage a write, or make a guessed source row real.
 * NULL input produces a synthetic runtime INLINE locator with row_id=0 and a
 * real catalog/table/snapshot/session binding; it changes no standard records.
 */
extern "C" Datum og_lake_test_locator(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_test_locator);
Datum og_lake_test_locator(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("native lake locator diagnostics require superuser privileges")));
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("native lake locator diagnostic target is NULL")));
    LakeRequireWriteVersion(CMD_UPDATE);
    Oid target = PG_GETARG_OID(0);
    Relation relation = heap_open(target, AccessShareLock);
    if (relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE || !IsSpecifiedFDWFromRelid(target, "og_lake_fdw"))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("native lake locator diagnostic requires an og_lake foreign table")));
    heap_close(relation, NoLock);
    LakeTxnTable* table = LakeTxnReadStatementTable(target,
        ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot());
    uint64 nonce = LakeTxnNonce();
    CommandId command = GetCurrentCommandId(false);
    Datum input;
    if (PG_ARGISNULL(1)) {
        if (!OidIsValid(table->binding->inline_oid))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake locator diagnostic needs an existing inline carrier")));
        LakeRowLocator seed;
        errno_t rc = memset_s(&seed, sizeof(seed), 0, sizeof(seed));
        securec_check(rc, "", "");
        seed.source = table->pending==NULL ? LAKE_LOCATOR_INLINE : LAKE_LOCATOR_TXN;
        seed.database_oid = u_sess->proc_cxt.MyDatabaseId;
        seed.relation_oid = table->binding->relation_oid;
        seed.server_oid = table->binding->server_oid;
        seed.schema_version = table->binding->schema_version;
        seed.identity = table->binding->identity;
        seed.begin_snapshot = table->pending==NULL ? table->binding->snapshot_id : 0;
        seed.read_snapshot = table->binding->snapshot_id;
        seed.inline_oid = table->pending==NULL ? table->binding->inline_oid : InvalidOid;
        if(table->pending!=NULL) {
            LakeTxnRow* cursor=LakeTxnJournalTable(table)->rows;uint64 last=0,generation=0;
            LakeTxnRow* actual=LakeTxnNextRow(table,&cursor,&last,&generation,command);
            if(actual==NULL) ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("local lake locator diagnostic needs a real visible private row")));
            seed.local_version_token=actual->local_token;
        }
        seed.transaction_nonce = nonce;
        seed.producer_command_id = command;
        input = PointerGetDatum(LakeEncodeRowLocator(&seed));
    } else {
        input = PG_GETARG_DATUM(1);
    }
    LakeRowLocator decoded = LakeDecodeRowLocator(input);
    LakeValidateInlineLocator(&decoded, table, nonce, command);
    if(table->pending!=NULL) (void)LakeTxnResolvePrivateVersion(table,&decoded,command);
    bytea* result=LakeEncodeRowLocator(&decoded);
    LakeTxnReleaseReadView(table);
    PG_RETURN_BYTEA_P(result);
}

/* No SQL entry is installed by the extension. The owned acceptance database
 * registers this C function explicitly, with execution restricted to its admin.
 * Arm in an autocommit SELECT, then issue one single-table top-level ANALYZE
 * without any intervening command on this session. (0,0,0) explicitly disarms.
 * The host forbids ANALYZE inside a user transaction block; do not bypass it. */
extern "C" Datum og_lake_test_analyze_pause(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_test_analyze_pause);
Datum og_lake_test_analyze_pause(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("native lake analyze diagnostics require superuser privileges")));
    if (!IS_SINGLE_NODE || StreamThreadAmI())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake analyze diagnostics require the single-node backend")));
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("native lake analyze diagnostic arguments cannot be NULL")));
    Oid target = PG_GETARG_OID(0);
    int64 afterRows = PG_GETARG_INT64(1);
    int64 key = PG_GETARG_INT64(2);
    LakeCommitProbe* probe = (LakeCommitProbe*)u_sess->og_lake_test_probe;
    if (!OidIsValid(target) && afterRows == 0 && key == 0) {
        if (probe != NULL) LakeDisarmAnalyzeProbe(probe);
        PG_RETURN_VOID();
    }
    if (IsTransactionBlock() || IsSubTransaction())
        ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
            errmsg("native lake analyze diagnostics must be armed by an autocommit statement")));
    if (u_sess->og_lake_precommit_callbacks)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake analyze diagnostics cannot be armed inside transaction callbacks")));
    if (GetUserId() != GetOuterUserId())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake analyze diagnostics require the outer command identity")));
    if (!OidIsValid(target))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("native lake analyze diagnostic target is invalid")));
    if (afterRows < 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("native lake analyze diagnostic row count must be positive")));
    Relation relation = heap_open(target, AccessShareLock);
    bool native = LakeIsNativeForeignRelation(relation) ||
        (LakeHasNativePartitionMarker(relation) && LakeNativePartitionAnalyzeVersionEnabled());
    heap_close(relation, NoLock);
    if (!native)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("native lake analyze diagnostic requires an og_lake foreign table")));
    if (probe == NULL) {
        probe = (LakeCommitProbe*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(LakeCommitProbe));
        u_sess->og_lake_test_probe = probe;
    }
    if (probe->analyze_state != LAKE_ANALYZE_PROBE_OFF || probe->active)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake diagnostic probe is already armed")));
    if (!probe->xact_registered) {
        RegisterXactCallback(LakeCommitProbeCallback, probe);
        probe->xact_registered = true;
    }
    if (!probe->subxact_registered) {
        RegisterSubXactCallback(LakeCommitProbeSubCallback, probe);
        probe->subxact_registered = true;
    }
    probe->analyze_database = u_sess->proc_cxt.MyDatabaseId;
    probe->analyze_session_user = GetSessionUserId();
    probe->analyze_outer_user = GetOuterUserId();
    probe->analyze_relation = target;
    probe->analyze_after_rows = (uint64)afterRows;
    probe->analyze_lock_key = key;
    probe->analyze_state = LAKE_ANALYZE_PROBE_ARMING;
    PG_RETURN_VOID();
}

/* Called only after a real visible row has entered the ANALYZE stream and
 * before any host statistics are written. The observer owns the same advisory
 * key and verifies our exact ungranted pg_locks row before release/cancel. */
void LakeTestAnalyzeAfterRow(Oid relationOid, uint64 visibleRows)
{
    LakeCommitProbe* probe = (LakeCommitProbe*)u_sess->og_lake_test_probe;
    if (probe == NULL || probe->analyze_state != LAKE_ANALYZE_PROBE_READY)
        return;
    if (probe->analyze_relation != relationOid) {
        LakeDisarmAnalyzeProbe(probe);
        return;
    }
    /* Native ANALYZE deliberately executes as the relation owner. Compare
     * the unchanged outer command identity, not its temporary effective role. */
    if (probe->analyze_database != u_sess->proc_cxt.MyDatabaseId ||
        probe->analyze_session_user != GetSessionUserId() ||
        probe->analyze_outer_user != GetOuterUserId() ||
        !(superuser_arg(GetOuterUserId()) || systemDBA_arg(GetOuterUserId()))) {
        LakeDisarmAnalyzeProbe(probe);
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("native lake analyze diagnostic session identity changed")));
    }
    if (visibleRows < probe->analyze_after_rows) return;
    /* One-shot even when LockAcquire is interrupted. No metadata SPI or role
     * switch runs in this diagnostic path. Lock ownership remains transactional. */
    uint64 key = (uint64)probe->analyze_lock_key;
    LakeDisarmAnalyzeProbe(probe);
    LOCKTAG tag;
    SET_LOCKTAG_ADVISORY(tag, u_sess->proc_cxt.MyDatabaseId,
        (uint32)(key >> 32), (uint32)key, 1);
    (void)LockAcquire(&tag, ExclusiveLock, false, false);
    if (!LockRelease(&tag, ExclusiveLock, false))
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("native lake analyze diagnostic lock was not acquired")));
    CHECK_FOR_INTERRUPTS();
}

/* The acceptance DB explicitly registers and revokes this private symbol.
 * mode0 reads counters, -1 resets only when no live lease, positive modes arm
 * exactly one matching physical source under unchanged session identity. */
extern "C" Datum og_lake_test_pq_probe(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_test_pq_probe);
Datum og_lake_test_pq_probe(PG_FUNCTION_ARGS)
{
    if(!superuser()||GetUserId()!=GetOuterUserId())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native lake PQ diagnostics require the outer superuser")));
    if(!IS_SINGLE_NODE||StreamThreadAmI())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake PQ diagnostics require a single-node ordinary backend")));
    if(PG_ARGISNULL(0)||PG_ARGISNULL(1)||PG_ARGISNULL(2))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("PQ probe arguments cannot be NULL")));
    int mode=PG_GETARG_INT32(0);int64 source=PG_GETARG_INT64(1),gate=PG_GETARG_INT64(2);
    if(mode<-1||mode>13||mode==4||(mode>0&&(source<0||((mode==1||mode==12)&&gate<=0)||(mode>=7&&gate<0))))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid PQ diagnostic mode/source/gate")));
    auto* probe=(LakeCommitProbe*)u_sess->og_lake_test_probe;
    if(!probe){probe=(LakeCommitProbe*)MemoryContextAllocZero(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),sizeof(LakeCommitProbe));u_sess->og_lake_test_probe=probe;}
    if(mode!=0&&(probe->pq_active||probe->ann_cache_live||probe->ann_cache_bytes))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("cannot arm/reset PQ diagnostics while a lease is live")));
    if(mode==-1){probe->pq_mode=0;probe->pq_distance_countdown=0;
        probe->pq_active=probe->pq_opened=probe->pq_closed=probe->pq_destroyed=probe->pq_owner=probe->pq_context=probe->pq_explicit=probe->pq_reparent=probe->pq_restart=probe->pq_subcommit=probe->pq_violations=probe->pq_distances=0;
        probe->ann_cache_opened=probe->ann_cache_closed=probe->ann_cache_loads=probe->ann_cache_hits=probe->ann_cache_evictions=probe->ann_cache_queries=probe->ann_cache_bytes=probe->ann_cache_peak=probe->ann_cache_reparent=probe->ann_cache_budget=0;}
    if(mode>0){
        if(probe->pq_mode)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("PQ diagnostic is already armed")));
        probe->pq_mode=mode;probe->pq_source=source;probe->pq_gate=gate;
        probe->pq_database=u_sess->proc_cxt.MyDatabaseId;probe->pq_user=GetSessionUserId();probe->pq_outer=GetOuterUserId();
        if(!probe->xact_registered){RegisterXactCallback(LakeCommitProbeCallback,probe);probe->xact_registered=true;}
    }
    char* output=psprintf("{\"active\":%llu,\"opened\":%llu,\"closed\":%llu,\"destroyed\":%llu,\"owner\":%llu,\"context\":%llu,\"explicit\":%llu,\"reparent\":%llu,\"restart\":%llu,\"subcommit\":%llu,\"violations\":%llu,\"distances\":%llu,\"armed\":%d}",
        (unsigned long long)probe->pq_active,(unsigned long long)probe->pq_opened,(unsigned long long)probe->pq_closed,
        (unsigned long long)probe->pq_destroyed,(unsigned long long)probe->pq_owner,(unsigned long long)probe->pq_context,
        (unsigned long long)probe->pq_explicit,(unsigned long long)probe->pq_reparent,(unsigned long long)probe->pq_restart,
        (unsigned long long)probe->pq_subcommit,(unsigned long long)probe->pq_violations,(unsigned long long)probe->pq_distances,probe->pq_mode);
    output[strlen(output)-1]='\0';
    char* combined=psprintf("%s,\"ann_rescan_cache\":{\"live\":%llu,\"opened\":%llu,\"closed\":%llu,\"loads\":%llu,\"hits\":%llu,\"evictions\":%llu,\"queries\":%llu,\"bytes\":%llu,\"peak_bytes\":%llu,\"reparent\":%llu,\"budget_bypasses\":%llu}}",
        output,(unsigned long long)probe->ann_cache_live,(unsigned long long)probe->ann_cache_opened,(unsigned long long)probe->ann_cache_closed,
        (unsigned long long)probe->ann_cache_loads,(unsigned long long)probe->ann_cache_hits,(unsigned long long)probe->ann_cache_evictions,(unsigned long long)probe->ann_cache_queries,
        (unsigned long long)probe->ann_cache_bytes,(unsigned long long)probe->ann_cache_peak,(unsigned long long)probe->ann_cache_reparent,(unsigned long long)probe->ann_cache_budget);
    PG_RETURN_TEXT_P(cstring_to_text(combined));
}
void LakeTestPqEvent(LakePqTestEvent event)
{
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;if(!p)return;
    switch(event){
        case LAKE_PQ_TEST_OPEN:++p->pq_opened;++p->pq_active;break;
        case LAKE_PQ_TEST_CLOSE:++p->pq_closed;if(p->pq_active)--p->pq_active;else ++p->pq_violations;if(!p->pq_active)p->pq_distance_countdown=0;break;
        case LAKE_PQ_TEST_DESTROY:++p->pq_destroyed;break;
        case LAKE_PQ_TEST_OWNER:++p->pq_owner;break;
        case LAKE_PQ_TEST_CONTEXT:++p->pq_context;break;
        case LAKE_PQ_TEST_EXPLICIT:++p->pq_explicit;break;
        case LAKE_PQ_TEST_REPARENT:++p->pq_reparent;break;
        case LAKE_PQ_TEST_RESTART:++p->pq_restart;break;
        case LAKE_PQ_TEST_SUBCOMMIT:++p->pq_subcommit;break;
        case LAKE_PQ_TEST_ARM_DISTANCE:p->pq_distance_countdown=3;break;
        case LAKE_PQ_TEST_DISTANCE:++p->pq_distances;break;
    }
}
bool LakeTestPqDiagnosticArmed(void)
{auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;return p&&((p->pq_mode>0&&p->pq_mode<10)||p->pq_active||p->pq_distance_countdown);}
bool LakeTestAnnCacheObserved(void){return u_sess->og_lake_test_probe!=NULL;}
bool LakeTestAnnCacheDiagnosticPending(void)
{
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;
    return p&&p->pq_mode>=10&&p->pq_mode<=13;
}
int LakeTestAnnCacheTakeMode(int64 source,int64* gate,bool beforeLoad)
{
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;
    if(!LakeTestAnnCacheDiagnosticPending()||p->pq_source!=source||((p->pq_mode==13)!=beforeLoad))return 0;
    if(p->pq_database!=u_sess->proc_cxt.MyDatabaseId||p->pq_user!=GetSessionUserId()||p->pq_outer!=GetOuterUserId()||!superuser()){
        p->pq_mode=0;ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("cache diagnostic identity changed")));}
    int mode=p->pq_mode;p->pq_mode=0;*gate=p->pq_gate;return mode;
}
void LakeTestAnnCachePause(int64 gate)
{
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;
    if(!p||p->ann_cache_live==0||p->ann_cache_bytes==0||gate<=0)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("cache gate requires a real live immutable image")));
    LOCKTAG tag;uint64 key=(uint64)gate;
    SET_LOCKTAG_ADVISORY(tag,u_sess->proc_cxt.MyDatabaseId,(uint32)(key>>32),(uint32)key,1);
    (void)LockAcquire(&tag,ExclusiveLock,false,false);
    if(!LockRelease(&tag,ExclusiveLock,false))ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("cache diagnostic gate lock not held")));
    CHECK_FOR_INTERRUPTS();
}
void LakeTestAnnCacheEvent(LakeAnnCacheEvent event,uint64 value)
{
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;if(!p)return;
    switch(event){
        case LAKE_ANN_CACHE_OPEN:++p->ann_cache_live;++p->ann_cache_opened;break;
        case LAKE_ANN_CACHE_CLOSE:Assert(p->ann_cache_live);--p->ann_cache_live;++p->ann_cache_closed;break;
        case LAKE_ANN_CACHE_LOAD:++p->ann_cache_loads;break;
        case LAKE_ANN_CACHE_HIT:++p->ann_cache_hits;break;
        case LAKE_ANN_CACHE_EVICT:++p->ann_cache_evictions;break;
        case LAKE_ANN_CACHE_QUERY:++p->ann_cache_queries;break;
        case LAKE_ANN_CACHE_BUDGET:++p->ann_cache_budget;break;
        case LAKE_ANN_CACHE_ALLOC:p->ann_cache_bytes+=value;p->ann_cache_peak=Max(p->ann_cache_peak,p->ann_cache_bytes);break;
        case LAKE_ANN_CACHE_FREE:Assert(value<=p->ann_cache_bytes);p->ann_cache_bytes-=value;break;
        case LAKE_ANN_CACHE_REPARENT:++p->ann_cache_reparent;break;
    }
}
int LakeTestPqTakeMode(int64 source,int64* gate)
{
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;if(!p||!p->pq_mode||p->pq_mode>=10||p->pq_source!=source)return 0;
    if(p->pq_database!=u_sess->proc_cxt.MyDatabaseId||p->pq_user!=GetSessionUserId()||p->pq_outer!=GetOuterUserId()||!superuser()){
        p->pq_mode=0;ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("PQ diagnostic identity changed")));}
    int mode=p->pq_mode;p->pq_mode=0;*gate=p->pq_gate;return mode;
}
void LakeTestPqPause(int64 gate)
{
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;
    if(!p||p->pq_active!=1||gate<=0)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("PQ diagnostic pause requires exactly one live frontier")));
    LOCKTAG tag;uint64 key=(uint64)gate;
    SET_LOCKTAG_ADVISORY(tag,u_sess->proc_cxt.MyDatabaseId,(uint32)(key>>32),(uint32)key,1);
    (void)LockAcquire(&tag,ExclusiveLock,false,false);
    if(!LockRelease(&tag,ExclusiveLock,false))ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("PQ diagnostic pause lock not held")));
    CHECK_FOR_INTERRUPTS();
}
bool LakeTestPqFailDistance(void)
{
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;
    return p&&p->pq_distance_countdown&&--p->pq_distance_countdown==0;
}

/* Explicitly registered only by an owned acceptance DB. State 1 is the arm
 * command, 2 the next transaction, 3 the actual native ensure loop. Counters
 * survive rollback for inspection, while every end/abort expires the gates.
 * This observer never changes an INSERT or SELECT result. */
static void LakeResetRowHeadProbe(LakeCommitProbe* p)
{
    p->rowhead_state=LAKE_ROWHEAD_OFF;
    p->rowhead_database=p->rowhead_user=p->rowhead_outer=p->rowhead_relation=InvalidOid;
    p->rowhead_session=0;p->rowhead_row=p->rowhead_gate=0;p->rowhead_binding[0]='\0';
    p->rowhead_attempts=p->rowhead_ensures=p->rowhead_inserted=p->rowhead_zero=p->rowhead_success=0;
    p->rowhead_exhausts=p->rowhead_aborts=p->rowhead_before_gates=p->rowhead_after_gates=0;
}

static LakeCommitProbe* LakeBoundRowHeadProbe(Oid relationOid,int64 rowId,const char* binding)
{
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;
    if(p==NULL || (p->rowhead_state!=LAKE_ROWHEAD_READY && p->rowhead_state!=LAKE_ROWHEAD_RUNNING) ||
       p->rowhead_relation!=relationOid || p->rowhead_row!=rowId) return NULL;
    if(p->rowhead_database!=u_sess->proc_cxt.MyDatabaseId ||
       p->rowhead_user!=GetSessionUserId() || p->rowhead_outer!=GetOuterUserId() ||
       p->rowhead_session!=(uint64)t_thrd.proc_cxt.MyProcPid || !superuser() || GetUserId()!=GetOuterUserId()) {
        p->rowhead_state=LAKE_ROWHEAD_OFF;
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native row-head diagnostic session identity changed")));
    }
    if(binding==NULL || strcmp(binding,p->rowhead_binding)!=0) {
        p->rowhead_state=LAKE_ROWHEAD_OFF;
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native row-head diagnostic canonical binding changed")));
    }
    return p;
}

static void LakeRowHeadProbeGate(LakeCommitProbe* p,int attempt,bool after)
{
    uint64 key=(uint64)p->rowhead_gate+(uint64)(attempt-1)*2+(after?1:0);
    LOCKTAG tag;
    /* Standard int8 SQL advisory namespace, exactly pg_advisory_lock(int8).
     * Transaction ownership guarantees cancellation/subabort cleanup. */
    SET_LOCKTAG_ADVISORY(tag,u_sess->proc_cxt.MyDatabaseId,(uint32)(key>>32),(uint32)key,1);
    (void)LockAcquire(&tag,ExclusiveLock,false,false);
    if(!LockRelease(&tag,ExclusiveLock,false))
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native row-head diagnostic gate was not acquired")));
    CHECK_FOR_INTERRUPTS();
}

void LakeTestRowHeadBeforeEnsure(Oid relationOid,int64 rowId,const char* binding,int attempt)
{
    auto* p=LakeBoundRowHeadProbe(relationOid,rowId,binding);if(p==NULL)return;
    if(attempt<1 || attempt>32 || p->rowhead_attempts+1!=(uint64)attempt ||
       p->rowhead_ensures!=p->rowhead_attempts)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native row-head diagnostic attempt order changed")));
    p->rowhead_state=LAKE_ROWHEAD_RUNNING;++p->rowhead_attempts;++p->rowhead_before_gates;
    LakeRowHeadProbeGate(p,attempt,false);
}

void LakeTestRowHeadAfterEnsure(Oid relationOid,int64 rowId,const char* binding,int attempt,uint32 inserted)
{
    auto* p=LakeBoundRowHeadProbe(relationOid,rowId,binding);if(p==NULL)return;
    if(p->rowhead_state!=LAKE_ROWHEAD_RUNNING || p->rowhead_attempts!=(uint64)attempt || inserted>1 ||
       p->rowhead_ensures+1!=p->rowhead_attempts)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native row-head diagnostic ensure result order changed")));
    ++p->rowhead_ensures;p->rowhead_inserted+=inserted;++p->rowhead_after_gates;
    LakeRowHeadProbeGate(p,attempt,true);
}

void LakeTestRowHeadLockResult(Oid relationOid,int64 rowId,const char* binding,int attempt,uint32 count)
{
    auto* p=LakeBoundRowHeadProbe(relationOid,rowId,binding);if(p==NULL)return;
    if(p->rowhead_state!=LAKE_ROWHEAD_RUNNING || p->rowhead_attempts!=(uint64)attempt || p->rowhead_ensures!=p->rowhead_attempts || count>1)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native row-head diagnostic lock result order changed")));
    if(count==0) ++p->rowhead_zero;
    else {++p->rowhead_success;p->rowhead_state=LAKE_ROWHEAD_OFF;}
}

void LakeTestRowHeadExhausted(Oid relationOid,int64 rowId,const char* binding)
{
    auto* p=LakeBoundRowHeadProbe(relationOid,rowId,binding);if(p==NULL)return;
    if(p->rowhead_state!=LAKE_ROWHEAD_RUNNING || p->rowhead_attempts!=32 || p->rowhead_ensures!=32 || p->rowhead_zero!=32 || p->rowhead_success!=0)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native row-head diagnostic did not observe real32-attempt exhaustion")));
    ++p->rowhead_exhausts;
}

extern "C" Datum og_lake_test_rowhead_probe(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_test_rowhead_probe);
Datum og_lake_test_rowhead_probe(PG_FUNCTION_ARGS)
{
    if(!superuser() || GetUserId()!=GetOuterUserId())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native row-head diagnostics require the outer superuser")));
    if(!IS_SINGLE_NODE || StreamThreadAmI() || IS_THREAD_POOL_WORKER)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native row-head diagnostics require the dedicated single-node backend")));
    if(PG_ARGISNULL(0))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("native row-head diagnostic mode cannot be NULL")));
    int mode=PG_GETARG_INT32(0);
    if(mode < -1 || mode > 1)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native row-head diagnostic mode must be -1,0,1")));
    auto* p=(LakeCommitProbe*)u_sess->og_lake_test_probe;
    if(p==NULL) {
        p=(LakeCommitProbe*)MemoryContextAllocZero(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),sizeof(LakeCommitProbe));
        u_sess->og_lake_test_probe=p;
    }
    if(mode!=0 && p->rowhead_state==LAKE_ROWHEAD_RUNNING)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native row-head diagnostic is executing")));
    if(mode==-1) LakeResetRowHeadProbe(p);
    if(mode==1) {
        if(PG_ARGISNULL(1)||PG_ARGISNULL(2)||PG_ARGISNULL(3))
            ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("native row-head diagnostic target,row,gate cannot be NULL")));
        if(IsTransactionBlock()||IsSubTransaction()||u_sess->og_lake_precommit_callbacks)
            ereport(ERROR,(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),errmsg("native row-head diagnostics must be armed by an autocommit command")));
        if(!LakeRowHeadReclaimEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native row-head retry diagnostics require SQL0.18")));
        if(p->rowhead_state!=LAKE_ROWHEAD_OFF || p->active || p->analyze_state!=LAKE_ANALYZE_PROBE_OFF || p->pq_mode || p->pq_active)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("another native diagnostic is armed")));
        Oid relationOid=PG_GETARG_OID(1);int64 rowId=PG_GETARG_INT64(2),gate=PG_GETARG_INT64(3);
        if(rowId<0 || rowId>=LAKE_REFERENCE_LOCAL_ID_START || gate<=0 || gate>PG_INT64_MAX-63)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native row-head diagnostic row/gate range")));
        Relation relation=heap_open(relationOid,AccessShareLock);
        bool native=LakeIsNativeForeignRelation(relation);heap_close(relation,NoLock);
        if(!native)ereport(ERROR,(errcode(ERRCODE_WRONG_OBJECT_TYPE),errmsg("native row-head diagnostic requires a native lake table")));
        Oid type=OIDOID;Datum arg=ObjectIdGetDatum(relationOid);LakeMetaRows* binding=NULL;
        Snapshot fresh=RegisterSnapshot(GetLatestSnapshot());
        PG_TRY();
        {
            binding=LakeMetadataQuery("SELECT h.binding_id::text FROM og_lake_internal.row_lock_binding h "
                "JOIN og_lake_internal.relation_binding r ON r.relation_oid=h.relation_oid AND r.binding_id=h.binding_id "
                "WHERE h.relation_oid=$1",1,&type,&arg,NULL,fresh,true,SPI_OK_SELECT);
            UnregisterSnapshot(fresh);
        }
        PG_CATCH();{UnregisterSnapshot(fresh);PG_RE_THROW();}PG_END_TRY();
        LakeMetadataRequireCount(binding,1,"row-head diagnostic canonical target");
        char* uuid=TextDatumGetCString(LakeMetadataValue(binding,0,1,TEXTOID));
        LakeResetRowHeadProbe(p);
        errno_t rc=strcpy_s(p->rowhead_binding,sizeof(p->rowhead_binding),uuid);securec_check(rc,"","");
        p->rowhead_relation=relationOid;p->rowhead_row=rowId;p->rowhead_gate=gate;
        p->rowhead_database=u_sess->proc_cxt.MyDatabaseId;p->rowhead_user=GetSessionUserId();p->rowhead_outer=GetOuterUserId();
        p->rowhead_session=(uint64)t_thrd.proc_cxt.MyProcPid;
        if(!p->xact_registered){RegisterXactCallback(LakeCommitProbeCallback,p);p->xact_registered=true;}
        if(!p->subxact_registered){RegisterSubXactCallback(LakeCommitProbeSubCallback,p);p->subxact_registered=true;}
        p->rowhead_state=LAKE_ROWHEAD_ARMING;
    }
    char* output=psprintf("{\"rowhead_state\":%d,\"rowhead_relation\":%u,\"rowhead_row\":%lld,\"rowhead_session\":%llu,"
        "\"rowhead_attempts\":%llu,\"rowhead_ensures\":%llu,\"rowhead_inserted\":%llu,\"rowhead_zero\":%llu,"
        "\"rowhead_success\":%llu,\"rowhead_exhausts\":%llu,\"rowhead_aborts\":%llu,\"rowhead_before_gates\":%llu,\"rowhead_after_gates\":%llu}",
        p->rowhead_state,p->rowhead_relation,(long long)p->rowhead_row,(unsigned long long)p->rowhead_session,
        (unsigned long long)p->rowhead_attempts,(unsigned long long)p->rowhead_ensures,(unsigned long long)p->rowhead_inserted,
        (unsigned long long)p->rowhead_zero,(unsigned long long)p->rowhead_success,(unsigned long long)p->rowhead_exhausts,
        (unsigned long long)p->rowhead_aborts,(unsigned long long)p->rowhead_before_gates,(unsigned long long)p->rowhead_after_gates);
    PG_RETURN_TEXT_P(cstring_to_text(output));
}
