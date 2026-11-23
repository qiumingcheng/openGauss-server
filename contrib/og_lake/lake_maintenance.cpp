/* Native launcher/worker maintenance, following openGauss autovacuum lifecycle.
 * SQL reads metadata; pending data is sampled with native TableAM. A worker
 * prepares and commits the same flush operation as the manual entry point.
 */
#include "postgres.h"
#include "lake_inline_sources.h"
#include "lake_autoanalyze.h"
#include "knl/knl_variable.h"
#include "access/og_lake_maintenance.h"
#include "lake_flush.h"
#include "lake_rewrite.h"
#include "lake_version.h"
#include "lake_row_lock.h"
#include "lake_index_maintenance.h"
#include "storage/lock/lock.h"
#include "utils/array.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "commands/dbcommands.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "gssignal/gs_signal.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "access/tupdesc.h"
#include "funcapi.h"
#include "storage/procarray.h"
#include "threadpool/threadpool.h"
#include "utils/tuplestore.h"

extern "C" Datum og_lake_flush(PG_FUNCTION_ARGS);

static bool LakeIndexRoundDue(void);
namespace {
enum LakeWorkPhase { LAKE_WORK_IDLE,LAKE_WORK_REQUESTED,LAKE_WORK_STARTING,LAKE_WORK_RUNNING };
static const int MAX_LAKE_SNAPSHOT_PINS = 256;
struct LakeSnapshotPinEntry {
    bool active;
    bool is_thread_pool;
    Oid database_oid;
    Oid server_oid;
    ThreadId backend_pid;
    uint64 session_id;
    uint64 handle;
    uint64 pin_gen;
    int64 snapshot_id;
    char pin_kind[24];
};
/* Bounded non-authoritative cursors; at most 4096 (DB,index) schedules.
 * Never silently evict a high-file cursor and restart it at a hot low file.
 * Overflow is explicit; definitions/reservations remain durable in SQL. */
struct LakeIndexCursorHint {Oid database;char uuid[37];int64 after,end,generation;uint64 last_used;TimestampTz next_index_at;char definition_after[37];};
struct LakeMaintenanceShared {
    slock_t lock;
    LakeWorkPhase phase;
    Oid database_oid;
    Oid after_relation;
    Oid next_relation;
    ThreadId worker_pid;
    ThreadId early_exit_pid;
    uint64 generation;
    uint64 completed_batches;
    uint64 failed_batches;
    uint64 merge_batches;
    uint64 merge_failed;
    bool job_failed;
    bool job_has_lake;
    bool hints_overflow;
    uint32 hint_count;
    Oid hints[64];
    uint64 index_checked,index_built,index_verified,index_failed,index_retry;
    uint32 index_cursor_count;
    uint64 index_cursor_clock;
    uint64 index_cursor_reclaims;
    uint64 index_cursor_resets;
    uint64 autoanalyze_attempts;
    uint64 autoanalyze_successes;
    uint64 autoanalyze_failures;
    LakeIndexCursorHint index_cursors[4096];
    /* Private one-shot test gate, never installed by extension SQL. */
    Oid index_gate_database;
    char index_gate_uuid[37],index_gate_segment[37],index_gate_token[37];
    int64 index_gate_source,index_gate_key;
    int index_gate_phase;
    Oid analyze_gate_database,analyze_gate_relation;
    int64 analyze_gate_key,analyze_gate_covered,analyze_gate_snapshot;
    int analyze_gate_phase,analyze_gate_error;
    uint64 analyze_gate_hits;
    ThreadId analyze_gate_pid;
    bool analyze_gate_live;
    int index_gate_error;
    int index_gate_action;
    uint64 index_gate_hits;
    ThreadId index_gate_pid;
    uint64 snapshot_pin_clock;
    LakeSnapshotPinEntry snapshot_pins[MAX_LAKE_SNAPSHOT_PINS];
};
struct LakeDatabaseCursor {
    Oid database_oid;
    Oid after_relation;
    TimestampTz retry_after;
    bool error_cooldown;
};
static LakeMaintenanceShared* lakeMaintenance=NULL;
static const int LAKE_SCAN_CANDIDATES=32;
static const int LAKE_ERROR_BACKOFF_MS=30000;

static void LakeMaintenanceSighup(SIGNAL_ARGS)
{
    int saved=errno;t_thrd.worker_sig_flags.got_SIGHUP=true;
    if(t_thrd.proc!=NULL)SetLatch(&t_thrd.proc->procLatch);
    errno=saved;
}
static void LakeMaintenanceSigterm(SIGNAL_ARGS)
{
    int saved=errno;t_thrd.worker_sig_flags.got_SIGTERM=true;
    if(!IsNativeLakeLauncherProcess()) {
        StatementCancelHandler(postgres_signal_arg);
    }
    if(t_thrd.proc!=NULL)SetLatch(&t_thrd.proc->procLatch);
    errno=saved;
}
static void LakeMaintenanceSignals(void)
{
    gspqsignal(SIGHUP,LakeMaintenanceSighup);
    gspqsignal(SIGINT,StatementCancelHandler);
    gspqsignal(SIGTERM,LakeMaintenanceSigterm);
    gspqsignal(SIGQUIT,quickdie);
    gspqsignal(SIGALRM,handle_sig_alarm);
    gspqsignal(SIGPIPE,SIG_IGN);
    gspqsignal(SIGUSR1,procsignal_sigusr1_handler);
    gspqsignal(SIGUSR2,SIG_IGN);
    gspqsignal(SIGFPE,FloatExceptionHandler);
    gspqsignal(SIGCHLD,SIG_DFL);
    gspqsignal(SIGURG,print_stack);
}
static void LakeMaintenanceUnblock(void)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig,NULL);
    (void)gs_signal_unblock_sigusr2();
}
static void LakeMaintenanceInitErrorContext(void)
{
    /* SPI error contexts call the native SQL/password masker, including when
     * cancellation interrupts a lock wait before the SPI query is planned. */
    t_thrd.mem_cxt.mask_password_mem_cxt=AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Lake maintenance password mask",ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    t_thrd.mem_cxt.msg_mem_cxt=AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Lake maintenance messages",ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
}
static void LakeMaintenanceRecover(bool report)
{
    HOLD_INTERRUPTS();
    t_thrd.int_cxt.QueryCancelPending=false;(void)disable_sig_alarm(true);
    t_thrd.int_cxt.QueryCancelPending=false;
    t_thrd.storage_cxt.timeIsPausing=false;t_thrd.storage_cxt.restimems=-1;
    if(report)EmitErrorReport();
    AbortOutOfAnyTransaction();MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
    FlushErrorState();pgstat_clear_snapshot();LakeMaintenanceUnblock();
    RESUME_INTERRUPTS();
}
static void LakeMaintenanceReload(void)
{
    if(t_thrd.worker_sig_flags.got_SIGHUP) {
        t_thrd.worker_sig_flags.got_SIGHUP=false;ProcessConfigFile(PGC_SIGHUP);
    }
}
static void LakeRecordBatch(bool success,Oid relation)
{
    SpinLockAcquire(&lakeMaintenance->lock);
    lakeMaintenance->next_relation=relation;
    if(success)++lakeMaintenance->completed_batches;
    else {++lakeMaintenance->failed_batches;lakeMaintenance->job_failed=true;}
    SpinLockRelease(&lakeMaintenance->lock);
}
static int LakeMaintenanceRemainingBudget(TimestampTz started,int budget)
{
    long seconds=0;int microseconds=0;
    TimestampDifference(started,GetCurrentTimestamp(),&seconds,&microseconds);
    if(seconds>budget/1000)return 0;
    int elapsed=(int)seconds*1000+microseconds/1000;
    return elapsed>=budget?0:budget-elapsed;
}

/* The eligibility check reads physical committed versions, not a business
 * projection. Ended versions and file-position marks still require transfer.
 * Count and age are scheduling hints; flush revalidates its original cutoff.
 */
static void LakeCountPendingRelation(Oid namespaceOid,Oid relationOid,Snapshot snapshot,
    const char* birthColumn,int64 limit,int64* count,int64* earliest)
{
    if(!OidIsValid(relationOid)||*count>=limit)return;
    Relation volatile relation=NULL;TableScanDesc volatile scan=NULL;
    PG_TRY();
    {
        relation=heap_open(relationOid,AccessShareLock);
        Oid owner=LakeMetadataOwner();
        LakeValidatePrivateRelation((Relation)relation,owner,false);
        LakeCheckMetadataObjectIdentity(namespaceOid,relationOid,owner);
        AttrNumber birth=get_attnum(relationOid,birthColumn);
        TupleDesc desc=RelationGetDescr((Relation)relation);
        if(birth<=0||desc->attrs[birth-1].atttypid!=INT8OID)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake maintenance found an invalid version carrier")));
        scan=tableam_scan_begin((Relation)relation,snapshot,0,NULL);
        while(*count<limit) {
            CHECK_FOR_INTERRUPTS();Tuple tuple=tableam_scan_getnexttuple((TableScanDesc)scan,ForwardScanDirection);
            if(tuple==NULL)break;
            bool absent=false;Datum value=tableam_tops_tuple_getattr(tuple,birth,desc,&absent);
            if(absent||DatumGetInt64(value)<0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake maintenance found an invalid birth snapshot")));
            int64 born=DatumGetInt64(value);
            if(*earliest<0||born<*earliest)*earliest=born;
            ++*count;
        }
        tableam_scan_end((TableScanDesc)scan);scan=NULL;
        heap_close((Relation)relation,NoLock);relation=NULL;
    }
    PG_CATCH();
    {
        if(scan!=NULL)tableam_scan_end((TableScanDesc)scan);
        if(relation!=NULL)heap_close((Relation)relation,NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
}
static bool LakeMaintenanceDue(LakeTxnTable* table,Snapshot snapshot)
{
    LakeInlineBinding* binding=table->binding;
    int64 count=0,earliest=-1;
    int64 threshold=g_instance.attr.attr_storage.lake_flush_min_rows;
    for(int source=0;source<LakeInlineReadSourceCount(binding)&&count<threshold;++source)
        LakeCountPendingRelation(binding->metadata_schema_oid,LakeInlineReadSourceOid(binding,source),snapshot,
            "_ducklake_begin_snapshot",threshold,&count,&earliest);
    char* markerName=psprintf("ducklake_inlined_delete_%lld",(long long)LakePermanentTableId(binding));
    Oid marks=get_relname_relid(markerName,binding->metadata_schema_oid);
    LakeCountPendingRelation(binding->metadata_schema_oid,marks,snapshot,"begin_snapshot",threshold,&count,&earliest);
    if(count==0)return false;
    if(count>=threshold)return true;
    Oid type=INT8OID;Datum argument=Int64GetDatum(earliest);
    char* snapshots=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_snapshot");
    LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT snapshot_time FROM %s WHERE snapshot_id=$1",snapshots),
        1,&type,&argument,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    /* An expired origin must not make old physical input wait forever. */
    if(rows->count==0)return true;
    LakeMetadataRequireCount(rows,1,"pending version origin time");
    TimestampTz origin=DatumGetTimestampTz(LakeMetadataValue(rows,0,1,TIMESTAMPTZOID));
    return TimestampDifferenceExceeds(origin,GetCurrentTimestamp(),g_instance.attr.attr_storage.lake_flush_max_delay);
}
static int LakeSelectRelations(Oid after,Oid* relations,bool* nativeParents)
{
    Oid type=OIDOID;Datum argument=ObjectIdGetDatum(after);
    const bool typed=LakeNativeRowHeadMaintenanceEnabled();
    const char* ordinary="SELECT r.relation_oid FROM og_lake_internal.relation_binding r "
        "JOIN og_lake_internal.table_binding t USING(binding_id) "
        "JOIN og_lake_internal.catalog_binding c ON c.server_oid=t.server_oid "
        "WHERE r.relation_oid>$1 AND t.state='active' AND NOT t.read_only AND NOT c.read_only "
        "ORDER BY r.relation_oid LIMIT 32";
    /* A parent is one scheduling candidate, regardless of its two leaf maps.
     * Its kind travels with the copied OID so it cannot fall into ordinary
     * LakeTxnReadStatementTable/flush merely because no head was reclaimed. */
    const char* withParents="SELECT relation_oid,native_parent FROM ("
        "SELECT r.relation_oid,false AS native_parent FROM og_lake_internal.relation_binding r "
        "JOIN og_lake_internal.table_binding t USING(binding_id) "
        "JOIN og_lake_internal.catalog_binding c ON c.server_oid=t.server_oid "
        "WHERE r.relation_oid>$1 AND t.state='active' AND NOT t.read_only AND NOT c.read_only "
        "UNION ALL SELECT DISTINCT p.parent_relid AS relation_oid,true AS native_parent "
        "FROM og_lake_internal.partition_binding p JOIN og_lake_internal.table_binding t USING(binding_id) "
        "JOIN og_lake_internal.catalog_binding c ON c.server_oid=t.server_oid "
        "WHERE p.parent_relid>$1 AND t.object_kind='partition' AND t.state='active' AND NOT t.read_only AND NOT c.read_only"
        ") candidates ORDER BY relation_oid LIMIT 32";
    LakeMetaRows* rows=LakeMetadataQuery(typed?withParents:ordinary,
        1,&type,&argument,NULL,GetActiveSnapshot(),true,SPI_OK_SELECT);
    if(rows->count>LAKE_SCAN_CANDIDATES)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake maintenance selection exceeded its bound")));
    for(uint32 i=0;i<rows->count;++i) {
        relations[i]=DatumGetObjectId(LakeMetadataValue(rows,i,1,OIDOID));
        nativeParents[i]=typed&&DatumGetBool(LakeMetadataValue(rows,i,2,BOOLOID));
        if(i>0 && relations[i]<=relations[i-1])
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate or unordered lake maintenance relation identity")));
    }
    return (int)rows->count;
}
/* The oldest completed/attempted vacuum wins among this fair relation window.
 * This avoids repeatedly vacuuming the first low-OID table each worker round. */
struct LakeHeadVacuumChoice {
    Oid relation_oid;
    LakeRowHeadMaintenanceTarget native_target;
};
static LakeHeadVacuumChoice LakeChooseHeadVacuum(const Oid* relations,int count)
{
    LakeHeadVacuumChoice choice={InvalidOid,{}};
    if(!LakeRowHeadReclaimEnabled() || count==0) return choice;
    Datum values[LAKE_SCAN_CANDIDATES];
    for(int i=0;i<count;++i) values[i]=ObjectIdGetDatum(relations[i]);
    ArrayType* array=construct_array(values,count,OIDOID,sizeof(Oid),true,'i');
    Oid type=get_array_type(OIDOID);Datum argument=PointerGetDatum(array);
    const bool typed=LakeNativeRowHeadMaintenanceEnabled();
    LakeMetaRows* rows=NULL;
    if(!typed) {
        rows=LakeMetadataQuery("SELECT relation_oid FROM og_lake_internal.row_lock_binding "
            "WHERE relation_oid=ANY($1) ORDER BY last_vacuum_at ASC NULLS FIRST,relation_oid LIMIT 1",
            1,&type,&argument,NULL,GetActiveSnapshot(),true,SPI_OK_SELECT);
    } else {
        const bool saved=u_sess->attr.attr_common.allow_concurrent_tuple_update;
        PG_TRY();
        {
            u_sess->attr.attr_common.allow_concurrent_tuple_update=true;
            rows=LakeMetadataQuery("SELECT h.relation_oid,h.leaf_partition_oid,h.partition_no,h.binding_id::text,h.row_head_oid,"
                "t.server_oid,t.table_id,t.table_uuid::text FROM og_lake_internal.row_lock_binding h "
                "JOIN og_lake_internal.table_binding t ON t.binding_id=h.binding_id "
                "JOIN og_lake_internal.catalog_binding c ON c.server_oid=t.server_oid "
                "LEFT JOIN og_lake_internal.partition_binding p ON p.parent_relid=h.relation_oid "
                "AND p.leaf_partition_oid=h.leaf_partition_oid AND p.partition_no=h.partition_no AND p.binding_id=h.binding_id "
                "LEFT JOIN og_lake_internal.relation_binding r ON r.relation_oid=h.relation_oid AND r.binding_id=h.binding_id "
                "WHERE h.relation_oid=ANY($1) AND t.state='active' AND NOT t.read_only AND NOT c.read_only "
                "AND ((h.leaf_partition_oid='0'::pg_catalog.oid AND h.partition_no=0 AND r.relation_oid IS NOT NULL "
                "AND t.object_kind='relation') OR (h.leaf_partition_oid<>'0'::pg_catalog.oid AND h.partition_no>0 "
                "AND p.parent_relid IS NOT NULL AND t.object_kind='partition')) "
                /* An attempted busy typed head advances only last_reclaim_at.
                 * Incorporate that attempt for fairness without claiming a
                 * completed VACUUM. Ordinary ranking remains unchanged. */
                "ORDER BY CASE WHEN h.leaf_partition_oid='0'::pg_catalog.oid THEN h.last_vacuum_at "
                "ELSE GREATEST(h.last_vacuum_at,h.last_reclaim_at) END ASC NULLS FIRST,h.relation_oid,h.leaf_partition_oid "
                "LIMIT 1 FOR UPDATE OF h SKIP LOCKED",
                1,&type,&argument,NULL,GetActiveSnapshot(),false,SPI_OK_SELECT);
            u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;
        }
        PG_CATCH();{u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;PG_RE_THROW();}PG_END_TRY();
    }
    if(rows->count>1) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid row-head vacuum selection")));
    if(rows->count==0) return choice;
    choice.relation_oid=DatumGetObjectId(LakeMetadataValue(rows,0,1,OIDOID));
    if(typed) {
        Oid leaf=DatumGetObjectId(LakeMetadataValue(rows,0,2,OIDOID));
        if(OidIsValid(leaf)) {
            LakeRowHeadMaintenanceTarget* target=&choice.native_target;
            target->relation_oid=choice.relation_oid;target->leaf_partition_oid=leaf;
            target->partition_no=DatumGetInt32(LakeMetadataValue(rows,0,3,INT4OID));
            target->head_oid=DatumGetObjectId(LakeMetadataValue(rows,0,5,OIDOID));
            target->server_oid=DatumGetObjectId(LakeMetadataValue(rows,0,6,OIDOID));
            target->table_id=DatumGetInt64(LakeMetadataValue(rows,0,7,INT8OID));
            char* binding=LakeMetadataText(rows,0,4);char* tableUuid=LakeMetadataText(rows,0,8);
            if(strlen(binding)!=36 || strlen(tableUuid)!=36 || target->partition_no<=0 ||
               !OidIsValid(target->head_oid) || !OidIsValid(target->server_oid) || target->table_id<0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid typed row-head vacuum identity")));
            errno_t rc=strcpy_s(target->binding_id,sizeof(target->binding_id),binding);securec_check(rc,"","");
            rc=strcpy_s(target->table_uuid,sizeof(target->table_uuid),tableUuid);securec_check(rc,"","");
        }
    }
    return choice;
}
}

Size LakeMaintenanceShmemSize(void){return MAXALIGN(sizeof(LakeMaintenanceShared));}
void LakeMaintenanceShmemInit(void)
{
    bool found=false;
    lakeMaintenance=(LakeMaintenanceShared*)ShmemInitStruct("Native lake maintenance",LakeMaintenanceShmemSize(),&found);
    if(!found) {
        errno_t rc=memset_s(lakeMaintenance,sizeof(*lakeMaintenance),0,sizeof(*lakeMaintenance));securec_check(rc,"","");
        SpinLockInit(&lakeMaintenance->lock);lakeMaintenance->phase=LAKE_WORK_IDLE;
    }
}
bool IsNativeLakeMaintenanceProcess(void)
{return t_thrd.role==LAKE_LAUNCHER||t_thrd.role==LAKE_WORKER;}
bool IsNativeLakeLauncherProcess(void){return t_thrd.role==LAKE_LAUNCHER;}
bool LakeMaintenanceTakeRequest(LakeWorkerStart* start)
{
    bool ready=false;SpinLockAcquire(&lakeMaintenance->lock);
    if(lakeMaintenance->phase==LAKE_WORK_REQUESTED) {
        start->database_oid=lakeMaintenance->database_oid;start->after_relation=lakeMaintenance->after_relation;
        start->generation=lakeMaintenance->generation;lakeMaintenance->phase=LAKE_WORK_STARTING;ready=true;
    }
    SpinLockRelease(&lakeMaintenance->lock);return ready;
}
void LakeMaintenanceWorkerSpawned(uint64 generation,ThreadId pid)
{
    SpinLockAcquire(&lakeMaintenance->lock);
    if(lakeMaintenance->generation==generation&&lakeMaintenance->phase==LAKE_WORK_STARTING) {
        if(lakeMaintenance->early_exit_pid==pid) {
            lakeMaintenance->phase=LAKE_WORK_IDLE;lakeMaintenance->worker_pid=0;
        } else lakeMaintenance->worker_pid=pid;
    }
    SpinLockRelease(&lakeMaintenance->lock);
}
void LakeMaintenanceWorkerSpawnFailed(uint64 generation)
{
    SpinLockAcquire(&lakeMaintenance->lock);
    if(lakeMaintenance->generation==generation&&lakeMaintenance->phase==LAKE_WORK_STARTING) {
        lakeMaintenance->phase=LAKE_WORK_IDLE;lakeMaintenance->worker_pid=0;lakeMaintenance->job_failed=true;
    }
    SpinLockRelease(&lakeMaintenance->lock);
}
void LakeMaintenanceWorkerExited(ThreadId pid)
{
    if(lakeMaintenance==NULL)return;
    SpinLockAcquire(&lakeMaintenance->lock);
    if(lakeMaintenance->worker_pid==pid) {
        lakeMaintenance->phase=LAKE_WORK_IDLE;lakeMaintenance->worker_pid=0;
    } else if(lakeMaintenance->phase==LAKE_WORK_STARTING&&lakeMaintenance->worker_pid==0)
        lakeMaintenance->early_exit_pid=pid;
    SpinLockRelease(&lakeMaintenance->lock);
}

void LakeMaintenanceWorkerMain(const LakeWorkerStart* argument)
{
    LakeWorkerStart work=*argument;
    IsUnderPostmaster=true;t_thrd.role=LAKE_WORKER;t_thrd.proc_cxt.MyProcPid=gs_thread_self();
    t_thrd.proc_cxt.MyStartTime=time(NULL);t_thrd.proc_cxt.MyProgName="LakeFlushWorker";
    init_ps_display("native lake flush worker","","","");
    SetProcessingMode(InitProcessing);LakeMaintenanceSignals();BaseInit();LakeMaintenanceInitErrorContext();
    MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);LakeMaintenanceUnblock();
    PG_TRY();
    {
        SpinLockAcquire(&lakeMaintenance->lock);
        bool valid=lakeMaintenance->generation==work.generation&&lakeMaintenance->database_oid==work.database_oid;
        if(valid){lakeMaintenance->worker_pid=gs_thread_self();lakeMaintenance->phase=LAKE_WORK_RUNNING;}
        SpinLockRelease(&lakeMaintenance->lock);
        if(!valid)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake worker request changed before startup")));
        t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(NULL,work.database_oid,NULL);
        t_thrd.proc_cxt.PostInit->InitAutoVacWorker();
        SetProcessingMode(NormalProcessing);pgstat_report_appname("og_lake flush worker");
        SetConfigOption("default_transaction_isolation","read committed",PGC_USERSET,PGC_S_SESSION);
        SetConfigOption("lockwait_timeout","1s",PGC_SUSET,PGC_S_SESSION);
        SetConfigOption("query_dop","1",PGC_USERSET,PGC_S_SESSION);
        const int workerBudget=g_instance.attr.attr_storage.lake_flush_worker_budget;
        const TimestampTz started=GetCurrentTimestamp();
        SetCurrentStatementStartTimestamp();
        if(!enable_sig_alarm(workerBudget,true))
            ereport(ERROR,(errcode(ERRCODE_SYSTEM_ERROR),errmsg("could not arm native lake maintenance timeout")));
        StartTransactionCommand();PushActiveSnapshot(GetTransactionSnapshot());
        bool enabled=OidIsValid(get_extension_oid("og_lake",true));
        if(enabled)enabled=LakeHasFlushVersion()&&OidIsValid(LakeMetadataOwner());
        const bool reclaimHeads=enabled&&LakeRowHeadReclaimEnabled();
        SpinLockAcquire(&lakeMaintenance->lock);lakeMaintenance->job_has_lake=enabled;SpinLockRelease(&lakeMaintenance->lock);
        bool claimed=enabled&&LakeTryMaintenanceClaim();
        Oid relations[LAKE_SCAN_CANDIDATES];bool nativeParents[LAKE_SCAN_CANDIDATES];
        int selected=claimed?LakeSelectRelations(work.after_relation,relations,nativeParents):0;
        /* The version lookup opens pg_extension and needs this transaction's
         * snapshot and ResourceOwner. The merge entry rechecks it per attempt. */
        const bool mergeEnabled=selected>0&&LakeMergeAdjacentVersionEnabled();
        LakeHeadVacuumChoice headVacuum={InvalidOid,{}};
        if(claimed&&reclaimHeads) headVacuum=LakeChooseHeadVacuum(relations,selected);
        PopActiveSnapshot();CommitTransactionCommand();(void)disable_sig_alarm(true);
        if(selected==0&&claimed){SpinLockAcquire(&lakeMaintenance->lock);lakeMaintenance->next_relation=InvalidOid;SpinLockRelease(&lakeMaintenance->lock);}
        volatile int batches=0,headBatches=0;
        /* Worker-local settings only. Native vacuum uses BAS_VACUUM and cost
         * accounting; no rewrite/truncate, foreground configuration or file GC. */
        SetConfigOption("vacuum_cost_delay","2ms",PGC_USERSET,PGC_S_SESSION);
        SetConfigOption("vacuum_truncate","off",PGC_USERSET,PGC_S_SESSION);
        for(int i=0;claimed&&i<selected&&!t_thrd.worker_sig_flags.got_SIGTERM;++i) {
            int remaining=LakeMaintenanceRemainingBudget(started,workerBudget);
            /* Do not let later due=false checks move the relation cursor past
             * tables whose head batch was never attempted. SQL<=17 retains
             * its original flush-only scan/batch behavior. */
            if(remaining<=0 || (reclaimHeads?headBatches:batches)>=g_instance.attr.attr_storage.lake_flush_max_batches) break;
            if(reclaimHeads) {
                PG_TRY();
                {
                    SetCurrentStatementStartTimestamp();
                    if(!enable_sig_alarm(Min(remaining,1000),true))
                        ereport(ERROR,(errcode(ERRCODE_SYSTEM_ERROR),errmsg("could not arm row-head reclamation timeout")));
                    StartTransactionCommand();PushActiveSnapshot(GetTransactionSnapshot());
                    LakeRowHeadReclaimResult reclaimed=nativeParents[i]?
                        LakeReclaimNativePartitionRowHeads(relations[i],Min(remaining,1000)):
                        LakeReclaimLogicalRowHeads(relations[i],Min(remaining,1000));
                    PopActiveSnapshot();CommitTransactionCommand();(void)disable_sig_alarm(true);
                    ++headBatches;
                    SpinLockAcquire(&lakeMaintenance->lock);lakeMaintenance->next_relation=relations[i];SpinLockRelease(&lakeMaintenance->lock);
                    if(reclaimed.pressure_raised)
                        ereport(LOG,(errmsg("native lake row-head backlog exceeds one worker deletion budget for relation %u",relations[i]),
                            errdetail("Live/dead estimates and safe-removal cutoff are recorded in row_lock_binding; byte high-water marks do not reject DML.")));
                    remaining=LakeMaintenanceRemainingBudget(started,workerBudget);
                    if(relations[i]==headVacuum.relation_oid &&
                       (nativeParents[i]?OidIsValid(headVacuum.native_target.head_oid):OidIsValid(reclaimed.head_oid)) && remaining>=1000) {
                        SetCurrentStatementStartTimestamp();
                        if(!enable_sig_alarm(Max(1,remaining/2),true))
                            ereport(ERROR,(errcode(ERRCODE_SYSTEM_ERROR),errmsg("could not arm row-head vacuum timeout")));
                        pgstat_report_activity(STATE_RUNNING,"native lake row-head vacuum");
                        if(nativeParents[i]) (void)LakeVacuumNativePartitionRowHeads(&headVacuum.native_target);
                        else (void)LakeVacuumLogicalRowHeads(relations[i],reclaimed.head_oid);
                        (void)disable_sig_alarm(true);
                    }
                }
                PG_CATCH();
                {
                    LakeMaintenanceRecover(!t_thrd.worker_sig_flags.got_SIGTERM);
                    LakeRecordBatch(false,relations[i]);
                }
                PG_END_TRY();
            }
            /* Typed parents have only the explicitly reviewed head lane here.
             * An empty/busy head or failed attempt must not dispatch a parent
             * OID through the still-ordinary flush/due/read-view interfaces. */
            if(nativeParents[i]) continue;
            remaining=LakeMaintenanceRemainingBudget(started,workerBudget);
            if(remaining<=0) break;
            if(batches>=g_instance.attr.attr_storage.lake_flush_max_batches) continue;
            bool prepared=false;
            PG_TRY();
            {
                LakeMaintenanceReload();SetCurrentStatementStartTimestamp();
                if(!enable_sig_alarm(remaining,true))
                    ereport(ERROR,(errcode(ERRCODE_SYSTEM_ERROR),errmsg("could not arm native lake maintenance timeout")));
                StartTransactionCommand();PushActiveSnapshot(GetTransactionSnapshot());
                LakeTxnTable* table=LakeTxnReadStatementTable(relations[i],GetActiveSnapshot());
                bool due=LakeMaintenanceDue(table,GetActiveSnapshot());
                LakeTxnReleaseReadView(table);
                if(due) {
                    DirectFunctionCall1(og_lake_flush,ObjectIdGetDatum(relations[i]));prepared=true;
                    pgstat_report_activity(STATE_RUNNING,"automatic native lake flush");
                }
                PopActiveSnapshot();CommitTransactionCommand();(void)disable_sig_alarm(true);
                if(prepared){LakeRecordBatch(true,relations[i]);++batches;}
                else {SpinLockAcquire(&lakeMaintenance->lock);lakeMaintenance->next_relation=relations[i];SpinLockRelease(&lakeMaintenance->lock);}
            }
            PG_CATCH();
            {
                LakeMaintenanceRecover(!t_thrd.worker_sig_flags.got_SIGTERM);
                LakeRecordBatch(false,relations[i]);
            }
            PG_END_TRY();
            if(!t_thrd.worker_sig_flags.got_SIGTERM)pg_usleep(10000L);
        }
        /* Bounded automatic small-file compaction for tables with persistent
         * auto_compact='true'. Runs in its own isolated transaction and never
         * alters flush batch counters or tables without explicit auto_compact. */
        if(mergeEnabled&&!t_thrd.worker_sig_flags.got_SIGTERM) {
            for(int i=0;i<selected&&!t_thrd.worker_sig_flags.got_SIGTERM;++i) {
                if(nativeParents[i])continue;
                int mergeRemaining=LakeMaintenanceRemainingBudget(started,workerBudget);
                if(mergeRemaining<200)break;
                bool merged=false;
                PG_TRY();{
                    (void)disable_sig_alarm(true);LakeMaintenanceReload();
                    SetCurrentStatementStartTimestamp();
                    if(!enable_sig_alarm(mergeRemaining,true))
                        ereport(ERROR,(errcode(ERRCODE_SYSTEM_ERROR),errmsg("could not arm native lake auto-compact timeout")));
                    StartTransactionCommand();PushActiveSnapshot(GetTransactionSnapshot());
                    pgstat_report_activity(STATE_RUNNING,"automatic native lake small-file merge");
                    merged=LakeTryStageMergeAdjacent(relations[i],OG_LAKE_REWRITE_MAX_SOURCES,-1,true,NULL,NULL,NULL,NULL);
                    PopActiveSnapshot();CommitTransactionCommand();(void)disable_sig_alarm(true);
                    if(merged) {
                        SpinLockAcquire(&lakeMaintenance->lock);
                        ++lakeMaintenance->merge_batches;
                        SpinLockRelease(&lakeMaintenance->lock);
                        break;
                    }
                }PG_CATCH();{
                    int mergeErr=geterrcode();
                    LakeMaintenanceRecover(!t_thrd.worker_sig_flags.got_SIGTERM);
                    SpinLockAcquire(&lakeMaintenance->lock);
                    ++lakeMaintenance->merge_failed;
                    if(mergeErr!=ERRCODE_T_R_SERIALIZATION_FAILURE&&mergeErr!=ERRCODE_LOCK_NOT_AVAILABLE)
                        lakeMaintenance->job_failed=true;
                    SpinLockRelease(&lakeMaintenance->lock);
                }PG_END_TRY();
            }
        }
        /* Separate ANALYZE transaction and budget. It is not constrained by
         * flush max_batches and never joins a flush publish transaction. One
         * oldest-due table per worker precedes long index work for fairness. */
        if(claimed&&enabled&&g_instance.attr.attr_storage.enable_lake_autoanalyze&&
           !t_thrd.worker_sig_flags.got_SIGTERM) {
            PG_TRY();{
                (void)disable_sig_alarm(true);LakeMaintenanceReload();
                (void)LakeAutoAnalyzeOne(LakeMaintenanceRecover);
            }PG_CATCH();{
                LakeMaintenanceRecover(!t_thrd.worker_sig_flags.got_SIGTERM);
                SpinLockAcquire(&lakeMaintenance->lock);lakeMaintenance->job_failed=true;SpinLockRelease(&lakeMaintenance->lock);
            }PG_END_TRY();
        }
        /* Index work has a separate complete-segment budget. Reusing the
         * short flush alarm would make a slow valid graph restart forever.
         * First slice deliberately keeps the same DB claim/single worker. */
        if(claimed&&enabled&&g_instance.attr.attr_storage.enable_lake_index_maintenance&&
           !t_thrd.worker_sig_flags.got_SIGTERM&&LakeIndexRoundDue()) {
            PG_TRY();{
                (void)disable_sig_alarm(true);SetCurrentStatementStartTimestamp();
                if(!enable_sig_alarm(g_instance.attr.attr_storage.lake_index_build_budget,true))
                    ereport(ERROR,(errcode(ERRCODE_SYSTEM_ERROR),errmsg("could not arm lake index segment timeout")));
                pgstat_report_activity(STATE_RUNNING,"automatic native lake file index maintenance");
                LakeIndexMaintenanceResult index=LakeMaintainOneFileIndex();
                (void)disable_sig_alarm(true);
                SpinLockAcquire(&lakeMaintenance->lock);
                if(index.attempted)++lakeMaintenance->index_checked;
                if(index.built)++lakeMaintenance->index_built;
                if(index.verified)++lakeMaintenance->index_verified;
                if(index.retry)++lakeMaintenance->index_retry;
                SpinLockRelease(&lakeMaintenance->lock);
            }PG_CATCH();{
                /* Observe the actual error rethrown by the coordinator before
                 * Recover flushes ErrorData. Never substitute an error/result. */
                int actualError=geterrcode();
                SpinLockAcquire(&lakeMaintenance->lock);
                if(lakeMaintenance->index_gate_database==u_sess->proc_cxt.MyDatabaseId&&
                   lakeMaintenance->index_gate_pid==gs_thread_self())lakeMaintenance->index_gate_error=actualError;
                SpinLockRelease(&lakeMaintenance->lock);
                LakeMaintenanceRecover(!t_thrd.worker_sig_flags.got_SIGTERM);
                SpinLockAcquire(&lakeMaintenance->lock);++lakeMaintenance->index_failed;lakeMaintenance->job_failed=true;SpinLockRelease(&lakeMaintenance->lock);
            }PG_END_TRY();
        }
        if(claimed)LakeReleaseMaintenanceClaim();
    }
    PG_CATCH();
    {
        LakeMaintenanceRecover(true);LakeRecordBatch(false,work.after_relation);proc_exit(1);
    }
    PG_END_TRY();
    proc_exit(0);
}

void LakeMaintenanceNoteCommit(Oid databaseOid)
{
    if(lakeMaintenance==NULL||!g_instance.attr.attr_storage.enable_lake_maintenance||
       IsNativeLakeMaintenanceProcess()||!OidIsValid(databaseOid))return;
    SpinLockAcquire(&lakeMaintenance->lock);
    bool duplicate=false;
    for(uint32 i=0;i<lakeMaintenance->hint_count;++i)
        if(lakeMaintenance->hints[i]==databaseOid){duplicate=true;break;}
    if(!duplicate) {
        if(lakeMaintenance->hint_count<64)lakeMaintenance->hints[lakeMaintenance->hint_count++]=databaseOid;
        else lakeMaintenance->hints_overflow=true;
    }
    SpinLockRelease(&lakeMaintenance->lock);
}
namespace {
static LakeDatabaseCursor* LakeCursor(HTAB* cache,Oid database)
{
    LakeDatabaseCursor* current=(LakeDatabaseCursor*)hash_search(cache,&database,HASH_FIND,NULL);
    if(current!=NULL)return current;
    if(hash_get_num_entries(cache)>=1024) {
        HASH_SEQ_STATUS walk;hash_seq_init(&walk,cache);
        LakeDatabaseCursor* victim=(LakeDatabaseCursor*)hash_seq_search(&walk);
        if(victim!=NULL){Oid key=victim->database_oid;hash_seq_term(&walk);hash_search(cache,&key,HASH_REMOVE,NULL);}
    }
    bool found=false;current=(LakeDatabaseCursor*)hash_search(cache,&database,HASH_ENTER,&found);
    if(!found){current->after_relation=InvalidOid;current->retry_after=0;current->error_cooldown=false;}
    return current;
}
static Oid LakeNextDatabase(Oid after,Oid urgent,HTAB* cache)
{
    Oid first=InvalidOid,next=InvalidOid;bool wanted=false;
    StartTransactionCommand();(void)GetTransactionSnapshot();
    Relation relation=heap_open(DatabaseRelationId,AccessShareLock);
    TableScanDesc scan=tableam_scan_begin(relation,SnapshotNow,0,NULL);
    HeapTuple tuple;TimestampTz now=GetCurrentTimestamp();
    while(HeapTupleIsValid(tuple=(HeapTuple)tableam_scan_getnexttuple(scan,ForwardScanDirection))) {
        CHECK_FOR_INTERRUPTS();Form_pg_database row=(Form_pg_database)GETSTRUCT(tuple);
        if(!row->datallowconn||row->datistemplate)continue;
        Oid id=HeapTupleGetOid(tuple);
        LakeDatabaseCursor* known=(LakeDatabaseCursor*)hash_search(cache,&id,HASH_FIND,NULL);
        if(known!=NULL&&known->retry_after>now)continue;
        if(id==urgent){wanted=true;continue;}
        if(!OidIsValid(first)||id<first)first=id;
        if(id>after&&(!OidIsValid(next)||id<next))next=id;
    }
    tableam_scan_end(scan);heap_close(relation,NoLock);CommitTransactionCommand();
    return wanted?urgent:(OidIsValid(next)?next:first);
}
static Oid LakePopHint(HTAB* cache)
{
    Oid urgent=InvalidOid;bool overflow=false;
    SpinLockAcquire(&lakeMaintenance->lock);
    if(lakeMaintenance->hint_count>0) {
        urgent=lakeMaintenance->hints[0];
        for(uint32 i=1;i<lakeMaintenance->hint_count;++i)lakeMaintenance->hints[i-1]=lakeMaintenance->hints[i];
        --lakeMaintenance->hint_count;
    }
    overflow=lakeMaintenance->hints_overflow;lakeMaintenance->hints_overflow=false;
    SpinLockRelease(&lakeMaintenance->lock);
    if(overflow) {
        HASH_SEQ_STATUS walk;hash_seq_init(&walk,cache);LakeDatabaseCursor* row;
        while((row=(LakeDatabaseCursor*)hash_seq_search(&walk))!=NULL)
            if(!row->error_cooldown)row->retry_after=0;
    }
    if(OidIsValid(urgent)) {
        LakeDatabaseCursor* cursor=LakeCursor(cache,urgent);
        /* New commits can reveal a previously empty catalog, but do not
         * excuse repeated failed I/O or publication attempts in its cooldown. */
        if(!cursor->error_cooldown)cursor->retry_after=0;
    }
    return urgent;
}
}
void LakeMaintenanceLauncherMain(void)
{
    IsUnderPostmaster=true;t_thrd.role=LAKE_LAUNCHER;t_thrd.proc_cxt.MyProcPid=gs_thread_self();
    t_thrd.proc_cxt.MyStartTime=time(NULL);t_thrd.proc_cxt.MyProgName="LakeFlushLauncher";
    init_ps_display("native lake flush launcher","","","");
    SetProcessingMode(InitProcessing);LakeMaintenanceSignals();BaseInit();LakeMaintenanceInitErrorContext();
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(NULL,InvalidOid,NULL);
    t_thrd.proc_cxt.PostInit->InitAutoVacLauncher();
    SetProcessingMode(NormalProcessing);LakeMaintenanceUnblock();
    MemoryContext keep=AllocSetContextCreate(t_thrd.top_mem_cxt,"Lake launcher cursors",ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    HASHCTL control;errno_t rc=memset_s(&control,sizeof(control),0,sizeof(control));securec_check(rc,"","");
    control.keysize=sizeof(Oid);control.entrysize=sizeof(LakeDatabaseCursor);control.hash=tag_hash;control.hcxt=keep;
    HTAB* cache=hash_create("Lake database cursors",128,&control,HASH_ELEM|HASH_FUNCTION|HASH_CONTEXT);
    volatile Oid previous=InvalidOid;volatile uint64 observed=0;volatile bool preferHint=true;
    while(!t_thrd.worker_sig_flags.got_SIGTERM) {
        PG_TRY();
        {
            LakeMaintenanceReload();CHECK_FOR_INTERRUPTS();
            SpinLockAcquire(&lakeMaintenance->lock);
            bool idle=lakeMaintenance->phase==LAKE_WORK_IDLE;
            bool requested=lakeMaintenance->phase==LAKE_WORK_REQUESTED;
            uint64 generation=lakeMaintenance->generation;Oid completedDb=lakeMaintenance->database_oid;
            Oid nextRelation=lakeMaintenance->next_relation;bool failed=lakeMaintenance->job_failed;
            bool hasLake=lakeMaintenance->job_has_lake;
            SpinLockRelease(&lakeMaintenance->lock);
            if(requested)SendPostmasterSignal(PMSIGNAL_START_LAKE_WORKER);
            if(idle) {
                if(generation!=observed&&OidIsValid(completedDb)) {
                    LakeDatabaseCursor* cursor=LakeCursor(cache,completedDb);
                    cursor->after_relation=nextRelation;
                    cursor->error_cooldown=failed;
                    cursor->retry_after=failed?GetCurrentTimestamp()+(int64)LAKE_ERROR_BACKOFF_MS*1000:
                        hasLake?0:GetCurrentTimestamp()+60000LL*1000;
                    observed=generation;
                }
                Oid urgent=preferHint?LakePopHint(cache):InvalidOid;preferHint=!preferHint;
                Oid database=LakeNextDatabase(previous,urgent,cache);
                if(OidIsValid(database)) {
                    LakeDatabaseCursor* cursor=LakeCursor(cache,database);
                    SpinLockAcquire(&lakeMaintenance->lock);
                    if(lakeMaintenance->phase==LAKE_WORK_IDLE) {
                        lakeMaintenance->database_oid=database;lakeMaintenance->after_relation=cursor->after_relation;
                        lakeMaintenance->next_relation=cursor->after_relation;lakeMaintenance->worker_pid=0;
                        lakeMaintenance->early_exit_pid=0;lakeMaintenance->job_failed=false;lakeMaintenance->job_has_lake=false;
                        ++lakeMaintenance->generation;lakeMaintenance->phase=LAKE_WORK_REQUESTED;
                    }
                    SpinLockRelease(&lakeMaintenance->lock);previous=database;
                    SendPostmasterSignal(PMSIGNAL_START_LAKE_WORKER);
                }
            }
            if(!t_thrd.worker_sig_flags.got_SIGTERM) {
                ResetLatch(&t_thrd.proc->procLatch);
                int events=WaitLatch(&t_thrd.proc->procLatch,WL_LATCH_SET|WL_TIMEOUT|WL_POSTMASTER_DEATH,
                    g_instance.attr.attr_storage.lake_flush_naptime);
                if(events&WL_POSTMASTER_DEATH)proc_exit(1);
            }
        }
        PG_CATCH();
        {
            LakeMaintenanceRecover(true);
        }
        PG_END_TRY();
    }
    proc_exit(0);
}
char* LakeMaintenanceStateJson(void)
{
    if(lakeMaintenance==NULL)return pstrdup("{\"initialized\":false}");
    SpinLockAcquire(&lakeMaintenance->lock);
    int phase=(int)lakeMaintenance->phase;Oid db=lakeMaintenance->database_oid;
    ThreadId worker=lakeMaintenance->worker_pid;uint64 ok=lakeMaintenance->completed_batches;
    uint64 failed=lakeMaintenance->failed_batches;uint64 generation=lakeMaintenance->generation;
    uint64 mergeOk=lakeMaintenance->merge_batches,mergeFailed=lakeMaintenance->merge_failed;
    uint64 indexChecked=lakeMaintenance->index_checked,indexBuilt=lakeMaintenance->index_built;
    uint64 indexVerified=lakeMaintenance->index_verified,indexFailed=lakeMaintenance->index_failed,indexRetry=lakeMaintenance->index_retry;
    SpinLockRelease(&lakeMaintenance->lock);
    return psprintf("{\"initialized\":true,\"enabled\":%s,\"launcher_pid\":%llu,\"worker_pid\":%llu,"
        "\"phase\":%d,\"database_oid\":%u,\"generation\":%llu,\"completed_batches\":%llu,\"failed_batches\":%llu,"
        "\"merge_batches\":%llu,\"merge_failed\":%llu,"
        "\"index_enabled\":%s,\"index_checked\":%llu,\"index_built\":%llu,\"index_verified\":%llu,"
        "\"index_failed\":%llu,\"index_retry\":%llu}",
        g_instance.attr.attr_storage.enable_lake_maintenance?"true":"false",
        (unsigned long long)g_instance.pid_cxt.LakeLauncherPID,(unsigned long long)worker,phase,db,
        (unsigned long long)generation,(unsigned long long)ok,(unsigned long long)failed,
        (unsigned long long)mergeOk,(unsigned long long)mergeFailed,
        g_instance.attr.attr_storage.enable_lake_index_maintenance?"true":"false",
        (unsigned long long)indexChecked,(unsigned long long)indexBuilt,(unsigned long long)indexVerified,
        (unsigned long long)indexFailed,(unsigned long long)indexRetry);
}

/* Only scheduler hints live here. Every task independently verifies durable
 * catalog/source identity and publication CAS; no cross-instance lease here. */
bool LakeIndexMaintenanceCursor(bool definition,const char* uuid,bool advance,int64* after,int64* end,char* definitionAfter,int64 generation)
{
    const char* key=definition?"":uuid;
    if(!lakeMaintenance||!key||strlen(key)>36)return false;
    bool full=false;
    SpinLockAcquire(&lakeMaintenance->lock);
    uint32 i=0;for(;i<lakeMaintenance->index_cursor_count;++i)
        if(lakeMaintenance->index_cursors[i].database==u_sess->proc_cxt.MyDatabaseId&&
           strcmp(lakeMaintenance->index_cursors[i].uuid,key)==0)break;
    if(i==lakeMaintenance->index_cursor_count){
        if(i<lengthof(lakeMaintenance->index_cursors)){
            LakeIndexCursorHint* hint=&lakeMaintenance->index_cursors[i];
            hint->database=u_sess->proc_cxt.MyDatabaseId;
            Size n=strlen(key);for(Size c=0;c<=n;++c)hint->uuid[c]=key[c];
            hint->after=hint->end=-1;hint->generation=-1;hint->next_index_at=0;hint->last_used=0;
            hint->definition_after[0]='\0';++lakeMaintenance->index_cursor_count;
        }else{
            uint32 victim=lengthof(lakeMaintenance->index_cursors);
            for(uint32 j=0;j<lakeMaintenance->index_cursor_count;++j){
                LakeIndexCursorHint* cand=&lakeMaintenance->index_cursors[j];
                if(cand->database==u_sess->proc_cxt.MyDatabaseId&&cand->uuid[0]=='\0')continue;
                if(victim>=lengthof(lakeMaintenance->index_cursors)){victim=j;continue;}
                LakeIndexCursorHint* cur=&lakeMaintenance->index_cursors[victim];
                bool candPerIndex=cand->uuid[0]!='\0',curPerIndex=cur->uuid[0]!='\0';
                if((candPerIndex&&!curPerIndex)||(candPerIndex==curPerIndex&&cand->last_used<cur->last_used))victim=j;
            }
            if(victim>=lengthof(lakeMaintenance->index_cursors))full=true;
            else{
                i=victim;LakeIndexCursorHint* hint=&lakeMaintenance->index_cursors[i];
                hint->database=u_sess->proc_cxt.MyDatabaseId;
                Size n=strlen(key);for(Size c=0;c<=n;++c)hint->uuid[c]=key[c];
                hint->after=hint->end=-1;hint->generation=-1;hint->next_index_at=0;hint->last_used=0;
                hint->definition_after[0]='\0';++lakeMaintenance->index_cursor_reclaims;
            }
        }
    }
    if(!full){
        LakeIndexCursorHint* hint=&lakeMaintenance->index_cursors[i];
        hint->last_used=++lakeMaintenance->index_cursor_clock;
        if(definition){
            if(advance){const char* from=definitionAfter?definitionAfter:"";Size n=strlen(from);
                for(Size c=0;c<=n&&c<sizeof(hint->definition_after);++c)hint->definition_after[c]=from[c];
                hint->definition_after[sizeof(hint->definition_after)-1]='\0';}
            else if(definitionAfter!=NULL){Size n=strlen(hint->definition_after);
                for(Size c=0;c<=n;++c)definitionAfter[c]=hint->definition_after[c];}
        }else if(advance){
            hint->after=*after;hint->end=*end;
            if(generation>=0)hint->generation=generation;
        }else{
            if(generation>=0&&hint->generation>=0&&hint->generation!=generation){
                hint->after=hint->end=-1;hint->generation=generation;++lakeMaintenance->index_cursor_resets;
            }else if(generation>=0&&hint->generation<0){hint->generation=generation;}
            *after=hint->after;*end=hint->end;
        }
    }
    SpinLockRelease(&lakeMaintenance->lock);
    if(full)ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake index scheduling cursor budget exhausted"),
        errdetail("At most 4096 database/index cursor hints fit this instance; no index publication was attempted.")));
    return !full;
}
void LakeIndexCursorReset(const char* uuid)
{
    if(!lakeMaintenance||!uuid||uuid[0]=='\0'||strlen(uuid)>36)return;
    SpinLockAcquire(&lakeMaintenance->lock);
    for(uint32 i=0;i<lakeMaintenance->index_cursor_count;++i){
        if(lakeMaintenance->index_cursors[i].database==u_sess->proc_cxt.MyDatabaseId&&
           strcmp(lakeMaintenance->index_cursors[i].uuid,uuid)==0){
            uint32 last=--lakeMaintenance->index_cursor_count;
            if(i!=last)lakeMaintenance->index_cursors[i]=lakeMaintenance->index_cursors[last];
            errno_t rc=memset_s(&lakeMaintenance->index_cursors[last],sizeof(LakeIndexCursorHint),0,sizeof(LakeIndexCursorHint));
            securec_check(rc,"","");++lakeMaintenance->index_cursor_resets;break;
        }
    }
    SpinLockRelease(&lakeMaintenance->lock);
}
void LakeRecordAutoAnalyzeOutcome(bool attempted,bool succeeded)
{
    if(!lakeMaintenance)return;
    SpinLockAcquire(&lakeMaintenance->lock);
    if(attempted)++lakeMaintenance->autoanalyze_attempts;
    if(succeeded)++lakeMaintenance->autoanalyze_successes;
    else if(attempted)++lakeMaintenance->autoanalyze_failures;
    SpinLockRelease(&lakeMaintenance->lock);
}
char* LakeMaintenanceTelemetryJson(void)
{
    if(lakeMaintenance==NULL)
        return pstrdup("{\"initialized\":false,\"index_cursors\":0,\"index_cursor_capacity\":4096,"
                       "\"index_cursor_reclaims\":0,\"index_cursor_resets\":0,"
                       "\"autoanalyze_attempts\":0,\"autoanalyze_successes\":0,\"autoanalyze_failures\":0,"
                       "\"completed_batches\":0,\"failed_batches\":0,\"merge_batches\":0,\"merge_failed\":0,\"hints_overflow\":false,"
                       "\"index_checked\":0,\"index_built\":0,\"index_verified\":0,\"index_failed\":0,\"index_retry\":0}");
    SpinLockAcquire(&lakeMaintenance->lock);
    uint32 cursors=lakeMaintenance->index_cursor_count;
    uint64 reclaims=lakeMaintenance->index_cursor_reclaims;
    uint64 resets=lakeMaintenance->index_cursor_resets;
    uint64 aaAttempts=lakeMaintenance->autoanalyze_attempts;
    uint64 aaSuccess=lakeMaintenance->autoanalyze_successes;
    uint64 aaFail=lakeMaintenance->autoanalyze_failures;
    uint64 ok=lakeMaintenance->completed_batches,failed=lakeMaintenance->failed_batches;
    uint64 mergeOk=lakeMaintenance->merge_batches,mergeFailed=lakeMaintenance->merge_failed;
    bool hintsOverflow=lakeMaintenance->hints_overflow;
    uint64 indexChecked=lakeMaintenance->index_checked,indexBuilt=lakeMaintenance->index_built;
    uint64 indexVerified=lakeMaintenance->index_verified,indexFailed=lakeMaintenance->index_failed,indexRetry=lakeMaintenance->index_retry;
    uint32 activePins=0;
    for(int i=0;i<MAX_LAKE_SNAPSHOT_PINS;++i)if(lakeMaintenance->snapshot_pins[i].active)++activePins;
    SpinLockRelease(&lakeMaintenance->lock);
    return psprintf("{\"initialized\":true,\"index_cursors\":%u,\"index_cursor_capacity\":%u,"
        "\"index_cursor_reclaims\":%llu,\"index_cursor_resets\":%llu,"
        "\"autoanalyze_attempts\":%llu,\"autoanalyze_successes\":%llu,\"autoanalyze_failures\":%llu,"
        "\"completed_batches\":%llu,\"failed_batches\":%llu,\"merge_batches\":%llu,\"merge_failed\":%llu,\"hints_overflow\":%s,"
        "\"index_checked\":%llu,\"index_built\":%llu,\"index_verified\":%llu,\"index_failed\":%llu,\"index_retry\":%llu,"
        "\"active_snapshot_pins\":%u}",
        cursors,(unsigned)lengthof(lakeMaintenance->index_cursors),
        (unsigned long long)reclaims,(unsigned long long)resets,
        (unsigned long long)aaAttempts,(unsigned long long)aaSuccess,(unsigned long long)aaFail,
        (unsigned long long)ok,(unsigned long long)failed,(unsigned long long)mergeOk,(unsigned long long)mergeFailed,
        hintsOverflow?"true":"false",
        (unsigned long long)indexChecked,(unsigned long long)indexBuilt,
        (unsigned long long)indexVerified,(unsigned long long)indexFailed,(unsigned long long)indexRetry,
        activePins);
}

static void LakeCurrentPinIdentity(bool* isPool,uint64* sessionId,ThreadId* backendPid)
{
    bool pool=IS_THREAD_POOL_WORKER;
    ThreadId pid=t_thrd.proc_cxt.MyProcPid;
    *isPool=pool;
    *backendPid=pid;
    *sessionId=(pool&&u_sess!=NULL)?(uint64)u_sess->session_id:(uint64)pid;
}

static void LakeSnapshotPinOnExit(int code,Datum arg)
{
    (void)code;(void)arg;
    if(lakeMaintenance==NULL)return;
    bool pool=IS_THREAD_POOL_WORKER;
    ThreadId pid=t_thrd.proc_cxt.MyProcPid;
    uint64 sid=(pool&&u_sess!=NULL)?(uint64)u_sess->session_id:0;
    SpinLockAcquire(&lakeMaintenance->lock);
    for(int i=0;i<MAX_LAKE_SNAPSHOT_PINS;++i){
        LakeSnapshotPinEntry* e=&lakeMaintenance->snapshot_pins[i];
        if(!e->active)continue;
        if(pool){
            if(e->is_thread_pool&&((sid!=0&&e->session_id==sid)||e->backend_pid==pid))e->active=false;
        }else{
            if(!e->is_thread_pool&&e->backend_pid==pid)e->active=false;
        }
    }
    SpinLockRelease(&lakeMaintenance->lock);
}

static void LakeReapDeadSnapshotPins(void)
{
    if(lakeMaintenance==NULL)return;
    struct LakePinProbe {
        int idx;
        uint64 pin_gen;
        bool is_thread_pool;
        uint64 session_id;
        ThreadId backend_pid;
        bool dead;
    };
    LakePinProbe probes[MAX_LAKE_SNAPSHOT_PINS];
    int count=0;
    SpinLockAcquire(&lakeMaintenance->lock);
    for(int i=0;i<MAX_LAKE_SNAPSHOT_PINS;++i){
        LakeSnapshotPinEntry* e=&lakeMaintenance->snapshot_pins[i];
        if(!e->active)continue;
        probes[count++]={i,e->pin_gen,e->is_thread_pool,e->session_id,e->backend_pid,false};
    }
    SpinLockRelease(&lakeMaintenance->lock);
    if(count==0)return;
    bool anyDead=false;
    for(int k=0;k<count;++k){
        bool alive=false;
        if(probes[k].is_thread_pool){
            alive=(g_threadPoolControler!=NULL&&g_threadPoolControler->GetSessionCtrl()!=NULL&&
                   g_threadPoolControler->GetSessionCtrl()->FindCtrlIdxBySessId(probes[k].session_id)>=0);
        }else{
            alive=(BackendPidGetProc(probes[k].backend_pid)!=NULL);
        }
        if(!alive){probes[k].dead=true;anyDead=true;}
    }
    if(!anyDead)return;
    SpinLockAcquire(&lakeMaintenance->lock);
    for(int k=0;k<count;++k){
        if(!probes[k].dead)continue;
        LakeSnapshotPinEntry* e=&lakeMaintenance->snapshot_pins[probes[k].idx];
        if(e->active&&e->pin_gen==probes[k].pin_gen)e->active=false;
    }
    SpinLockRelease(&lakeMaintenance->lock);
}

void LakeSnapshotPinRegister(Oid serverOid,int64 snapshotId,const char* pinKind,uint64 handle)
{
    if(lakeMaintenance==NULL||!LakeSnapshotPinVersionEnabled()||
       !OidIsValid(serverOid)||snapshotId<0||!pinKind||IsNativeLakeMaintenanceProcess())
        return;
    static THR_LOCAL bool lakeSnapshotExitHooked=false;
    if(!lakeSnapshotExitHooked){
        on_shmem_exit(LakeSnapshotPinOnExit,(Datum)0);
        lakeSnapshotExitHooked=true;
    }
    bool isPool=false;uint64 sid=0;ThreadId pid=0;
    LakeCurrentPinIdentity(&isPool,&sid,&pid);
    Oid db=u_sess->proc_cxt.MyDatabaseId;
    if(!OidIsValid(db))return;

    bool full=false;
    for(int attempt=0;attempt<2;++attempt){
        int freeSlot=-1;int existingSlot=-1;
        SpinLockAcquire(&lakeMaintenance->lock);
        for(int i=0;i<MAX_LAKE_SNAPSHOT_PINS;++i){
            LakeSnapshotPinEntry* e=&lakeMaintenance->snapshot_pins[i];
            if(!e->active){if(freeSlot<0)freeSlot=i;continue;}
            if(e->database_oid==db&&e->is_thread_pool==isPool&&e->session_id==sid&&
               e->handle==handle&&strcmp(e->pin_kind,pinKind)==0){
                existingSlot=i;break;
            }
        }
        int target=existingSlot>=0?existingSlot:freeSlot;
        if(target>=0){
            LakeSnapshotPinEntry* e=&lakeMaintenance->snapshot_pins[target];
            e->active=true;
            e->is_thread_pool=isPool;
            e->database_oid=db;
            e->server_oid=serverOid;
            e->backend_pid=pid;
            e->session_id=sid;
            e->handle=handle;
            e->pin_gen=++lakeMaintenance->snapshot_pin_clock;
            e->snapshot_id=snapshotId;
            errno_t rc=strncpy_s(e->pin_kind,sizeof(e->pin_kind),pinKind,sizeof(e->pin_kind)-1);
            securec_check(rc,"","");
            e->pin_kind[sizeof(e->pin_kind)-1]='\0';
            SpinLockRelease(&lakeMaintenance->lock);
            return;
        }
        SpinLockRelease(&lakeMaintenance->lock);
        if(attempt==0)LakeReapDeadSnapshotPins();
        else full=true;
    }
    if(full)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("active lake reader snapshot pin registry exhausted"),
            errdetail("At most %d concurrent reader snapshot pins are supported on this single instance.",MAX_LAKE_SNAPSHOT_PINS)));
}

void LakeSnapshotPinUnregisterHandle(uint64 handle)
{
    if(lakeMaintenance==NULL||handle==0)return;
    bool isPool=false;uint64 sid=0;ThreadId pid=0;
    LakeCurrentPinIdentity(&isPool,&sid,&pid);
    Oid db=u_sess->proc_cxt.MyDatabaseId;
    SpinLockAcquire(&lakeMaintenance->lock);
    for(int i=0;i<MAX_LAKE_SNAPSHOT_PINS;++i){
        LakeSnapshotPinEntry* e=&lakeMaintenance->snapshot_pins[i];
        if(!e->active)continue;
        if(e->database_oid==db&&e->is_thread_pool==isPool&&e->session_id==sid&&e->handle==handle)
            e->active=false;
    }
    SpinLockRelease(&lakeMaintenance->lock);
}

void LakeSnapshotPinReleaseSession(void)
{
    if(lakeMaintenance==NULL)return;
    bool isPool=false;uint64 sid=0;ThreadId pid=0;
    LakeCurrentPinIdentity(&isPool,&sid,&pid);
    Oid db=u_sess->proc_cxt.MyDatabaseId;
    SpinLockAcquire(&lakeMaintenance->lock);
    for(int i=0;i<MAX_LAKE_SNAPSHOT_PINS;++i){
        LakeSnapshotPinEntry* e=&lakeMaintenance->snapshot_pins[i];
        if(!e->active)continue;
        if(e->database_oid==db&&e->is_thread_pool==isPool&&e->session_id==sid)
            e->active=false;
    }
    SpinLockRelease(&lakeMaintenance->lock);
}

void LakeSnapshotPinStatsForCatalog(Oid databaseOid,Oid serverOid,int* countOut,int64* minOut)
{
    if(countOut)*countOut=0;
    if(minOut)*minOut=-1;
    if(lakeMaintenance==NULL||!OidIsValid(databaseOid)||!OidIsValid(serverOid))return;
    LakeReapDeadSnapshotPins();
    int count=0;
    int64 minPinned=-1;
    SpinLockAcquire(&lakeMaintenance->lock);
    for(int i=0;i<MAX_LAKE_SNAPSHOT_PINS;++i){
        LakeSnapshotPinEntry* e=&lakeMaintenance->snapshot_pins[i];
        if(!e->active||e->database_oid!=databaseOid||e->server_oid!=serverOid)continue;
        ++count;
        if(e->snapshot_id>=0&&(minPinned<0||e->snapshot_id<minPinned))
            minPinned=e->snapshot_id;
    }
    SpinLockRelease(&lakeMaintenance->lock);
    if(countOut)*countOut=count;
    if(minOut)*minOut=minPinned;
}

int64 LakeSnapshotPinMinForCatalog(Oid databaseOid,Oid serverOid)
{
    int64 minPinned=-1;
    LakeSnapshotPinStatsForCatalog(databaseOid,serverOid,NULL,&minPinned);
    return minPinned;
}

extern "C" Datum og_lake_active_snapshot_pins(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_active_snapshot_pins);
Datum og_lake_active_snapshot_pins(PG_FUNCTION_ARGS)
{
    if(!superuser())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("active_snapshot_pins requires superuser")));
    if(!LakeSnapshotPinVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("active_snapshot_pins requires installed SQL0.68")));
    ReturnSetInfo* result=(ReturnSetInfo*)fcinfo->resultinfo;
    if(!result||!IsA(result,ReturnSetInfo)||!(result->allowedModes&SFRM_Materialize)||!result->econtext)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("active_snapshot_pins requires materialize mode")));

    LakeReapDeadSnapshotPins();
    LakeSnapshotPinEntry entries[MAX_LAKE_SNAPSHOT_PINS];
    int count=0;
    Oid db=u_sess->proc_cxt.MyDatabaseId;
    if(lakeMaintenance!=NULL){
        SpinLockAcquire(&lakeMaintenance->lock);
        for(int i=0;i<MAX_LAKE_SNAPSHOT_PINS;++i){
            if(lakeMaintenance->snapshot_pins[i].active&&lakeMaintenance->snapshot_pins[i].database_oid==db)
                entries[count++]=lakeMaintenance->snapshot_pins[i];
        }
        SpinLockRelease(&lakeMaintenance->lock);
    }

    MemoryContext callerCtx=MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc desc=CreateTemplateTupleDesc(7,false);
    TupleDescInitEntry(desc,1,"database_oid",OIDOID,-1,0);
    TupleDescInitEntry(desc,2,"server_oid",OIDOID,-1,0);
    TupleDescInitEntry(desc,3,"backend_pid",INT8OID,-1,0);
    TupleDescInitEntry(desc,4,"session_id",INT8OID,-1,0);
    TupleDescInitEntry(desc,5,"snapshot_id",INT8OID,-1,0);
    TupleDescInitEntry(desc,6,"pin_kind",TEXTOID,-1,DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc,7,"is_thread_pool",BOOLOID,-1,0);
    desc=BlessTupleDesc(desc);
    Tuplestorestate* store=tuplestore_begin_heap(true,false,Max(64,Min(u_sess->attr.attr_memory.work_mem,1024)));
    for(int i=0;i<count;++i){
        Datum values[7];bool nulls[7]={false};
        values[0]=ObjectIdGetDatum(entries[i].database_oid);
        values[1]=ObjectIdGetDatum(entries[i].server_oid);
        values[2]=Int64GetDatum((int64)entries[i].backend_pid);
        values[3]=Int64GetDatum((int64)entries[i].session_id);
        values[4]=Int64GetDatum(entries[i].snapshot_id);
        values[5]=CStringGetTextDatum(entries[i].pin_kind);
        values[6]=BoolGetDatum(entries[i].is_thread_pool);
        tuplestore_putvalues(store,desc,values,nulls);
    }
    tuplestore_donestoring(store);
    MemoryContextSwitchTo(callerCtx);
    result->returnMode=SFRM_Materialize;
    result->setResult=store;
    result->setDesc=desc;
    return (Datum)0;
}
/* Per-database scheduling cadence is independent of DML commit hints and the
 * short flush cadence; healthy artifacts are not reread every flush tick. */
static bool LakeIndexRoundDue(void)
{
    int64 ignored=0;char after[37];
    if(!LakeIndexMaintenanceCursor(true,NULL,false,&ignored,&ignored,after))return false;
    TimestampTz now=GetCurrentTimestamp();bool due=false;
    SpinLockAcquire(&lakeMaintenance->lock);
    for(uint32 i=0;i<lakeMaintenance->index_cursor_count;++i){LakeIndexCursorHint* hint=&lakeMaintenance->index_cursors[i];
        if(hint->database!=u_sess->proc_cxt.MyDatabaseId||hint->uuid[0])continue;
        hint->last_used=++lakeMaintenance->index_cursor_clock;
        if(hint->next_index_at<=now){due=true;hint->next_index_at=now+(int64)g_instance.attr.attr_storage.lake_index_maintenance_naptime*1000;}
        break;}
    SpinLockRelease(&lakeMaintenance->lock);return due;
}

extern "C" Datum og_lake_test_index_maintenance_gate(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_test_index_maintenance_gate);
Datum og_lake_test_index_maintenance_gate(PG_FUNCTION_ARGS)
{
    if(!superuser())ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("index maintenance diagnostic requires superuser")));
    for(int i=0;i<5;++i)if(PG_ARGISNULL(i))ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("index maintenance diagnostic arguments are required")));
    if(!lakeMaintenance)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake maintenance shared state is unavailable")));
    const char* uuid=DatumGetCString(DirectFunctionCall1(uuid_out,PG_GETARG_DATUM(0)));
    int phase=PG_GETARG_INT32(2),action=PG_GETARG_INT32(4);int64 source=PG_GETARG_INT64(1),key=PG_GETARG_INT64(3);
    if(phase < -1||phase>2||source<0||action<0||action>2)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid index maintenance diagnostic gate")));
    bool busy=false;char segment[37],token[37];uint64 hits;ThreadId pid;int error;
    SpinLockAcquire(&lakeMaintenance->lock);
    if(phase>0){busy=lakeMaintenance->index_gate_phase!=0;
        if(!busy){lakeMaintenance->index_gate_database=u_sess->proc_cxt.MyDatabaseId;
            for(int i=0;i<37;++i)lakeMaintenance->index_gate_uuid[i]=uuid[i];
            lakeMaintenance->index_gate_source=source;lakeMaintenance->index_gate_key=key;
            lakeMaintenance->index_gate_phase=phase;lakeMaintenance->index_gate_action=action;
            lakeMaintenance->index_gate_hits=0;lakeMaintenance->index_gate_pid=0;lakeMaintenance->index_gate_error=0;
            lakeMaintenance->index_gate_segment[0]=lakeMaintenance->index_gate_token[0]='\0';}}
    else if(phase<0&&lakeMaintenance->index_gate_database==u_sess->proc_cxt.MyDatabaseId)lakeMaintenance->index_gate_phase=0;
    hits=lakeMaintenance->index_gate_hits;pid=lakeMaintenance->index_gate_pid;error=lakeMaintenance->index_gate_error;
    for(int i=0;i<37;++i){segment[i]=lakeMaintenance->index_gate_segment[i];token[i]=lakeMaintenance->index_gate_token[i];}
    SpinLockRelease(&lakeMaintenance->lock);
    if(busy)ereport(ERROR,(errcode(ERRCODE_OBJECT_IN_USE),errmsg("an index maintenance diagnostic gate is already armed")));
    PG_RETURN_TEXT_P(cstring_to_text(psprintf("{\"hits\":%llu,\"worker_pid\":%llu,\"segment\":\"%s\",\"token\":\"%s\",\"error_sqlstate\":\"%s\"}",
        (unsigned long long)hits,(unsigned long long)pid,segment,token,error?unpack_sql_state(error):"")));
}
void LakeIndexMaintenanceTestGate(const LakeIndexArtifactTask* task,int phase)
{
    bool hit=false;int action=0;int64 key=0;
    SpinLockAcquire(&lakeMaintenance->lock);
    if(lakeMaintenance->index_gate_phase==phase&&lakeMaintenance->index_gate_database==u_sess->proc_cxt.MyDatabaseId&&
       strcmp(lakeMaintenance->index_gate_uuid,task->uuid)==0&&lakeMaintenance->index_gate_source==task->binding->files[0].file_id){
        hit=true;action=lakeMaintenance->index_gate_action;key=lakeMaintenance->index_gate_key;lakeMaintenance->index_gate_phase=0;
        ++lakeMaintenance->index_gate_hits;lakeMaintenance->index_gate_pid=gs_thread_self();
        for(int i=0;i<37;++i){lakeMaintenance->index_gate_segment[i]=task->segment[i];lakeMaintenance->index_gate_token[i]=task->token[i];}}
    SpinLockRelease(&lakeMaintenance->lock);if(!hit)return;
    int nest=NewGUCNestLevel();
    PG_TRY();{
        /* Diagnostic gate only, never a business lock policy change. */
        SetConfigOption("lockwait_timeout","60s",PGC_SUSET,PGC_S_SESSION);
        LOCKTAG tag;uint64 value=(uint64)key;
        SET_LOCKTAG_ADVISORY(tag,u_sess->proc_cxt.MyDatabaseId,(uint32)(value>>32),(uint32)value,1);
        (void)LockAcquire(&tag,ExclusiveLock,false,false);
        if(!LockRelease(&tag,ExclusiveLock,false))ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("index maintenance gate lock vanished")));
        AtEOXact_GUC(false,nest);
    }PG_CATCH();{AtEOXact_GUC(false,nest);PG_RE_THROW();}PG_END_TRY();
    CHECK_FOR_INTERRUPTS();
    if(action==2)proc_exit(1); /* Simulate loss after durable prepare; private worker only. */
    if(action==1)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("injected error after actual index maintenance phase %d",phase)));
}


/* Private one-shot gate after actual sample return, before native stats writes.
 * Only exact owned test databases install a temporary SQL declaration. */
extern "C" Datum og_lake_test_autoanalyze_gate(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_test_autoanalyze_gate);
Datum og_lake_test_autoanalyze_gate(PG_FUNCTION_ARGS)
{
    if(!superuser())ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("automatic ANALYZE diagnostic requires superuser")));
    for(int i=0;i<3;++i)if(PG_ARGISNULL(i))ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("automatic ANALYZE diagnostic arguments are required")));
    Oid target=PG_GETARG_OID(0);int64 key=PG_GETARG_INT64(1);int phase=PG_GETARG_INT32(2);
    if(!lakeMaintenance||phase < -1||phase>1||key<0)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid automatic ANALYZE gate")));
    if(phase>0){Relation host=heap_open(target,AccessShareLock);bool valid=LakeIsNativeForeignRelation(host);heap_close(host,NoLock);if(!valid)ereport(ERROR,(errcode(ERRCODE_WRONG_OBJECT_TYPE),errmsg("automatic ANALYZE gate requires a native lake table")));}
    bool busy=false;uint64 hits=0;ThreadId pid=0;int64 covered=0,snapshot=0;int error=0;bool live=false;
    SpinLockAcquire(&lakeMaintenance->lock);
    if(phase>0){busy=lakeMaintenance->analyze_gate_phase!=0||lakeMaintenance->analyze_gate_live;
        if(!busy){lakeMaintenance->analyze_gate_database=u_sess->proc_cxt.MyDatabaseId;lakeMaintenance->analyze_gate_relation=target;
            lakeMaintenance->analyze_gate_key=key;lakeMaintenance->analyze_gate_phase=1;lakeMaintenance->analyze_gate_hits=0;
            lakeMaintenance->analyze_gate_pid=0;lakeMaintenance->analyze_gate_error=0;
            lakeMaintenance->analyze_gate_covered=lakeMaintenance->analyze_gate_snapshot=0;}}
    else if(phase<0&&lakeMaintenance->analyze_gate_database==u_sess->proc_cxt.MyDatabaseId){
        lakeMaintenance->analyze_gate_phase=0;
        if(lakeMaintenance->phase==LAKE_WORK_IDLE||lakeMaintenance->worker_pid!=lakeMaintenance->analyze_gate_pid)lakeMaintenance->analyze_gate_live=false;
    }
    if(lakeMaintenance->analyze_gate_database==u_sess->proc_cxt.MyDatabaseId&&lakeMaintenance->analyze_gate_relation==target){
        hits=lakeMaintenance->analyze_gate_hits;pid=lakeMaintenance->analyze_gate_pid;
        covered=lakeMaintenance->analyze_gate_covered;snapshot=lakeMaintenance->analyze_gate_snapshot;
        error=lakeMaintenance->analyze_gate_error;live=lakeMaintenance->analyze_gate_live;}
    SpinLockRelease(&lakeMaintenance->lock);
    if(busy)ereport(ERROR,(errcode(ERRCODE_OBJECT_IN_USE),errmsg("automatic ANALYZE gate is already active")));
    PG_RETURN_TEXT_P(cstring_to_text(psprintf("{\"hits\":%llu,\"worker_pid\":%llu,\"covered\":%lld,\"snapshot\":%lld,\"live\":%s,\"error_sqlstate\":\"%s\"}",
        (unsigned long long)hits,(unsigned long long)pid,(long long)covered,(long long)snapshot,live?"true":"false",error?unpack_sql_state(error):"")));
}
void LakeAutoAnalyzeTestError(Oid relation,int error)
{
    if(!lakeMaintenance)return;SpinLockAcquire(&lakeMaintenance->lock);
    if(lakeMaintenance->analyze_gate_database==u_sess->proc_cxt.MyDatabaseId&&
       lakeMaintenance->analyze_gate_relation==relation&&lakeMaintenance->analyze_gate_pid==gs_thread_self())lakeMaintenance->analyze_gate_error=error;
    SpinLockRelease(&lakeMaintenance->lock);
}
void LakeAutoAnalyzeTestGate(Oid relation,int64 covered,int64 snapshot)
{
    if(!lakeMaintenance)return;bool hit=false;int64 key=0;
    SpinLockAcquire(&lakeMaintenance->lock);
    if(lakeMaintenance->analyze_gate_phase==1&&lakeMaintenance->analyze_gate_database==u_sess->proc_cxt.MyDatabaseId&&
       lakeMaintenance->analyze_gate_relation==relation){
        hit=true;key=lakeMaintenance->analyze_gate_key;lakeMaintenance->analyze_gate_phase=0;
        ++lakeMaintenance->analyze_gate_hits;lakeMaintenance->analyze_gate_pid=gs_thread_self();
        lakeMaintenance->analyze_gate_live=true;lakeMaintenance->analyze_gate_covered=covered;lakeMaintenance->analyze_gate_snapshot=snapshot;}
    SpinLockRelease(&lakeMaintenance->lock);if(!hit)return;
    int nest=NewGUCNestLevel();
    PG_TRY();{
        SetConfigOption("lockwait_timeout","60s",PGC_SUSET,PGC_S_SESSION);
        LOCKTAG tag;uint64 value=(uint64)key;SET_LOCKTAG_ADVISORY(tag,u_sess->proc_cxt.MyDatabaseId,(uint32)(value>>32),(uint32)value,1);
        (void)LockAcquire(&tag,ExclusiveLock,false,false);
        if(!LockRelease(&tag,ExclusiveLock,false))ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("automatic ANALYZE gate lock vanished")));
        AtEOXact_GUC(false,nest);
    }PG_CATCH();{AtEOXact_GUC(false,nest);SpinLockAcquire(&lakeMaintenance->lock);lakeMaintenance->analyze_gate_live=false;SpinLockRelease(&lakeMaintenance->lock);PG_RE_THROW();}PG_END_TRY();
    SpinLockAcquire(&lakeMaintenance->lock);lakeMaintenance->analyze_gate_live=false;SpinLockRelease(&lakeMaintenance->lock);
    CHECK_FOR_INTERRUPTS();
}
