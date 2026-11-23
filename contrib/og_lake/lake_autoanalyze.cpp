/* Automatic ANALYZE uses the native worker, FDW sampler and typanalyze chain.
 * A committed watermark, not pgstat/carrier counters, preserves concurrent DML.
 * There is no business-row scan, artifact I/O or lock held by a new scheduler.
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_version.h"
#include "lake_autoanalyze.h"
#include "lake_native_partition.h"
#include "lake_inline_private.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/og_lake_maintenance.h"
#include "access/og_lake.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "catalog/dependency.h"
#include "commands/extension.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

namespace {
struct LakeAnalyzeAttempt {
    Oid relation;
    char binding[37];
    char table[37];
    int64 covered;
    int64 snapshot;
    bool captured;
    bool sampled;
};
/* Dedicated LAKE_WORKER only. No cross-session cache or global callback. */
static THR_LOCAL LakeAnalyzeAttempt* currentAttempt=NULL;
static void LakeAnalyzeBadState(void)
{ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake automatic ANALYZE control is not trusted")));}
static Datum LakeAnalyzeUuid(const char* text)
{return DirectFunctionCall1(uuid_in,CStringGetDatum(text));}
static void LakeAnalyzeCopyUuid(char* target,const char* source)
{
    if(source==NULL||strlen(source)!=36)LakeAnalyzeBadState();
    errno_t rc=memcpy_s(target,37,source,37);securec_check(rc,"","");
}
static void LakeAnalyzeBegin(MemoryContext keep)
{StartTransactionCommand();PushActiveSnapshot(GetTransactionSnapshot());MemoryContextSwitchTo(keep);}
static void LakeAnalyzeCommit(MemoryContext keep)
{PopActiveSnapshot();CommitTransactionCommand();MemoryContextSwitchTo(keep);}
static Oid LakeAnalyzeStateOid(void)
{
    Oid ns=get_namespace_oid("og_lake_internal",true);
    return OidIsValid(ns)?get_relname_relid("analyze_state",ns):InvalidOid;
}
static void LakeAnalyzeCheckState(void)
{
    Oid owner=LakeMetadataOwner();
    if(!OidIsValid(owner)){LakeInlineRequireAdministrator();owner=GetUserId();}
    Relation relation=heap_open(LakeAnalyzeStateOid(),AccessShareLock);
    LakeValidatePrivateRelation(relation,owner,true);heap_close(relation,NoLock);
}
static void LakeAnalyzeSeed(const LakeAnalyzeAttempt* attempt,Snapshot snapshot)
{
    Oid types[3]={UUIDOID,OIDOID,UUIDOID};
    Datum args[3]={LakeAnalyzeUuid(attempt->binding),ObjectIdGetDatum(attempt->relation),LakeAnalyzeUuid(attempt->table)};
    LakeMetadataQuery("INSERT INTO og_lake_internal.analyze_state "
        "(binding_id,relation_oid,table_uuid,committed_changes,analyzed_changes,analyzed_snapshot,last_success,last_attempt,retry_after,last_error) "
        "VALUES($1,$2,$3,0,0,NULL,NULL,NULL,NULL,NULL) ON CONFLICT(binding_id) DO NOTHING",
        3,types,args,NULL,snapshot,false,SPI_OK_INSERT);
    LakeMetaRows* changed=LakeMetadataQuery("UPDATE og_lake_internal.analyze_state SET last_attempt=pg_catalog.clock_timestamp() "
        "WHERE binding_id=$1 AND relation_oid=$2 AND table_uuid=$3 RETURNING binding_id",
        3,types,args,NULL,snapshot,false,SPI_OK_UPDATE_RETURNING);
    LakeMetadataRequireCount(changed,1,"automatic ANALYZE candidate identity");
}
static bool LakeAnalyzeSelect(LakeAnalyzeAttempt* attempt,Snapshot snapshot)
{
    /* Host-control timestamps provide fairness across rounds, independent of
     * the flush cursor/batch quota. Read-only lake bindings remain analyzable.
     * Private SPI must not join pg_class: its namespace and ownership guard
     * intentionally admits only the bound private catalog. Native syscache
     * supplies advisory relation statistics; sampling revalidates the binding
     * after its relation lock under the existing fresh statement snapshot.
     * Keyset batches keep result memory bounded without skipping later eligible
     * relations when the oldest candidates do not yet need another sample. */
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext batch=AllocSetContextCreate(caller,"Lake ANALYZE discovery batch",ALLOCSET_SMALL_SIZES);
    Oid types[4]={INT4OID,BOOLOID,TIMESTAMPTZOID,OIDOID};
    Datum args[4]={Int32GetDatum(g_instance.attr.attr_storage.lake_autoanalyze_max_delay),
        BoolGetDatum(false),TimestampTzGetDatum(0),ObjectIdGetDatum(InvalidOid)};
    char nulls[5]={' ',' ','n',' ','\0'};
    for(;;){
        CHECK_FOR_INTERRUPTS();MemoryContextSwitchTo(batch);
        LakeMetaRows* rows=LakeMetadataQuery(
            "SELECT r.relation_oid,t.binding_id::text,t.table_uuid::text, "
            "(s.binding_id IS NULL OR s.last_success IS NULL OR "
            "s.last_success<=pg_catalog.clock_timestamp()-$1*interval '1 millisecond'), "
            "COALESCE(s.committed_changes-s.analyzed_changes,0),s.last_attempt "
            "FROM og_lake_internal.relation_binding r JOIN og_lake_internal.table_binding t USING(binding_id) "
            "LEFT JOIN og_lake_internal.analyze_state s ON s.binding_id=t.binding_id "
            "WHERE t.state='active' AND "
            "(s.binding_id IS NULL OR s.retry_after IS NULL OR s.retry_after<=pg_catalog.clock_timestamp()) AND "
            "(NOT $2 OR (s.last_attempt IS NULL AND $3 IS NULL AND r.relation_oid>$4) OR "
            "(s.last_attempt IS NOT NULL AND ($3 IS NULL OR s.last_attempt>$3 OR "
            "(s.last_attempt=$3 AND r.relation_oid>$4)))) "
            "ORDER BY s.last_attempt ASC NULLS FIRST,r.relation_oid LIMIT 64",
            4,types,args,nulls,snapshot,true,SPI_OK_SELECT);
        for(uint32 i=0;i<rows->count;++i){
            CHECK_FOR_INTERRUPTS();
            Oid relationOid=DatumGetObjectId(LakeMetadataValue(rows,i,1,OIDOID));
            bool forced=DatumGetBool(LakeMetadataValue(rows,i,4,BOOLOID));
            int64 pending=DatumGetInt64(LakeMetadataValue(rows,i,5,INT8OID));
            if(pending<0)LakeAnalyzeBadState();
            bool absent=false;
            Datum stamp=LakeMetadataValue(rows,i,6,TIMESTAMPTZOID,true,&absent);
            args[1]=BoolGetDatum(true);args[2]=absent?TimestampTzGetDatum(0):stamp;
            args[3]=ObjectIdGetDatum(relationOid);nulls[2]=absent?'n':' ';
            HeapTuple tuple=SearchSysCache1(RELOID,ObjectIdGetDatum(relationOid));
            if(!HeapTupleIsValid(tuple))continue;
            Form_pg_class cls=(Form_pg_class)GETSTRUCT(tuple);
            /* Preserve the previous FLOAT4 SPI parameter rounding. */
            double threshold=(double)u_sess->attr.attr_storage.autovacuum_anl_thresh+
                (double)(float4)u_sess->attr.attr_storage.autovacuum_anl_scale*Max((double)cls->reltuples,0.0);
            bool due=cls->relkind==RELKIND_FOREIGN_TABLE&&
                (forced||cls->relpages<=0||cls->reltuples<0||(double)pending>threshold);
            ReleaseSysCache(tuple);
            if(!due)continue;
            attempt->relation=relationOid;
            LakeAnalyzeCopyUuid(attempt->binding,LakeMetadataText(rows,i,2));
            LakeAnalyzeCopyUuid(attempt->table,LakeMetadataText(rows,i,3));
            MemoryContextSwitchTo(caller);MemoryContextDelete(batch);
            LakeAnalyzeSeed(attempt,snapshot);return true;
        }
        bool more=rows->count==64;
        MemoryContextSwitchTo(caller);MemoryContextReset(batch);
        if(!more){MemoryContextDelete(batch);return false;}
    }
}
static void LakeAnalyzePublishCoverage(const LakeAnalyzeAttempt* attempt,Snapshot snapshot)
{
    if(!attempt->captured||!attempt->sampled)LakeAnalyzeBadState();
    Oid types[5]={UUIDOID,OIDOID,UUIDOID,INT8OID,INT8OID};
    Datum args[5]={LakeAnalyzeUuid(attempt->binding),ObjectIdGetDatum(attempt->relation),LakeAnalyzeUuid(attempt->table),
        Int64GetDatum(attempt->covered),Int64GetDatum(attempt->snapshot)};
    /* This UPDATE is deliberately the last write before COMMIT. Never lock
     * analyze_state during source reading; never clear committed_changes. */
    LakeMetaRows* rows=LakeMetadataQuery("UPDATE og_lake_internal.analyze_state "
        "SET analyzed_changes=GREATEST(analyzed_changes,$4),analyzed_snapshot=$5,"
        "last_success=pg_catalog.clock_timestamp(),retry_after=NULL,last_error=NULL "
        "WHERE binding_id=$1 AND relation_oid=$2 AND table_uuid=$3 AND committed_changes>=$4 "
        "RETURNING binding_id",5,types,args,NULL,snapshot,false,SPI_OK_UPDATE_RETURNING);
    LakeMetadataRequireCount(rows,1,"completed automatic ANALYZE coverage");
    LakeRecordAutoAnalyzeOutcome(true,true);
}
static void LakeAnalyzeRetry(const LakeAnalyzeAttempt* attempt,int sqlstate)
{
    LakeRecordAutoAnalyzeOutcome(true,false);
    if(!OidIsValid(attempt->relation)||!LakeAutoAnalyzeCatalogAvailable())return;
    LakeAnalyzeCheckState();Oid types[4]={UUIDOID,OIDOID,UUIDOID,INT4OID};
    Datum args[4]={LakeAnalyzeUuid(attempt->binding),ObjectIdGetDatum(attempt->relation),
        LakeAnalyzeUuid(attempt->table),Int32GetDatum(sqlstate)};
    LakeMetadataQuery("UPDATE og_lake_internal.analyze_state SET "
        "retry_after=pg_catalog.clock_timestamp()+interval '30 seconds',last_error=NULLIF($4,0) "
        "WHERE binding_id=$1 AND relation_oid=$2 AND table_uuid=$3",4,types,args,NULL,
        GetActiveSnapshot(),false,SPI_OK_UPDATE);
}
}

bool LakeAutoAnalyzeCatalogAvailable(void)
{
    /* Closed version gate: original SQL0.18 never acquires this capability. */
    Relation relation=heap_open(ExtensionRelationId,AccessShareLock);ScanKeyData key;
    ScanKeyInit(&key,Anum_pg_extension_extname,BTEqualStrategyNumber,F_NAMEEQ,CStringGetDatum("og_lake"));
    SysScanDesc scan=systable_beginscan(relation,ExtensionNameIndexId,true,NULL,1,&key);
    HeapTuple tuple=systable_getnext(scan);bool enabled=false;
    if(HeapTupleIsValid(tuple)){
        bool absent=false;Datum value=tableam_tops_tuple_getattr(tuple,Anum_pg_extension_extversion,RelationGetDescr(relation),&absent);
        if(!absent){char* version=TextDatumGetCString(value);enabled=(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version))))));pfree(version);}
    }
    systable_endscan(scan);heap_close(relation,AccessShareLock);
    if(enabled&&!OidIsValid(LakeAnalyzeStateOid())){if(creating_extension)return false;LakeAnalyzeBadState();}
    return enabled;
}

void LakeCheckAutoAnalyzeControl(Relation relation,Oid owner)
{
    const char* names[]={"binding_id","relation_oid","table_uuid","committed_changes","analyzed_changes",
        "analyzed_snapshot","last_success","last_attempt","retry_after","last_error"};
    const Oid types[]={UUIDOID,OIDOID,UUIDOID,INT8OID,INT8OID,INT8OID,TIMESTAMPTZOID,TIMESTAMPTZOID,TIMESTAMPTZOID,INT4OID};
    TupleDesc desc=RelationGetDescr(relation);
    if(desc->natts!=10||relation->trigdesc!=NULL||
       getExtensionOfObject(RelationRelationId,RelationGetRelid(relation))!=get_extension_oid("og_lake",false))LakeAnalyzeBadState();
    for(int i=0;i<10;++i)if(strcmp(NameStr(desc->attrs[i].attname),names[i])||
        desc->attrs[i].atttypid!=types[i]||desc->attrs[i].attisdropped||desc->attrs[i].attnotnull!=(i<5))LakeAnalyzeBadState();
    if(desc->constr&&(desc->constr->num_check||desc->constr->num_defval))LakeAnalyzeBadState();
    List* indexes=RelationGetIndexList(relation);if(list_length(indexes)!=1)LakeAnalyzeBadState();
    HeapTuple tuple=SearchSysCache1(INDEXRELID,ObjectIdGetDatum(linitial_oid(indexes)));
    if(!HeapTupleIsValid(tuple))LakeAnalyzeBadState();
    Form_pg_index index=(Form_pg_index)GETSTRUCT(tuple);
    if(!index->indisprimary||!index->indisunique||!index->indimmediate||!index->indisvalid||
       index->indnatts!=1||index->indkey.values[0]!=1)LakeAnalyzeBadState();
    ReleaseSysCache(tuple);list_free(indexes);
    /* The caller also validates exact owner/ACL, no defaults, builtin AM,
     * opclasses, no expressions/predicates and no executable triggers. */
    (void)owner;
}

void LakeAutoAnalyzeNotePublished(const LakeInlineBinding* binding,int64 changedRows,Snapshot snapshot)
{
    if(changedRows==0||!LakeAutoAnalyzeCatalogAvailable())return;
    if(changedRows<0)LakeAnalyzeBadState();LakeAnalyzeCheckState();
    Oid types[4]={UUIDOID,OIDOID,UUIDOID,INT8OID};
    Datum args[4]={LakeAnalyzeUuid(binding->binding_id),ObjectIdGetDatum(binding->relation_oid),
        LakeAnalyzeUuid(binding->table_uuid),Int64GetDatum(changedRows)};
    LakeMetaRows* rows=LakeMetadataQuery("INSERT INTO og_lake_internal.analyze_state "
        "(binding_id,relation_oid,table_uuid,committed_changes,analyzed_changes,analyzed_snapshot,last_success,last_attempt,retry_after,last_error) "
        "VALUES($1,$2,$3,$4,0,NULL,NULL,NULL,NULL,NULL) ON CONFLICT(binding_id) DO UPDATE "
        "SET committed_changes=analyze_state.committed_changes+EXCLUDED.committed_changes "
        "WHERE analyze_state.relation_oid=EXCLUDED.relation_oid AND analyze_state.table_uuid=EXCLUDED.table_uuid "
        "AND analyze_state.committed_changes>=0 AND analyze_state.analyzed_changes>=0 "
        "AND analyze_state.analyzed_changes<=analyze_state.committed_changes "
        "AND analyze_state.committed_changes<=9223372036854775807-EXCLUDED.committed_changes RETURNING binding_id",
        4,types,args,NULL,snapshot,false,SPI_OK_INSERT_RETURNING);
    LakeMetadataRequireCount(rows,1,"committed logical ANALYZE change count");
}

void LakeAutoAnalyzeSampleStart(Relation relation,const LakeInlineBinding* binding,Snapshot snapshot)
{
    LakeAnalyzeAttempt* attempt=currentAttempt;if(attempt==NULL)return;
    if(attempt->captured||attempt->relation!=RelationGetRelid(relation)||
        strcmp(attempt->binding,binding->binding_id)||strcmp(attempt->table,binding->table_uuid))LakeAnalyzeBadState();
    Oid types[3]={UUIDOID,OIDOID,UUIDOID};Datum args[3]={LakeAnalyzeUuid(attempt->binding),
        ObjectIdGetDatum(attempt->relation),LakeAnalyzeUuid(attempt->table)};
    LakeMetaRows* rows=LakeMetadataQuery("SELECT committed_changes,analyzed_changes FROM og_lake_internal.analyze_state "
        "WHERE binding_id=$1 AND relation_oid=$2 AND table_uuid=$3",3,types,args,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(rows,1,"sampled automatic ANALYZE origin");
    int64 covered=DatumGetInt64(LakeMetadataValue(rows,0,1,INT8OID));
    int64 previous=DatumGetInt64(LakeMetadataValue(rows,0,2,INT8OID));
    if(previous<0||covered<previous||binding->snapshot_id<0)LakeAnalyzeBadState();
    attempt->covered=covered;attempt->snapshot=binding->snapshot_id;attempt->captured=true;
}
void LakeAutoAnalyzeSampleReturned(Oid relationOid)
{
    if(currentAttempt==NULL)return;
    if(currentAttempt->relation!=relationOid||!currentAttempt->captured)LakeAnalyzeBadState();
    currentAttempt->sampled=true;
    LakeAutoAnalyzeTestGate(relationOid,currentAttempt->covered,currentAttempt->snapshot);
}

bool LakeAutoAnalyzeOne(void (*recover)(bool))
{
    if(!g_instance.attr.attr_storage.enable_lake_autoanalyze)return false;
    if(!IsNativeLakeMaintenanceProcess()||recover==NULL)LakeAnalyzeBadState();
    MemoryContext keep=AllocSetContextCreate(t_thrd.mem_cxt.msg_mem_cxt,"Lake automatic ANALYZE task",ALLOCSET_DEFAULT_SIZES);
    LakeAnalyzeAttempt* attempt=(LakeAnalyzeAttempt*)MemoryContextAllocZero(keep,sizeof(LakeAnalyzeAttempt));
    MemoryContext originalPortal=t_thrd.mem_cxt.portal_mem_cxt;
    bool completed=false;
    PG_TRY();
    {
        SetCurrentStatementStartTimestamp();
        if(!enable_sig_alarm(g_instance.attr.attr_storage.lake_autoanalyze_budget,true))
            ereport(ERROR,(errcode(ERRCODE_SYSTEM_ERROR),errmsg("could not arm automatic lake ANALYZE budget")));
        LakeAnalyzeBegin(keep);
        bool candidate=LakeAutoAnalyzeCatalogAvailable();
        if(candidate){LakeAnalyzeCheckState();candidate=LakeAnalyzeSelect(attempt,GetActiveSnapshot());}
        LakeAnalyzeCommit(keep);
        if(candidate){
            /* No state write lock survives the discovery commit. A fresh
             * statement snapshot is retained for binding + data + C0. */
            t_thrd.mem_cxt.portal_mem_cxt=keep;LakeAnalyzeBegin(keep);
            if(ConditionalLockRelationOid(attempt->relation,ShareUpdateExclusiveLock)){
                Relation relation=try_relation_open(attempt->relation,NoLock);
                if(relation!=NULL){
                    bool trusted=LakeIsNativeForeignRelation(relation);relation_close(relation,NoLock);
                    if(trusted){
                        currentAttempt=attempt;
                        VacuumStmt statement;errno_t rc=memset_s(&statement,sizeof(statement),0,sizeof(statement));securec_check(rc,"","");
                        statement.type=T_VacuumStmt;statement.options=VACOPT_ANALYZE|VACOPT_NOWAIT;
                        statement.flags=VACFLG_SIMPLE_HEAP;statement.rely_oid=InvalidOid;
                        statement.freeze_min_age=-1;statement.freeze_table_age=-1;
                        pgstat_report_activity(STATE_RUNNING,"automatic native lake ANALYZE");
                        /* LAKE_WORKER is not AUTOVACUUM_WORKER; a single-OID
                         * ANALYZE-only call does not commit this transaction. */
                        vacuum(&statement,attempt->relation,false,NULL,true);
                        currentAttempt=NULL;
                        if(attempt->sampled){LakeAnalyzePublishCoverage(attempt,GetActiveSnapshot());completed=true;}
                    }
                }
            }
            if(!completed)LakeAnalyzeRetry(attempt,0);
            LakeAnalyzeCommit(keep);
        }
        currentAttempt=NULL;(void)disable_sig_alarm(true);
        t_thrd.mem_cxt.portal_mem_cxt=originalPortal;
        MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        t_thrd.vacuum_cxt.vac_context=NULL;MemoryContextDelete(keep);
    }
    PG_CATCH();
    {
        currentAttempt=NULL;t_thrd.mem_cxt.portal_mem_cxt=originalPortal;
        MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);ErrorData* original=CopyErrorData();
        LakeAutoAnalyzeTestError(attempt->relation,original->sqlerrcode);
        recover(false);
        /* Retry metadata has its own short transaction/alarm. A blocked or
         * corrupt state never masks the original sampling/native SQLSTATE. */
        PG_TRY();{
            SetCurrentStatementStartTimestamp();
            if(enable_sig_alarm(5000,true)){
                LakeAnalyzeBegin(keep);LakeAnalyzeRetry(attempt,original->sqlerrcode);LakeAnalyzeCommit(keep);
                (void)disable_sig_alarm(true);
            }
        }PG_CATCH();{recover(false);}PG_END_TRY();
        MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        t_thrd.vacuum_cxt.vac_context=NULL;MemoryContextDelete(keep);
        ReThrowError(original);
    }
    PG_END_TRY();
    return completed;
}


void LakeAutoAnalyzeForgetRelation(Oid relationOid,Snapshot snapshot)
{
    if(!LakeAutoAnalyzeCatalogAvailable())return;LakeAnalyzeCheckState();
    Oid type=OIDOID;Datum arg=ObjectIdGetDatum(relationOid);
    LakeMetaRows* rows=LakeMetadataQuery("DELETE FROM og_lake_internal.analyze_state WHERE "
        "(binding_id,relation_oid,table_uuid) IN (SELECT t.binding_id,r.relation_oid,t.table_uuid "
        "FROM og_lake_internal.table_binding t JOIN og_lake_internal.relation_binding r USING(binding_id) "
        "WHERE r.relation_oid=$1)",1,&type,&arg,NULL,snapshot,false,SPI_OK_DELETE);
    if(rows->count>1)LakeAnalyzeBadState();
}
void LakeAutoAnalyzeForgetServer(Oid serverOid,Snapshot snapshot)
{
    if(!LakeAutoAnalyzeCatalogAvailable())return;LakeAnalyzeCheckState();
    Oid type=OIDOID;Datum arg=ObjectIdGetDatum(serverOid);
    LakeMetadataQuery("DELETE FROM og_lake_internal.analyze_state WHERE "
        "(binding_id,relation_oid,table_uuid) IN (SELECT t.binding_id,r.relation_oid,t.table_uuid "
        "FROM og_lake_internal.table_binding t JOIN og_lake_internal.relation_binding r USING(binding_id) "
        "WHERE t.server_oid=$1)",1,&type,&arg,NULL,snapshot,false,SPI_OK_DELETE);
}
