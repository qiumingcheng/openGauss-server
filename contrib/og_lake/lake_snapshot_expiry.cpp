/* Native logical snapshot expiry, adapted from DuckLake 0aaa8e10d4adfe2ca37da77134e991d20106d0e7
 * functions/ducklake_expire_snapshots.cpp and DeleteSnapshotsSql() in
 * storage/ducklake_metadata_manager.cpp (MIT, lake_catalog_ddl.inc notice).
 * This bounded first slice deliberately retains ALL non-snapshot metadata,
 * inline versions and files. In particular snapshot_changes is never deleted.
 * It grants no public time-travel API and performs no SDK/file operation. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_version.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/reltrigger.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

extern "C" Datum og_lake_expire_snapshots(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_expire_snapshots);

static const int LakeSnapshotExpiryLimit=512;
/* MetadataQuery deliberately admits one trusted namespace per invocation.
 * Bound the separate control-schema identity transfer without truncating a
 * protection set: overflow aborts the entire call before any DELETE. */
static const int LakeSnapshotExpiryCertificateLimit=65536;

static void LakeExpiryRelationShape(Relation relation)
{
    const char* names[]={"snapshot_id","snapshot_time","schema_version","next_catalog_id","next_file_id"};
    const Oid types[]={INT8OID,TIMESTAMPTZOID,INT8OID,INT8OID,INT8OID};
    TupleDesc descriptor=RelationGetDescr(relation);
    TupleConstr* constraints=descriptor->constr;
    /* An incoming cascading FK also creates triggers on this relation. Never
     * turn expiry into an indirect deletion of changes or other catalog data. */
    if(descriptor->natts!=5||relation->trigdesc!=NULL||relation->rd_rules!=NULL||
       (constraints&&(constraints->num_check||constraints->num_defval)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("snapshot expiry requires the unmodified DuckLake 1.1 snapshot relation")));
    for(int i=0;i<5;++i){
        Form_pg_attribute column=&descriptor->attrs[i];
        if(column->attisdropped||strcmp(NameStr(column->attname),names[i])||column->atttypid!=types[i])
            ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("snapshot expiry catalog column layout changed")));
    }
    bool primary=false;List* indexes=RelationGetIndexList(relation);ListCell* cell=NULL;
    foreach(cell,indexes){
        Relation index=index_open(lfirst_oid(cell),AccessShareLock);
        if(index->rd_index->indisprimary&&index->rd_index->indisvalid&&index->rd_index->indimmediate&&
           index->rd_index->indnatts==1&&index->rd_index->indkey.values[0]==1&&index->rd_rel->relam==BTREE_AM_OID)
            primary=true;
        index_close(index,NoLock);
    }
    list_free(indexes);
    if(!primary)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("snapshot expiry requires the native snapshot primary key")));
}

static ArrayType* LakeExpiryVersions(FunctionCallInfo fcinfo,bool* specified)
{
    *specified=!PG_ARGISNULL(1);
    if(!*specified)return construct_empty_array(INT8OID);
    ArrayType* input=PG_GETARG_ARRAYTYPE_P(1);
    if(ARR_ELEMTYPE(input)!=INT8OID||ARR_NDIM(input)>1||
       ArrayGetNItems(ARR_NDIM(input),ARR_DIMS(input))>LakeSnapshotExpiryLimit)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("snapshot expiry admits at most512 bigint version entries")));
    Datum* values=NULL;bool* nulls=NULL;int count=0;
    deconstruct_array(input,INT8OID,8,FLOAT8PASSBYVAL,'d',&values,&nulls,&count);
    for(int i=0;i<count;++i)
        if(!nulls[i]&&DatumGetInt64(values[i])<0)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("snapshot versions must be nonnegative")));
    return input; /* ANY preserves harmless duplicates and ignores NULL entries. */
}

static ArrayType* LakeExpiryIds(const LakeMetaRows* rows)
{
    if(rows->count>MaxAllocSize/sizeof(Datum)||rows->count>(uint32)PG_INT32_MAX)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("snapshot protection identity set is too large")));
    Datum* values=(Datum*)palloc(Max((Size)1,(Size)rows->count*sizeof(Datum)));int count=0;
    for(uint32 i=0;i<rows->count;++i)
        values[count++]=LakeMetadataValue(rows,i,1,INT8OID);
    return count?construct_array(values,count,INT8OID,8,FLOAT8PASSBYVAL,'d'):construct_empty_array(INT8OID);
}

static LakeMetaRows* LakeExpiryCandidates(LakeInlineBinding* binding,ArrayType* versions,ArrayType* protectedIds,
    bool hasVersions,bool hasTime,TimestampTz older,int64 minPinned,Snapshot snapshot)
{
    Oid types[6]={INT8OID,INT8ARRAYOID,BOOLOID,TIMESTAMPTZOID,INT8ARRAYOID,INT8OID};
    Datum args[6]={Int64GetDatum(binding->snapshot_id),PointerGetDatum(versions),BoolGetDatum(hasVersions),
        TimestampTzGetDatum(older),PointerGetDatum(protectedIds),Int64GetDatum(minPinned)};
    LakeMetaRows* rows=LakeMetadataQuery(psprintf(
        "SELECT snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id FROM %s "
        "WHERE snapshot_id<>$1 AND snapshot_id<>ALL($5) AND ($6<0 OR snapshot_id<$6) "
        "AND (($3 AND snapshot_id=ANY($2)) OR (NOT $3 AND snapshot_time<$4)) "
        "ORDER BY snapshot_id LIMIT 512",LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_snapshot")),
        6,types,args,hasTime?NULL:"   n  ",snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    for(uint32 i=0;i<rows->count;++i){
        int64 id=DatumGetInt64(LakeMetadataValue(rows,i,1,INT8OID));
        if(id<0||id>=binding->snapshot_id||
           (i&&DatumGetInt64(LakeMetadataValue(rows,i-1,1,INT8OID))>=id))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("snapshot expiry candidate range is inconsistent")));
        for(int column=3;column<=5;++column)
            if(DatumGetInt64(LakeMetadataValue(rows,i,column,INT8OID))<0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("snapshot expiry candidate counters are invalid")));
    }
    return rows;
}

static LakeMetaRows* LakeExpiryCertificates(LakeInlineBinding* binding,Snapshot snapshot)
{
    Oid type=OIDOID;Datum argument=ObjectIdGetDatum(binding->server_oid);
    /* Native parent certificates still resolve the anchor's schema_version.
     * Reuse this existing durable dependency as a protected-snapshot set;
     * there is no new pin/tag catalog, and no old-reader or PID enumeration. */
    LakeMetaRows* protectedRows=LakeMetadataQuery(
        "SELECT DISTINCT p.certified_snapshot FROM og_lake_internal.partition_binding p "
        "JOIN og_lake_internal.table_binding t USING(binding_id) WHERE t.server_oid=$1 "
        "AND t.state='active' AND t.object_kind='partition' ORDER BY p.certified_snapshot LIMIT 65537",
        1,&type,&argument,NULL,snapshot,true,SPI_OK_SELECT);
    if(protectedRows->count>LakeSnapshotExpiryCertificateLimit)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("snapshot expiry certificate protection exceeds65536 distinct anchors")));
    for(uint32 i=0;i<protectedRows->count;++i){
        int64 id=DatumGetInt64(LakeMetadataValue(protectedRows,i,1,INT8OID));
        if(id<0||id>binding->snapshot_id||
           (i&&DatumGetInt64(LakeMetadataValue(protectedRows,i-1,1,INT8OID))>=id))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("snapshot certificate protection returned an invalid identity")));
    }
    return protectedRows;
}

static void LakeExpiryDelete(LakeInlineBinding* binding,const LakeMetaRows* rows,Snapshot snapshot)
{
    ArrayType* ids=LakeExpiryIds(rows);int count=ArrayGetNItems(ARR_NDIM(ids),ARR_DIMS(ids));
    if(!count)return;
    Oid types[2]={INT8ARRAYOID,INT8OID};Datum args[2]={PointerGetDatum(ids),Int64GetDatum(binding->snapshot_id)};
    /* Predicate rechecks the selected range and the protected MAX. The
     * registration/SRE locks keep publisher and certificate mutations out. */
    LakeMetaRows* changed=LakeMetadataQuery(psprintf(
        "DELETE FROM %s WHERE snapshot_id=ANY($1) AND snapshot_id<>$2 RETURNING snapshot_id",
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_snapshot")),
        2,types,args,NULL,snapshot,false,SPI_OK_DELETE_RETURNING,binding->metadata_schema_oid);
    if((int)changed->count!=count)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("snapshot expiry selected set changed before deletion")));
    for(uint32 i=0;i<changed->count;++i){
        int64 id=DatumGetInt64(LakeMetadataValue(changed,i,1,INT8OID));bool found=false;
        for(uint32 j=0;j<rows->count;++j)
            if(DatumGetInt64(LakeMetadataValue(rows,j,1,INT8OID))==id){found=true;break;}
        if(!found)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("snapshot expiry deleted an unselected identity")));
    }
}

static void LakeExpiryAppendRelativePart(StringInfo target, const char* part)
{
    if(part==NULL||*part=='\0')return;
    if(*part=='/'||strchr(part,':')!=NULL)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("expired lake file contains non-relative path component")));
    const char* at=part;
    while(*at){
        const char* end=strchr(at,'/');
        if(end==NULL)end=at+strlen(at);
        Size len=end-at;
        if((len==1&&at[0]=='.')||(len==2&&at[0]=='.'&&at[1]=='.'))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("expired lake file contains invalid path component")));
        if(len>0){
            if(target->len>0&&target->data[target->len-1]!='/')appendStringInfoChar(target,'/');
            appendBinaryStringInfo(target,at,len);
        }
        at=*end?end+1:end;
    }
}

static void LakeExpiryScheduleBatch(Oid ns, const LakeMetaRows* rows, const char* scheduledRel)
{
    for(uint32 i=0;i<rows->count;++i){
        int64 fid=DatumGetInt64(LakeMetadataValue(rows,i,1,INT8OID));
        bool fNull=false,tNull=false,sNull=false;
        Datum fDat=LakeMetadataValue(rows,i,2,TEXTOID,true,&fNull);
        bool fRel=DatumGetBool(LakeMetadataValue(rows,i,3,BOOLOID));
        Datum tDat=LakeMetadataValue(rows,i,4,TEXTOID,true,&tNull);
        bool tRel=DatumGetBool(LakeMetadataValue(rows,i,5,BOOLOID));
        Datum sDat=LakeMetadataValue(rows,i,6,TEXTOID,true,&sNull);
        bool sRel=DatumGetBool(LakeMetadataValue(rows,i,7,BOOLOID));
        const char* fPath=fNull?"":TextDatumGetCString(fDat);
        const char* tPath=tNull?"":TextDatumGetCString(tDat);
        const char* sPath=sNull?"":TextDatumGetCString(sDat);
        bool allRelative=fRel&&tRel&&sRel;
        const char* outPath=fPath;
        if(allRelative){
            StringInfoData rel;initStringInfo(&rel);
            LakeExpiryAppendRelativePart(&rel,sPath);
            LakeExpiryAppendRelativePart(&rel,tPath);
            LakeExpiryAppendRelativePart(&rel,fPath);
            if(rel.len==0||rel.len>=MAXPGPATH)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("expired lake file produced invalid catalog-relative path")));
            outPath=rel.data;
        }
        Oid st[3]={INT8OID,TEXTOID,BOOLOID};
        Datum sa[3]={Int64GetDatum(fid),CStringGetTextDatum(outPath),BoolGetDatum(allRelative)};
        (void)LakeMetadataQuery(psprintf(
            "INSERT INTO %s(data_file_id,path,path_is_relative,schedule_start) "
            "SELECT $1,$2,$3,pg_catalog.now() WHERE NOT EXISTS (SELECT 1 FROM %s sc WHERE sc.data_file_id=$1 AND sc.path=$2)",
            scheduledRel,scheduledRel),3,st,sa,NULL,GetLatestSnapshot(),false,SPI_OK_INSERT,ns);
    }
}

static void LakeExpiryScheduleUnusedFiles(LakeInlineBinding* binding, Snapshot snapshot)
{
    Oid ns=binding->metadata_schema_oid;
    char* dataRel=LakeMetadataRelation(ns,"ducklake_data_file");
    char* delRel=LakeMetadataRelation(ns,"ducklake_delete_file");
    char* tblRel=LakeMetadataRelation(ns,"ducklake_table");
    char* schRel=LakeMetadataRelation(ns,"ducklake_schema");
    char* snapRel=LakeMetadataRelation(ns,"ducklake_snapshot");
    char* schedRel=LakeMetadataRelation(ns,"ducklake_files_scheduled_for_deletion");
    char* colStatsRel=LakeMetadataRelation(ns,"ducklake_file_column_stats");
    char* partValRel=LakeMetadataRelation(ns,"ducklake_file_partition_value");

    CommandCounterIncrement();
    Snapshot workSnap=RegisterSnapshot(GetLatestSnapshot());
    PG_TRY();{
        LakeMetaRows* dataRows=LakeMetadataQuery(psprintf(
            "SELECT DISTINCT ON (f.data_file_id) "
            "f.data_file_id, f.path::text, f.path_is_relative, "
            "COALESCE(t.path::text,''), COALESCE(t.path_is_relative,true), "
            "COALESCE(s.path::text,''), COALESCE(s.path_is_relative,true) "
            "FROM %s f "
            "JOIN %s t ON t.table_id=f.table_id AND f.begin_snapshot>=t.begin_snapshot AND (t.end_snapshot IS NULL OR f.begin_snapshot<t.end_snapshot) "
            "JOIN %s s ON s.schema_id=t.schema_id AND t.begin_snapshot>=s.begin_snapshot AND (s.end_snapshot IS NULL OR t.begin_snapshot<s.end_snapshot) "
            "WHERE ((f.end_snapshot IS NOT NULL AND NOT EXISTS ("
            "  SELECT 1 FROM %s sn WHERE sn.snapshot_id>=f.begin_snapshot AND sn.snapshot_id<f.end_snapshot)) "
            " OR (t.end_snapshot IS NOT NULL AND NOT EXISTS ("
            "  SELECT 1 FROM %s sn WHERE sn.snapshot_id>=t.begin_snapshot AND sn.snapshot_id<t.end_snapshot) "
            "  AND NOT EXISTS (SELECT 1 FROM %s t2 WHERE t2.table_id=t.table_id AND (t2.end_snapshot IS NULL OR EXISTS ("
            "   SELECT 1 FROM %s sn2 WHERE sn2.snapshot_id>=t2.begin_snapshot AND sn2.snapshot_id<t2.end_snapshot))))) "
            "ORDER BY f.data_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC",
            dataRel,tblRel,schRel,snapRel,snapRel,tblRel,snapRel),
            0,NULL,NULL,NULL,workSnap,true,SPI_OK_SELECT,ns);

        if(dataRows->count>0){
            Datum* fids=(Datum*)palloc(sizeof(Datum)*dataRows->count);
            for(uint32 i=0;i<dataRows->count;++i)
                fids[i]=LakeMetadataValue(dataRows,i,1,INT8OID);
            ArrayType* fidArr=construct_array(fids,dataRows->count,INT8OID,8,FLOAT8PASSBYVAL,'d');
            Oid dt[1]={INT8ARRAYOID};Datum da[1]={PointerGetDatum(fidArr)};
            (void)LakeMetadataQuery(psprintf("DELETE FROM %s WHERE data_file_id=ANY($1)",colStatsRel),
                1,dt,da,NULL,workSnap,false,SPI_OK_DELETE,ns);
            (void)LakeMetadataQuery(psprintf("DELETE FROM %s WHERE data_file_id=ANY($1)",partValRel),
                1,dt,da,NULL,workSnap,false,SPI_OK_DELETE,ns);
            (void)LakeMetadataQuery(psprintf("DELETE FROM %s WHERE data_file_id=ANY($1)",dataRel),
                1,dt,da,NULL,workSnap,false,SPI_OK_DELETE,ns);
            LakeExpiryScheduleBatch(ns,dataRows,schedRel);
            CommandCounterIncrement();
        }

        Snapshot delSnap=RegisterSnapshot(GetLatestSnapshot());
        PG_TRY();{
            LakeMetaRows* delRows=LakeMetadataQuery(psprintf(
                "SELECT DISTINCT ON (d.delete_file_id) "
                "d.delete_file_id, d.path::text, d.path_is_relative, "
                "COALESCE(t.path::text,''), COALESCE(t.path_is_relative,true), "
                "COALESCE(s.path::text,''), COALESCE(s.path_is_relative,true) "
                "FROM %s d "
                "JOIN %s t ON t.table_id=d.table_id AND d.begin_snapshot>=t.begin_snapshot AND (t.end_snapshot IS NULL OR d.begin_snapshot<t.end_snapshot) "
                "JOIN %s s ON s.schema_id=t.schema_id AND t.begin_snapshot>=s.begin_snapshot AND (s.end_snapshot IS NULL OR t.begin_snapshot<s.end_snapshot) "
                "WHERE ((d.end_snapshot IS NOT NULL AND NOT EXISTS ("
                "  SELECT 1 FROM %s sn WHERE sn.snapshot_id>=d.begin_snapshot AND sn.snapshot_id<d.end_snapshot)) "
                " OR NOT EXISTS (SELECT 1 FROM %s df WHERE df.data_file_id=d.data_file_id)) "
                "ORDER BY d.delete_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC",
                delRel,tblRel,schRel,snapRel,dataRel),
                0,NULL,NULL,NULL,delSnap,true,SPI_OK_SELECT,ns);
            if(delRows->count>0){
                Datum* dids=(Datum*)palloc(sizeof(Datum)*delRows->count);
                for(uint32 i=0;i<delRows->count;++i)
                    dids[i]=LakeMetadataValue(delRows,i,1,INT8OID);
                ArrayType* didArr=construct_array(dids,delRows->count,INT8OID,8,FLOAT8PASSBYVAL,'d');
                Oid dt[1]={INT8ARRAYOID};Datum da[1]={PointerGetDatum(didArr)};
                (void)LakeMetadataQuery(psprintf("DELETE FROM %s WHERE delete_file_id=ANY($1)",delRel),
                    1,dt,da,NULL,delSnap,false,SPI_OK_DELETE,ns);
                LakeExpiryScheduleBatch(ns,delRows,schedRel);
            }
            UnregisterSnapshot(delSnap);
        }PG_CATCH();{
            UnregisterSnapshot(delSnap);
            PG_RE_THROW();
        }PG_END_TRY();
        UnregisterSnapshot(workSnap);
    }PG_CATCH();{
        UnregisterSnapshot(workSnap);
        PG_RE_THROW();
    }PG_END_TRY();
}

Datum og_lake_expire_snapshots(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();LakeRequireAccessPolicy();
    if(!LakeSnapshotExpiryVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("logical snapshot expiry requires its exact installed SQL0.50 contract")));
    if(PG_ARGISNULL(0)||PG_ARGISNULL(3))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("snapshot expiry catalog and dry_run cannot be NULL")));
    bool dry=PG_GETARG_BOOL(3),hasVersions=false,hasTime=!PG_ARGISNULL(2);
    ArrayType* versions=LakeExpiryVersions(fcinfo,&hasVersions);
    if(hasVersions&&hasTime)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("snapshot expiry versions and older_than are mutually exclusive")));
    if(!dry){
        if(u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("snapshot expiry requires READ COMMITTED")));
        if(u_sess->attr.attr_common.XactReadOnly)
            ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot expire snapshots in a read-only transaction")));
        LakeTxnRejectBindingDdl();LakeLockCatalogRegistration();
    }
    ReturnSetInfo* result=(ReturnSetInfo*)fcinfo->resultinfo;
    if(result==NULL||!IsA(result,ReturnSetInfo)||!(result->allowedModes&SFRM_Materialize)||!result->econtext)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("snapshot expiry requires materialize mode")));
    ForeignServer* server=GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)),false);
    MemoryContext caller=MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc descriptor=CreateTemplateTupleDesc(5,false);
    TupleDescInitEntry(descriptor,1,"snapshot_id",INT8OID,-1,0);
    TupleDescInitEntry(descriptor,2,"snapshot_time",TIMESTAMPTZOID,-1,0);
    TupleDescInitEntry(descriptor,3,"schema_version",INT8OID,-1,0);
    TupleDescInitEntry(descriptor,4,"next_catalog_id",INT8OID,-1,0);
    TupleDescInitEntry(descriptor,5,"next_file_id",INT8OID,-1,0);
    descriptor=BlessTupleDesc(descriptor);
    Tuplestorestate* store=tuplestore_begin_heap(true,false,Max(64,Min(u_sess->attr.attr_memory.work_mem,1024)));
    Snapshot volatile snapshot=NULL;
    PG_TRY();{
        Snapshot initial=ActiveSnapshotSet()?GetActiveSnapshot():GetTransactionSnapshot();
        LakeInlineBinding* binding=LakeLoadCatalogHeader(server->serverid,initial);
        Oid ns=binding->metadata_schema_oid;
        if(!dry&&binding->read_only)
            ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot expire a read-only lake catalog")));
        Relation snapshots=heap_open(get_relname_relid("ducklake_snapshot",ns),dry?AccessShareLock:ShareRowExclusiveLock);
        LakeExpiryRelationShape(snapshots);heap_close(snapshots,NoLock);
        /* Actual expiry may have waited for a publisher's RowExclusiveLock.
         * Take a fresh RC snapshot only AFTER obtaining the conflicting lock.
         * All transaction locks stay until caller COMMIT/ROLLBACK, not return. */
        if(!dry)CommandCounterIncrement();
        snapshot=RegisterSnapshot(dry?initial:GetLatestSnapshot());
        LakeInlineBinding* fresh=LakeLoadCatalogHeader(server->serverid,snapshot);
        if(fresh->metadata_schema_oid!=ns||fresh->server_oid!=binding->server_oid||
           strcmp(fresh->data_root,binding->data_root)||(!dry&&fresh->read_only))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("snapshot expiry catalog binding changed")));
        if(hasVersions||hasTime){
            /* Exclude protected anchors BEFORE the limit, so a long protected
             * prefix cannot starve an older_than caller's next bounded batch. */
            LakeMetaRows* certificates=LakeExpiryCertificates(fresh,snapshot);
            int64 minPinned=LakeSnapshotPinVersionEnabled()?
                LakeSnapshotPinMinForCatalog(u_sess->proc_cxt.MyDatabaseId,fresh->server_oid):-1;
            LakeMetaRows* rows=LakeExpiryCandidates(fresh,versions,LakeExpiryIds(certificates),hasVersions,hasTime,
                hasTime?PG_GETARG_TIMESTAMPTZ(2):(TimestampTz)0,minPinned,snapshot);
            if(!dry){
                /* Recheck the head/certified/pinned sets directly before mutation. No
                 * snapshot_changes, counters, schema, inline or files mutate. */
                LakeInlineBinding* check=LakeLoadCatalogHeader(server->serverid,snapshot);
                if(check->snapshot_id!=fresh->snapshot_id||check->schema_version!=fresh->schema_version)
                    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("snapshot expiry head changed before deletion")));
                int64 minPinnedAgain=LakeSnapshotPinVersionEnabled()?
                    LakeSnapshotPinMinForCatalog(u_sess->proc_cxt.MyDatabaseId,fresh->server_oid):-1;
                if(minPinnedAgain>=0){
                    for(uint32 i=0;i<rows->count;++i){
                        if(DatumGetInt64(LakeMetadataValue(rows,i,1,INT8OID))>=minPinnedAgain)
                            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                errmsg("active snapshot pin changed before snapshot deletion")));
                    }
                }
                LakeMetaRows* again=LakeExpiryCertificates(fresh,snapshot);
                if(certificates->count!=again->count)
                    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("snapshot certificate protection changed")));
                for(uint32 i=0;i<certificates->count;++i)
                    if(DatumGetInt64(LakeMetadataValue(certificates,i,1,INT8OID))!=
                       DatumGetInt64(LakeMetadataValue(again,i,1,INT8OID)))
                        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("snapshot certificate protection changed")));
                LakeExpiryDelete(fresh,rows,snapshot);
                if(rows->count>0){
                    LakeTxnMarkSnapshotExpiry();
                    if(LakePhysicalCleanupVersionEnabled())
                        LakeExpiryScheduleUnusedFiles(fresh,snapshot);
                }
            }
            for(uint32 i=0;i<rows->count;++i){
                Datum values[5];bool nulls[5]={false};
                for(int j=0;j<5;++j)values[j]=LakeMetadataValue(rows,i,j+1,
                    j==1?TIMESTAMPTZOID:INT8OID,j==1,&nulls[j]);
                tuplestore_putvalues(store,descriptor,values,nulls);
            }
        }
        UnregisterSnapshot(snapshot);snapshot=NULL;
        tuplestore_donestoring(store);result->returnMode=SFRM_Materialize;result->setResult=store;result->setDesc=descriptor;
        MemoryContextSwitchTo(caller);
    }PG_CATCH();{
        if(snapshot)UnregisterSnapshot(snapshot);
        MemoryContextSwitchTo(caller);PG_RE_THROW();
    }PG_END_TRY();
    PG_RETURN_NULL();
}
