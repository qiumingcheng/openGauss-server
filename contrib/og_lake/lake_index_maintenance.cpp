/* One physical segment per worker. Transactions containing business/catalog
 * locks never perform artifact I/O. BUILDING + inactive coverage is a durable
 * source reservation, not query coverage; future physical GC must honor it.
 */
#include "postgres.h"
#include "lake_version.h"
#include "knl/knl_variable.h"
#include "lake_index_maintenance.h"
#include "lake_index_private.h"
#include "lake_inline_ann.h"
#include "lake_scalar_key.h"
#include "lake_file_pq.h"
#include "access/heapam.h"
#include "access/og_lake_maintenance.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include <sys/stat.h>
#include <stdlib.h>

namespace {
static Datum LakeIndexUuid(const char* text)
{return DirectFunctionCall1(uuid_in,CStringGetDatum(text));}
static void LakeIndexBegin(MemoryContext owner)
{StartTransactionCommand();PushActiveSnapshot(GetTransactionSnapshot());MemoryContextSwitchTo(owner);}
static void LakeIndexCommit(MemoryContext owner)
{PopActiveSnapshot();CommitTransactionCommand();MemoryContextSwitchTo(owner);}
static void LakeIndexChanged(const char* detail)
{ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("background lake index input changed"),errdetail("%s",detail)));}
static bool LakeIndexSameFile(const LakeInlineBinding* a,const LakeInlineBinding* b)
{
    if(a->file_count!=1||b->file_count!=1||a->relation_oid!=b->relation_oid||a->server_oid!=b->server_oid||
       a->metadata_schema_oid!=b->metadata_schema_oid||a->column_count!=b->column_count||b->read_only||
       LakePermanentTableId(a)!=LakePermanentTableId(b)||strcmp(a->binding_id,b->binding_id)||
       strcmp(a->table_uuid,b->table_uuid)||strcmp(a->data_root,b->data_root))return false;
    for(int i=0;i<a->column_count;++i)if(a->columns[i].column_id!=b->columns[i].column_id||
       a->columns[i].type!=b->columns[i].type||a->columns[i].typmod!=b->columns[i].typmod||
       a->columns[i].element_column_id!=b->columns[i].element_column_id)return false;
    const LakeDataFile *x=&a->files[0],*y=&b->files[0];
    return x->file_id==y->file_id&&x->begin_snapshot==y->begin_snapshot&&x->row_id_start==y->row_id_start&&
        x->row_id_start_null==y->row_id_start_null&&x->record_count==y->record_count&&x->file_bytes==y->file_bytes&&
        x->row_group_count==y->row_group_count&&x->partial_max==y->partial_max&&strcmp(x->path,y->path)==0;
}
static char* LakeIndexRoot(Oid server,Snapshot snapshot)
{
    Oid type=OIDOID;Datum arg=ObjectIdGetDatum(server);
    LakeMetaRows* rows=LakeMetadataQuery("SELECT index_root::text FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
        1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(rows,1,"background index store");bool absent=false;
    Datum value=LakeMetadataValue(rows,0,1,TEXTOID,true,&absent);
    return absent?NULL:TextDatumGetCString(value);
}
/* These catalog copies are allocated in task context by LakeMetadataQuery,
 * then reduced to owned strings/integers. No Relation/TupleDesc/Datum is kept
 * across COMMIT. This is also the expected-current-coverage CAS identity. */
static void LakeIndexKnown(LakeIndexArtifactTask* task,Snapshot snapshot,bool compare)
{
    Oid types[3]={UUIDOID,INT8OID,INT8OID};Datum args[3]={LakeIndexUuid(task->uuid),Int64GetDatum(task->generation),
        Int64GetDatum(task->binding->files[0].file_id)};
    LakeMetaRows* rows=LakeMetadataQuery("SELECT s.segment_uuid::text,s.build_token::text,s.artifact_root_uri::text,"
        "s.manifest_hash::text,s.source_manifest_hash::text,c.schema_mapping_hash::text,c.source_checksum::text,"
        "c.source_path::text,s.algorithm_version,s.format_version,c.physical_row_count,c.source_file_bytes "
        "FROM og_lake_internal.index_segment s JOIN og_lake_internal.index_coverage c "
        "USING(segment_uuid,index_uuid,definition_generation) WHERE c.index_uuid=$1 AND c.definition_generation=$2 "
        "AND c.source_file_id=$3 AND c.active AND s.state='READY' LIMIT 2",3,types,args,NULL,snapshot,true,SPI_OK_SELECT);
    if(rows->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("multiple live background index coverages")));
    if(compare){
        if((rows->count==1)!=task->known)LakeIndexChanged("active source coverage changed");
        if(rows->count){const char* expected[]={task->known_segment,task->known_token,task->known_directory,task->known_manifest,
                task->known_source,task->known_schema,task->known_checksum,task->known_path};
            for(int i=0;i<8;++i)if(strcmp(LakeMetadataText(rows,0,i+1),expected[i]))LakeIndexChanged("active segment/coverage identity changed");
            if(DatumGetInt32(LakeMetadataValue(rows,0,9,INT4OID))!=task->known_algorithm||
               DatumGetInt32(LakeMetadataValue(rows,0,10,INT4OID))!=task->known_format||
               DatumGetInt64(LakeMetadataValue(rows,0,11,INT8OID))!=task->known_rows||
               DatumGetInt64(LakeMetadataValue(rows,0,12,INT8OID))!=task->known_bytes)LakeIndexChanged("active coverage physical extent changed");}
        return;
    }
    task->known=rows->count==1;if(!task->known)return;
    task->known_segment=LakeMetadataText(rows,0,1);task->known_token=LakeMetadataText(rows,0,2);
    task->known_directory=LakeMetadataText(rows,0,3);task->known_manifest=LakeMetadataText(rows,0,4);
    task->known_source=LakeMetadataText(rows,0,5);task->known_schema=LakeMetadataText(rows,0,6);
    task->known_checksum=LakeMetadataText(rows,0,7);task->known_path=LakeMetadataText(rows,0,8);
    task->known_algorithm=DatumGetInt32(LakeMetadataValue(rows,0,9,INT4OID));
    task->known_format=DatumGetInt32(LakeMetadataValue(rows,0,10,INT4OID));
    task->known_rows=DatumGetInt64(LakeMetadataValue(rows,0,11,INT8OID));task->known_bytes=DatumGetInt64(LakeMetadataValue(rows,0,12,INT8OID));
}
static LakeMetaRows* LakeIndexDefinition(const char* uuid,Snapshot snapshot,bool lock)
{
    Oid type=UUIDOID;Datum arg=LakeIndexUuid(uuid);
    return LakeMetadataQuery(psprintf("SELECT r.relation_oid,d.binding_id::text,d.server_oid,d.table_id,d.table_uuid::text,"
        "d.column_id,d.type_oid,d.definition_generation,d.definition_hash::text,COALESCE(d.definition->>'kind',''),"
        "COALESCE(d.definition->>'method',''),COALESCE(d.definition->>'metric',''),"
        "COALESCE((d.definition->>'pq')::boolean,false),COALESCE((d.definition->>'pq_chunks')::integer,1) "
        "FROM og_lake_internal.index_definition d JOIN og_lake_internal.relation_binding r USING(binding_id) "
        "WHERE d.index_uuid=$1 AND d.state='ACTIVE'%s",lock?" FOR UPDATE OF d":""),
        1,&type,&arg,NULL,snapshot,!lock,SPI_OK_SELECT);
}
/* Revalidate using fresh metadata only. Never read a footer/object or call
 * Describe/Check/Build here: registration/host/definition locks are held. */
static void LakeIndexRevalidate(LakeIndexArtifactTask* task)
{
    if(task->binding->columns[task->column].type==INT2OID&&!LakeInt2ScalarIndexVersionEnabled())
        LakeIndexChanged("installed INT2 scalar contract changed before reservation/publication");
    Relation host=heap_open(task->binding->relation_oid,AccessShareLock);
    LakeLockCatalogRegistration();LakeAcquireCatalogGuard(task->binding->server_oid);
    Snapshot snapshot=GetLatestSnapshot();LakeMetaRows* rows=LakeIndexDefinition(task->uuid,snapshot,true);
    if(rows->count!=1)LakeIndexChanged("index definition was dropped");
    if(DatumGetObjectId(LakeMetadataValue(rows,0,1,OIDOID))!=task->binding->relation_oid||
       strcmp(LakeMetadataText(rows,0,2),task->binding->binding_id)||
       DatumGetObjectId(LakeMetadataValue(rows,0,3,OIDOID))!=task->binding->server_oid||
       DatumGetInt64(LakeMetadataValue(rows,0,4,INT8OID))!=LakePermanentTableId(task->binding)||
       strcmp(LakeMetadataText(rows,0,5),task->binding->table_uuid)||
       DatumGetInt64(LakeMetadataValue(rows,0,6,INT8OID))!=task->binding->columns[task->column].column_id||
       DatumGetObjectId(LakeMetadataValue(rows,0,7,OIDOID))!=task->binding->columns[task->column].type||
       DatumGetInt64(LakeMetadataValue(rows,0,8,INT8OID))!=task->generation||
       strcmp(LakeMetadataText(rows,0,9),task->definition_hash))LakeIndexChanged("definition generation/binding changed");
    LakeInlineBinding* fresh=LakeLoadIndexSourceBinding(task->binding->relation_oid,task->binding->files[0].file_id,snapshot);
    if(!LakeIndexSameFile(task->binding,fresh))LakeIndexChanged("physical source/schema binding changed");
    char* root=LakeIndexRoot(fresh->server_oid,snapshot);
    if(root==NULL||strcmp(root,task->root))LakeIndexChanged("index store changed");
    LakeIndexKnown(task,snapshot,true);heap_close(host,NoLock);
}
static void LakeIndexCleanupReservation(const LakeIndexArtifactTask* task)
{
    LakeLockCatalogRegistration();(void)LakeIndexCatalog();
    Oid types[3]={UUIDOID,UUIDOID,UUIDOID};Datum args[3]={LakeIndexUuid(task->segment),LakeIndexUuid(task->uuid),LakeIndexUuid(task->token)};
    Snapshot snapshot=GetLatestSnapshot();
    /* DROP may have changed BUILDING to FAILED, but cannot certify that its
     * writer has ended. Only the same worker, after Abort, removes this pin. */
    (void)LakeMetadataQuery("DELETE FROM og_lake_internal.index_coverage c USING og_lake_internal.index_segment s "
        "WHERE c.segment_uuid=s.segment_uuid AND s.segment_uuid=$1 AND s.index_uuid=$2 AND s.build_token=$3 "
        "AND s.state IN ('BUILDING','FAILED') AND s.manifest_hash IS NULL AND NOT c.active",
        3,types,args,NULL,snapshot,false,SPI_OK_DELETE);
    CommandCounterIncrement();
    (void)LakeMetadataQuery("UPDATE og_lake_internal.index_segment SET state='FAILED' WHERE segment_uuid=$1 "
        "AND index_uuid=$2 AND build_token=$3 AND state='BUILDING' AND manifest_hash IS NULL",
        3,types,args,NULL,GetLatestSnapshot(),false,SPI_OK_UPDATE);
}
/* Called only by the worker owning the database-wide native session claim.
 * A previous worker cannot still have an SDK writer when a new owner holds
 * that claim. Recover at most 16 orphan pins per round; no physical deletion. */
static void LakeIndexRecoverReservations(void)
{
    LakeLockCatalogRegistration();
    LakeMetaRows* rows=LakeMetadataQuery("SELECT s.segment_uuid::text,s.index_uuid::text,s.build_token::text "
        "FROM og_lake_internal.index_segment s WHERE s.state IN ('BUILDING','FAILED') AND s.manifest_hash IS NULL "
        "AND EXISTS(SELECT 1 FROM og_lake_internal.index_coverage c WHERE c.segment_uuid=s.segment_uuid AND NOT c.active) "
        "ORDER BY s.segment_uuid LIMIT 16",0,NULL,NULL,NULL,GetActiveSnapshot(),true,SPI_OK_SELECT);
    for(uint32 i=0;i<rows->count;++i){LakeIndexArtifactTask orphan={};orphan.segment=LakeMetadataText(rows,i,1);
        orphan.uuid=LakeMetadataText(rows,i,2);orphan.token=LakeMetadataText(rows,i,3);LakeIndexCleanupReservation(&orphan);}
}
static bool LakeIndexDiscover(LakeIndexArtifactTask* task)
{
    if(!LakeHasIndexRootVersion())return false;(void)LakeIndexCatalog();
    char after[37]={0};int64 ignored=0;
    if(!LakeIndexMaintenanceCursor(true,NULL,false,&ignored,&ignored,after))return false;
    /* Typed UUID comparison and ordering share the same native order; the
     * initial NULL cursor does not depend on database text collation.
     * Iterate active candidates in round-robin wrap order so idle/empty
     * indexes cannot starve active ones. */
    Oid type=UUIDOID;Datum arg=*after?LakeIndexUuid(after):(Datum)0;
    const char* nulls=*after?NULL:"n";
    LakeMetaRows* rows=LakeMetadataQuery(
        "SELECT d.index_uuid::text FROM og_lake_internal.index_definition d "
        "JOIN og_lake_internal.relation_binding r USING(binding_id) JOIN og_lake_internal.catalog_binding b USING(server_oid) "
        "WHERE d.state='ACTIVE' AND b.index_root IS NOT NULL "
        "ORDER BY (CASE WHEN $1 IS NOT NULL AND d.index_uuid>$1 THEN 0 ELSE 1 END), d.index_uuid",
        1,&type,&arg,nulls,GetActiveSnapshot(),true,SPI_OK_SELECT);
    if(!rows->count)return false;
    for(uint32 cand=0;cand<rows->count;++cand){
        CHECK_FOR_INTERRUPTS();
        memset(task,0,sizeof(*task));
        task->uuid=LakeMetadataText(rows,cand,1);
        errno_t copy=strcpy_s(after,sizeof(after),task->uuid);securec_check(copy,"","");
        (void)LakeIndexMaintenanceCursor(true,NULL,true,&ignored,&ignored,after);
        LakeMetaRows* definition=LakeIndexDefinition(task->uuid,GetActiveSnapshot(),false);
        if(definition->count!=1)continue;
        Oid host=DatumGetObjectId(LakeMetadataValue(definition,0,1,OIDOID));
        Oid server=DatumGetObjectId(LakeMetadataValue(definition,0,3,OIDOID));
        int64 table=DatumGetInt64(LakeMetadataValue(definition,0,4,INT8OID));
        task->generation=DatumGetInt64(LakeMetadataValue(definition,0,8,INT8OID));
        /* Same order as foreground DDL: business host before catalog guard. */
        Relation business=heap_open(host,AccessShareLock);heap_close(business,NoLock);
        LakeInlineBinding* header=LakeLoadCatalogHeader(server,GetActiveSnapshot());
        int64 afterFile=-1,end=-1;
        if(!LakeIndexMaintenanceCursor(false,task->uuid,false,&afterFile,&end,NULL,task->generation))return false;
        char* files=LakeMetadataRelation(header->metadata_schema_oid,"ducklake_data_file");
        Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
        Datum args[4]={Int64GetDatum(table),Int64GetDatum(header->snapshot_id),Int64GetDatum(afterFile),Int64GetDatum(end)};
        if(end<0){
            LakeMetaRows* last=LakeMetadataQuery(psprintf("SELECT COALESCE(MAX(data_file_id),-1)::bigint FROM %s "
                "WHERE table_id=$1 AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2)",files),
                2,types,args,NULL,GetActiveSnapshot(),true,SPI_OK_SELECT,header->metadata_schema_oid);
            end=DatumGetInt64(LakeMetadataValue(last,0,1,INT8OID));args[3]=Int64GetDatum(end);
        }
        LakeMetaRows* next=LakeMetadataQuery(psprintf("SELECT data_file_id FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 "
            "AND (end_snapshot IS NULL OR end_snapshot>$2) AND data_file_id>$3 AND data_file_id<=$4 ORDER BY data_file_id LIMIT 1",files),
            4,types,args,NULL,GetActiveSnapshot(),true,SPI_OK_SELECT,header->metadata_schema_oid);
        if(!next->count&&end>=0){
            LakeMetaRows* last=LakeMetadataQuery(psprintf("SELECT COALESCE(MAX(data_file_id),-1)::bigint FROM %s "
                "WHERE table_id=$1 AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2)",files),
                2,types,args,NULL,GetActiveSnapshot(),true,SPI_OK_SELECT,header->metadata_schema_oid);
            int64 freshEnd=DatumGetInt64(LakeMetadataValue(last,0,1,INT8OID));
            if(freshEnd>end&&freshEnd>afterFile){
                end=freshEnd;args[3]=Int64GetDatum(end);
                next=LakeMetadataQuery(psprintf("SELECT data_file_id FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 "
                    "AND (end_snapshot IS NULL OR end_snapshot>$2) AND data_file_id>$3 AND data_file_id<=$4 ORDER BY data_file_id LIMIT 1",files),
                    4,types,args,NULL,GetActiveSnapshot(),true,SPI_OK_SELECT,header->metadata_schema_oid);
            }
        }
        if(!next->count){
            afterFile=end=-1;
            (void)LakeIndexMaintenanceCursor(false,task->uuid,true,&afterFile,&end,NULL,task->generation);
            continue;
        }
        afterFile=DatumGetInt64(LakeMetadataValue(next,0,1,INT8OID));
        (void)LakeIndexMaintenanceCursor(false,task->uuid,true,&afterFile,&end,NULL,task->generation);
        task->binding=LakeLoadIndexSourceBinding(host,afterFile,GetActiveSnapshot());
        if(task->binding->file_count!=1||task->binding->read_only)continue;
        if(strcmp(task->binding->binding_id,LakeMetadataText(definition,0,2))||server!=task->binding->server_oid||
           table!=LakePermanentTableId(task->binding)||strcmp(task->binding->table_uuid,LakeMetadataText(definition,0,5)))
            LakeIndexChanged("discovered definition no longer matches source binding");
        int64 column=DatumGetInt64(LakeMetadataValue(definition,0,6,INT8OID));Oid keyType=DatumGetObjectId(LakeMetadataValue(definition,0,7,OIDOID));
        task->column=-1;for(int i=0;i<task->binding->column_count;++i)
            if(task->binding->columns[i].column_id==column&&task->binding->columns[i].type==keyType)task->column=i;
        if(task->column<0)LakeIndexChanged("index key no longer maps to a physical source column");
        task->definition_hash=LakeMetadataText(definition,0,9);
        const char *kind=LakeMetadataText(definition,0,10),*method=LakeMetadataText(definition,0,11);
        if(keyType==VECTOROID){if(strcmp(kind,"vector")||strcmp(method,"diskann"))continue;
            task->metric=LakeAnnMetric(LakeMetadataText(definition,0,12));}
        else{if(!LakeScalarIndexedType(keyType)||strcmp(kind,"scalar")||(strcmp(method,"btree")&&strcmp(method,"bitmap")))continue;
            task->bitmap=!strcmp(method,"bitmap");}
#ifndef USE_LAKE_BITMAP
        if(task->bitmap)continue;
#endif
        task->pq=DatumGetBool(LakeMetadataValue(definition,0,13,BOOLOID));
        task->pq_chunks=DatumGetInt32(LakeMetadataValue(definition,0,14,INT4OID));
        if(task->pq&&(!task->metric||!LakePqCompiled()))continue;
        task->root=LakeIndexRoot(server,GetActiveSnapshot());if(!task->root)continue;
        task->segment=DatumGetCString(DirectFunctionCall1(uuid_out,OidFunctionCall0(F_GEN_RANDOM_UUID)));
        task->token=DatumGetCString(DirectFunctionCall1(uuid_out,OidFunctionCall0(F_GEN_RANDOM_UUID)));
        LakeIndexKnown(task,GetActiveSnapshot(),false);return true;
    }
    return false;
}
static void LakeIndexCheckPaths(const LakeIndexArtifactTask* task)
{
    const char* paths[2]={task->binding->files[0].path,task->root};
    for(int i=0;i<2;++i){char* actual=realpath(paths[i],NULL);
        if(actual==NULL)ereport(ERROR,(errcode_for_file_access(),errmsg("cannot resolve background lake index input: %m")));
        Size n=strlen(paths[i]);while(n>1&&paths[i][n-1]=='/')--n;
        bool equal=strlen(actual)==n&&strncmp(actual,paths[i],n)==0;free(actual);
        if(!equal)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("background lake index path is no longer canonical")));}
}
static void LakeIndexReserve(LakeIndexArtifactTask* task)
{
    LakeIndexRevalidate(task);const LakeDataFile* file=&task->binding->files[0];
    Oid types[9]={UUIDOID,UUIDOID,INT8OID,UUIDOID,INT8OID,TEXTOID,INT4OID,INT4OID,TEXTOID};
    Datum args[9]={LakeIndexUuid(task->segment),LakeIndexUuid(task->uuid),Int64GetDatum(task->generation),LakeIndexUuid(task->token),
        Int64GetDatum(task->binding->snapshot_id),CStringGetTextDatum(task->source),Int32GetDatum(task->algorithm),Int32GetDatum(task->format),CStringGetTextDatum(task->directory)};
    LakeMetaRows* added=LakeMetadataQuery("INSERT INTO og_lake_internal.index_segment(segment_uuid,index_uuid,definition_generation,build_token,"
        "build_snapshot,source_manifest_hash,algorithm_version,format_version,artifact_root_uri,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'BUILDING')",
        9,types,args,NULL,GetLatestSnapshot(),false,SPI_OK_INSERT);LakeMetadataRequireCount(added,1,"background BUILDING reservation");
    CommandCounterIncrement();
    Oid ct[9]={UUIDOID,INT8OID,UUIDOID,INT8OID,TEXTOID,TEXTOID,INT8OID,INT8OID,TEXTOID};
    Datum ca[9]={args[0],Int64GetDatum(file->file_id),args[1],args[2],CStringGetTextDatum(psprintf("object-version-v1:%s",task->version)),
        CStringGetTextDatum(task->schema),Int64GetDatum(file->record_count),Int64GetDatum(file->file_bytes),CStringGetTextDatum(file->path)};
    added=LakeMetadataQuery("INSERT INTO og_lake_internal.index_coverage(segment_uuid,source_file_id,index_uuid,definition_generation,source_checksum,"
        "schema_mapping_hash,physical_row_count,active,source_file_bytes,source_path) VALUES($1,$2,$3,$4,$5,$6,$7,false,$8,$9)",
        9,ct,ca,NULL,GetLatestSnapshot(),false,SPI_OK_INSERT);LakeMetadataRequireCount(added,1,"inactive physical source pin");
}
static void LakeIndexPublish(LakeIndexArtifactTask* task)
{
    LakeIndexRevalidate(task);
    Oid types[9]={UUIDOID,UUIDOID,INT8OID,UUIDOID,TEXTOID,TEXTOID,TEXTOID,INT4OID,INT8OID};
    Datum args[9]={LakeIndexUuid(task->segment),LakeIndexUuid(task->uuid),Int64GetDatum(task->generation),LakeIndexUuid(task->token),
        CStringGetTextDatum(task->source),CStringGetTextDatum(task->directory),CStringGetTextDatum(task->manifest),Int32GetDatum(task->format),
        Int64GetDatum(task->binding->files[0].file_id)};
    LakeMetaRows* ready=LakeMetadataQuery("UPDATE og_lake_internal.index_segment SET state='READY',manifest_hash=$7,format_version=$8 "
        "WHERE segment_uuid=$1 AND index_uuid=$2 AND definition_generation=$3 AND build_token=$4 AND source_manifest_hash=$5 "
        "AND artifact_root_uri=$6 AND state='BUILDING' AND manifest_hash IS NULL",8,types,args,NULL,GetLatestSnapshot(),false,SPI_OK_UPDATE);
    if(ready->count!=1)LakeIndexChanged("BUILDING reservation/token was retired");
    CommandCounterIncrement();
    (void)LakeMetadataQuery("UPDATE og_lake_internal.index_coverage SET active=false WHERE index_uuid=$2 "
        "AND definition_generation=$3 AND source_file_id=$9 AND active",9,types,args,NULL,GetLatestSnapshot(),false,SPI_OK_UPDATE);
    CommandCounterIncrement();
    LakeMetaRows* covered=LakeMetadataQuery("UPDATE og_lake_internal.index_coverage SET active=true WHERE segment_uuid=$1 "
        "AND index_uuid=$2 AND definition_generation=$3 AND source_file_id=$9 AND NOT active",9,types,args,NULL,GetLatestSnapshot(),false,SPI_OK_UPDATE);
    if(covered->count!=1)LakeIndexChanged("physical source reservation disappeared");
    CommandCounterIncrement();
    (void)LakeMetadataQuery("UPDATE og_lake_internal.index_segment s SET state='RETIRED' WHERE index_uuid=$1 AND state='READY' "
        "AND NOT EXISTS(SELECT 1 FROM og_lake_internal.index_coverage c WHERE c.segment_uuid=s.segment_uuid AND c.active)",
        1,&types[1],&args[1],NULL,GetLatestSnapshot(),false,SPI_OK_UPDATE);
    for(int i=0;i<task->candidate_count;++i)LakeFileCandidatePublish(task->candidates[i]);
    LakeInvalidateRelationSafe(task->binding->relation_oid);
}
}

LakeIndexArtifactHealth LakeIndexArtifactFiles(const char* directory,const char* const* names,int count)
{
    struct stat st;if(lstat(directory,&st)!=0)return errno==ENOENT?LAKE_ARTIFACT_REBUILD:LAKE_ARTIFACT_RETRY;
    if(!S_ISDIR(st.st_mode))return LAKE_ARTIFACT_REBUILD;
    for(int i=0;i<count;++i){CHECK_FOR_INTERRUPTS();char* path=psprintf("%s/%s",directory,names[i]);
        if(lstat(path,&st)!=0)return errno==ENOENT?LAKE_ARTIFACT_REBUILD:LAKE_ARTIFACT_RETRY;
        if(!S_ISREG(st.st_mode)||st.st_size<12)return LAKE_ARTIFACT_REBUILD;}
    return LAKE_ARTIFACT_VALID;
}
bool LakeIndexArtifactMetadata(const LakeIndexArtifactTask* task)
{
    if(!task->known)return false;const LakeDataFile* file=&task->binding->files[0];
    if(strcmp(task->known_directory,psprintf("%s%s",task->root,task->known_segment)))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("background coverage escaped its configured index store")));
    return !strcmp(task->known_source,task->source)&&!strcmp(task->known_schema,task->schema)&&
        !strcmp(task->known_checksum,psprintf("object-version-v1:%s",task->version))&&
        !strcmp(task->known_path,file->path)&&task->known_rows==file->record_count&&task->known_bytes==file->file_bytes;
}
LakeIndexMaintenanceResult LakeMaintainOneFileIndex(void)
{
    if(t_thrd.role!=LAKE_WORKER||IsTransactionState())
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("file-index maintenance requires an idle native maintenance worker")));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext keep=AllocSetContextCreate(caller,"Lake index task across transactions",ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    auto* task=(LakeIndexArtifactTask*)MemoryContextAllocZero(keep,sizeof(LakeIndexArtifactTask));
    LakeIndexMaintenanceResult result={};volatile bool reserved=false;
    PG_TRY();{
        /* Recovery has only registration/control locks and its own short
         * transaction. Do not carry its registration lock into discovery's
         * host lock acquisition (DDL takes host before registration). */
        LakeIndexBegin(keep);
        if(LakeHasIndexRootVersion()){(void)LakeIndexCatalog();LakeIndexRecoverReservations();}
        LakeIndexCommit(keep);
        LakeIndexBegin(keep);bool found=LakeIndexDiscover(task);LakeIndexCommit(keep);
        if(found){
            result.attempted=true;result.relation_oid=task->binding->relation_oid;result.source_file=task->binding->files[0].file_id;
            errno_t copy=strcpy_s(result.index_uuid,sizeof(result.index_uuid),task->uuid);securec_check(copy,"","");
            /* Separate preflight IO transaction. Discovery's host/catalog
             * locks were released, and task owns only copied schema/scalars. */
            LakeIndexBegin(keep);LakeIndexCheckPaths(task);
            if(task->metric)LakeAnnDescribeMaintenanceTask(task);else LakeScalarDescribeMaintenanceTask(task);
            LakeIndexArtifactHealth health=task->metric?LakeAnnCheckMaintenanceTask(task):LakeScalarCheckMaintenanceTask(task);
            LakeIndexCommit(keep);
            if(health==LAKE_ARTIFACT_VALID)result.verified=true;
            else if(health==LAKE_ARTIFACT_RETRY)result.retry=true;
            else{
                LakeIndexBegin(keep);LakeIndexReserve(task);reserved=true;LakeIndexCommit(keep);
                /* Durable reservation now pins exact source. Native temp-heap
                 * staging + SDK writers belong only to this private txn. */
                LakeIndexBegin(keep);LakeIndexMaintenanceTestGate(task,1);
                LakeIndexCheckPaths(task);LakeEnsureLocalDirectory(task->directory);
                if(task->metric)LakeAnnBuildMaintenanceTask(task);else LakeScalarBuildMaintenanceTask(task);
                LakeIndexMaintenanceTestGate(task,2);LakeIndexPublish(task);LakeIndexCommit(keep);
                reserved=false;result.built=true;
            }
        }
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);ErrorData* original=CopyErrorData();FlushErrorState();
        HOLD_INTERRUPTS();
        (void)disable_sig_alarm(true);
        t_thrd.storage_cxt.timeIsPausing=false;t_thrd.storage_cxt.restimems=-1;
        if(!t_thrd.worker_sig_flags.got_SIGTERM&&!t_thrd.int_cxt.ProcDiePending)
            t_thrd.int_cxt.QueryCancelPending=false;
        AbortOutOfAnyTransaction();MemoryContextSwitchTo(keep);
        RESUME_INTERRUPTS();
        if(reserved){PG_TRY();{
                /* Failure/cancel cleanup must not retain the database claim
                 * behind an arbitrarily long DDL transaction. The source pin
                 * is durable; a later claim owner can recover it if this short
                 * independent cleanup budget expires. Preserve original ERROR. */
                SetCurrentStatementStartTimestamp();
                int cleanupBudget=Min(5000,g_instance.attr.attr_storage.lake_flush_worker_budget);
                if(!enable_sig_alarm(cleanupBudget,true))
                    ereport(ERROR,(errcode(ERRCODE_SYSTEM_ERROR),errmsg("could not arm background lake index cleanup timeout")));
                LakeIndexBegin(keep);LakeIndexCleanupReservation(task);LakeIndexCommit(keep);
                (void)disable_sig_alarm(true);
            }PG_CATCH();{
                HOLD_INTERRUPTS();
                (void)disable_sig_alarm(true);
                t_thrd.storage_cxt.timeIsPausing=false;t_thrd.storage_cxt.restimems=-1;
                if(!t_thrd.worker_sig_flags.got_SIGTERM&&!t_thrd.int_cxt.ProcDiePending)
                    t_thrd.int_cxt.QueryCancelPending=false;
                EmitErrorReport();FlushErrorState();
                AbortOutOfAnyTransaction();MemoryContextSwitchTo(keep);
                RESUME_INTERRUPTS();
            }PG_END_TRY();}
        MemoryContextSwitchTo(caller);MemoryContextDelete(keep);ReThrowError(original);
    }PG_END_TRY();
    MemoryContextSwitchTo(caller);MemoryContextDelete(keep);return result;
}
