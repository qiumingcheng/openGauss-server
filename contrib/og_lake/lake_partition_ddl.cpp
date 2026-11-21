/* Owner-only standard DuckLake partition definition publication.
 * Deliberately no autonomous commit, candidate I/O or host partition object. */
#include "postgres.h"
#include "lake_version.h"
#include "lake_inline_sources.h"
#include "lake_partition.h"
#include "lake_txn_private.h"
#include "lake_create_publish.h"
#include "access/og_lake.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

extern void LakeTxnForgetReadOnlyTableForRename(Oid relationOid);
extern "C" Datum og_lake_set_partition(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_reset_partition(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_get_partition(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_set_partition);
PG_FUNCTION_INFO_V1(og_lake_reset_partition);
PG_FUNCTION_INFO_V1(og_lake_get_partition);

struct LakePartitionKeyNames { int count; char** names; LakePartitionTransform* transforms; int64* parameters; };
static LakePartitionKeyNames LakePartitionKeyArgument(FunctionCallInfo fcinfo)
{
    if(PG_ARGISNULL(1))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("partition key cannot be NULL")));
    ArrayType* array=PG_GETARG_ARRAYTYPE_P(1);
    if(ARR_ELEMTYPE(array)!=TEXTOID||ARR_NDIM(array)!=1)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("partition keys require one-dimensional text[]")));
    Datum* values=NULL;bool* nulls=NULL;int count=0;
    deconstruct_array(array,TEXTOID,-1,false,'i',&values,&nulls,&count);
    bool expanded=LakeIdentityPartitionKeysVersionEnabled();
    if(!expanded&&(count!=1||nulls[0]))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("identity partition slice requires one non-NULL column name")));
    if(count<1||count>MaxHeapAttributeNumber)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake partition key count exceeds its limit")));
    LakePartitionKeyNames result={};result.count=count;result.names=(char**)palloc0(sizeof(char*)*count);
    result.transforms=(LakePartitionTransform*)palloc0(sizeof(LakePartitionTransform)*count);
    result.parameters=(int64*)palloc0(sizeof(int64)*count);
    Datum* transformValues=NULL;bool* transformNulls=NULL;int transformCount=0;ArrayType* transforms=NULL;
    if(PG_NARGS()>2){
        if(!LakeBucketPartitionVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake explicit partition transforms are not enabled in this build")));
        if(PG_ARGISNULL(2))ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("partition transforms cannot be NULL")));
        transforms=PG_GETARG_ARRAYTYPE_P(2);
        if(ARR_ELEMTYPE(transforms)!=TEXTOID||ARR_NDIM(transforms)!=1)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("partition transforms require one-dimensional text[]")));
        deconstruct_array(transforms,TEXTOID,-1,false,'i',&transformValues,&transformNulls,&transformCount);
        if(transformCount!=count)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("partition columns and transforms differ in length")));
    }
    for(int k=0;k<count;++k){
        result.transforms[k]=LAKE_PARTITION_IDENTITY;
        if(transformValues){
            if(transformNulls[k])ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("partition transform cannot be NULL")));
            char* transformName=TextDatumGetCString(transformValues[k]);
            if(LakePartitionIsTruncateTransform(transformName)&&!LakeTruncatePartitionVersionEnabled())
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("DuckLake partition transform \"%s\" is not supported",transformName),
                    errdetail("Pinned DuckLake only supports identity, year, month, day, hour, epoch_year, epoch_month, epoch_day, epoch_hour, and bucket(N); truncate(N) is not persisted or emulated.")));
            if(!LakePartitionParseTransform(transformName,&result.transforms[k],&result.parameters[k])||
               (LakePartitionIsTimeTransform(result.transforms[k])&&!LakeTimePartitionVersionEnabled())||
               (result.transforms[k]==LAKE_PARTITION_TRUNCATE&&!LakeTruncatePartitionVersionEnabled()))
                ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("partition transform must be identity or canonical bucket(N)/truncate(N) with 1<=N<=2147483647")));
            pfree(transformName);
        }
        if(nulls[k])ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("partition column name cannot be NULL")));
        char* key=TextDatumGetCString(values[k]);
        if(!*key||strlen(key)>=NAMEDATALEN)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid identity partition column name")));
        for(int j=0;j<k;++j)if(strcmp(result.names[j],key)==0&&result.transforms[j]==result.transforms[k]&&result.parameters[j]==result.parameters[k])
            ereport(ERROR,(errcode(ERRCODE_DUPLICATE_COLUMN),errmsg("duplicate identity partition column name")));
        result.names[k]=key;
    }
    if(transformValues){pfree(transformValues);pfree(transformNulls);}
    if(transforms&&(Pointer)transforms!=PG_GETARG_POINTER(2))pfree(transforms);
    pfree(values);pfree(nulls);
    if((Pointer)array!=PG_GETARG_POINTER(1))pfree(array);
    return result; /* Literal names only; never interpreted as SQL expressions. */
}

static void LakePartitionNoCarrier(const LakeInlineBinding* binding,Snapshot snapshot)
{
    Oid type=INT8OID;Datum arg=Int64GetDatum(LakePermanentTableId(binding));
    LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT count(*)::bigint FROM %s WHERE table_id=$1",
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_inlined_data_tables")),
        1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(rows,1,"partition inline registration check");
    if(OidIsValid(binding->inline_oid)||DatumGetInt64(LakeMetadataValue(rows,0,1,INT8OID))!=0)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("identity partition SET/RESET requires a table without an inline carrier"),
            errhint("Existing-carrier partition evolution requires the source-schema adapter; an empty carrier is still a carrier.")));
}

static LakePartitionSpec LakePartitionRequested(const LakeInlineBinding* binding,const LakePartitionKeyNames* names)
{
    LakePartitionSpec result={};result.key_count=names->count;result.enabled=names->count>0;
    if(!names->count)return result;
    if(names->count>(LakeBucketPartitionVersionEnabled()?MaxHeapAttributeNumber:binding->column_count))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("partition keys exceed the lake table column count")));
    result.keys=(LakePartitionField*)palloc0(sizeof(LakePartitionField)*names->count);
    for(int k=0;k<names->count;++k){
        const char* name=names->names[k];int found=-1;
        for(int c=0;c<binding->column_count;++c)if(strcmp(binding->columns[c].name,name)==0){
            if(found>=0)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ambiguous native lake partition column")));
            found=c;
        }
        if(found<0)ereport(ERROR,(errcode(ERRCODE_UNDEFINED_COLUMN),errmsg("partition column \"%s\" does not exist",name)));
        Oid type=binding->columns[found].type;
        if(!LakeIdentityPartitionKeysVersionEnabled()&&type!=INT4OID)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("identity partition slice requires int4")));
        auto* key=&result.keys[k];key->column_id=binding->columns[found].column_id;key->column_index=found;
        key->type=type;key->typmod=binding->columns[found].typmod;key->transform=names->transforms[k];key->parameter=names->parameters[k];
        key->result_type=LakePartitionResultType(key->transform,type);
        if(!LakePartitionFieldSupported(key))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake partition transform is not supported for its source type")));
    }
    return result;
}

static bool LakePartitionRequestEqual(const LakePartitionSpec* a,const LakePartitionSpec* b)
{
    if(a->enabled!=b->enabled||a->key_count!=b->key_count)return false;
    for(int k=0;k<a->key_count;++k)if(a->keys[k].column_id!=b->keys[k].column_id||
        a->keys[k].type!=b->keys[k].type||a->keys[k].result_type!=b->keys[k].result_type||
        a->keys[k].transform!=b->keys[k].transform||a->keys[k].parameter!=b->keys[k].parameter)return false;
    return true;
}

static void LakePublishPartitionSetting(Oid relationOid,const LakePartitionKeyNames* names)
{
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot change a lake partition in a read-only transaction")));
    LakeTxnRejectBindingDdl();
    if(LakeTxnFindCreatedTable(relationOid)!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cannot partition a transaction-local lake table")));
    LakeTxnState* state=LakeTxnMarkPartitionDefinition();
    LakeLockCatalogRegistration();
    /* Registration precedes the stronger target lock; no external I/O follows.
     * Ordinary readers keep AccessShare and old immutable bindings. */
    LockRelationOid(relationOid,ShareRowExclusiveLock);
    LakeTxnForgetReadOnlyTableForRename(relationOid); /* Same no-active-view contract. */
    MemoryContext caller=CurrentMemoryContext;ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    MemoryContext context=AllocSetContextCreate(caller,"Native lake partition definition",ALLOCSET_DEFAULT_SIZES);
    volatile bool inside=false;bool complete=false;
    void* hostSlot=NULL;int64 winningSnapshot=-1;
    PG_TRY();{
        MemoryContextSwitchTo(context);
        LakeInlineBinding* original=LakeLoadCommittedInlineBinding(relationOid,GetLatestSnapshot());
        if(!LakePartitionComboVersionEnabled()){
            LakeRejectUuidAcceleration(original,"partition rule changes");
            LakeRejectJsonAcceleration(original,"partition rule changes");
            LakeRejectNumericAcceleration(original,"partition rule changes");
        }
        if(original->read_only||!OidIsValid(LakeMetadataOwner()))
            ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("partition setting requires a writable private catalog")));
        LakePartitionSpec requested=LakePartitionRequested(original,names);
        hostSlot=LakeHostPrepareImmediatePublish(original,GetCurrentSubTransactionId());
        for(int attempt=0;attempt<16&&!complete;++attempt){
            CHECK_FOR_INTERRUPTS();
            MemoryContext attemptContext=AllocSetContextCreate(context,"Native lake partition claim attempt",ALLOCSET_DEFAULT_SIZES);
            state->publication_subtransaction=true;BeginInternalSubTransaction(NULL);inside=true;MemoryContextSwitchTo(attemptContext);
            Oid ns=original->metadata_schema_oid;
            (void)LakeMetadataRelation(ns,"ducklake_metadata");
            Relation options=heap_open(get_relname_relid("ducklake_metadata",ns),ShareLock);heap_close(options,NoLock);
            Snapshot fresh=GetLatestSnapshot();
            LakeInlineBinding* now=LakeLoadCommittedInlineBinding(relationOid,fresh);
            LakePartitionSpec nowRequested=LakePartitionRequested(now,names);
            if(now->read_only||now->metadata_schema_oid!=ns||strcmp(now->table_uuid,original->table_uuid)||
               LakePermanentTableId(now)!=LakePermanentTableId(original)||!LakePartitionRequestEqual(&nowRequested,&requested))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake partition target changed")));
            if(LakePartitionEvolutionVersionEnabled()&&now->inline_sources_loaded)
                LakeValidateInlineSources(now); /* Only ordinary native CREATE loader grants this path. */
            else LakePartitionNoCarrier(now,fresh);
            if(LakePartitionRequestEqual(&now->partition,&requested)){
                RollbackAndReleaseCurrentSubTransaction();inside=false;state->publication_subtransaction=false;
                MemoryContextSwitchTo(context);t_thrd.utils_cxt.CurrentResourceOwner=owner;
                MemoryContextDelete(attemptContext);complete=true;break;
            }
            if(now->inline_sources_loaded&&now->inline_source_count>=LAKE_INLINE_SOURCE_LIMIT)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("native lake partition evolution requires an available inline source descriptor")));
            const char* snapshots=LakeMetadataRelation(ns,"ducklake_snapshot");
            LakeMetaRows* head=LakeMetadataQuery(psprintf("SELECT snapshot_id,schema_version,next_catalog_id,next_file_id "
                "FROM %s ORDER BY snapshot_id DESC LIMIT 1",snapshots),0,NULL,NULL,NULL,fresh,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(head,1,"partition publication head");
            int64 current=DatumGetInt64(LakeMetadataValue(head,0,1,INT8OID));
            int64 schema=DatumGetInt64(LakeMetadataValue(head,0,2,INT8OID));
            int64 catalog=DatumGetInt64(LakeMetadataValue(head,0,3,INT8OID));
            int64 file=DatumGetInt64(LakeMetadataValue(head,0,4,INT8OID));
            if(current<0||current==PG_INT64_MAX||schema<0||schema==PG_INT64_MAX||catalog<0||
               catalog>=LAKE_REFERENCE_LOCAL_ID_START||file<0)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("partition publication counters exceed permanent range")));
            Oid collisionType=INT8OID;Datum collisionArg=Int64GetDatum(catalog);
            LakeMetaRows* collision=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE partition_id=$1)",
                LakeMetadataRelation(ns,"ducklake_partition_info")),1,&collisionType,&collisionArg,NULL,fresh,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(collision,1,"partition allocator collision check");
            if(DatumGetBool(LakeMetadataValue(collision,0,1,BOOLOID)))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("partition allocator would reuse a committed catalog identity")));
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(current+1),Int64GetDatum(schema+1),Int64GetDatum(catalog+1),Int64GetDatum(file)};
            LakeMetaRows* claimed=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
                "VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",snapshots),
                4,types,args,NULL,fresh,false,SPI_OK_INSERT_RETURNING,ns);
            if(claimed->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate partition publication claim")));
            if(claimed->count==1){
                Snapshot own=LakeCreatePushOwnWriteSnapshot(GetLatestSnapshot());
                const char* info=LakeMetadataRelation(ns,"ducklake_partition_info");
                Oid it[4]={INT8OID,INT8OID,INT8OID,INT8OID};
                Datum ia[4]={Int64GetDatum(catalog),Int64GetDatum(LakePermanentTableId(now)),args[0],Int64GetDatum(0)};
                if(now->partition.present){
                    Oid dt[3]={INT8OID,INT8OID,INT8OID};Datum da[3]={ia[1],Int64GetDatum(now->partition.partition_id),ia[2]};
                    LakeMetaRows* ended=LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$3 WHERE table_id=$1 "
                        "AND partition_id=$2 AND end_snapshot IS NULL",info),3,dt,da,NULL,own,false,SPI_OK_UPDATE,ns);
                    LakeMetadataRequireCount(ended,1,"retired partition specification");
                }
                LakeMetaRows* written=LakeMetadataQuery(psprintf("INSERT INTO %s(partition_id,table_id,begin_snapshot,end_snapshot) "
                    "VALUES($1,$2,$3,NULL)",info),3,it,ia,NULL,own,false,SPI_OK_INSERT,ns);
                LakeMetadataRequireCount(written,1,"new partition specification");
                for(int k=0;k<requested.key_count;++k){
                    Oid kt[5]={INT8OID,INT8OID,INT8OID,INT8OID,TEXTOID};
                    Datum ka[5]={ia[0],ia[1],Int64GetDatum(k),Int64GetDatum(requested.keys[k].column_id),
                        CStringGetTextDatum(LakePartitionTransformText(&requested.keys[k]))};
                    written=LakeMetadataQuery(psprintf("INSERT INTO %s(partition_id,table_id,partition_key_index,column_id,transform) "
                        "VALUES($1,$2,$3,$4,$5)",LakeMetadataRelation(ns,"ducklake_partition_column")),
                        5,kt,ka,NULL,own,false,SPI_OK_INSERT,ns);
                    LakeMetadataRequireCount(written,1,"identity partition field");
                } /* RESET deliberately appends an empty standard spec. */
                Oid vt[3]={INT8OID,INT8OID,INT8OID};Datum va[3]={args[0],args[1],ia[1]};
                written=LakeMetadataQuery(psprintf("INSERT INTO %s(begin_snapshot,schema_version,table_id) VALUES($1,$2,$3)",
                    LakeDefinitionRelation(ns,"ducklake_schema_versions")),3,vt,va,NULL,own,false,SPI_OK_INSERT,ns);
                LakeMetadataRequireCount(written,1,"partition table schema version");
                Oid ct[2]={INT8OID,TEXTOID};Datum ca[2]={args[0],CStringGetTextDatum(psprintf("altered_table:" INT64_FORMAT,LakePermanentTableId(now)))};
                written=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,changes_made,author,commit_message,commit_extra_info) "
                    "VALUES($1,$2,NULL,NULL,NULL)",LakeMetadataRelation(ns,"ducklake_snapshot_changes")),
                    2,ct,ca,NULL,own,false,SPI_OK_INSERT,ns);
                LakeMetadataRequireCount(written,1,"partition changeset");
                PopActiveSnapshot();ReleaseCurrentSubTransaction();inside=false;state->publication_subtransaction=false;complete=true;
                winningSnapshot=current+1;
            }else{RollbackAndReleaseCurrentSubTransaction();inside=false;state->publication_subtransaction=false;}
            MemoryContextSwitchTo(context);t_thrd.utils_cxt.CurrentResourceOwner=owner;
            MemoryContextDelete(attemptContext);
            if(!complete)pg_usleep(1000L*(attempt+1));
        }
        if(!complete)ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("partition publication exceeded retry limit")));
        MemoryContextSwitchTo(caller);MemoryContextDelete(context);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);ErrorData* failure=CopyErrorData();FlushErrorState();
        if(inside)RollbackAndReleaseCurrentSubTransaction();
        state->publication_subtransaction=false;
        t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(context);ReThrowError(failure);
    }PG_END_TRY();
    CacheInvalidateRelcacheByRelid(relationOid);CommandCounterIncrement();
    if(winningSnapshot>=0&&hostSlot!=NULL)LakeHostActivateImmediatePublish(hostSlot,winningSnapshot);
}

static Relation LakePartitionOwnerTarget(FunctionCallInfo fcinfo)
{
    if(PG_ARGISNULL(0))ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("partition table cannot be NULL")));
    LakeRequireAccessPolicy();LakeRequireIdentityPartitionVersion();
    Relation relation=heap_open(PG_GETARG_OID(0),AccessShareLock);
    if(!LakeIsNativeForeignRelation(relation))
        ereport(ERROR,(errcode(ERRCODE_WRONG_OBJECT_TYPE),errmsg("partition target is not a native lake table")));
    if(relation->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT||RelationIsPartitioned(relation))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("identity partition target must be a permanent nonpartitioned host table")));
    if(!pg_class_ownercheck(RelationGetRelid(relation),GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(relation));
    return relation;
}
Datum og_lake_set_partition(PG_FUNCTION_ARGS)
{
    Relation target=LakePartitionOwnerTarget(fcinfo);LakePartitionKeyNames names=LakePartitionKeyArgument(fcinfo);
    LakePublishPartitionSetting(RelationGetRelid(target),&names);heap_close(target,NoLock);PG_RETURN_VOID();
}
Datum og_lake_reset_partition(PG_FUNCTION_ARGS)
{
    Relation target=LakePartitionOwnerTarget(fcinfo);
    LakePartitionKeyNames names={};LakePublishPartitionSetting(RelationGetRelid(target),&names);heap_close(target,NoLock);PG_RETURN_VOID();
}

Datum og_lake_get_partition(PG_FUNCTION_ARGS)
{
    Relation target=LakePartitionOwnerTarget(fcinfo);
    ReturnSetInfo* result=(ReturnSetInfo*)fcinfo->resultinfo;
    if(result==NULL||!IsA(result,ReturnSetInfo)||!(result->allowedModes&SFRM_Materialize)||!result->econtext)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("partition listing requires materialize mode")));
    MemoryContext caller=MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc descriptor=CreateTemplateTupleDesc(3,false);
    TupleDescInitEntry(descriptor,1,"partition_key_index",INT8OID,-1,0);
    TupleDescInitEntry(descriptor,2,"column_name",TEXTOID,-1,0);
    TupleDescInitEntry(descriptor,3,"transform",TEXTOID,-1,0);
    descriptor=BlessTupleDesc(descriptor);
    Tuplestorestate* store=tuplestore_begin_heap(true,false,Max(64,Min(u_sess->attr.attr_memory.work_mem,1024)));
    LakeInlineBinding* binding=LakeLoadCommittedInlineBinding(RelationGetRelid(target),
        ActiveSnapshotSet()?GetActiveSnapshot():GetTransactionSnapshot());
    for(int k=0;k<binding->partition.key_count;++k){
        Datum values[3]={Int64GetDatum(k),CStringGetTextDatum(binding->columns[binding->partition.keys[k].column_index].name),
            CStringGetTextDatum(LakePartitionTransformText(&binding->partition.keys[k]))};
        bool nulls[3]={false,false,false};tuplestore_putvalues(store,descriptor,values,nulls);
    }
    tuplestore_donestoring(store);result->returnMode=SFRM_Materialize;result->setResult=store;result->setDesc=descriptor;
    MemoryContextSwitchTo(caller);heap_close(target,NoLock);PG_RETURN_NULL();
}
