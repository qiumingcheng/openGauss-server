/* Fixed input -> finished immutable candidate; no metadata writes in this file. */
#include "postgres.h"
#include "lake_rewrite.h"
#include "lake_version.h"
#include "lake_column_layout.h"
#include "lib/stringinfo.h"
#include "lake_types.h"
#include "lake_arrow_builder.h"
#include "lake_file_reader.h"
#include "lake_inline_reader.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

static void LakeRewriteConflict(const char* reason)
{ ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake rewrite source changed: %s",reason))); }


/* Full ordered row encodings are length framed, including SQL NULL markers.
 * Counts alone cannot detect a changed transform, key ordinal or file value. */
static char* LakeRewritePartitionRows(const LakeMetaRows* rows)
{
    if(rows->count>(uint32)MaxHeapAttributeNumber+1)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("rewrite partition metadata exceeds its key bound")));
    const Size limit=16UL*1024UL*1024UL;StringInfoData text;initStringInfo(&text);
    appendStringInfo(&text,"%u:",rows->count);
    for(uint32 r=0;r<rows->count;++r){
        char* record=LakeMetadataText(rows,r,1);Size bytes=strlen(record);
        if(bytes>limit||text.len>(int)(limit-bytes)||limit-bytes-(Size)text.len<32)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("rewrite partition metadata exceeds its byte bound")));
        appendStringInfo(&text,"%zu:",bytes);appendBinaryStringInfo(&text,record,(int)bytes);
    }
    return text.data;
}
static void LakeRewritePartitionRecord(char** expected,const LakeMetaRows* rows,bool capture)
{
    char* current=LakeRewritePartitionRows(rows);
    if(capture)*expected=current;
    else if(*expected==NULL||strcmp(*expected,current))LakeRewriteConflict("partition policy/spec/key/value metadata");
}

/* Metadata only, including after a winning snapshot PK. No object/footer IO.
 * Native's file loader already resolves ended historical specs by exact id. */
static void LakeRewriteCheckPartition(LakeRewriteOperation* op,Snapshot snapshot,bool capture)
{
    if(!op->partitioned)return;
    if(!LakePartitionRewriteVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("partitioned rewrite requires exact installed SQL0.48")));
    LakeInlineBinding* b=op->table->binding;Oid ns=b->metadata_schema_oid;
    const char* info=LakeMetadataRelation(ns,"ducklake_partition_info");
    const char* keys=LakeMetadataRelation(ns,"ducklake_partition_column");
    const char* values=LakeMetadataRelation(ns,"ducklake_file_partition_value");
    const char* sort=LakeMetadataRelation(ns,"ducklake_sort_info");
    Oid types[3]={INT8OID,INT8OID,INT8OID};Datum args[3]={Int64GetDatum(LakePermanentTableId(b)),
        Int64GetDatum(op->partition.spec.partition_id),(Datum)0};
    /* New partitioned slice keeps all sort history closed; legacy rewrite's
     * original current-sort rule remains unchanged below. */
    LakeMetaRows* sorted=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1)",sort),
        1,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeMetadataRequireCount(sorted,1,"partitioned rewrite sort policy");
    if(DatumGetBool(LakeMetadataValue(sorted,0,1,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("partitioned rewrite does not support retained sort specifications")));
    LakeMetaRows* policy=LakeMetadataQuery(psprintf("SELECT ROW(p.partition_id,p.table_id,p.begin_snapshot,p.end_snapshot,"
        "c.table_id,c.partition_key_index,c.column_id,c.transform)::text FROM %s p LEFT JOIN %s c "
        "ON c.partition_id=p.partition_id WHERE p.table_id=$1 AND p.end_snapshot IS NULL "
        "ORDER BY p.partition_id,c.partition_key_index LIMIT %d",info,keys,MaxHeapAttributeNumber+2),
        1,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeRewritePartitionRecord(&op->partition_policy_record,policy,capture);
    LakeMetaRows* spec=LakeMetadataQuery(psprintf("SELECT ROW(p.partition_id,p.table_id,p.begin_snapshot,p.end_snapshot,"
        "c.table_id,c.partition_key_index,c.column_id,c.transform)::text FROM %s p LEFT JOIN %s c "
        "ON c.partition_id=p.partition_id WHERE p.partition_id=$2 ORDER BY c.partition_key_index LIMIT %d",
        info,keys,MaxHeapAttributeNumber+2),2,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeRewritePartitionRecord(&op->partition_spec_record,spec,capture);
    for(int f=0;f<op->source_count;++f){
        args[2]=Int64GetDatum(op->source_ids[f]);
        LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT ROW(data_file_id,table_id,partition_key_index,partition_value)::text "
            "FROM %s WHERE data_file_id=$3 ORDER BY partition_key_index,table_id LIMIT %d",values,op->partition.spec.key_count+1),
            3,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
        LakeRewritePartitionRecord(&op->partition_value_records[f],rows,capture);
        LakeDataFile fresh={};fresh.file_id=op->source_ids[f];
        LakeLoadFileIdentityPartition(b,&fresh,op->partition.spec.partition_id,snapshot);
        if(!LakeIdentityPartitionFileEqual(&op->partition,&fresh.partition))
            LakeRewriteConflict("selected historical partition descriptor");
    }
}

/* After output finish, before EACH claim attempt and before registration/PK
 * locks. This compares observable immutable-object generations, not content
 * checksums or an adversarial lease; the provider's immutable storage contract
 * still governs changes after this check. Never call this inside the claim. */
void LakeRewriteCheckSourceObjects(LakeRewriteOperation* op)
{
    if(!op->partitioned)return;
    if(!LakePartitionRewriteVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("partitioned rewrite requires exact installed SQL0.48")));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(caller,"Rewrite source identity recheck",ALLOCSET_DEFAULT_SIZES);
    PG_TRY();{
        MemoryContextSwitchTo(scratch);
        for(int f=0;f<op->source_count;++f){
            char* current=LakeFileObjectVersion(op->input,f);
            if(!current||!op->source_versions[f]||strcmp(current,op->source_versions[f]))
                LakeRewriteConflict("immutable source object generation");
        }
        MemoryContextSwitchTo(caller);MemoryContextDelete(scratch);
    }PG_CATCH();{MemoryContextSwitchTo(caller);MemoryContextDelete(scratch);PG_RE_THROW();}PG_END_TRY();
}

static void LakeRewriteCapturePartition(LakeRewriteOperation* op)
{
    const LakePartitionFile* first=&op->input->files[0].partition;
    if(!first->spec.enabled||!first->spec.present||first->spec.key_count<1)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("partitioned rewrite requires a complete historical partition key")));
    op->partition.spec=LakePartitionCopySpec(&first->spec,op->context);
    op->partition.tuple=LakePartitionCopyTuple(&first->spec,&first->tuple,op->context);
    for(int k=0;k<op->partition.spec.key_count;++k){
        const auto* key=&op->partition.spec.keys[k];
        if(!key->host_bound||key->column_index<0||key->column_index>=op->input->column_count||
           op->input->columns[key->column_index].column_id!=key->column_id)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("rewrite historical key is not a current protected business column")));
    }
    for(int f=0;f<op->source_count;++f)
        if(!LakeIdentityPartitionFileEqual(&op->partition,&op->input->files[f].partition))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("rewrite sources require the same historical spec and complete ordered partition tuple")));
}

void LakeRewriteCheckBinding(const LakeRewriteOperation* op,const LakeInlineBinding* now)
{
    const LakeInlineBinding* old=op->table->binding;
    if(now->read_only||now->server_oid!=old->server_oid||now->metadata_schema_oid!=old->metadata_schema_oid||
       LakePermanentTableId(now)!=LakePermanentTableId(old)||strcmp(now->binding_id,old->binding_id)||
       strcmp(now->table_uuid,old->table_uuid)||strcmp(now->data_root,old->data_root)||now->column_count!=old->column_count||
       !LakeColumnLayoutsEqual(old,now))
        LakeRewriteConflict("table/catalog identity");
    if(op->partitioned){
        if(!now->column_evolution||
           (!LakeMergeAdjacentVersionEnabled()&&(now->has_added_columns||now->has_dropped_columns))||
           !LakeIdentityPartitionSpecEqual(&old->partition,&now->partition))
            LakeRewriteConflict("protected business schema or current partition policy");
        for(int f=0;f<op->source_count;++f){
            bool found=false;
            for(int j=0;j<now->file_count;++j)if(now->files[j].file_id==op->source_ids[f]){
                found=true;if(!LakeIdentityPartitionFileEqual(&op->partition,&now->files[j].partition))
                    LakeRewriteConflict("fresh source historical partition key");
            }
            if(!found)LakeRewriteConflict("selected partition source was retired");
        }
    }
    for(int c=0;c<old->column_count;++c){const auto* a=&old->columns[c];const auto* b=&now->columns[c];
        if(a->column_id!=b->column_id||a->type!=b->type||a->typmod!=b->typmod||
           a->element_column_id!=b->element_column_id||a->introduced_snapshot!=b->introduced_snapshot||
           a->not_null!=b->not_null||a->initial_null_proven!=b->initial_null_proven||
           a->initial_value_proven!=b->initial_value_proven||strcmp(a->name,b->name))
            LakeRewriteConflict("column schema");}
}

/* Metadata only. Repeat after winning the snapshot PK; never open an SDK
 * object, look up an index artifact, or scan business rows under that claim. */
void LakeRewriteCheckSources(LakeRewriteOperation* op,Snapshot snapshot,bool capture)
{
    LakeRewriteCheckPartition(op,snapshot,capture);
    LakeInlineBinding* b=op->table->binding;Oid ns=b->metadata_schema_oid;
    char* data=LakeMetadataRelation(ns,"ducklake_data_file");
    char* deleted=LakeMetadataRelation(ns,"ducklake_delete_file");
    char* partition=LakeMetadataRelation(ns,"ducklake_partition_info");
    char* sort=LakeMetadataRelation(ns,"ducklake_sort_info");
    Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
    Datum args[4]={Int64GetDatum(LakePermanentTableId(b)),Int64GetDatum(op->cutoff),
        Int64GetDatum(op->next_file_at_cutoff),(Datum)0};
    LakeMetaRows* policy=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1 "
        "AND end_snapshot IS NULL) OR EXISTS(SELECT 1 FROM %s WHERE table_id=$1 AND end_snapshot IS NULL)",partition,sort),
        1,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeMetadataRequireCount(policy,1,"rewrite partition/sort policy");
    if(!op->partitioned&&DatumGetBool(LakeMetadataValue(policy,0,1,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native rewrite does not support partition or sort specifications")));
    const char* inlineName=psprintf("ducklake_inlined_delete_%lld",(long long)LakePermanentTableId(b));
    bool inlineExists=OidIsValid(get_relname_relid(inlineName,ns));
    char* marks=inlineExists?LakeMetadataRelation(ns,inlineName):NULL;
    for(int f=0;f<op->source_count;++f){
        args[3]=Int64GetDatum(op->source_ids[f]);
        LakeMetaRows* source=LakeMetadataQuery(psprintf("SELECT "
            "ROW(data_file_id,table_id,begin_snapshot,end_snapshot,file_order,path,path_is_relative,file_format,"
            "record_count,file_size_bytes,footer_size,row_id_start,partition_id,encryption_key,mapping_id,partial_max,row_group_count)::text,"
            "%s AND encryption_key IS NULL AND mapping_id IS NULL "
            "FROM %s WHERE table_id=$1 AND data_file_id=$4 AND begin_snapshot<=$2 AND end_snapshot IS NULL",op->partitioned?"partition_id IS NOT NULL":"partition_id IS NULL",data),
            4,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
        if(source->count!=1)LakeRewriteConflict("selected file missing or retired");
        if(!DatumGetBool(LakeMetadataValue(source,0,2,BOOLOID)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native rewrite requires unpartitioned unmapped unencrypted files")));
        char* record=LakeMetadataText(source,0,1);
        if(capture)op->source_records[f]=record;
        else if(strcmp(op->source_records[f],record))LakeRewriteConflict("immutable selected metadata");
        LakeMetaRows* baseline=LakeMetadataQuery(psprintf("SELECT ROW(delete_file_id,table_id,begin_snapshot,end_snapshot,"
            "data_file_id,path,path_is_relative,format,delete_count,file_size_bytes,footer_size,encryption_key,partial_max,row_group_count)::text "
            "FROM %s WHERE table_id=$1 AND data_file_id=$4 AND end_snapshot IS NULL ORDER BY delete_file_id LIMIT 2",deleted),
            4,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
        if(baseline->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ambiguous rewrite deletion source")));
        char* deletion=baseline->count?LakeMetadataText(baseline,0,1):pstrdup("");
        if(capture)op->delete_records[f]=deletion;
        else if(strcmp(op->delete_records[f],deletion))LakeRewriteConflict("selected deletion metadata removed or replaced");
        LakeMetaRows* changes=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s "
            "WHERE table_id=$1 AND data_file_id=$4 AND (begin_snapshot>$2 OR delete_file_id>=$3))",deleted),
            4,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
        LakeMetadataRequireCount(changes,1,"rewrite file-delete conflicts");
        if(DatumGetBool(LakeMetadataValue(changes,0,1,BOOLOID)))LakeRewriteConflict("selected delete file added or replaced");
        if(marks){
            changes=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE file_id=$4 AND begin_snapshot>$2)",marks),
                4,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(changes,1,"rewrite inline file-delete conflicts");
            if(DatumGetBool(LakeMetadataValue(changes,0,1,BOOLOID)))LakeRewriteConflict("selected file updated/deleted");
        }
    }
}

static void LakeRewritePrepare(LakeRewriteOperation* op,const char* path)
{
    LakeInlineBinding* b=op->input;int count=b->column_count+2;
    op->minima=(char**)MemoryContextAllocZero(op->context,sizeof(char*)*b->column_count);
    op->maxima=(char**)MemoryContextAllocZero(op->context,sizeof(char*)*b->column_count);
    bool collectBounds=LakeStatsExtendedVersionEnabled();
    OgLakeSdkField* fields=(OgLakeSdkField*)palloc0(sizeof(OgLakeSdkField)*count);
    OgLakeSdkListField* lists=(OgLakeSdkListField*)palloc0(sizeof(OgLakeSdkListField)*b->column_count);int listCount=0;
    for(int c=0;c<b->column_count;++c){const auto* col=&b->columns[c];
        LakeNativeTypeRequireSdk(col->type,LakeFileProvider());LakeNativeTypeRequireWrite(col->type);
        fields[c]={col->column_id,LakeNativeTypeByOid(col->type)->sdk_type,1,col->name};
        if(col->type==VECTOROID)lists[listCount++]={col->column_id,col->element_column_id,1};}
    fields[count-2]={LAKE_FILE_ROW_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_row_id"};
    fields[count-1]={LAKE_FILE_SNAPSHOT_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_snapshot_id"};
    LakeFileReader* volatile reader=NULL;MemoryContext caller=CurrentMemoryContext;
    PG_TRY();{
        /* table=NULL means pure committed input, never the caller journal's
         * own-write overlay. Only the explicit selected files enter the reader. */
        if(op->partitioned)for(int f=0;f<op->source_count;++f){
            op->source_versions[f]=LakeFileObjectVersion(b,f);
            if(!op->source_versions[f])ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("partitioned rewrite requires immutable source identities")));
        }
        reader=LakeFileReaderOpen(b,NULL,InvalidCommandId,NULL,op->partitioned?op->source_versions:NULL);
        if(op->partitioned)LakeFileReaderVerifyRewritePartition(reader);
        for(int f=0;f<op->source_count;++f){const char* version=LakeFileReaderObjectVersion(reader,f);
            if(!version||!*version)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("rewrite requires immutable source identities")));
            if(op->partitioned&&strcmp(version,op->source_versions[f]))LakeRewriteConflict("source admission object generation");
            if(!op->partitioned)op->source_versions[f]=pstrdup(version);}
        LakeArrowBuilder* batch=LakeArrowBuilderCreate(op->context,fields,count,4096,1024*1024,LakeFlushTypmods(b,count));
        Datum* values=(Datum*)palloc0(sizeof(Datum)*count);bool* nulls=(bool*)palloc0(sizeof(bool)*count);
        int* partitionAttrs=NULL;
        if(op->partitioned){partitionAttrs=(int*)palloc(sizeof(int)*b->column_count);
            for(int c=0;c<b->column_count;++c)partitionAttrs[c]=c;}
        MemoryContext row=AllocSetContextCreate(op->context,"Native rewrite source row",ALLOCSET_DEFAULT_SIZES);
        for(;;){
            CHECK_FOR_INTERRUPTS();MemoryContextReset(row);MemoryContextSwitchTo(row);
            LakeInlineRowIdentity id={};
            if(!LakeFileReaderNext(reader,values,nulls,&id))break;
            if(id.row_id<0||id.row_id>=LAKE_REFERENCE_LOCAL_ID_START||id.begin_snapshot<0||id.begin_snapshot>op->cutoff||id.ended)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("rewrite received an invalid visible version")));
            if(op->partitioned&&!LakePartitionTupleMatches(&op->partition.spec,&op->partition.tuple,values,nulls,partitionAttrs))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("visible rewrite row differs from its independently captured partition tuple")));
            if(op->rows==PG_INT64_MAX)ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("rewrite row count overflow")));
            values[count-2]=Int64GetDatum(id.row_id);values[count-1]=Int64GetDatum(id.begin_snapshot);
            nulls[count-2]=nulls[count-1]=false;
            if(!op->output.candidate){MemoryContextSwitchTo(op->context);
                op->output.candidate=LakeFlushOpen(path,fields,count,listCount,listCount?lists:NULL,b);MemoryContextSwitchTo(row);}
            LakeFlushAppend(batch,op->output.candidate,values,nulls);
            for(int c=0;c<b->column_count;++c){
                if(nulls[c])++op->null_counts[c];
                else if(collectBounds)
                    LakeObserveColumnBounds(op->context,b->columns[c].type,b->columns[c].typmod,
                        values[c],&op->minima[c],&op->maxima[c]);
            }
            ++op->rows;
        }
        MemoryContextSwitchTo(op->context);LakeFileReaderClose(reader);reader=NULL;MemoryContextDelete(row);
        if(op->output.candidate){
            LakeArrowBuilderWrite(batch,op->output.candidate->api,op->output.candidate->writer);
            LakeFinishFileOutput(op->context,b,&op->output,fields,count);
            if(op->output.candidate->written.num_rows!=op->rows)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("rewrite output population differs from complete input")));
        }
        LakeArrowBuilderDestroy(batch);MemoryContextSwitchTo(caller);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);if(reader)LakeFileReaderClose(reader);PG_RE_THROW();
    }PG_END_TRY();
}

PG_FUNCTION_INFO_V1(og_lake_rewrite_data_files);
extern "C" Datum og_lake_rewrite_data_files(PG_FUNCTION_ARGS)
{
    if(PG_ARGISNULL(0)||PG_ARGISNULL(1))ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("rewrite table and selected files are required")));
    if(!LakeRewriteVersionEnabled())ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native rewrite requires extension0.21")));
    LakeRequireAccessPolicy();Oid oid=PG_GETARG_OID(0);Relation host=heap_open(oid,AccessShareLock);
    if(!LakeIsNativeForeignRelation(host))ereport(ERROR,(errcode(ERRCODE_WRONG_OBJECT_TYPE),errmsg("rewrite requires a native lake table")));
    if(!pg_class_ownercheck(oid,GetUserId()))ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("rewrite requires the business table owner")));
    ArrayType* array=PG_GETARG_ARRAYTYPE_P(1);Datum* ids=NULL;bool* absent=NULL;int n=0;
    if(ARR_ELEMTYPE(array)!=INT8OID||ARR_NDIM(array)!=1)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("rewrite requires one-dimensional bigint file IDs")));
    const int sourceLimit=LakeRewriteSourceLimit();
    /* Bound detoasted element expansion before deconstruct_array. This is a
     * selected-group bound, never a bound on the table's total file count. */
    if(ARR_DIMS(array)[0]<1||ARR_DIMS(array)[0]>sourceLimit)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg(sourceLimit==2?"rewrite selects one or two complete current files":
                "rewrite selects one to sixteen complete current files")));
    deconstruct_array(array,INT8OID,8,true,'d',&ids,&absent,&n);
    for(int i=0;i<n;++i){
        if(absent[i]||DatumGetInt64(ids[i])<0||DatumGetInt64(ids[i])>=LAKE_REFERENCE_LOCAL_ID_START)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("rewrite file IDs must be distinct permanent nonnegative values")));
        for(int j=0;j<i;++j)if(DatumGetInt64(ids[j])==DatumGetInt64(ids[i]))
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("rewrite file IDs must be distinct permanent nonnegative values")));
    }
    LakeTxnTable* table=LakeTxnReadStatementTable(oid,GetActiveSnapshot());
    bool partitioned=LakeIdentityPartitioned(table->binding);
    /* SQL0.60 unlocks cross-evolution (ADD/DROP COLUMN) multi-file and
     * partitioned rewrite via LakeColumnLayoutsEqual + rewritten_birth_upper. */
    if(n>2&&(!table->binding->column_evolution||!LakeNativeProtectedCreate(oid)||
       RelationIsPartitioned(host)||host->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT||
       (!LakeMergeAdjacentVersionEnabled()&&(table->binding->has_added_columns||table->binding->has_dropped_columns))))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("multi-file rewrite requires a stable ordinary protected CREATE schema")));
    if(partitioned){
        if(!LakePartitionRewriteVersionEnabled()||!table->binding->column_evolution||
           !LakeNativeProtectedCreate(oid)||RelationIsPartitioned(host)||
           host->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT||
           (!LakeMergeAdjacentVersionEnabled()&&(table->binding->has_added_columns||table->binding->has_dropped_columns)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("partitioned rewrite requires SQL0.48 and a stable ordinary protected CREATE schema")));
    }else if(!table->binding->column_evolution||!LakeNativeProtectedCreate(oid)||RelationIsPartitioned(host)||
             host->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT||!LakeMergeAdjacentVersionEnabled()){
        /* SQL0.60 reads only selected Parquet files of an ordinary protected
         * CREATE table. Old inline carriers remain in the original binding;
         * they are never rewrite input and are not retired at publication. */
        LakeRejectIdentityPartitionIntersection(table->binding,"file rewrite");
    }
    MemoryContext parent=LakeTxnBeginRewrite(table),caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(parent,"Native lake file rewrite",ALLOCSET_DEFAULT_SIZES);
    PG_TRY();{
        MemoryContextSwitchTo(context);auto* op=(LakeRewriteOperation*)palloc0(sizeof(LakeRewriteOperation));
        op->partitioned=partitioned;op->context=context;op->table=table;op->owner_subid=GetCurrentSubTransactionId();op->cutoff=table->binding->snapshot_id;op->source_count=n;
        op->input=(LakeInlineBinding*)palloc(sizeof(LakeInlineBinding));*op->input=*table->binding;
        op->input->file_count=n;op->input->files=(LakeDataFile*)palloc0(sizeof(LakeDataFile)*n);op->input->inline_oid=InvalidOid;
        /* This is a file-only reader, even when the original table owns one
         * current inline descriptor. Do not inherit its readable carriers. */
        op->input->inline_sources_loaded=false;op->input->inline_sources=NULL;
        op->input->inline_source_count=0;op->input->inline_write_index=-1;
        for(int f=0;f<n;++f){op->source_ids[f]=DatumGetInt64(ids[f]);bool found=false;
            for(int j=0;j<table->binding->file_count;++j)if(table->binding->files[j].file_id==op->source_ids[f]){
                op->input->files[f]=table->binding->files[j];found=true;break;}
            if(!found)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("rewrite selected file is not current in this table")));}
        /* Keep every source descriptor paired with its ID. Stable ascending
         * order also makes the final source the true allocation frontier. */
        for(int i=1;i<n;++i){
            int64 id=op->source_ids[i];LakeDataFile file=op->input->files[i];int j=i;
            while(j>0&&op->source_ids[j-1]>id){
                op->source_ids[j]=op->source_ids[j-1];op->input->files[j]=op->input->files[j-1];--j;}
            op->source_ids[j]=id;op->input->files[j]=file;
        }
        if(op->partitioned)LakeRewriteCapturePartition(op);
        Oid type=INT8OID;Datum value=Int64GetDatum(op->cutoff);
        LakeMetaRows* head=LakeMetadataQuery(psprintf("SELECT next_file_id FROM %s WHERE snapshot_id=$1",LakeMetadataRelation(table->binding->metadata_schema_oid,"ducklake_snapshot")),
            1,&type,&value,NULL,LakeTxnBusinessSnapshot(table),true,SPI_OK_SELECT,table->binding->metadata_schema_oid);
        LakeMetadataRequireCount(head,1,"rewrite input header");op->next_file_at_cutoff=DatumGetInt64(LakeMetadataValue(head,0,1,INT8OID));
        if(op->next_file_at_cutoff<0||op->source_ids[n-1]>=op->next_file_at_cutoff)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid rewrite file frontier")));
        LakeRewriteCheckSources(op,LakeTxnBusinessSnapshot(table),true);
        (void)LakeFileLifecycleProvider();
        for(int c=0;c<table->binding->column_count;++c){LakeNativeTypeRequireSdk(table->binding->columns[c].type,LakeFileProvider());
            if(table->binding->columns[c].type==VECTOROID)(void)LakeFileNestedProvider();}
        char* dir=LakePrepareFlushDirectory(table->binding,LakeTxnBusinessSnapshot(table));
        char* uuid=DatumGetCString(DirectFunctionCall1(uuid_out,OidFunctionCall0(F_GEN_RANDOM_UUID)));
        op->output.relative_path=psprintf("og-rewrite-%s.parquet",uuid);op->null_counts=(int64*)palloc0(sizeof(int64)*table->binding->column_count);
        LakeRewritePrepare(op,psprintf("%s/%s",dir,op->output.relative_path));
        LakeTxnAdoptRewrite(op);MemoryContextSwitchTo(caller);heap_close(host,NoLock);
    }PG_CATCH();{MemoryContextSwitchTo(caller);MemoryContextDelete(context);heap_close(host,NoLock);PG_RE_THROW();}PG_END_TRY();
    PG_RETURN_VOID();
}

static const int64 LAKE_DEFAULT_TARGET_FILE_SIZE = INT64CONST(536870912); /* 512 MiB */

static void LakeResolveMergeOptions(const LakeInlineBinding* binding, Snapshot snapshot,
    int64* targetFileSizeOut, bool* autoCompactOut)
{
    const char* options = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_metadata");
    const char* tables = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_table");
    Oid types[2] = {INT8OID, INT8OID};
    Datum args[2] = {Int64GetDatum(LakePermanentTableId(binding)), Int64GetDatum(binding->snapshot_id)};
    LakeMetaRows* rows = LakeMetadataQuery(psprintf(
        "SELECT key::text, scope::text, value::text FROM %s "
        "WHERE key IN ('target_file_size', 'auto_compact') "
        "AND (scope IS NULL OR (scope='table' AND scope_id=$1) OR (scope='schema' AND scope_id=(SELECT schema_id "
        "FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2))))",
        options, tables), 2, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    bool seenSize[3] = {false, false, false};
    int64 sizes[3] = {LAKE_DEFAULT_TARGET_FILE_SIZE, LAKE_DEFAULT_TARGET_FILE_SIZE, LAKE_DEFAULT_TARGET_FILE_SIZE};
    bool seenAuto[3] = {false, false, false};
    bool autos[3] = {false, false, false};
    for (uint32 i = 0; i < rows->count; ++i) {
        const char* key = LakeMetadataText(rows, i, 1);
        bool global = false;
        Datum scopeDatum = LakeMetadataValue(rows, i, 2, TEXTOID, true, &global);
        int rank = global ? 0 : (pg_strcasecmp(TextDatumGetCString(scopeDatum), "schema") == 0 ? 1 : 2);
        const char* val = LakeMetadataText(rows, i, 3);
        if (pg_strcasecmp(key, "target_file_size") == 0) {
            if (seenSize[rank])
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate native lake target_file_size option")));
            if (*val == '\0')
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("empty native lake target_file_size option")));
            for (const char* c = val; *c != '\0'; ++c)
                if (*c < '0' || *c > '9')
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("native lake target_file_size option must be a positive byte integer")));
            int64 parsed = DatumGetInt64(DirectFunctionCall1(int8in, CStringGetDatum(val)));
            if (parsed <= 0)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake target_file_size option must be greater than zero")));
            sizes[rank] = parsed;
            seenSize[rank] = true;
        } else if (pg_strcasecmp(key, "auto_compact") == 0) {
            if (seenAuto[rank])
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate native lake auto_compact option")));
            if (pg_strcasecmp(val, "true") == 0 || strcmp(val, "1") == 0 || pg_strcasecmp(val, "on") == 0)
                autos[rank] = true;
            else if (pg_strcasecmp(val, "false") == 0 || strcmp(val, "0") == 0 || pg_strcasecmp(val, "off") == 0)
                autos[rank] = false;
            else
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake auto_compact option must be boolean")));
            seenAuto[rank] = true;
        }
    }
    *targetFileSizeOut = seenSize[2] ? sizes[2] : (seenSize[1] ? sizes[1] : sizes[0]);
    *autoCompactOut = seenAuto[2] ? autos[2] : (seenAuto[1] ? autos[1] : (seenAuto[0] ? autos[0] : false));
}

/* DuckLake CompactionFilePicker distance-to-target greedy selector:
 * picks the first adjacent run of 2..maxFiles files within one partition group
 * where adding each subsequent file strictly reduces |total_bytes - targetSize|. */
static int LakePickAdjacentRun(const LakeDataFile* const* group, int groupCount, int maxBatch,
    int64 targetSize, const LakeDataFile** chosenOut)
{
    for (int start = 0; start + 1 < groupCount; ++start) {
        int count = 1;
        chosenOut[0] = group[start];
        int64 currentSize = group[start]->file_bytes;
        for (int idx = start + 1; idx < groupCount && count < maxBatch; ++idx) {
            int64 addBytes = group[idx]->file_bytes;
            if (addBytes < 0 || currentSize > PG_INT64_MAX - addBytes)
                break;
            int64 mergedSize = currentSize + addBytes;
            uint64 curDist = currentSize >= targetSize ? (uint64)(currentSize - targetSize) : (uint64)(targetSize - currentSize);
            uint64 newDist = mergedSize >= targetSize ? (uint64)(mergedSize - targetSize) : (uint64)(targetSize - mergedSize);
            if (newDist < curDist || (addBytes == 0 && mergedSize <= targetSize)) {
                chosenOut[count++] = group[idx];
                currentSize = mergedSize;
            } else {
                break;
            }
        }
        if (count >= 2)
            return count;
    }
    return 0;
}

bool LakeTryStageMergeAdjacent(Oid relationOid, int maxFiles, int64 targetFileSizeOverride,
    bool backgroundCall, char** schemaNameOut, char** tableNameOut, int64* filesProcessedOut, int64* filesCreatedOut)
{
    if (!LakeMergeAdjacentVersionEnabled()) {
        if (backgroundCall)
            return false;
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake merge_adjacent_files requires installed SQL0.60")));
    }
    LakeRequireAccessPolicy();
    int limit = Min(maxFiles, LakeRewriteSourceLimit());
    if (limit < 2)
        return false;

    Relation volatile host = try_relation_open(relationOid, AccessShareLock);
    if (host == NULL) {
        if (backgroundCall)
            return false;
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation with OID %u does not exist", relationOid)));
    }
    if (!LakeIsNativeForeignRelation((Relation)host)) {
        relation_close((Relation)host, NoLock);
        if (backgroundCall)
            return false;
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("merge_adjacent_files requires a native lake table")));
    }
    if (!backgroundCall && !pg_class_ownercheck(relationOid, GetUserId())) {
        relation_close((Relation)host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("merge_adjacent_files requires the business table owner")));
    }
    ForeignTable* foreign = GetForeignTable(relationOid);
    if (!LakeTrustedForeignServer(foreign->serverid)) {
        relation_close((Relation)host, NoLock);
        if (backgroundCall)
            return false;
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("merge_adjacent_files target server is not a trusted og_lake server")));
    }
    LakeTxnRejectBoundSnapshotWrite(foreign->serverid);
    if (u_sess->utils_cxt.XactIsoLevel != XACT_READ_COMMITTED) {
        relation_close((Relation)host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("merge_adjacent_files requires READ COMMITTED")));
    }
    if (u_sess->attr.attr_common.XactReadOnly) {
        relation_close((Relation)host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
            errmsg("cannot merge native lake files in a read-only transaction")));
    }
    LakeTxnState* existing = LakeHostExistingTransactionState();
    if (existing != NULL && (existing->phase != LAKE_TXN_ACTIVE || OidIsValid(existing->writable_server) ||
        existing->binding_ddl != NULL || existing->flush != NULL || existing->rewrite != NULL ||
        existing->renames != NULL || existing->drops != NULL || existing->object_events != NULL)) {
        relation_close((Relation)host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake merge_adjacent_files requires its own lake maintenance transaction")));
    }
    if (RelationIsPartitioned((Relation)host) || ((Relation)host)->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
        LakeTxnFindCreatedTable(relationOid, false) != NULL || !LakeNativeProtectedCreate(relationOid)) {
        relation_close((Relation)host, NoLock);
        if (backgroundCall)
            return false;
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("merge_adjacent_files requires a committed ordinary protected CREATE table")));
    }

    MemoryContext caller = CurrentMemoryContext;
    MemoryContext volatile probeCtx = AllocSetContextCreate(caller, "Native merge_adjacent probe", ALLOCSET_DEFAULT_SIZES);
    MemoryContext volatile rewriteCtx = NULL;
    LakeTxnTable* volatile table = NULL;
    volatile bool adopted = false;
    char* resolvedSchema = NULL;
    char* resolvedTable = NULL;
    int64 processedCount = 0;
    int64 createdCount = 0;

    PG_TRY();
    {
        table = LakeTxnReadStatementTable(relationOid, GetActiveSnapshot());
        LakeInlineBinding* b = table->binding;
        if (b->read_only) {
            if (!backgroundCall)
                ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                    errmsg("cannot merge files in a read-only native lake catalog")));
        } else if (!b->column_evolution || !OidIsValid(LakeMetadataOwner())) {
            if (!backgroundCall)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("merge_adjacent_files requires a writable protected schema and private metadata owner")));
        } else {
            MemoryContextSwitchTo(probeCtx);
            Snapshot snap = LakeTxnBusinessSnapshot(table);
            Oid ns = b->metadata_schema_oid;
            int64 configuredSize = LAKE_DEFAULT_TARGET_FILE_SIZE;
            bool autoCompact = false;
            LakeResolveMergeOptions(b, snap, &configuredSize, &autoCompact);
            int64 targetSize = targetFileSizeOverride > 0 ? targetFileSizeOverride : configuredSize;

            bool proceed = !backgroundCall || autoCompact;
            bool partitioned = LakeIdentityPartitioned(b);
            if (proceed && partitioned && !LakePartitionRewriteVersionEnabled()) {
                if (backgroundCall)
                    proceed = false;
                else
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("partitioned merge_adjacent_files requires SQL0.48")));
            }
            if (proceed) {
                Oid tType = INT8OID;
                Datum tArg = Int64GetDatum(LakePermanentTableId(b));
                LakeMetaRows* sortRows = LakeMetadataQuery(psprintf(
                    "SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1)",
                    LakeMetadataRelation(ns, "ducklake_sort_info")),
                    1, &tType, &tArg, NULL, snap, true, SPI_OK_SELECT, ns);
                LakeMetadataRequireCount(sortRows, 1, "merge_adjacent sort policy");
                if (DatumGetBool(LakeMetadataValue(sortRows, 0, 1, BOOLOID))) {
                    if (backgroundCall)
                        proceed = false;
                    else
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("merge_adjacent_files does not support retained sort specifications")));
                }
            }
            if (proceed && !partitioned) {
                if (backgroundCall) {
                    Oid tType = INT8OID;
                    Datum tArg = Int64GetDatum(LakePermanentTableId(b));
                    LakeMetaRows* partHist = LakeMetadataQuery(psprintf(
                        "SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1)",
                        LakeMetadataRelation(ns, "ducklake_partition_info")),
                        1, &tType, &tArg, NULL, snap, true, SPI_OK_SELECT, ns);
                    if (DatumGetBool(LakeMetadataValue(partHist, 0, 1, BOOLOID)))
                        proceed = false;
                }
            }
            if (proceed && b->file_count >= 2) {
                /* Query ordered unencrypted unmapped small files without delete files or inline delete marks. */
                const char* dataRel = LakeMetadataRelation(ns, "ducklake_data_file");
                const char* delRel = LakeMetadataRelation(ns, "ducklake_delete_file");
                const char* inlineName = psprintf("ducklake_inlined_delete_%lld", (long long)LakePermanentTableId(b));
                bool inlineExists = OidIsValid(get_relname_relid(inlineName, ns));
                const char* marksClause = inlineExists ?
                    psprintf("AND NOT EXISTS (SELECT 1 FROM %s m WHERE m.file_id=df.data_file_id)",
                        LakeMetadataRelation(ns, inlineName)) : "";
                Oid qTypes[3] = {INT8OID, INT8OID, INT8OID};
                Datum qArgs[3] = {
                    Int64GetDatum(LakePermanentTableId(b)),
                    Int64GetDatum(b->snapshot_id),
                    Int64GetDatum(targetSize)
                };
                LakeMetaRows* candRows = LakeMetadataQuery(psprintf(
                    "SELECT df.data_file_id FROM %s df "
                    "WHERE df.table_id=$1 AND df.begin_snapshot<=$2 AND df.end_snapshot IS NULL "
                    "AND df.file_size_bytes>=0 AND df.file_size_bytes<$3 "
                    "AND df.encryption_key IS NULL AND df.mapping_id IS NULL AND %s "
                    "AND NOT EXISTS (SELECT 1 FROM %s d WHERE d.table_id=$1 AND d.data_file_id=df.data_file_id) %s "
                    "ORDER BY COALESCE(df.file_order, df.data_file_id), df.data_file_id",
                    dataRel, partitioned ? "df.partition_id IS NOT NULL" : "df.partition_id IS NULL",
                    delRel, marksClause),
                    3, qTypes, qArgs, NULL, snap, true, SPI_OK_SELECT, ns);

                int eligibleCount = 0;
                const LakeDataFile** eligible = candRows->count > 0 ?
                    (const LakeDataFile**)palloc0(sizeof(const LakeDataFile*) * candRows->count) : NULL;
                for (uint32 r = 0; r < candRows->count; ++r) {
                    int64 fid = DatumGetInt64(LakeMetadataValue(candRows, r, 1, INT8OID));
                    int low = 0, high = b->file_count;
                    const LakeDataFile* matched = NULL;
                    while (low < high) {
                        int mid = low + (high - low) / 2;
                        if (b->files[mid].file_id < fid)
                            low = mid + 1;
                        else
                            high = mid;
                    }
                    if (low < b->file_count && b->files[low].file_id == fid)
                        matched = &b->files[low];
                    if (matched == NULL || matched->deletes != NULL || matched->inline_delete_count > 0 ||
                        matched->file_bytes < 0 || matched->file_bytes >= targetSize)
                        continue;
                    if (partitioned) {
                        const LakePartitionSpec* spec = &matched->partition.spec;
                        if (!spec->enabled || !spec->present || spec->key_count < 1 || !spec->keys)
                            continue;
                        bool validKeys = true;
                        for (int k = 0; k < spec->key_count; ++k) {
                            const auto* key = &spec->keys[k];
                            if (!key->host_bound || key->column_index < 0 || key->column_index >= b->column_count ||
                                b->columns[key->column_index].column_id != key->column_id) {
                                validKeys = false;
                                break;
                            }
                        }
                        if (!validKeys)
                            continue;
                    }
                    eligible[eligibleCount++] = matched;
                }

                const LakeDataFile* chosen[OG_LAKE_REWRITE_MAX_SOURCES] = {NULL};
                int chosenCount = 0;
                if (eligibleCount >= 2) {
                    if (!partitioned) {
                        chosenCount = LakePickAdjacentRun(eligible, eligibleCount, limit, targetSize, chosen);
                    } else {
                        bool* visited = (bool*)palloc0(sizeof(bool) * eligibleCount);
                        const LakeDataFile** groupBuf = (const LakeDataFile**)palloc(sizeof(const LakeDataFile*) * eligibleCount);
                        for (int i = 0; i < eligibleCount && chosenCount < 2; ++i) {
                            if (visited[i])
                                continue;
                            int gCount = 0;
                            for (int j = i; j < eligibleCount; ++j) {
                                if (!visited[j] && LakeIdentityPartitionFileEqual(&eligible[i]->partition, &eligible[j]->partition)) {
                                    visited[j] = true;
                                    groupBuf[gCount++] = eligible[j];
                                }
                            }
                            if (gCount >= 2)
                                chosenCount = LakePickAdjacentRun(groupBuf, gCount, limit, targetSize, chosen);
                        }
                    }
                }

                if (chosenCount >= 2) {
                    Oid nameTypes[2] = {INT8OID, INT8OID};
                    Datum nameArgs[2] = {Int64GetDatum(LakePermanentTableId(b)), Int64GetDatum(b->snapshot_id)};
                    LakeMetaRows* nameRows = LakeMetadataQuery(psprintf(
                        "SELECT s.schema_name::text, t.table_name::text "
                        "FROM %s t JOIN %s s ON s.schema_id=t.schema_id "
                        "AND s.begin_snapshot<=$2 AND (s.end_snapshot IS NULL OR s.end_snapshot>$2) "
                        "WHERE t.table_id=$1 AND t.begin_snapshot<=$2 AND (t.end_snapshot IS NULL OR t.end_snapshot>$2)",
                        LakeMetadataRelation(ns, "ducklake_table"),
                        LakeMetadataRelation(ns, "ducklake_schema")),
                        2, nameTypes, nameArgs, NULL, snap, true, SPI_OK_SELECT, ns);
                    LakeMetadataRequireCount(nameRows, 1, "merge_adjacent table identity");
                    char* tmpSchema = LakeMetadataText(nameRows, 0, 1);
                    char* tmpTable = LakeMetadataText(nameRows, 0, 2);

                    MemoryContextSwitchTo(caller);
                    if (schemaNameOut != NULL)
                        resolvedSchema = pstrdup(tmpSchema);
                    if (tableNameOut != NULL)
                        resolvedTable = pstrdup(tmpTable);

                    MemoryContext parent = LakeTxnBeginRewrite(table);
                    rewriteCtx = AllocSetContextCreate(parent, "Native lake merge_adjacent_files", ALLOCSET_DEFAULT_SIZES);
                    MemoryContextSwitchTo(rewriteCtx);
                    auto* op = (LakeRewriteOperation*)palloc0(sizeof(LakeRewriteOperation));
                    op->partitioned = partitioned;
                    op->context = rewriteCtx;
                    op->table = table;
                    op->owner_subid = GetCurrentSubTransactionId();
                    op->cutoff = b->snapshot_id;
                    op->source_count = chosenCount;
                    op->input = (LakeInlineBinding*)palloc(sizeof(LakeInlineBinding));
                    *op->input = *b;
                    op->input->file_count = chosenCount;
                    op->input->files = (LakeDataFile*)palloc0(sizeof(LakeDataFile) * chosenCount);
                    op->input->inline_oid = InvalidOid;
                    op->input->inline_sources_loaded = false;
                    op->input->inline_sources = NULL;
                    op->input->inline_source_count = 0;
                    op->input->inline_write_index = -1;
                    for (int f = 0; f < chosenCount; ++f) {
                        op->source_ids[f] = chosen[f]->file_id;
                        op->input->files[f] = *chosen[f];
                    }
                    for (int i = 1; i < chosenCount; ++i) {
                        int64 id = op->source_ids[i];
                        LakeDataFile file = op->input->files[i];
                        int j = i;
                        while (j > 0 && op->source_ids[j - 1] > id) {
                            op->source_ids[j] = op->source_ids[j - 1];
                            op->input->files[j] = op->input->files[j - 1];
                            --j;
                        }
                        op->source_ids[j] = id;
                        op->input->files[j] = file;
                    }
                    if (op->partitioned)
                        LakeRewriteCapturePartition(op);
                    Oid hType = INT8OID;
                    Datum hVal = Int64GetDatum(op->cutoff);
                    LakeMetaRows* head = LakeMetadataQuery(psprintf(
                        "SELECT next_file_id FROM %s WHERE snapshot_id=$1",
                        LakeMetadataRelation(ns, "ducklake_snapshot")),
                        1, &hType, &hVal, NULL, snap, true, SPI_OK_SELECT, ns);
                    LakeMetadataRequireCount(head, 1, "merge_adjacent input header");
                    op->next_file_at_cutoff = DatumGetInt64(LakeMetadataValue(head, 0, 1, INT8OID));
                    if (op->next_file_at_cutoff < 0 || op->source_ids[chosenCount - 1] >= op->next_file_at_cutoff)
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid merge_adjacent file frontier")));
                    LakeRewriteCheckSources(op, snap, true);
                    (void)LakeFileLifecycleProvider();
                    for (int c = 0; c < b->column_count; ++c) {
                        LakeNativeTypeRequireSdk(b->columns[c].type, LakeFileProvider());
                        if (b->columns[c].type == VECTOROID)
                            (void)LakeFileNestedProvider();
                    }
                    char* dir = LakePrepareFlushDirectory(b, snap);
                    char* uuid = DatumGetCString(DirectFunctionCall1(uuid_out, OidFunctionCall0(F_GEN_RANDOM_UUID)));
                    op->output.relative_path = psprintf("og-merge-%s.parquet", uuid);
                    op->null_counts = (int64*)palloc0(sizeof(int64) * b->column_count);
                    LakeRewritePrepare(op, psprintf("%s/%s", dir, op->output.relative_path));
                    LakeTxnAdoptRewrite(op);
                    adopted = true;
                    processedCount = chosenCount;
                    createdCount = op->output.candidate != NULL ? 1 : 0;
                }
            }
        }
        MemoryContextSwitchTo(caller);
        if (probeCtx != NULL) {
            MemoryContextDelete(probeCtx);
            probeCtx = NULL;
        }
        if (!adopted && table != NULL) {
            LakeTxnReleaseReadView(table);
            table = NULL;
        }
        relation_close((Relation)host, NoLock);
        host = NULL;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if (probeCtx != NULL)
            MemoryContextDelete(probeCtx);
        if (!adopted && rewriteCtx != NULL)
            MemoryContextDelete(rewriteCtx);
        if (!adopted && table != NULL)
            LakeTxnReleaseReadView(table);
        if (host != NULL)
            relation_close((Relation)host, NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (!adopted)
        return false;
    if (schemaNameOut != NULL)
        *schemaNameOut = resolvedSchema;
    if (tableNameOut != NULL)
        *tableNameOut = resolvedTable;
    if (filesProcessedOut != NULL)
        *filesProcessedOut = processedCount;
    if (filesCreatedOut != NULL)
        *filesCreatedOut = createdCount;
    return true;
}

PG_FUNCTION_INFO_V1(og_lake_merge_adjacent_files);
extern "C" Datum og_lake_merge_adjacent_files(PG_FUNCTION_ARGS)
{
    if (!LakeMergeAdjacentVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake merge_adjacent_files requires installed SQL0.60")));
    LakeRequireAccessPolicy();
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("merge_adjacent_files target relation cannot be NULL")));
    if (PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("merge_adjacent_files max_files cannot be NULL")));
    int maxFiles = PG_GETARG_INT32(1);
    if (maxFiles < 2 || maxFiles > OG_LAKE_REWRITE_MAX_SOURCES)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("merge_adjacent_files max_files must be between 2 and %d", OG_LAKE_REWRITE_MAX_SOURCES)));
    int64 targetOverride = -1;
    if (!PG_ARGISNULL(2)) {
        targetOverride = PG_GETARG_INT64(2);
        if (targetOverride <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("merge_adjacent_files target_file_size must be greater than zero")));
    }

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("merge_adjacent_files requires materialize mode")));

    Oid relationOid = PG_GETARG_OID(0);
    char* schemaName = NULL;
    char* tableName = NULL;
    int64 filesProcessed = 0;
    int64 filesCreated = 0;
    bool staged = LakeTryStageMergeAdjacent(relationOid, maxFiles, targetOverride, false,
        &schemaName, &tableName, &filesProcessed, &filesCreated);

    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc desc = CreateTemplateTupleDesc(4, false);
    TupleDescInitEntry(desc, 1, "schema_name", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 2, "table_name", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 3, "files_processed", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 4, "files_created", INT8OID, -1, 0);
    desc = BlessTupleDesc(desc);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    if (staged) {
        Datum vals[4] = {
            CStringGetTextDatum(schemaName),
            CStringGetTextDatum(tableName),
            Int64GetDatum(filesProcessed),
            Int64GetDatum(filesCreated)
        };
        bool nulls[4] = {false, false, false, false};
        tuplestore_putvalues(store, desc, vals, nulls);
    }
    tuplestore_donestoring(store);
    MemoryContextSwitchTo(callerCtx);

    result->returnMode = SFRM_Materialize;
    result->setResult = store;
    result->setDesc = desc;
    fcinfo->isnull = true;
    return (Datum)0;
}
