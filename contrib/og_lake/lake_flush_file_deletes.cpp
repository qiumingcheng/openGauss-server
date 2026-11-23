/* Standard position-delete consolidation; data files and logical IDs stay fixed. */
#include "postgres.h"
#include "lake_flush.h"
#include "lake_file_reader.h"
#include "lake_arrow_builder.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

struct LakeMergeDeleteMark { int64 position; int64 snapshot; };
struct LakeMergeDeleteInput {
    LakeMergeDeleteMark* marks;
    int64 count;
    int64 capacity;
    int64 cutoff;
};
static void LakeMergeAddDelete(int64 position,int64 snapshot,void* argument)
{
    LakeMergeDeleteInput* input=(LakeMergeDeleteInput*)argument;
    if(input->count>=input->capacity||snapshot<0||snapshot>input->cutoff)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush deletion input population or snapshot")));
    input->marks[input->count++]={position,snapshot};
}
static int LakeMergeCompareDelete(const void* left,const void* right)
{
    const LakeMergeDeleteMark* a=(const LakeMergeDeleteMark*)left;
    const LakeMergeDeleteMark* b=(const LakeMergeDeleteMark*)right;
    if(a->position!=b->position)return a->position<b->position?-1:1;
    return a->snapshot<b->snapshot?-1:a->snapshot>b->snapshot?1:0;
}

static void LakePrepareFileDeleteFlushInternal(LakeFlushOperation* op,const char* directory,const char* identity)
{
    LakeInlineBinding* binding=op->table->binding;Oid ns=binding->metadata_schema_oid;
    if(binding->file_count==0)return;
    char* name=psprintf("ducklake_inlined_delete_%lld",(long long)LakePermanentTableId(binding));
    if(!OidIsValid(get_relname_relid(name,ns)))return;
    char* carrier=LakeMetadataRelation(ns,name);
    int count=0;
    for(int i=0;i<binding->file_count;++i)if(binding->files[i].inline_delete_count>0)++count;
    /* Consolidate a bounded file prefix; markers for the rest remain visible
     * in their original inline carrier for the next maintenance call. */
    count=Min(count,LakeFileCandidateSlotsAvailable());
    if(count==0)return;
    op->file_deletion_count=count;
    op->file_deletions=(LakeFlushFileDeletion*)palloc0(sizeof(LakeFlushFileDeletion)*count);
    LakeSdkHandles* handles=LakeFileHandles(op->context);
    int next=0;
    for(int i=0;i<binding->file_count&&next<count;++i){
        LakeDataFile* file=&binding->files[i];
        if(file->inline_delete_count==0)continue;
        CHECK_FOR_INTERRUPTS();
        LakeFlushFileDeletion* output=&op->file_deletions[next++];output->file=file;
        int64 maximum=file->inline_delete_count+(file->deletes?file->deletes->record_count:0);
        if(maximum<=0||maximum>LAKE_FILE_DELETE_POSITION_LIMIT)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native flush deletion input exceeds position budget")));
        LakeMergeDeleteInput input={(LakeMergeDeleteMark*)palloc(sizeof(LakeMergeDeleteMark)*(Size)maximum),0,maximum,op->cutoff};
        if(file->deletes)LakeFileVisitDeletes(file,handles,LakeMergeAddDelete,&input);
        Oid types[2]={INT8OID,INT8OID};Datum args[2]={Int64GetDatum(file->file_id),Int64GetDatum(op->cutoff)};
        LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT row_id,begin_snapshot FROM %s WHERE file_id=$1 "
            "AND begin_snapshot<=$2 ORDER BY row_id,begin_snapshot LIMIT %d",carrier,LAKE_FILE_DELETE_POSITION_LIMIT+1),
            2,types,args,NULL,LakeTxnBusinessSnapshot(op->table),true,SPI_OK_SELECT,ns);
        LakeMetadataRequireCount(rows,(uint32)file->inline_delete_count,"flush inline deletion source");
        output->inline_rows=rows->count;
        for(uint32 r=0;r<rows->count;++r){
            int64 position=DatumGetInt64(LakeMetadataValue(rows,r,1,INT8OID));
            int64 snapshot=DatumGetInt64(LakeMetadataValue(rows,r,2,INT8OID));
            if(position<0||position>=file->record_count)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native flush inline deletion exceeds its data file")));
            LakeMergeAddDelete(position,snapshot,&input);
        }
        qsort(input.marks,(Size)input.count,sizeof(LakeMergeDeleteMark),LakeMergeCompareDelete);
        int64 distinct=0;
        for(int64 r=0;r<input.count;++r)
            if(distinct==0||input.marks[r].position!=input.marks[distinct-1].position)
                input.marks[distinct++]=input.marks[r]; /* First/earliest snapshot wins. */
        if(distinct<=0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native flush deletion merge is unexpectedly empty")));
        output->rows=distinct;output->min_snapshot=PG_INT64_MAX;output->max_snapshot=-1;
        char* relative=psprintf("og-%s-file-%lld-delete.parquet",identity,(long long)file->file_id);
        output->output.relative_path=relative;
        OgLakeSdkField fields[3]={{LAKE_DELETE_PATH_ID,OG_LAKE_SDK_UTF8,0,"file_path"},
            {LAKE_DELETE_POSITION_ID,OG_LAKE_SDK_INT64,0,"pos"},
            {LAKE_FILE_SNAPSHOT_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_snapshot_id"}};
        output->output.candidate=LakeFlushOpen(psprintf("%s/%s",directory,relative),fields,3);
        LakeArrowBuilder* batch=LakeArrowBuilderCreate(op->context,fields,3,4096,1024*1024);
        Datum values[3]={CStringGetTextDatum(file->path),0,0};bool nulls[3]={false,false,false};
        for(int64 r=0;r<distinct;++r){
            CHECK_FOR_INTERRUPTS();
            values[1]=Int64GetDatum(input.marks[r].position);values[2]=Int64GetDatum(input.marks[r].snapshot);
            output->min_snapshot=Min(output->min_snapshot,input.marks[r].snapshot);
            output->max_snapshot=Max(output->max_snapshot,input.marks[r].snapshot);
            LakeFlushAppend(batch,output->output.candidate,values,nulls);
        }
        LakeArrowBuilderWrite(batch,output->output.candidate->api,output->output.candidate->writer);
        LakeArrowBuilderDestroy(batch);LakeFlushFinish(op,&output->output,fields,3);pfree(input.marks);
    }
}

void LakePrepareFileDeleteFlush(LakeFlushOperation* op,const char* directory,const char* identity)
{
    MemoryContext caller=MemoryContextSwitchTo(op->context);
    PG_TRY();
    {
        LakePrepareFileDeleteFlushInternal(op,directory,identity);
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        PG_RE_THROW(); /* SQL preparation owner reclaims an unadopted operation. */
    }
    PG_END_TRY();
}
