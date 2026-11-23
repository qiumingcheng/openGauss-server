/* Test-only real TableAM -> native Arrow buffers -> existing Parquet SDK. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_arrow_builder.h"
#include "lake_inline_private.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(lake_test_write_inline_versions);

struct LakeWriterProbe {
    const OgLakeParquetSdkApi* api;
    OgLakeSdkWriter* writer;
    Relation relation;
    TableScanDesc scan;
    LakeArrowBuilder* batch;
    MemoryContext scan_context;
    MemoryContext row_context;
};

static uint32 LakeProbeType(Oid type)
{
    if(type==BOOLOID)return OG_LAKE_SDK_BOOL;
    if(type==INT4OID)return OG_LAKE_SDK_INT32;
    if(type==INT8OID)return OG_LAKE_SDK_INT64;
    if(type==TEXTOID)return OG_LAKE_SDK_UTF8;
    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported test writer type")));return 0;
}

extern "C" Datum lake_test_write_inline_versions(PG_FUNCTION_ARGS)
{
    if(!superuser())ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("test writer requires administrator")));
    LakeInlineBinding* binding=LakeLoadInlineBinding(PG_GETARG_OID(0),GetActiveSnapshot());
    if(!OidIsValid(binding->inline_oid)||binding->file_count!=0)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("test writer requires a genuine pure-inline source")));
    char* path=text_to_cstring(PG_GETARG_TEXT_PP(1));int capacity=PG_GETARG_INT32(2);
    int fields=binding->column_count+2;
    OgLakeSdkField* schema=(OgLakeSdkField*)palloc0(sizeof(OgLakeSdkField)*fields);
    for(int i=0;i<binding->column_count;++i)
        schema[i]={binding->columns[i].column_id,LakeProbeType(binding->columns[i].type),1,binding->columns[i].name};
    schema[fields-2]={LAKE_FILE_ROW_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_row_id"};
    schema[fields-1]={LAKE_FILE_SNAPSHOT_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_snapshot_id"};
    LakeWriterProbe* state=(LakeWriterProbe*)palloc0(sizeof(LakeWriterProbe));state->api=LakeFileProvider();
    if(!(state->api->capabilities&OG_LAKE_SDK_WRITER)||state->api->writer_open==NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("test provider has no writer")));
    OgLakeSdkWriteOptions options={};options.struct_size=sizeof(options);options.path=path;
    options.limits=LakeFileLimits();options.num_fields=fields;options.fields=schema;
    options.compression=OG_LAKE_SDK_ZSTD;options.row_group_rows=capacity;
    MemoryContext caller=CurrentMemoryContext;int rows=0;StringInfoData mapping;initStringInfo(&mapping);
    appendStringInfoString(&mapping,"{\"mapping\":[");OgLakeSdkWrittenFile written={};written.struct_size=sizeof(written);
    PG_TRY();
    {
        OgLakeSdkError error;
        HOLD_INTERRUPTS();int opened=state->api->writer_open(&options,&state->writer,&error);RESUME_INTERRUPTS();
        LakeFileCheckStatus(opened,&error);CHECK_FOR_INTERRUPTS();
        state->batch=LakeArrowBuilderCreate(caller,schema,fields,capacity,1024*1024);
        state->relation=heap_open(binding->inline_oid,AccessShareLock);TupleDesc descriptor=RelationGetDescr(state->relation);
        if(descriptor->natts!=binding->column_count+3||get_attnum(binding->inline_oid,"_ducklake_row_id")!=1||
           get_attnum(binding->inline_oid,"_ducklake_begin_snapshot")!=2||get_attnum(binding->inline_oid,"_ducklake_end_snapshot")!=3)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("unexpected genuine inline shape")));
        Datum* raw=(Datum*)palloc(sizeof(Datum)*descriptor->natts);bool* rawNull=(bool*)palloc(sizeof(bool)*descriptor->natts);
        Datum* values=(Datum*)palloc(sizeof(Datum)*fields);bool* nulls=(bool*)palloc(sizeof(bool)*fields);
        state->scan_context=AllocSetContextCreate(caller,"Test native writer TableAM",ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        state->row_context=AllocSetContextCreate(caller,"Test native writer row",ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        MemoryContextSwitchTo(state->scan_context);state->scan=tableam_scan_begin(state->relation,GetActiveSnapshot(),0,NULL);
        for(;;) {
            CHECK_FOR_INTERRUPTS();MemoryContextSwitchTo(state->scan_context);
            Tuple tuple=tableam_scan_getnexttuple(state->scan,ForwardScanDirection);
            if(tuple==NULL)break;
            tableam_tops_deform_tuple(tuple,descriptor,raw,rawNull);
            if(rawNull[0]||rawNull[1]||DatumGetInt64(raw[0])<0||DatumGetInt64(raw[1])<0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid test source identity")));
            if(DatumGetInt64(raw[1])>binding->snapshot_id)continue;
            if(rows>=32768)ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("test writer source limit exceeded")));
            MemoryContextSwitchTo(state->row_context);
            for(int i=0;i<binding->column_count;++i) {
                int source=get_attnum(binding->inline_oid,binding->columns[i].name)-1;
                if(source<3||source>=descriptor->natts)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("missing test business column")));
                values[i]=raw[source];nulls[i]=rawNull[source];
            }
            values[fields-2]=raw[0];values[fields-1]=raw[1];nulls[fields-2]=nulls[fields-1]=false;
            if(!LakeArrowBuilderAppend(state->batch,values,nulls)) {
                LakeArrowBuilderWrite(state->batch,state->api,state->writer);LakeArrowBuilderReset(state->batch);
                if(!LakeArrowBuilderAppend(state->batch,values,nulls))
                    ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("empty native Arrow batch rejected a row")));
            }
            MemoryContextSwitchTo(caller);
            if(rows)appendStringInfoChar(&mapping,',');
            appendStringInfo(&mapping,"[%d,%lld,%lld,",rows,(long long)DatumGetInt64(raw[0]),(long long)DatumGetInt64(raw[1]));
            if(rawNull[2])appendStringInfoString(&mapping,"null]");
            else appendStringInfo(&mapping,"%lld]",(long long)DatumGetInt64(raw[2]));
            ++rows;MemoryContextReset(state->row_context);
        }
        MemoryContextSwitchTo(caller);tableam_scan_end(state->scan);state->scan=NULL;
        heap_close(state->relation,NoLock);state->relation=NULL;
        LakeArrowBuilderWrite(state->batch,state->api,state->writer);
        LakeArrowBuilderDestroy(state->batch);state->batch=NULL;
        HOLD_INTERRUPTS();int finished=state->api->writer_finish(state->writer,&written,&error);RESUME_INTERRUPTS();
        LakeFileCheckStatus(finished,&error);CHECK_FOR_INTERRUPTS();
        HOLD_INTERRUPTS();int closed=state->api->writer_close(&state->writer,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(closed,&error);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);OgLakeSdkError error;
        HOLD_INTERRUPTS();int closed=state->api->writer_close(&state->writer,&error);RESUME_INTERRUPTS();
        if(closed!=OG_LAKE_SDK_OK)elog(WARNING,"test writer cleanup failed: %.511s",error.message);
        if(state->scan!=NULL)tableam_scan_end(state->scan);
        if(state->relation!=NULL)heap_close(state->relation,NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    appendStringInfo(&mapping,"],\"rows\":%lld,\"bytes\":%lld,\"local_fsync_complete\":%u}",
        (long long)written.num_rows,(long long)written.file_bytes,written.local_fsync_complete);
    PG_RETURN_TEXT_P(cstring_to_text_with_len(mapping.data,mapping.len));
}
