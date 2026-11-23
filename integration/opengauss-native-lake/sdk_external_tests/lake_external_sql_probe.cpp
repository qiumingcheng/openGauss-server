/* Private, initial-user-only SQL probe. Never installed as a product API. */
#include "postgres.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lake_external_path.h"
#include "lake_file_sdk.h"
#include "lake_parquet_fd_sdk.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"
#include "vendor/arrow20/abi.h"

#include <dlfcn.h>
#include <unistd.h>

#ifndef LAKE_PRIVATE_EXTERNAL_SDK_PATH
#error "A fixed, verified candidate SDK path is required"
#endif

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(lake_external_parquet_probe);

namespace {
const int MAX_PROBE_COLUMNS=16;
const int64 MAX_PROBE_ROWS=16384;

struct ExternalProbe {
    int fd;
    void* library;
    const OgLakeParquetSdkApi* api;
    OgLakeSdkFooter* footer;
    OgLakeSdkReader* reader;
    OgLakeSdkBatch* batch;
    MemoryContext row;
};

void Close(ExternalProbe* s)
{
    HOLD_INTERRUPTS();
    if(s->api){
        s->api->batch_release(&s->batch);
        s->api->reader_close(&s->reader);
        s->api->footer_close(&s->footer);
    }
    if(s->fd>=0){(void)close(s->fd);s->fd=-1;}
    if(s->library){(void)dlclose(s->library);s->library=NULL;}
    if(s->row){MemoryContextDelete(s->row);s->row=NULL;}
    RESUME_INTERRUPTS();
}

void Require(bool condition,int code,const char* message)
{
    if(!condition)ereport(ERROR,(errcode(code),errmsg_internal("%s",message)));
}

uint32 FieldType(Oid type)
{
    switch(type){
        case BOOLOID:return OG_LAKE_SDK_BOOL;
        case INT4OID:return OG_LAKE_SDK_INT32;
        case INT8OID:return OG_LAKE_SDK_INT64;
        case TEXTOID:return OG_LAKE_SDK_UTF8;
        default:ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported external Parquet SQL type")));
    }
    return 0;
}

Datum ReadValue(const ArrowArray* a,const ArrowSchema* s,int64 row,Oid type,bool* is_null)
{
    Require(a&&s&&s->format&&a->buffers&&a->n_buffers>=2&&a->n_children==0&&s->n_children==0&&
            !a->dictionary&&!s->dictionary&&a->length>row&&a->offset>=0&&a->offset<=PG_INT64_MAX-a->length,
            ERRCODE_DATA_CORRUPTED,"invalid external Parquet field layout");
    int64 index=a->offset+row;
    const uint8* bitmap=(const uint8*)a->buffers[0];
    *is_null=bitmap&&!((bitmap[index/8]>>(index%8))&1);
    if(*is_null)return (Datum)0;
    const void* data=a->buffers[1];
    Require(data!=NULL,ERRCODE_DATA_CORRUPTED,"external Parquet value buffer is missing");
    if(type==INT4OID&&strcmp(s->format,"i")==0)return Int32GetDatum(((const int32*)data)[index]);
    if(type==INT8OID&&strcmp(s->format,"l")==0)return Int64GetDatum(((const int64*)data)[index]);
    if(type==BOOLOID&&strcmp(s->format,"b")==0)return BoolGetDatum((((const uint8*)data)[index/8]>>(index%8))&1);
    bool large=strcmp(s->format,"U")==0;
    Require(type==TEXTOID&&a->n_buffers==3&&(large||strcmp(s->format,"u")==0),ERRCODE_DATA_CORRUPTED,
        "external Parquet Arrow type differs from the declared column");
    int64 lo=large?((const int64*)data)[index]:((const int32*)data)[index];
    int64 hi=large?((const int64*)data)[index+1]:((const int32*)data)[index+1];
    Require(lo>=0&&hi>=lo&&hi<=MaxAllocSize&&(lo==hi||a->buffers[2]),ERRCODE_DATA_CORRUPTED,
        "external Parquet text offsets are invalid");
    Require(hi-lo<=1024*1024,ERRCODE_PROGRAM_LIMIT_EXCEEDED,"external Parquet probe text exceeds one MiB");
    const char* value=hi==lo?"":(const char*)a->buffers[2]+lo;
    const int n=(int)(hi-lo);
    Require(!memchr(value,0,n),ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,"external Parquet text contains a zero byte");
    (void)pg_verify_mbstr(PG_UTF8,value,n,false);
    char* converted=pg_any_to_server(value,n,PG_UTF8);
    return PointerGetDatum(cstring_to_text_with_len(converted,converted==value?n:strlen(converted)));
}
}

extern "C" Datum lake_external_parquet_probe(PG_FUNCTION_ARGS)
{
    Require(initialuser()&&GetUserId()==GetSessionUserId(),ERRCODE_INSUFFICIENT_PRIVILEGE,
        "external Parquet probe requires the invoking initial administrator");
    Require(IS_SINGLE_NODE&&!StreamThreadAmI()&&!IS_SPQ_RUNNING,ERRCODE_FEATURE_NOT_SUPPORTED,
        "external Parquet probe requires a serial single-node plan");
    Require(!PG_ARGISNULL(0)&&!PG_ARGISNULL(1),ERRCODE_NULL_VALUE_NOT_ALLOWED,"external Parquet root and path must be present");
    ReturnSetInfo* result=(ReturnSetInfo*)fcinfo->resultinfo;
    Require(result&&IsA(result,ReturnSetInfo)&&(result->allowedModes&SFRM_Materialize)&&result->econtext,
        ERRCODE_FEATURE_NOT_SUPPORTED,"external Parquet probe requires materialize mode");
    TupleDesc expected=NULL;
    Require(get_call_result_type(fcinfo,NULL,&expected)==TYPEFUNC_COMPOSITE&&expected&&
            expected->natts>0&&expected->natts<=MAX_PROBE_COLUMNS,ERRCODE_FEATURE_NOT_SUPPORTED,
        "external Parquet probe requires a FROM clause with a bounded explicit column list");
    for(int i=0;i<expected->natts;++i){
        Form_pg_attribute a=TupleDescAttr(expected,i);
        Require(!a->attisdropped&&a->atttypmod==-1,ERRCODE_FEATURE_NOT_SUPPORTED,"unsupported external Parquet SQL descriptor");
        (void)FieldType(a->atttypid);
        for(int j=0;j<i;++j)
            Require(strcmp(NameStr(a->attname),NameStr(TupleDescAttr(expected,j)->attname))!=0,ERRCODE_INVALID_PARAMETER_VALUE,
                "duplicate external Parquet SQL column");
    }
    MemoryContext caller=CurrentMemoryContext;
    MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc desc=BlessTupleDesc(CreateTupleDescCopy(expected));
    Tuplestorestate* store=tuplestore_begin_heap(true,false,1024);
    MemoryContextSwitchTo(caller);
    ExternalProbe* state=(ExternalProbe*)palloc0(sizeof(ExternalProbe));state->fd=-1;
    PG_TRY();{
        state->row=AllocSetContextCreate(caller,"External Parquet probe row",ALLOCSET_SMALL_SIZES);
        state->fd=og_lake_open_external_parquet(text_to_cstring(PG_GETARG_TEXT_PP(0)),text_to_cstring(PG_GETARG_TEXT_PP(1)));
        Require(state->fd>=0,ERRCODE_FDW_ERROR,"cannot open an admitted external Parquet file");
        HOLD_INTERRUPTS();
        state->library=dlopen(LAKE_PRIVATE_EXTERNAL_SDK_PATH,RTLD_NOW|RTLD_LOCAL|RTLD_NODELETE);
        auto entry=state->library?(OgLakeParquetFdEntrypoint)dlsym(state->library,"og_lake_parquet_sdk_v6"):NULL;
        const OgLakeParquetFdApi* api=entry?entry():NULL;
        RESUME_INTERRUPTS();
        Require(api&&api->abi_version==OG_LAKE_PARQUET_FD_ABI&&api->struct_size>=sizeof(*api)&&
                (api->capabilities&OG_LAKE_SDK_READONLY_FD)&&api->footer_open_fd&&api->reader_open_fd&&api->base&&
                api->base->abi_version==OG_LAKE_PARQUET_ABI&&api->base->struct_size>=sizeof(*api->base),
            ERRCODE_FEATURE_NOT_SUPPORTED,"external Parquet descriptor SDK unavailable");
        state->api=api->base;
        OgLakeSdkLimits limits=LakeFileLimits();OgLakeSdkError error;
        HOLD_INTERRUPTS();int status=api->footer_open_fd(state->fd,&limits,&state->footer,&error);RESUME_INTERRUPTS();
        LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
        const OgLakeSdkFooterInfo* footer=state->api->footer_info(state->footer);
        Require(footer&&footer->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,object_version)&&
                footer->object_version&&footer->fields&&footer->num_fields>0&&footer->num_fields<=1024&&
                footer->num_rows>=0&&footer->num_rows<=MAX_PROBE_ROWS,ERRCODE_PROGRAM_LIMIT_EXCEEDED,
            "external Parquet probe footer exceeds admitted bounds");
        int64 ordinals[MAX_PROBE_COLUMNS];
        for(int c=0;c<desc->natts;++c){
            Form_pg_attribute attr=TupleDescAttr(desc,c);ordinals[c]=-1;
            for(int f=0;f<footer->num_fields;++f){
                if(!footer->fields[f].name||strcmp(footer->fields[f].name,NameStr(attr->attname))!=0)continue;
                Require(ordinals[c]<0&&footer->fields[f].type==FieldType(attr->atttypid),ERRCODE_DATATYPE_MISMATCH,
                    "ambiguous or incompatible external Parquet field");
                ordinals[c]=f;
            }
            Require(ordinals[c]>=0,ERRCODE_UNDEFINED_COLUMN,"external Parquet field not found");
        }
        const char* version=pstrdup(footer->object_version);int64 count=footer->num_rows;
        OgLakeSdkReadOptions opts={};opts.struct_size=sizeof(opts);opts.limits=limits;opts.batch_rows=128;
        opts.projection_kind=OG_LAKE_SDK_BY_ORDINAL;opts.num_field_ids=desc->natts;opts.field_ids=ordinals;opts.num_row_groups=-1;
        HOLD_INTERRUPTS();status=api->reader_open_fd(state->fd,version,&opts,&state->reader,&error);RESUME_INTERRUPTS();
        LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
        HOLD_INTERRUPTS();state->api->footer_close(&state->footer);(void)close(state->fd);state->fd=-1;RESUME_INTERRUPTS();
        int64 position=0;Size total_bytes=0;
        for(;;){
            HOLD_INTERRUPTS();status=state->api->reader_next(state->reader,&state->batch,&error);RESUME_INTERRUPTS();
            CHECK_FOR_INTERRUPTS();if(status==OG_LAKE_SDK_EOF)break;
            LakeFileCheckStatus(status,&error);
            OgLakeSdkBatch* batch=state->batch;
            Require(batch&&batch->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkBatch,object_version)&&batch->object_version&&
                    strcmp(version,batch->object_version)==0&&batch->array&&batch->schema&&batch->original_positions&&
                    batch->array->children&&batch->schema->children&&batch->array->offset==0&&batch->array->length==batch->num_rows&&
                    batch->num_rows>0&&batch->num_rows<=128&&batch->array->n_children==desc->natts&&batch->schema->n_children==desc->natts,
                ERRCODE_DATA_CORRUPTED,"external Parquet batch has invalid identity or layout");
            for(int64 r=0;r<batch->num_rows;++r){
                CHECK_FOR_INTERRUPTS();
                Require(position<count&&batch->original_positions[r]==position++,ERRCODE_DATA_CORRUPTED,"external Parquet row offset changed");
                Datum values[MAX_PROBE_COLUMNS];bool nulls[MAX_PROBE_COLUMNS];
                MemoryContextReset(state->row);MemoryContextSwitchTo(state->row);
                for(int c=0;c<desc->natts;++c){
                    Oid type=TupleDescAttr(desc,c)->atttypid;
                    values[c]=ReadValue(batch->array->children[c],batch->schema->children[c],r,type,&nulls[c]);
                    if(!nulls[c]&&type==TEXTOID)total_bytes+=VARSIZE_ANY(DatumGetPointer(values[c]));
                }
                Require(total_bytes<=16*1024*1024,ERRCODE_PROGRAM_LIMIT_EXCEEDED,"external Parquet probe result exceeds sixteen MiB");
                tuplestore_putvalues(store,desc,values,nulls);MemoryContextSwitchTo(caller);
            }
            HOLD_INTERRUPTS();state->api->batch_release(&state->batch);RESUME_INTERRUPTS();
        }
        Require(position==count,ERRCODE_DATA_CORRUPTED,"external Parquet row count changed");Close(state);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);Close(state);PG_RE_THROW();
    }PG_END_TRY();
    MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);tuplestore_donestoring(store);
    result->returnMode=SFRM_Materialize;result->setResult=store;result->setDesc=desc;MemoryContextSwitchTo(caller);
    fcinfo->isnull=true;return (Datum)0;
}