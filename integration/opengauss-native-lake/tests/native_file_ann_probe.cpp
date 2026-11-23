/* Test-only administrator entrypoints. Imports actual gaussdb format/search
 * symbols; never part of extension installation or business DML paths. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_file_ann.h"
#include "lake_file_sdk.h"
#include "access/datavec/vector.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "ann_probe_config.h"
#include <dlfcn.h>
#include <cstring>
#include <initializer_list>
#include <sys/stat.h>

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(lake_test_ann_graph_make);
PG_FUNCTION_INFO_V1(lake_test_ann_graph_search);

static const char* context="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
static void CheckDirectory(const char* directory)
{
    if(!superuser())ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("file ANN diagnostic requires administrator")));
    size_t length=strlen(OG_LAKE_TEST_ROOT);
    if(strncmp(directory,OG_LAKE_TEST_ROOT,length)!=0||directory[length]!='/'||strstr(directory,"/../")||strstr(directory,"/./"))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("diagnostic path is outside the owned test root")));
}
static const OgLakeParquetLifecycleApi* Provider(void)
{
    /* Retain the real isolated provider for the process lifetime, as does the
     * product loader. No library swap, wrapper or fake data implementation. */
    static void* handle=NULL;static const OgLakeParquetLifecycleApi* lifecycle=NULL;
    if(lifecycle)return lifecycle;
    handle=dlopen(OG_LAKE_TEST_PROVIDER,RTLD_NOW|RTLD_LOCAL);
    if(!handle)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cannot open test provider")));
    auto entry=(OgLakeParquetLifecycleEntrypoint)dlsym(handle,"og_lake_parquet_sdk_v2");
    lifecycle=entry?entry():NULL;
    if(!lifecycle||lifecycle->abi_version!=OG_LAKE_PARQUET_LIFECYCLE_ABI||
        !(lifecycle->capabilities&OG_LAKE_SDK_ABORT_FINISHED)||!lifecycle->writer_abort||
        !lifecycle->base||!(lifecycle->base->capabilities&OG_LAKE_SDK_BINARY_VALUES))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("test provider lacks required capabilities")));
    return lifecycle;
}
static OgLakeAnnSpec Spec(int metric,int count,bool bridge)
{
    if(metric<1||metric>3||count<0||count>40000||(bridge&&count<3))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid test graph specification")));
    OgLakeAnnSpec spec={sizeof(spec),OG_LAKE_ANN_FORMAT_VERSION,3,(uint32)metric,(uint32)count,count?1u:0u,count-(bridge?1:0),context};
    return spec;
}
static void CloseWriter(const OgLakeParquetSdkApi* api,OgLakeSdkWriter** writer,bool abort)
{
    OgLakeSdkError error;HOLD_INTERRUPTS();
    if(abort)(void)Provider()->writer_abort(writer,&error);else(void)api->writer_close(writer,&error);
    RESUME_INTERRUPTS();
}

extern "C" Datum lake_test_ann_graph_make(PG_FUNCTION_ARGS)
{
    char* directory=text_to_cstring(PG_GETARG_TEXT_PP(0));CheckDirectory(directory);
    int metric=PG_GETARG_INT32(1),count=PG_GETARG_INT32(2);bool bridge=PG_GETARG_BOOL(3);auto spec=Spec(metric,count,bridge);
    if(mkdir(directory,0700)!=0)ereport(ERROR,(errcode_for_file_access(),errmsg("could not create owned ANN diagnostic directory: %m")));
    const auto* api=Provider()->base;auto limits=LakeFileLimits();OgLakeSdkWriter* volatile nodes=NULL;OgLakeSdkWriter* volatile lookup=NULL;
    OgLakeAnnWriter* volatile graph=NULL;char* manifest=NULL;
    PG_TRY();{
        for(int which=0;which<2;++which){char* path=psprintf("%s/%s",directory,which?OG_LAKE_ANN_LOOKUP_FILE:OG_LAKE_ANN_NODES_FILE);
            int32 fields=0;auto definitions=og_lake_ann_fields(which,&fields);
            OgLakeSdkWriteOptions options={sizeof(options),path,limits,fields,definitions,OG_LAKE_SDK_ZSTD,OG_LAKE_ANN_PAGE_ROWS};OgLakeSdkError error;
            HOLD_INTERRUPTS();int status=api->writer_open(&options,which?(OgLakeSdkWriter**)&lookup:(OgLakeSdkWriter**)&nodes,&error);RESUME_INTERRUPTS();
            LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();}
        OgLakeSdkError error;HOLD_INTERRUPTS();int status=og_lake_ann_writer_open(api,&limits,&spec,nodes,lookup,(OgLakeAnnWriter**)&graph,&error);RESUME_INTERRUPTS();
        LakeFileCheckStatus(status,&error);
        for(int i=0;i<count;++i){CHECK_FOR_INTERRUPTS();Vector* original=InitVector(3);
            original->x[0]=i+.25f;original->x[1]=(i*i%19)+.5f;original->x[2]=(i%13)+.125f;
            Vector* encoded=metric==3?DatumGetVector(DirectFunctionCall1(l2_normalize,PointerGetDatum(original))):original;
            uint32 next=(i+1)%count;OgLakeAnnAddress address={0,i,100000+i,17};
            OgLakeAnnNode row={(uint32)i,i==0?1u:0u,(bridge&&i==1)?1u:0u,1,(bridge&&i==1)?0u:1u,encoded->x,&next,&address};
            HOLD_INTERRUPTS();status=og_lake_ann_writer_append(graph,&row,1,&error);RESUME_INTERRUPTS();
            LakeFileCheckStatus(status,&error);if(encoded!=original)pfree(encoded);pfree(original);}
        OgLakeAnnWritten written={};written.struct_size=sizeof(written);
        HOLD_INTERRUPTS();status=og_lake_ann_writer_finish(graph,&written,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(status,&error);
        manifest=pstrdup(written.manifest_hash);HOLD_INTERRUPTS();og_lake_ann_writer_close((OgLakeAnnWriter**)&graph);RESUME_INTERRUPTS();
        for(auto writer:{nodes,lookup}){OgLakeSdkWrittenFile file={};file.struct_size=sizeof(file);
            HOLD_INTERRUPTS();status=api->writer_finish(writer,&file,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(status,&error);}
        CloseWriter(api,(OgLakeSdkWriter**)&nodes,false);CloseWriter(api,(OgLakeSdkWriter**)&lookup,false);
    }PG_CATCH();{
        HOLD_INTERRUPTS();og_lake_ann_writer_close((OgLakeAnnWriter**)&graph);RESUME_INTERRUPTS();
        CloseWriter(api,(OgLakeSdkWriter**)&nodes,true);CloseWriter(api,(OgLakeSdkWriter**)&lookup,true);PG_RE_THROW();}PG_END_TRY();
    PG_RETURN_TEXT_P(cstring_to_text(manifest));
}
static int Cancel(void* value){return *(int*)value;}
extern "C" Datum lake_test_ann_graph_search(PG_FUNCTION_ARGS)
{
    char* directory=text_to_cstring(PG_GETARG_TEXT_PP(0));CheckDirectory(directory);char* manifest=text_to_cstring(PG_GETARG_TEXT_PP(1));
    int metric=PG_GETARG_INT32(2),count=PG_GETARG_INT32(3);bool bridge=PG_GETARG_BOOL(4);auto spec=Spec(metric,count,bridge);
    int beam=PG_GETARG_INT32(6),cancel=0;auto limits=LakeFileLimits();
    if(PG_GETARG_BOOL(7)){limits.should_cancel=Cancel;limits.cancel_context=&cancel;}
    OgLakeAnnReader* volatile reader=NULL;StringInfoData output;initStringInfo(&output);
    const auto* api=Provider()->base;
    PG_TRY();{
        auto nodes=psprintf("%s/%s",directory,OG_LAKE_ANN_NODES_FILE),lookup=psprintf("%s/%s",directory,OG_LAKE_ANN_LOOKUP_FILE);
        OgLakeSdkError error;HOLD_INTERRUPTS();int status=og_lake_ann_reader_open(api,&limits,&spec,manifest,nodes,lookup,(OgLakeAnnReader**)&reader,&error);RESUME_INTERRUPTS();
        LakeFileCheckStatus(status,&error);cancel=PG_GETARG_BOOL(7)?1:0;
        LakeAnnFileMask mask={};bool filtered=PG_NARGS()==9;
        if(filtered){int first=PG_GETARG_INT32(8);
            if(first<0||first>count)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid test scalar mask")));
            for(int i=first;i<count;++i){mask.positions=bms_add_member(mask.positions,i);++mask.count;}}
        auto result=LakeSearchAnnFile(reader,PG_GETARG_DATUM(5),(uint32)beam,filtered?&mask:NULL);
        if(result->budget_exhausted)ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("test graph exhausted its filtered navigation budget")));
        appendStringInfoChar(&output,'[');
        for(uint32 i=0;i<result->count;++i){const auto& row=result->rows[i];if(i)appendStringInfoChar(&output,',');
            char* distance=DatumGetCString(DirectFunctionCall1(float8out,Float8GetDatum(row.internal_distance)));
            appendStringInfo(&output,"[%u,%lld,%lld,%lld,%lld,\"%s\"]",row.node_id,(long long)row.address.file_id,
                (long long)row.address.row_position,(long long)row.address.row_id,(long long)row.address.birth_snapshot,distance);}
        appendStringInfoChar(&output,']');HOLD_INTERRUPTS();og_lake_ann_reader_close((OgLakeAnnReader**)&reader);RESUME_INTERRUPTS();
    }PG_CATCH();{HOLD_INTERRUPTS();og_lake_ann_reader_close((OgLakeAnnReader**)&reader);RESUME_INTERRUPTS();PG_RE_THROW();}PG_END_TRY();
    PG_RETURN_TEXT_P(cstring_to_text_with_len(output.data,output.len));
}
