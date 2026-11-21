#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_file_pq.h"
#include "lake_pq_test.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "utils/resowner.h"
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <memory>
#include <new>

static bool LakePqHex(const char* value)
{
    if(!value||strlen(value)!=64)return false;
    for(int i=0;i<64;++i)if(!((value[i]>='0'&&value[i]<='9')||(value[i]>='a'&&value[i]<='f')))return false;
    return true;
}
static bool LakePqNumber(const char** cursor,uint32* value,char separator)
{
    const char* p=*cursor;if(*p<'1'||*p>'9')return false;uint64 n=0;
    do{n=n*10+(*p++-'0');if(n>1048576)return false;}while(*p>='0'&&*p<='9');
    if(*p!=separator)return false;*value=(uint32)n;*cursor=p+(separator?1:0);return true;
}
bool LakePqParseDescriptor(const char* text,int format,LakePqDescriptor* output)
{
    if(!text||!output)return false;LakePqDescriptor value={};
    if(format==1){if(!LakePqHex(text))return false;memcpy(value.graph_root,text,65);*output=value;return true;}
    static const char prefix[]="og_lake_ann_pq_v2|";
    if(format!=2||strncmp(text,prefix,sizeof(prefix)-1)!=0||strlen(text)>240)return false;
    const char* p=text+sizeof(prefix)-1;
    if(strlen(p)<130||p[64]!='|'||p[129]!='|')return false;
    memcpy(value.graph_root,p,64);memcpy(value.pq_root,p+65,64);p+=130;
    if(!LakePqHex(value.graph_root)||!LakePqHex(value.pq_root)||
       !LakePqNumber(&p,&value.chunks,'|')||!LakePqNumber(&p,&value.training_rows,'|')||
       !LakePqNumber(&p,&value.node_count,'\0')||value.chunks>1536||value.training_rows<256||value.training_rows>1024||
       value.node_count<256||value.training_rows>value.node_count)return false;
    *output=value;return true;
}

#ifndef USE_LAKE_PQ_CPU
bool LakePqCompiled(void){return false;}
struct LakePqLease {};
LakePqBuilt LakePqBuild(OgLakeAnnReader*,int64,const char*,const char*,const char*,const char*,uint32)
{return LakePqBuilt{};}
bool LakePqVerify(const OgLakeParquetSdkApi*,const OgLakeAnnSpec*,const LakePqDescriptor*,int64,const char*,const char*,const char*,int* status)
{if(status)*status=OG_LAKE_SDK_UNSUPPORTED;return false;}
LakePqLease* LakePqOpen(const OgLakeParquetSdkApi*,const OgLakeAnnSpec*,const LakePqDescriptor*,int64,const char*,const char*,const char*,const float*)
{return NULL;}
bool LakePqPrecomputeDistances(LakePqLease*,uint32,float*){return false;}
bool LakePqDistance(LakePqLease*,uint32,float*){return false;}
void LakePqClose(LakePqLease** lease){if(lease)*lease=NULL;}
#else
#include "pq_parquet_frontier_draft.h"

/* This record lives outside both host contexts. The two references make any
 * order of owner/context cleanup safe. Close only uses nonthrowing C APIs;
 * SDK candidate I/O remains in the existing after-lock transaction protocol. */
struct LakePqLease {
    ResourceOwner owner;
    uint32 references;
    bool remembered,closed,counted;
    PqParquetFrontier* frontier;
    PqCpuModel* model;
    PqParquetWriter* writer;
    PqParquetReader* validation;
    PqParquetQuery* direct_query;
    OgLakeSdkLimits direct_limits;
    uint32 direct_nodes;
};
static void LakePqReleaseHandles(LakePqLease* lease)
{
    if(lease->closed)return;lease->closed=true;
    pq_parquet_frontier_close(&lease->frontier);
    pq_parquet_query_close(&lease->direct_query);
    pq_parquet_reader_close(&lease->validation);
    pq_parquet_writer_close(&lease->writer);
    pq_cpu_model_close(&lease->model);
    if(lease->counted)LakeTestPqEvent(LAKE_PQ_TEST_CLOSE);
}
static void LakePqUnref(LakePqLease* lease){if(--lease->references==0){if(lease->counted)LakeTestPqEvent(LAKE_PQ_TEST_DESTROY);std::free(lease);}}
static void LakePqOwnerRelease(void* pointer)
{
    auto* lease=(LakePqLease*)pointer;if(lease->counted&&!lease->closed)LakeTestPqEvent(LAKE_PQ_TEST_OWNER);lease->remembered=false;lease->owner=NULL;
    LakePqReleaseHandles(lease);LakePqUnref(lease);
}
static void LakePqReparent(void* pointer,ResourceOwner owner){auto* lease=(LakePqLease*)pointer;lease->owner=owner;if(lease->counted)LakeTestPqEvent(LAKE_PQ_TEST_REPARENT);}
static void LakePqContextRelease(std::shared_ptr<void> pointer)
{
    auto* lease=(LakePqLease*)pointer.get();
    if(lease->counted&&!lease->closed)LakeTestPqEvent(LAKE_PQ_TEST_CONTEXT);
    if(lease->remembered){ResourceOwnerForgetLakePq(lease->owner,lease);lease->remembered=false;lease->owner=NULL;LakePqUnref(lease);}
    LakePqReleaseHandles(lease);LakePqUnref(lease);
}
static LakePqLease* LakePqReserve(void)
{
    ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    if(!owner)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("PQ requires a native resource owner")));
    ResourceOwnerEnlargeLakePq(owner); /* may ERROR, no foreign handle exists */
    auto* callback=(MemoryContextCallback*)palloc(sizeof(MemoryContextCallback));new(callback) MemoryContextCallback();
    auto* lease=(LakePqLease*)std::calloc(1,sizeof(LakePqLease));
    if(!lease)ereport(ERROR,(errcode(ERRCODE_OUT_OF_MEMORY),errmsg("could not allocate lake PQ lease")));
    lease->references=2;lease->owner=owner;lease->remembered=true;
    ResourceOwnerRememberLakePq(owner,lease,LakePqOwnerRelease,LakePqReparent); /* allocation-free */
    callback->func=LakePqContextRelease;callback->arg=std::shared_ptr<void>(std::shared_ptr<void>(),lease);
    MemoryContextRegisterResetCallback(CurrentMemoryContext,callback);
    return lease;
}
void LakePqClose(LakePqLease** pointer)
{
    if(!pointer||!*pointer)return;auto* lease=*pointer;*pointer=NULL;
    if(lease->counted&&!lease->closed)LakeTestPqEvent(LAKE_PQ_TEST_EXPLICIT);
    LakePqReleaseHandles(lease);
    if(lease->remembered){ResourceOwnerForgetLakePq(lease->owner,lease);lease->remembered=false;lease->owner=NULL;LakePqUnref(lease);}
    /* Context reference remains until its registered callback runs. */
}
bool LakePqCompiled(void){return true;}
static void LakePqStatus(int status,const OgLakeSdkError* error)
{LakeFileCheckStatus(status,error);CHECK_FOR_INTERRUPTS();}
static bool LakePqRecoverable(int status)
{return status==OG_LAKE_SDK_INVALID||status==OG_LAKE_SDK_MEMORY||status==OG_LAKE_SDK_UNSUPPORTED||status==OG_LAKE_SDK_IO||status==OG_LAKE_SDK_STATE;}
static PqParquetSpec LakePqSpec(const OgLakeAnnSpec* graph,int64 source,const char* sourceHash,const char* schema,uint32 chunks,uint32 train)
{
    PqParquetSpec value={};value.struct_size=sizeof(value);value.version=PQ_PARQUET_VERSION;
    value.dimension=graph->dimension;value.metric=graph->metric;value.chunks=chunks;value.training_rows=train;
    value.node_count=graph->node_count;value.source_file_id=source;value.source_version_hash=sourceHash;value.schema_hash=schema;
    return value;
}
static OgLakeSdkLimits LakePqLimitsFor(uint32 dim,uint32 nodes,uint32 chunks,uint64 extra=0)
{
    OgLakeSdkLimits limits=LakeFileLimits();
    uint64 est=UINT64CONST(64)*1024*1024+extra+(uint64)nodes*Max(chunks,1U)*4ULL+(uint64)dim*65536ULL;
    if(est>(uint64)limits.memory_bytes)
        limits.memory_bytes=(int64)Min(est,UINT64CONST(512)*1024*1024);
    return limits;
}
static PqParquetPaths LakePqPaths(const char* directory)
{
    PqParquetPaths paths={};paths.struct_size=sizeof(paths);
    paths.model_path=psprintf("%s/%s",directory,LAKE_PQ_MODEL_FILE);
    paths.codes_path=psprintf("%s/%s",directory,LAKE_PQ_CODES_FILE);
    paths.manifest_path=psprintf("%s/%s",directory,LAKE_PQ_MANIFEST_FILE);return paths;
}
static bool LakePqReaderBudget(const PqParquetSpec* spec,OgLakeSdkLimits* limits,int* diagnosticStatus=NULL)
{
    uint64 adapter=0,cpu=0,sdk=0;OgLakeSdkError error;
    HOLD_INTERRUPTS();int code=pq_parquet_budget(spec,limits,&adapter,&cpu,&sdk,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    if(diagnosticStatus)*diagnosticStatus=code;
    if(code!=OG_LAKE_SDK_OK){if(!LakePqRecoverable(code))LakePqStatus(code,&error);return false;}
    /* The component reserves a smaller SDK pool per handle than the outer
     * query allowance. The footer cap must fit that same pool, including the
     * independent reopen used for READY verification and query restoration. */
    limits->footer_bytes=Min(limits->footer_bytes,(int64)sdk);
    return true;
}
static LakeFileCandidate* LakePqCandidate(const char* path,uint32 file,const OgLakeSdkLimits& limits)
{
    OgLakeSdkWriteOptions options={};options.struct_size=sizeof(options);options.path=path;
    options.fields=pq_parquet_fields(file,&options.num_fields);options.limits=limits;
    options.compression=OG_LAKE_SDK_ZSTD;options.row_group_rows=1024;return LakeFileCandidateOpen(&options);
}
LakePqBuilt LakePqBuild(OgLakeAnnReader* graph,int64 source,const char* sourceHash,
    const char* schema,const char* graphRoot,const char* directory,uint32 chunks)
{
    LakePqBuilt result={};const OgLakeAnnSpec* shape=og_lake_ann_reader_spec(graph);
    /* A real256-centroid model requires >=256 graph nodes. Raw topology is
     * retained and explicitly published as format1 for this fallback. */
    if(shape->node_count<256){ereport(NOTICE,(errmsg("lake PQ uses raw graph: fewer than256 training nodes")));return result;}
    if(!chunks||chunks>shape->dimension||shape->dimension%chunks)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("pq_chunks must divide the vector dimension")));
    uint32 train=Min(shape->node_count,(uint32)1024);
    /* Explicit host scratch is removed from the component allowance. The raw
     * graph reader has its existing separately bounded SDK/page budget. */
    uint64 scratch=(uint64)Max(train,(uint32)1024)*shape->dimension*sizeof(float);
    OgLakeSdkLimits limits=LakePqLimitsFor(shape->dimension,shape->node_count,chunks,scratch);
    if(scratch>=(uint64)limits.memory_bytes)return result;limits.memory_bytes-=scratch;
    PqParquetSpec spec=LakePqSpec(shape,source,sourceHash,schema,chunks,train);char context[65];OgLakeSdkError error;
    HOLD_INTERRUPTS();int code=pq_parquet_graph_context(&spec,shape->context_hash,graphRoot,context,&error);RESUME_INTERRUPTS();LakePqStatus(code,&error);
    spec.context_hash=context;uint64 adapter=0,cpu=0,sdk=0;
    HOLD_INTERRUPTS();code=pq_parquet_budget(&spec,&limits,&adapter,&cpu,&sdk,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    if(code==OG_LAKE_SDK_MEMORY){ereport(NOTICE,(errmsg("lake PQ uses raw graph: bounded model/code working set exceeds budget")));return result;}
    LakePqStatus(code,&error);
    limits.footer_bytes=Min(limits.footer_bytes,(int64)sdk);
    MemoryContext parent=CurrentMemoryContext;MemoryContext work=AllocSetContextCreate(parent,"Lake PQ build lease",ALLOCSET_DEFAULT_SIZES);
    LakePqLease* volatile lease=NULL;
    PG_TRY();{
        MemoryContextSwitchTo(work);lease=LakePqReserve();
        float* values=(float*)palloc(scratch);
        for(uint32 i=0;i<train;++i){uint32 id=(uint64)i*shape->node_count/train;const OgLakeAnnNode* node=NULL;
            HOLD_INTERRUPTS();code=og_lake_ann_reader_node(graph,id,&node,&error);RESUME_INTERRUPTS();LakePqStatus(code,&error);CHECK_FOR_INTERRUPTS();
            memcpy(values+(uint64)i*shape->dimension,node->vector,shape->dimension*sizeof(float));}
        PqCpuTrainSpec ts={sizeof(ts),PQ_CPU_VERSION,shape->dimension,chunks,shape->metric,train,context};
        PqCpuLimits cl={sizeof(cl),cpu,limits.should_cancel,limits.cancel_context};PqCpuError ce;
        HOLD_INTERRUPTS();int trained=pq_cpu_train(&ts,&cl,values,(uint64)train*shape->dimension,&lease->model,&ce);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
        if(trained==PQ_CPU_CANCELLED)ereport(ERROR,(errcode(ERRCODE_QUERY_CANCELED),errmsg("lake PQ training cancelled")));
        if(trained!=PQ_CPU_OK){
            if(trained!=PQ_CPU_INVALID&&trained!=PQ_CPU_MEMORY&&trained!=PQ_CPU_UNSUPPORTED)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake PQ training failed: %.255s",ce.message)));
            ereport(NOTICE,(errmsg("lake PQ uses raw graph: %.255s",ce.message)));
        }else{
            PqParquetPaths paths=LakePqPaths(directory);OgLakeSdkLimits sdkLimits=limits;sdkLimits.memory_bytes=sdk;
            /* Candidate records are session-owned; rollback owns all files
             * even if any following host allocation raises ERROR. */
            MemoryContextSwitchTo(parent);
            result.files[0]=LakePqCandidate(paths.model_path,PQ_PARQUET_MODEL,sdkLimits);
            result.files[1]=LakePqCandidate(paths.codes_path,PQ_PARQUET_CODES,sdkLimits);
            result.files[2]=LakePqCandidate(paths.manifest_path,PQ_PARQUET_MANIFEST,sdkLimits);
            MemoryContextSwitchTo(work);
            HOLD_INTERRUPTS();code=pq_parquet_writer_open(LakeFileProvider(),&spec,&limits,lease->model,
                result.files[0]->writer,result.files[1]->writer,result.files[2]->writer,&lease->writer,&error);RESUME_INTERRUPTS();LakePqStatus(code,&error);
            for(uint32 first=0;first<shape->node_count;){uint32 count=Min((uint32)1024,shape->node_count-first);
                for(uint32 i=0;i<count;++i){const OgLakeAnnNode* node=NULL;
                    HOLD_INTERRUPTS();code=og_lake_ann_reader_node(graph,first+i,&node,&error);RESUME_INTERRUPTS();LakePqStatus(code,&error);CHECK_FOR_INTERRUPTS();
                    memcpy(values+(uint64)i*shape->dimension,node->vector,shape->dimension*sizeof(float));}
                HOLD_INTERRUPTS();code=pq_parquet_writer_append(lease->writer,&limits,first,count,values,(uint64)count*shape->dimension,&error);RESUME_INTERRUPTS();LakePqStatus(code,&error);first+=count;}
            PqParquetWritten written={};written.struct_size=sizeof(written);
            HOLD_INTERRUPTS();code=pq_parquet_writer_finish(lease->writer,&limits,&written,&error);RESUME_INTERRUPTS();LakePqStatus(code,&error);
            HOLD_INTERRUPTS();pq_parquet_writer_close(&lease->writer);pq_cpu_model_close(&lease->model);RESUME_INTERRUPTS();
            MemoryContextSwitchTo(parent);
            for(int i=0;i<3;++i)LakeFileCandidateFinish(result.files[i]);
            if(result.files[0]->written.num_rows!=256||result.files[1]->written.num_rows!=shape->node_count||result.files[2]->written.num_rows!=20)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("PQ sidecar durable row count differs from dense graph coverage")));
            MemoryContextSwitchTo(work);
            HOLD_INTERRUPTS();code=pq_parquet_reader_open(LakeFileProvider(),&spec,&limits,&paths,written.manifest_hash,&lease->validation,&error);RESUME_INTERRUPTS();LakePqStatus(code,&error);
            LakePqClose((LakePqLease**)&lease);MemoryContextSwitchTo(parent);
            result.descriptor=psprintf("og_lake_ann_pq_v2|%s|%s|%u|%u|%u",graphRoot,written.manifest_hash,chunks,train,shape->node_count);
            result.enabled=true;
        }
        LakePqClose((LakePqLease**)&lease);MemoryContextSwitchTo(parent);MemoryContextDelete(work);
    }PG_CATCH();{LakePqClose((LakePqLease**)&lease);MemoryContextSwitchTo(parent);MemoryContextDelete(work);PG_RE_THROW();}PG_END_TRY();
    return result;
}
bool LakePqVerify(const OgLakeParquetSdkApi* api,const OgLakeAnnSpec* graph,const LakePqDescriptor* descriptor,
    int64 source,const char* sourceHash,const char* schema,const char* directory,int* diagnosticStatus)
{
    if(diagnosticStatus)*diagnosticStatus=OG_LAKE_SDK_INVALID;
    if(!descriptor||descriptor->node_count!=graph->node_count||!descriptor->chunks||graph->dimension%descriptor->chunks)return false;
    PqParquetSpec spec=LakePqSpec(graph,source,sourceHash,schema,descriptor->chunks,descriptor->training_rows);
    char context[65];OgLakeSdkError error;
    HOLD_INTERRUPTS();int code=pq_parquet_graph_context(&spec,graph->context_hash,descriptor->graph_root,context,&error);RESUME_INTERRUPTS();LakePqStatus(code,&error);
    spec.context_hash=context;PqParquetPaths paths=LakePqPaths(directory);
    OgLakeSdkLimits limits=LakePqLimitsFor(graph->dimension,descriptor->node_count,descriptor->chunks);
    if(!LakePqReaderBudget(&spec,&limits,diagnosticStatus))return false;
    LakePqLease* lease=LakePqReserve();
    HOLD_INTERRUPTS();code=pq_parquet_reader_open(api,&spec,&limits,&paths,descriptor->pq_root,&lease->validation,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    if(diagnosticStatus)*diagnosticStatus=code;
    LakePqClose(&lease);if(code!=OG_LAKE_SDK_OK&&!LakePqRecoverable(code))LakePqStatus(code,&error);
    return code==OG_LAKE_SDK_OK;
}
LakePqLease* LakePqOpen(const OgLakeParquetSdkApi* api,const OgLakeAnnSpec* graph,const LakePqDescriptor* descriptor,
    int64 source,const char* sourceHash,const char* schema,const char* directory,const float* query)
{
    if(!descriptor||!descriptor->pq_root[0])return NULL;
    if(descriptor->node_count!=graph->node_count||!descriptor->chunks||graph->dimension%descriptor->chunks)return NULL;
    PqParquetSpec spec=LakePqSpec(graph,source,sourceHash,schema,descriptor->chunks,descriptor->training_rows);
    char context[65];OgLakeSdkError error;
    HOLD_INTERRUPTS();int code=pq_parquet_graph_context(&spec,graph->context_hash,descriptor->graph_root,context,&error);RESUME_INTERRUPTS();LakePqStatus(code,&error);
    spec.context_hash=context;PqParquetPaths paths=LakePqPaths(directory);
    OgLakeSdkLimits limits=LakePqLimitsFor(graph->dimension,descriptor->node_count,descriptor->chunks);
    if(!LakePqReaderBudget(&spec,&limits))return NULL;
    int64 gate=0;int mode=LakeTestPqTakeMode(source,&gate);
    MemoryContext caller=CurrentMemoryContext;ResourceOwner savedOwner=t_thrd.utils_cxt.CurrentResourceOwner;
    ResourceOwner volatile privateOwner=NULL;ResourceOwner volatile concatSource=NULL;
    bool volatile inside=false;LakePqLease* volatile lease=NULL;
    PG_TRY();{
        /* Diagnostic modes alone create a scope. Production open has exactly
         * its original owner/context lifetime and no transaction changes. */
        if(mode==3){BeginInternalSubTransaction(NULL);inside=true;MemoryContextSwitchTo(caller);}
        if(mode==5||(mode>=7&&mode<=9)){
            privateOwner=ResourceOwnerCreate(savedOwner,"Lake PQ diagnostic child",caller);
            t_thrd.utils_cxt.CurrentResourceOwner=privateOwner;
            if(mode>=7){
                concatSource=ResourceOwnerCreate(privateOwner,"Lake PQ Concat source",caller);
                t_thrd.utils_cxt.CurrentResourceOwner=concatSource;
                /* Mode8 exercises Concat's descendant walk, not subcommit. */
                if(mode==8)t_thrd.utils_cxt.CurrentResourceOwner=
                    ResourceOwnerCreate(concatSource,"Lake PQ Concat descendant",caller);
            }
        }
        lease=LakePqReserve();
        lease->direct_limits=limits;lease->direct_nodes=(uint32)spec.node_count;
        if(mode==0&&!LakeTestPqDiagnosticArmed()){
            /* Fast production path: authenticate reader + LUT directly so callers
             * can compute all node distances in one batched C call without frontier per-node wrappers. */
            HOLD_INTERRUPTS();code=pq_parquet_reader_open(api,&spec,&limits,&paths,descriptor->pq_root,&lease->validation,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
            if(code==OG_LAKE_SDK_OK){
                HOLD_INTERRUPTS();code=pq_parquet_query_open(lease->validation,&limits,query,graph->dimension,&lease->direct_query,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
            }
        }else{
            HOLD_INTERRUPTS();code=pq_parquet_frontier_open(api,&spec,&limits,&paths,descriptor->pq_root,
                graph->context_hash,descriptor->graph_root,query,graph->dimension,&lease->frontier,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
        }
        if(code==OG_LAKE_SDK_OK){lease->counted=true;LakeTestPqEvent(LAKE_PQ_TEST_OPEN);}
        if(mode==3){ReleaseCurrentSubTransaction();inside=false;MemoryContextSwitchTo(caller);
            t_thrd.utils_cxt.CurrentResourceOwner=savedOwner;if(code==OG_LAKE_SDK_OK)LakeTestPqEvent(LAKE_PQ_TEST_SUBCOMMIT);}
        else t_thrd.utils_cxt.CurrentResourceOwner=savedOwner;
        if(mode>=7&&mode<=9){
            /* A real successfully opened PQ frontier is tracked by this
             * source owner. Preserve the implementation's normal transfer,
             * reparent callback and source-delete assertions. Never fake an
             * ownership transfer or release the caller's unrelated resources. */
            ResourceOwnerConcat(privateOwner,concatSource);
            ResourceOwnerDelete(concatSource);concatSource=NULL;
            if(code==OG_LAKE_SDK_OK && lease &&
               (!lease->remembered||lease->owner!=privateOwner||lease->closed))
                ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("PQ diagnostic Concat lost its live transferred lease")));
        }
        if(mode==5){
            ResourceOwnerRelease(privateOwner,RESOURCE_RELEASE_BEFORE_LOCKS,false,false);
            ResourceOwnerRelease(privateOwner,RESOURCE_RELEASE_LOCKS,false,false);
            ResourceOwnerRelease(privateOwner,RESOURCE_RELEASE_AFTER_LOCKS,false,false);
            ResourceOwnerDelete(privateOwner);privateOwner=NULL;
            LakePqLease* old=lease;LakePqClose(&old);lease=NULL;
        }
        if(code!=OG_LAKE_SDK_OK){LakePqLease* old=lease;LakePqClose(&old);lease=NULL;if(!LakePqRecoverable(code))LakePqStatus(code,&error);
            ereport(DEBUG1,(errmsg("lake PQ unavailable; using verified raw graph"),errdetail("status=%d %.511s",code,error.message)));}
        if(code==OG_LAKE_SDK_OK&&lease){
            if(mode==1||((mode==3||mode==7||mode==8)&&gate>0))LakeTestPqPause(gate);
            if(mode==2)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("controlled PQ diagnostic error with a live frontier")));
            if(mode==9)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("controlled PQ error after real ResourceOwnerConcat and source deletion")));
            if(mode==6)LakeTestPqEvent(LAKE_PQ_TEST_ARM_DISTANCE);
        }
        if(mode>=7&&mode<=9){
            /* Complete success restores the real caller ownership. Gate/ERROR
             * exits above instead release the private target in PG_CATCH,
             * proving owner-first cleanup after source deletion. */
            ResourceOwnerConcat(savedOwner,privateOwner);
            ResourceOwnerDelete(privateOwner);privateOwner=NULL;
            if(code==OG_LAKE_SDK_OK && lease &&
               (!lease->remembered||lease->owner!=savedOwner||lease->closed))
                ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("PQ diagnostic Concat failed to restore caller ownership")));
        }
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);ErrorData* failure=CopyErrorData();FlushErrorState();
        if(inside)RollbackAndReleaseCurrentSubTransaction();
        t_thrd.utils_cxt.CurrentResourceOwner=savedOwner;
        if(concatSource){ResourceOwnerRelease(concatSource,RESOURCE_RELEASE_BEFORE_LOCKS,false,false);
            ResourceOwnerRelease(concatSource,RESOURCE_RELEASE_LOCKS,false,false);ResourceOwnerRelease(concatSource,RESOURCE_RELEASE_AFTER_LOCKS,false,false);ResourceOwnerDelete(concatSource);}
        if(privateOwner){ResourceOwnerRelease(privateOwner,RESOURCE_RELEASE_BEFORE_LOCKS,false,false);
            ResourceOwnerRelease(privateOwner,RESOURCE_RELEASE_LOCKS,false,false);ResourceOwnerRelease(privateOwner,RESOURCE_RELEASE_AFTER_LOCKS,false,false);ResourceOwnerDelete(privateOwner);}
        ReThrowError(failure);
    }PG_END_TRY();
    return lease;
}
bool LakePqPrecomputeDistances(LakePqLease* lease,uint32 nodeCount,float* output)
{
    if(!lease||lease->closed||!lease->validation||!lease->direct_query||!output||
       nodeCount==0||nodeCount!=lease->direct_nodes||LakeTestPqDiagnosticArmed())return false;
    OgLakeSdkError error;OgLakeSdkLimits limits=lease->direct_limits;
    HOLD_INTERRUPTS();int code=pq_parquet_distances(lease->validation,lease->direct_query,&limits,0,nodeCount,output,nodeCount,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    if(code==OG_LAKE_SDK_OK)return true;
    if(!LakePqRecoverable(code))LakePqStatus(code,&error);LakePqReleaseHandles(lease);return false;
}
bool LakePqDistance(LakePqLease* lease,uint32 node,float* distance)
{
    if(!lease||lease->closed||(!lease->frontier&&(!lease->validation||!lease->direct_query)))return false;
    OgLakeSdkError error;OgLakeSdkLimits limits=lease->direct_limits;
    if(limits.struct_size!=sizeof(limits))limits=LakeFileLimits();
    if(LakeTestPqFailDistance()){LakeTestPqEvent(LAKE_PQ_TEST_RESTART);LakePqReleaseHandles(lease);return false;}
    HOLD_INTERRUPTS();
    int code=lease->frontier?
        pq_parquet_frontier_distance(lease->frontier,&limits,node,distance,&error):
        pq_parquet_distances(lease->validation,lease->direct_query,&limits,node,1,distance,1,&error);
    RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    if(code==OG_LAKE_SDK_OK){LakeTestPqEvent(LAKE_PQ_TEST_DISTANCE);return true;}
    if(!LakePqRecoverable(code))LakePqStatus(code,&error);LakePqReleaseHandles(lease);return false;
}
#endif
