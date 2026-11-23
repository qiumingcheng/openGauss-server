/* Native C++14 host; no Arrow C++ objects or exceptions cross this boundary. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_file_sdk.h"
#include "lake_numeric.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "port.h"
#include "utils/memutils.h"
#include <dlfcn.h>
#include <new>
#include <pthread.h>

static pthread_mutex_t lake_provider_mutex=PTHREAD_MUTEX_INITIALIZER;
/* Process lifetime retention is intentional: batches may outlive readers and
 * thread-pool sessions. No dlclose while any native callback can hold an API. */
static void* lake_provider_library=NULL;
static const OgLakeParquetSdkApi* lake_provider_api=NULL;
static const OgLakeParquetLifecycleApi* lake_provider_lifecycle=NULL;
static const OgLakeParquetMaintenanceApi* lake_provider_maintenance=NULL;
static const OgLakeParquetNestedApi* lake_provider_nested=NULL;
static const OgLakeParquetDecimalApi* lake_provider_decimal=NULL;
static pthread_mutex_t lake_ext_provider_mutex=PTHREAD_MUTEX_INITIALIZER;
static void* lake_ext_provider_library=NULL;
static LakeExternalFdSdk lake_ext_provider_sdk={NULL,NULL,NULL};
static bool lake_ext_provider_loaded=false;

const OgLakeParquetSdkApi* LakeFileProvider(void)
{
    char directory[MAXPGPATH],path[MAXPGPATH];
    get_pkglib_path(my_exec_path,directory);
    int length=snprintf(path,sizeof(path),"%s/og_lake_sdk/libog_lake_sdk_arrow.so",directory);
    if(length<0||length>=(int)sizeof(path))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake provider installation path is too long")));
    char reason[OG_LAKE_SDK_ERROR_BYTES]={0};
    const OgLakeParquetSdkApi* api=NULL;
    /* No palloc, interrupt processing or database ERROR while this process-wide
     * mutex is held. Every failure leaves the mutex and cache in a valid state. */
    HOLD_INTERRUPTS();
    (void)pthread_mutex_lock(&lake_provider_mutex);
    if(lake_provider_api!=NULL)api=lake_provider_api;
    else {
        void* library=dlopen(path,RTLD_NOW|RTLD_LOCAL);
        if(library==NULL){
            const char* message=dlerror();
            (void)snprintf(reason,sizeof(reason),"%s",message?message:"dlopen failed");
        }else {
            OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(library,"og_lake_parquet_sdk_v1");
            api=entry?entry():NULL;
            const uint64 required=OG_LAKE_SDK_LOCAL_FILES|OG_LAKE_SDK_FIELD_IDS|OG_LAKE_SDK_ORIGINAL_POSITIONS|
                OG_LAKE_SDK_IO_STATS|OG_LAKE_SDK_POOL_BUDGET|OG_LAKE_SDK_POLL_CANCEL;
            if(api==NULL||api->abi_version!=OG_LAKE_PARQUET_ABI||api->struct_size<sizeof(*api)||
                (api->capabilities&required)!=required||!api->footer_open||!api->footer_info||!api->footer_close||
                !api->reader_open||!api->reader_next||!api->reader_stats||!api->reader_close||!api->batch_release){
                api=NULL;(void)snprintf(reason,sizeof(reason),"provider C ABI or required read capability is incompatible");
                (void)dlclose(library);
            }else{
                /* Lifecycle is optional: an old ABI1 provider remains usable
                 * for reads, but cannot prepare transaction-owned outputs. */
                OgLakeParquetLifecycleEntrypoint lifecycleEntry=
                    (OgLakeParquetLifecycleEntrypoint)dlsym(library,"og_lake_parquet_sdk_v2");
                const OgLakeParquetLifecycleApi* lifecycle=lifecycleEntry?lifecycleEntry():NULL;
                if(lifecycle!=NULL&&lifecycle->abi_version==OG_LAKE_PARQUET_LIFECYCLE_ABI&&
                    lifecycle->struct_size>=sizeof(*lifecycle)&&lifecycle->base==api&&
                    (lifecycle->capabilities&OG_LAKE_SDK_ABORT_FINISHED)&&lifecycle->writer_abort!=NULL)
                    lake_provider_lifecycle=lifecycle;
                OgLakeParquetMaintenanceEntrypoint maintenanceEntry=
                    (OgLakeParquetMaintenanceEntrypoint)dlsym(library,"og_lake_parquet_sdk_v3");
                const OgLakeParquetMaintenanceApi* maintenance=maintenanceEntry?maintenanceEntry():NULL;
                if(maintenance!=NULL&&maintenance->abi_version==OG_LAKE_PARQUET_MAINTENANCE_ABI&&
                    maintenance->struct_size>=sizeof(*maintenance)&&maintenance->base==api&&
                    (maintenance->capabilities&OG_LAKE_SDK_LOCAL_DURABLE_DELETE)&&maintenance->delete_file!=NULL)
                    lake_provider_maintenance=maintenance;
                OgLakeParquetNestedEntrypoint nestedEntry=
                    (OgLakeParquetNestedEntrypoint)dlsym(library,"og_lake_parquet_sdk_v4");
                const OgLakeParquetNestedApi* nested=nestedEntry?nestedEntry():NULL;
                if(nested!=NULL&&nested->abi_version==OG_LAKE_PARQUET_NESTED_ABI&&
                    nested->struct_size>=sizeof(*nested)&&nested->base==api&&
                    (nested->capabilities&OG_LAKE_SDK_WRITE_FLOAT32_LIST)&&nested->writer_open!=NULL)
                    lake_provider_nested=nested;
                OgLakeParquetDecimalEntrypoint decimalEntry=
                    (OgLakeParquetDecimalEntrypoint)dlsym(library,"og_lake_parquet_sdk_v5");
                const OgLakeParquetDecimalApi* decimal=decimalEntry?decimalEntry():NULL;
                if(decimal!=NULL&&decimal->abi_version==OG_LAKE_PARQUET_DECIMAL_ABI&&
                    decimal->struct_size>=sizeof(*decimal)&&decimal->base==api&&
                    (decimal->capabilities&OG_LAKE_SDK_DECIMAL_PARAMETERS)&&decimal->footer_decimal&&decimal->writer_open)
                    lake_provider_decimal=decimal;
                lake_provider_library=library;lake_provider_api=api;
            }
        }
    }
    (void)pthread_mutex_unlock(&lake_provider_mutex);
    RESUME_INTERRUPTS();
    CHECK_FOR_INTERRUPTS();
    if(api==NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake file provider is unavailable"),
            errdetail("%s",reason)));
    return api;
}

const OgLakeParquetLifecycleApi* LakeFileLifecycleProvider(void)
{
    const OgLakeParquetSdkApi* api=LakeFileProvider();
    if(lake_provider_lifecycle==NULL||!(api->capabilities&OG_LAKE_SDK_WRITER)||
        !(api->capabilities&OG_LAKE_SDK_FOOTER_BYTES)||
        !(api->capabilities&OG_LAKE_SDK_WRITE_ORDER)||!api->writer_open||
        !api->writer_append||!api->writer_finish||!api->writer_close)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake file preparation requires explicit finished-candidate abort support")));
    return lake_provider_lifecycle;
}

const OgLakeParquetMaintenanceApi* LakeFileMaintenanceProvider(void)
{
    (void)LakeFileProvider();
    if(lake_provider_maintenance==NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake physical cleanup requires explicit durable deletion support")));
    return lake_provider_maintenance;
}

const OgLakeParquetNestedApi* LakeFileNestedProvider(void)
{
    const OgLakeParquetSdkApi* api=LakeFileProvider();
    if(lake_provider_nested==NULL||!(api->capabilities&OG_LAKE_SDK_FLOAT32_LIST_VALUES))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake vector file preparation requires an explicit nested writer")));
    return lake_provider_nested;
}

const OgLakeParquetDecimalApi* LakeFileDecimalProvider(void)
{
    const OgLakeParquetSdkApi* api=LakeFileProvider();
    if(lake_provider_decimal==NULL||!(api->capabilities&OG_LAKE_SDK_DECIMAL128_VALUES))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake numeric requires an explicit optional SDK decimal parameter API")));
    return lake_provider_decimal;
}
void LakeFileReadDecimalParameters(const OgLakeSdkFooter* footer,int ordinal,int* precision,int* scale)
{
    const OgLakeParquetDecimalApi* api=LakeFileDecimalProvider();
    OgLakeSdkDecimalField actual={};actual.struct_size=sizeof(actual);OgLakeSdkError error;
    HOLD_INTERRUPTS();int status=api->footer_decimal(footer,ordinal,&actual,&error);RESUME_INTERRUPTS();
    LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
    if(actual.struct_size!=sizeof(actual)||actual.field_ordinal!=ordinal||!LakeDecimal128Parameters(actual.precision,actual.scale))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake decimal footer parameters are invalid")));
    if(precision)*precision=actual.precision;
    if(scale)*scale=actual.scale;
}

void LakeFileCheckDecimalWith(const OgLakeParquetDecimalApi* api,
    const OgLakeSdkFooter* footer,int ordinal,int32 typmod)
{
    int precision,scale;LakeNumericParameters(typmod,&precision,&scale);
    if(api==NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake numeric requires an explicit optional SDK decimal parameter API")));
    OgLakeSdkDecimalField actual={};actual.struct_size=sizeof(actual);OgLakeSdkError error;
    HOLD_INTERRUPTS();int status=api->footer_decimal(footer,ordinal,&actual,&error);RESUME_INTERRUPTS();
    LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
    if(actual.struct_size!=sizeof(actual)||actual.field_ordinal!=ordinal||actual.precision!=precision||actual.scale!=scale)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake decimal footer parameters differ from its original column definition")));
}

void LakeFileCheckDecimal(const OgLakeSdkFooter* footer,int ordinal,int32 typmod)
{
    LakeFileCheckDecimalWith(LakeFileDecimalProvider(),footer,ordinal,typmod);
}

const LakeExternalFdSdk* LakeFileExternalFdProvider(void)
{
    char directory[MAXPGPATH],path[MAXPGPATH];
    get_pkglib_path(my_exec_path,directory);
    int length=snprintf(path,sizeof(path),"%s/og_lake_external_sdk/libog_lake_sdk_arrow.so",directory);
    if(length<0||length>=(int)sizeof(path))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("native lake external provider installation path is too long")));
    char reason[OG_LAKE_SDK_ERROR_BYTES]={0};
    bool ok=false;
    HOLD_INTERRUPTS();
    (void)pthread_mutex_lock(&lake_ext_provider_mutex);
    if(lake_ext_provider_loaded){
        ok=true;
    }else{
        void* library=dlopen(path,RTLD_NOW|RTLD_LOCAL);
        if(library==NULL){
            const char* message=dlerror();
            (void)snprintf(reason,sizeof(reason),"%s",message?message:"dlopen failed");
        }else{
            OgLakeParquetFdEntrypoint fdEntry=(OgLakeParquetFdEntrypoint)dlsym(library,"og_lake_parquet_sdk_v6");
            const OgLakeParquetFdApi* fdApi=fdEntry?fdEntry():NULL;
            const OgLakeParquetSdkApi* base=fdApi?fdApi->base:NULL;
            const uint64 requiredBase=OG_LAKE_SDK_LOCAL_FILES|OG_LAKE_SDK_ORIGINAL_POSITIONS|
                OG_LAKE_SDK_IO_STATS|OG_LAKE_SDK_POOL_BUDGET|OG_LAKE_SDK_POLL_CANCEL;
            if(fdApi==NULL||fdApi->abi_version!=OG_LAKE_PARQUET_FD_ABI||fdApi->struct_size<sizeof(*fdApi)||
                !(fdApi->capabilities&OG_LAKE_SDK_READONLY_FD)||!fdApi->footer_open_fd||!fdApi->reader_open_fd||
                base==NULL||base->abi_version!=OG_LAKE_PARQUET_ABI||base->struct_size<sizeof(*base)||
                (base->capabilities&requiredBase)!=requiredBase||!base->footer_info||!base->footer_close||
                !base->reader_next||!base->reader_stats||!base->reader_close||!base->batch_release){
                (void)snprintf(reason,sizeof(reason),"external provider C ABI v6 or read capabilities are incompatible");
                (void)dlclose(library);
            }else{
                OgLakeParquetDecimalEntrypoint decimalEntry=
                    (OgLakeParquetDecimalEntrypoint)dlsym(library,"og_lake_parquet_sdk_v5");
                const OgLakeParquetDecimalApi* decimal=decimalEntry?decimalEntry():NULL;
                if(decimal!=NULL&&(decimal->abi_version!=OG_LAKE_PARQUET_DECIMAL_ABI||
                    decimal->struct_size<sizeof(*decimal)||decimal->base!=base||
                    !(decimal->capabilities&OG_LAKE_SDK_DECIMAL_PARAMETERS)||!decimal->footer_decimal))
                    decimal=NULL;
                lake_ext_provider_library=library;
                lake_ext_provider_sdk.base=base;
                lake_ext_provider_sdk.decimal=decimal;
                lake_ext_provider_sdk.fd=fdApi;
                lake_ext_provider_loaded=true;
                ok=true;
            }
        }
    }
    (void)pthread_mutex_unlock(&lake_ext_provider_mutex);
    RESUME_INTERRUPTS();
    CHECK_FOR_INTERRUPTS();
    if(!ok)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake external Parquet FD provider is unavailable"),
            errdetail("%s",reason)));
    return &lake_ext_provider_sdk;
}

int LakeFileCandidateSlotsAvailable(void)
{
    int used=0;
    for(LakeFileCandidate* item=*LakeHostFileCandidatesAddress();item!=NULL;item=item->next)
        if(++used>=16)return 0;
    return 16-used;
}

LakeFileCandidate* LakeFileCandidateOpen(const OgLakeSdkWriteOptions* options,
    int listCount,const OgLakeSdkListField* lists,int decimalCount,const OgLakeSdkDecimalField* decimals)
{
    if(options==NULL||options->path==NULL||options->path[0]!='/'||strlen(options->path)>=MAXPGPATH)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native lake candidate path")));
    LakeFileCandidate** head=LakeHostFileCandidatesAddress();
    const OgLakeParquetLifecycleApi* lifecycle=LakeFileLifecycleProvider();
    if(listCount<0||(listCount==0)!=(lists==NULL)||listCount>options->num_fields)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native lake nested candidate schema")));
    if(decimalCount<0||(decimalCount==0)!=(decimals==NULL)||decimalCount>options->num_fields)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native lake decimal candidate schema")));
    const OgLakeParquetDecimalApi* decimal=decimalCount?LakeFileDecimalProvider():NULL;
    const OgLakeParquetNestedApi* nested=!decimal&&listCount?LakeFileNestedProvider():NULL;
    int count=0;
    for(LakeFileCandidate* item=*head;item!=NULL;item=item->next)++count;
    /* Each finished candidate retains a directory FD and an inode anchor.
     * This bounds host handles, not complete SDK memory or final flush size. */
    if(count>=16)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("too many native lake candidates in one transaction")));
    LakeFileCandidate* candidate=(LakeFileCandidate*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),sizeof(LakeFileCandidate));
    candidate->api=lifecycle->base;candidate->lifecycle=lifecycle;
    candidate->owner_subid=GetCurrentSubTransactionId();
    candidate->written.struct_size=sizeof(candidate->written);
    errno_t rc=strcpy_s(candidate->path,sizeof(candidate->path),options->path);securec_check(rc,"","");
    /* Register before the first external side effect, including open errors
     * and a pending interrupt discovered immediately after the SDK returns. */
    candidate->next=*head;*head=candidate;
    OgLakeSdkError error;
    HOLD_INTERRUPTS();
    int status=decimal?decimal->writer_open(options,listCount,lists,decimalCount,decimals,&candidate->writer,&error):
        nested?nested->writer_open(options,listCount,lists,&candidate->writer,&error):
        candidate->api->writer_open(options,&candidate->writer,&error);
    RESUME_INTERRUPTS();
    LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
    return candidate;
}

void LakeFileCandidateFinish(LakeFileCandidate* candidate)
{
    if(candidate==NULL||candidate->writer==NULL||candidate->finished||candidate->published)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake candidate cannot be finished")));
    OgLakeSdkError error;
    HOLD_INTERRUPTS();int status=candidate->api->writer_finish(candidate->writer,&candidate->written,&error);RESUME_INTERRUPTS();
    LakeFileCheckStatus(status,&error);
    if(candidate->written.struct_size<sizeof(candidate->written)||candidate->written.local_fsync_complete!=1||
        candidate->written.num_rows<0||candidate->written.file_bytes<=0)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake candidate has no valid local durability acknowledgement")));
    candidate->finished=true;
    CHECK_FOR_INTERRUPTS();
}

void LakeFileCandidatePublish(LakeFileCandidate* candidate)
{
    if(candidate==NULL||!candidate->finished||candidate->published||candidate->writer==NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake candidate is not publishable")));
    candidate->published=true;
}

void LakeFileCandidatesSubCommit(LakeFileCandidate* head,SubTransactionId child,SubTransactionId parent)
{
    for(LakeFileCandidate* item=head;item!=NULL;item=item->next)
        if(item->owner_subid==child)item->owner_subid=parent;
}

void LakeFileCandidatesEnd(LakeFileCandidate** head,bool committed,SubTransactionId child)
{
    if(head==NULL)return;
    LakeFileCandidate** link=head;
    while(*link!=NULL){
        LakeFileCandidate* candidate=*link;
        if(child!=InvalidSubTransactionId&&candidate->owner_subid!=child){link=&candidate->next;continue;}
        *link=candidate->next;
        OgLakeSdkError error;
        HOLD_INTERRUPTS();
        int status=committed&&candidate->published?
            candidate->api->writer_close(&candidate->writer,&error):
            candidate->lifecycle->writer_abort(&candidate->writer,&error);
        RESUME_INTERRUPTS();
        /* Transaction outcome is already final. Never turn cleanup failure
         * into ERROR or touch rolled-back catalog rows. Retain path/error in
         * the server log; durable crash/orphan recovery is a separate gate. */
        if(status!=OG_LAKE_SDK_OK)
            write_stderr("WARNING: native lake candidate cleanup failed for %s: status %d, %.511s\n",
                candidate->path,status,error.message);
        pfree(candidate);
    }
}

static int LakeFileShouldCancel(void*)
{
    /* The provider only polls. ProcessInterrupts may longjmp and therefore
     * belongs after the C API returns, never inside its C++ stack. */
    return InterruptPending?1:0;
}

OgLakeSdkLimits LakeFileLimits(void)
{
    OgLakeSdkLimits limits={sizeof(OgLakeSdkLimits),64*1024*1024,16*1024*1024,LakeFileShouldCancel,NULL};
    return limits;
}

void LakeFileCheckStatus(int status,const OgLakeSdkError* error)
{
    if(status==OG_LAKE_SDK_OK)return;
    CHECK_FOR_INTERRUPTS();
    int code=status==OG_LAKE_SDK_MEMORY?ERRCODE_OUT_OF_MEMORY:
        status==OG_LAKE_SDK_CANCELLED?ERRCODE_QUERY_CANCELED:
        status==OG_LAKE_SDK_UNSUPPORTED?ERRCODE_FEATURE_NOT_SUPPORTED:ERRCODE_FDW_ERROR;
    ereport(ERROR,(errcode(code),errmsg("native lake file operation failed"),
        errdetail("Provider status %d: %.511s",status,error?error->message:"no error detail")));
}

void LakeFileHandlesClose(LakeSdkHandles* handles)
{
    if(handles==NULL||handles->api==NULL)return;
    HOLD_INTERRUPTS();
    handles->api->batch_release(&handles->batch);
    handles->api->reader_close(&handles->reader);
    handles->api->footer_close(&handles->footer);
    RESUME_INTERRUPTS();
}

static void LakeFileReset(std::shared_ptr<void> argument)
{
    /* This callback performs only synchronous close/release, with no host
     * allocations, ERROR, publication or asynchronous callback dispatch. */
    LakeFileHandlesClose((LakeSdkHandles*)argument.get());
}

LakeSdkHandles* LakeFileHandles(MemoryContext context)
{
    const OgLakeParquetSdkApi* api=LakeFileProvider();
    LakeSdkHandles* handles=(LakeSdkHandles*)MemoryContextAllocZero(context,sizeof(LakeSdkHandles));
    handles->api=api;
    MemoryContextCallback* callback=(MemoryContextCallback*)MemoryContextAlloc(context,sizeof(MemoryContextCallback));
    new(callback) MemoryContextCallback();
    callback->func=LakeFileReset;
    /* Empty-owner alias has no control block or heap allocation. The enclosing
     * memory context, not a C++ shared_ptr, owns the palloc'ed handle record. */
    callback->arg=std::shared_ptr<void>(std::shared_ptr<void>(),handles);
    MemoryContextRegisterResetCallback(context,callback);
    return handles;
}
