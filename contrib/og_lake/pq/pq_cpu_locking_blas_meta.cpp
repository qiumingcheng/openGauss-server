#include "pq_cpu_locking_blas_meta.h"
#include <cblas.h>
#include <pthread.h>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <new>

namespace {
pthread_once_t once=PTHREAD_ONCE_INIT;
std::atomic<uint32_t> initialized{0},source_reads{0};
std::atomic<unsigned> fault{0};
int status=PQ_CPU_STATE;
char configuration[256]={0};
const char* reason="BLAS metadata not initialized";
void Initialize() noexcept
{
    initialized.fetch_add(1,std::memory_order_relaxed);
    try{
        source_reads.fetch_add(1,std::memory_order_relaxed);
        const char* value=oglpq_openblas_get_config();
#ifdef PQ_CPU_METADATA_TEST_FAULTS
        char oversized[300];
        unsigned mode=fault.load(std::memory_order_relaxed);
        if(mode==1)value=nullptr;
        if(mode==2){std::memset(oversized,'x',sizeof(oversized));oversized[sizeof(oversized)-1]=0;value=oversized;}
        if(mode==3)throw std::bad_alloc();
#endif
        if(!value){status=PQ_CPU_INVALID;reason="BLAS metadata provider returned NULL";return;}
        size_t length=0;while(length<sizeof(configuration)&&value[length])++length;
        if(length==0||length>=sizeof(configuration)){status=PQ_CPU_INVALID;reason="BLAS metadata length exceeds private immutable buffer";return;}
        std::memcpy(configuration,value,length+1);status=PQ_CPU_OK;reason="";
    }catch(const std::bad_alloc&){status=PQ_CPU_MEMORY;reason="BLAS metadata initialization allocation failure";}
    catch(...){status=PQ_CPU_STATE;reason="unexpected BLAS metadata initialization exception";}
}
}
extern "C" int pq_cpu_blas_metadata_initialize(PqCpuError* error)
{
    if(error)*error={};int code=pthread_once(&once,Initialize);int result=code?PQ_CPU_STATE:status;
    if(error&&result){error->code=result;std::snprintf(error->message,sizeof(error->message),"%s",code?"pthread_once failed":reason);}return result;
}
extern "C" const char* pq_cpu_blas_immutable_config(void)
{
    /* Never hand NULL to frozen v1's strlen. Every numeric API first calls the
     * checked parallel getter, which fails closed if initialization failed. */
    return pq_cpu_blas_metadata_initialize(nullptr)==PQ_CPU_OK?configuration:"<unavailable BLAS metadata>";
}
extern "C" int pq_cpu_blas_parallel_checked(void)
{return pq_cpu_blas_metadata_initialize(nullptr)==PQ_CPU_OK?oglpq_openblas_get_parallel():-1;}
extern "C" int pq_cpu_blas_num_threads_checked(void)
{return pq_cpu_blas_metadata_initialize(nullptr)==PQ_CPU_OK?oglpq_openblas_get_num_threads():0;}
extern "C" uint32_t pq_cpu_blas_metadata_init_calls(void){return initialized.load(std::memory_order_relaxed);}
extern "C" uint32_t pq_cpu_blas_metadata_source_reads(void){return source_reads.load(std::memory_order_relaxed);}
extern "C" int pq_cpu_blas_metadata_test_fault(unsigned mode)
{
#ifdef PQ_CPU_METADATA_TEST_FAULTS
    if(mode>3||initialized.load(std::memory_order_relaxed))return PQ_CPU_STATE;
    fault.store(mode,std::memory_order_relaxed);return PQ_CPU_OK;
#else
    (void)mode;return PQ_CPU_UNSUPPORTED;
#endif
}
