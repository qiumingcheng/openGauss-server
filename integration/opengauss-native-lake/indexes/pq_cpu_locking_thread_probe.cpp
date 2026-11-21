/* Independent concurrent consumer. Barriers synchronize starts only; no lock
 * surrounds PQ/BLAS calls. Fault bookkeeping and cancellation are thread-local. */
#include "pq_cpu.h"
#include "pq_cpu_locking_blas_meta.h"
#include <cblas.h>
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
namespace {
struct Allocation {void* p;size_t bytes;unsigned kind;};
struct Tracking {bool enabled=false;size_t calls=0,at=0,failures=0,live=0,live_bytes=0,kinds[2]{},live_kinds[2]{};unsigned failed_kind=0;Allocation owned[4096]{};};
thread_local Tracking memory;
thread_local unsigned rank=999;
void* Allocate(size_t n,unsigned kind)
{if(memory.enabled){++memory.calls;++memory.kinds[kind];if(memory.at&&memory.calls==memory.at){++memory.failures;memory.failed_kind=kind;throw std::bad_alloc();}}
    void* p=std::malloc(n?n:1);if(!p)throw std::bad_alloc();if(memory.enabled){for(auto& a:memory.owned)if(!a.p){a={p,n,kind};++memory.live;memory.live_bytes+=n;++memory.live_kinds[kind];return p;}std::abort();}return p;}
void Release(void* p)
{if(!p)return;for(auto& a:memory.owned)if(a.p==p){--memory.live;memory.live_bytes-=a.bytes;--memory.live_kinds[a.kind];a={nullptr,0,0};break;}std::free(p);}
struct Barrier {std::atomic<unsigned> arrived{0},generation{0};unsigned count;explicit Barrier(unsigned n):count(n){}
    void Wait(){unsigned g=generation.load(std::memory_order_acquire);if(arrived.fetch_add(1,std::memory_order_acq_rel)+1==count){arrived.store(0,std::memory_order_relaxed);generation.fetch_add(1,std::memory_order_release);}else while(generation.load(std::memory_order_acquire)==g)std::this_thread::yield();}};
void Check(bool ok,const char* text){if(!ok)throw std::runtime_error(text);}
struct Meter {std::atomic<unsigned> active{0},peak{0};};
struct PollState {unsigned owner;uint64_t polls=0,after=0,wrong=0;Meter* meter=nullptr;bool entered=false;};
void Leave(PollState& s){if(s.entered){s.meter->active.fetch_sub(1,std::memory_order_acq_rel);s.entered=false;}}
int Poll(void* p){auto* s=(PollState*)p;++s->polls;
    if(s->meter&&!s->entered){s->entered=true;unsigned n=s->meter->active.fetch_add(1,std::memory_order_acq_rel)+1,old=s->meter->peak.load(std::memory_order_relaxed);while(n>old&&!s->meter->peak.compare_exchange_weak(old,n,std::memory_order_relaxed)){};}
    if(s->owner!=rank){++s->wrong;return 1;}return s->after&&s->polls>=s->after;}
PqCpuLimits Limits(PollState* p){return {sizeof(PqCpuLimits),64*1024*1024,Poll,p};}
struct Handles {PqCpuModel* model=nullptr;PqCpuCodes* codes=nullptr;PqCpuQuery* query=nullptr;~Handles(){Close();}
    void Close(){pq_cpu_query_close(&query);pq_cpu_codes_close(&codes);pq_cpu_model_close(&model);}};
void Context(char* output,unsigned owner){std::memset(output,'a',64);const char hex[]="0123456789abcdef";output[62]=hex[(owner>>4)&15];output[63]=hex[owner&15];output[64]=0;}
std::vector<float> Data(unsigned d,unsigned rows=257)
{std::vector<float> out((size_t)d*rows);for(unsigned i=0;i<rows;++i)for(unsigned j=0;j<d;++j)out[(size_t)i*d+j]=(float)((int)(i%256)-128)/256+(float)(j%8)/16;return out;}
PqCpuInfo Info(PqCpuModel* p){PqCpuInfo info{};info.struct_size=sizeof(info);PqCpuError e{};Check(pq_cpu_model_info(p,&info,&e)==0,"model info");return info;}
void Train(Handles& h,unsigned d,unsigned m,PollState& poll,unsigned contextId)
{char context[65];Context(context,contextId);auto data=Data(d,256);PqCpuTrainSpec s{sizeof(s),1,d,m,1,256,context};auto limits=Limits(&poll);PqCpuError e{};
    int code=pq_cpu_train(&s,&limits,data.data(),data.size(),&h.model,&e);Leave(poll);Check(code==0,"prepare real model");}
void Encode(Handles& h,PollState& poll)
{auto i=Info(h.model);auto data=Data(i.dimension);PqCpuEncodeSpec s{sizeof(s),1,71,257,i.context_hash,i.model_hash};auto limits=Limits(&poll);PqCpuError e{};
    int code=pq_cpu_encode(h.model,&s,&limits,data.data(),data.size(),&h.codes,&e);Leave(poll);Check(code==0,"prepare real codes");}
void Query(Handles& h,PollState& poll)
{auto i=Info(h.model);auto data=Data(i.dimension);auto limits=Limits(&poll);PqCpuError e{};
    int code=pq_cpu_query_open(h.model,&limits,data.data()+13*i.dimension,i.dimension,&h.query,&e);Leave(poll);Check(code==0,"prepare real LUT");}
std::vector<float> Distances(PqCpuCodes* codes,PqCpuQuery* query,PollState& poll)
{std::vector<float> out(257);auto limits=Limits(&poll);PqCpuError e{};int code=pq_cpu_distances(codes,query,&limits,0,257,out.data(),out.size(),&e);Leave(poll);Check(code==0,"real distance call");return out;}
void Oracle(PqCpuModel* model,PqCpuCodes* codes,const std::vector<float>& distances)
{
    auto i=Info(model);std::vector<float> centers(256*i.dimension);std::vector<uint8_t> bytes(257*i.chunks);PqCpuError e{};
    Check(pq_cpu_model_centers(model,centers.data(),centers.size(),&e)==0&&pq_cpu_codes_export(codes,bytes.data(),bytes.size(),&e)==0,"export numeric oracle");
    auto input=Data(i.dimension);unsigned part=i.dimension/i.chunks;
    for(unsigned row=0;row<257;++row){double expected=0;for(unsigned j=0;j<i.dimension;++j){double delta=(double)input[13*i.dimension+j]-centers[(unsigned)bytes[row*i.chunks+j/part]*i.dimension+j];expected+=delta*delta;}
        Check(std::isfinite(distances[row])&&std::fabs(distances[row]-expected)<=0.0001*std::max(1.0,std::fabs(expected)),"concurrent distances match reconstructed-vector oracle");}
}
void ColdMetadata()
{
    constexpr unsigned T=8;Barrier barrier(T);int codes[T]{};std::thread threads[T];
    for(unsigned t=0;t<T;++t)threads[t]=std::thread([&,t]{barrier.Wait();PqCpuError e{};codes[t]=pq_cpu_blas_metadata_initialize(&e);});
    for(auto& thread:threads)thread.join();for(int code:codes)Check(code==0,"concurrent once metadata initialization");
    Check(pq_cpu_blas_metadata_init_calls()==1&&pq_cpu_blas_metadata_source_reads()==1,"exactly one raw mutable-config read");
    Check(pq_cpu_blas_parallel_checked()==0&&pq_cpu_blas_num_threads_checked()==1,"actual serial private locking library");
    std::printf("{\"status\":\"pass\",\"workers\":8,\"initializers\":1,\"raw_config_reads\":1,\"config\":\"%s\"}\n",pq_cpu_blas_immutable_config());
}
void MetadataFailure(unsigned mode)
{
    Check(pq_cpu_blas_metadata_test_fault(mode)==0,"metadata fault armed before once");PqCpuError error{};int code=pq_cpu_blas_metadata_initialize(&error);
    Check(code==(mode==3?PQ_CPU_MEMORY:PQ_CPU_INVALID)&&error.code==code,"metadata initializer maps NULL/length/allocation exception");
    Check(pq_cpu_blas_immutable_config()!=nullptr&&pq_cpu_blas_parallel_checked()==-1,"metadata failure cannot reach NULL strlen or numeric admission");
    auto input=Data(3,256);char context[65];Context(context,0);PqCpuTrainSpec s{sizeof(s),1,3,1,1,256,context};PollState p{0};rank=0;auto limits=Limits(&p);Handles h;
    Check(pq_cpu_train(&s,&limits,input.data(),input.size(),&h.model,&error)==PQ_CPU_UNSUPPORTED&&!h.model,"failed backend metadata never publishes a model");
    Check(pq_cpu_blas_metadata_init_calls()==1&&pq_cpu_blas_metadata_source_reads()==1,"failed initialization remains once-only");
    std::printf("{\"status\":\"pass\",\"mode\":%u,\"initializer_code\":%d,\"train_code\":%d,\"raw_config_reads\":1}\n",mode,code,error.code);
}
void RawConfigRisk()
{
    constexpr unsigned T=8,N=10000;char expected[256];const char* initial=oglpq_openblas_get_config();std::snprintf(expected,sizeof(expected),"%s",initial);
    Barrier b(T);std::atomic<uint64_t> mismatches{0};std::thread threads[T];
    for(unsigned t=0;t<T;++t)threads[t]=std::thread([&]{b.Wait();for(unsigned n=0;n<N;++n){const char* p=oglpq_openblas_get_config();char copy[256];unsigned at=0;for(;at<255&&p[at];++at)copy[at]=p[at];copy[at]=0;if(std::strcmp(copy,expected)!=0)mismatches.fetch_add(1,std::memory_order_relaxed);}});
    for(auto& t:threads)t.join();std::printf("{\"scope\":\"raw upstream mutable-config negative, never native\",\"workers\":8,\"calls\":80000,\"mismatched_snapshots\":%llu}\n",(unsigned long long)mismatches.load());
}
void SharedImmutableModel()
{
    rank=0;PollState serialPoll{0};Handles shared;Train(shared,32,8,serialPoll,90);Encode(shared,serialPoll);Query(shared,serialPoll);auto serial=Distances(shared.codes,shared.query,serialPoll);Oracle(shared.model,shared.codes,serial);
    constexpr unsigned T=4,R=16;Barrier barrier(T);Meter meter;std::thread threads[T];bool ok[T]={};uint64_t wrong[T]{};
    for(unsigned t=0;t<T;++t)threads[t]=std::thread([&,t]{rank=t;PollState p{t};p.meter=&meter;try{barrier.Wait();for(unsigned r=0;r<R;++r){
            Handles local;auto i=Info(shared.model);auto input=Data(i.dimension);auto limits=Limits(&p);PqCpuError e{};PqCpuEncodeSpec spec{sizeof(spec),1,71,257,i.context_hash,i.model_hash};
            int code=pq_cpu_encode(shared.model,&spec,&limits,input.data(),input.size(),&local.codes,&e);Leave(p);Check(code==0,"parallel encode using shared immutable model");
            code=pq_cpu_query_open(shared.model,&limits,input.data()+13*i.dimension,i.dimension,&local.query,&e);Leave(p);Check(code==0,"parallel query using shared immutable model");
            auto actual=Distances(local.codes,local.query,p);Oracle(shared.model,local.codes,actual);
            for(size_t j=0;j<actual.size();++j)Check(actual[j]==serial[j],"parallel result equals same-codebook serial result");
        }ok[t]=true;}catch(...){Leave(p);ok[t]=false;}wrong[t]=p.wrong;});
    for(auto& t:threads)t.join();for(unsigned t=0;t<T;++t)Check(ok[t]&&wrong[t]==0,"shared immutable model concurrent numeric result/poll isolation");
    Check(meter.active.load()==0&&meter.peak.load()>=2,"actual API callbacks demonstrate overlapping PQ operations");
    std::printf("{\"status\":\"pass\",\"workers\":4,\"rounds_per_worker\":16,\"parallel_encode_query_runs\":64,\"serial_result_values_compared\":16448,\"peak_overlapping_api_operations\":%u,\"wrong_poll_owners\":0}\n",meter.peak.load());
}
struct Outcome {bool ok=false;int code=0;size_t calls=0,failures=0,live=0,scalar=0,array=0;uint64_t wrong=0;unsigned failed_kind=0;};
void ConcurrentOperation(const std::string& operation,const std::string& mode)
{
    constexpr unsigned T=4;Barrier barrier(T);Meter meter;std::thread threads[T];Outcome results[T];
    for(unsigned t=0;t<T;++t)threads[t]=std::thread([&,t]{rank=t;PollState p{t};Handles h;try{
        if(operation!="train")Train(h,1,1,p,10+t);if(operation=="query"||operation=="distance")Encode(h,p);if(operation=="distance")Query(h,p);
        auto info=operation=="train"?PqCpuInfo{}:Info(h.model);auto input=Data(1);auto train=Data(1,256);char context[65];Context(context,10+t);
        PqCpuTrainSpec ts{sizeof(ts),1,1,1,1,256,context};PqCpuEncodeSpec es{sizeof(es),1,71,257,info.context_hash,info.model_hash};auto limits=Limits(&p);PqCpuError error{};std::vector<float> out(257,777);
        int expected=0;size_t point=0;
        if(mode=="cancel"&&t==0){p.after=operation=="train"?20:operation=="encode"?10:operation=="query"?4:3;expected=PQ_CPU_CANCELLED;}
        if(mode=="fault"||(mode=="mixed"&&t==2)){size_t trainPoints[T]={1,10,274,538},encodePoints[T]={1,4,8,12};point=operation=="train"?trainPoints[t]:operation=="encode"?encodePoints[t]:operation=="query"?(t%2)+1:1;expected=PQ_CPU_MEMORY;}
        if(mode=="mixed"&&t==0){p.after=operation=="train"?20:operation=="encode"?10:operation=="query"?4:3;expected=PQ_CPU_CANCELLED;}
        p.polls=0;p.meter=&meter;barrier.Wait();memory.enabled=true;memory.at=point;int code;
        if(operation=="train")code=pq_cpu_train(&ts,&limits,train.data(),train.size(),&h.model,&error);
        else if(operation=="encode")code=pq_cpu_encode(h.model,&es,&limits,input.data(),input.size(),&h.codes,&error);
        else if(operation=="query")code=pq_cpu_query_open(h.model,&limits,train.data(),1,&h.query,&error);
        else code=pq_cpu_distances(h.codes,h.query,&limits,0,257,out.data(),out.size(),&error);
        Leave(p);p.meter=nullptr;memory.enabled=false;Check(code==expected&&error.code==code,"concurrent status isolation");
        if(expected){if(operation=="train")Check(!h.model,"failed train has no handle");if(operation=="encode")Check(!h.codes,"failed encode has no handle");if(operation=="query")Check(!h.query,"failed query has no handle");if(operation=="distance")Check(std::all_of(out.begin(),out.end(),[](float x){return x==777;}),"failed output unchanged");}
        if(point)Check(memory.failures==1,"one exact allocation failure in this thread");
        if(code==0&&operation=="train"){auto i=Info(h.model);Check(std::strcmp(i.context_hash,context)==0,"independent model context is not overwritten by another trainer");std::vector<float> centers(256);Check(pq_cpu_model_centers(h.model,centers.data(),centers.size(),&error)==0,"concurrent trained centers");std::sort(centers.begin(),centers.end());Check(centers==train,"independent local kmeans RNG retains complete unique training set");}
        auto& r=results[t];r.code=code;r.calls=memory.calls;r.failures=memory.failures;r.failed_kind=memory.failed_kind;
        h.Close();Check(memory.live==0&&memory.live_bytes==0&&memory.live_kinds[0]==0&&memory.live_kinds[1]==0,"concurrent failure releases scalar/array allocations");
        /* Retry on the SAME thread after error proves active poll scope reset. */
        p.after=0;p.polls=0;Handles retry;Train(retry,1,1,p,10+t);Encode(retry,p);Query(retry,p);auto distances=Distances(retry.codes,retry.query,p);Oracle(retry.model,retry.codes,distances);
        r.live=memory.live;r.scalar=memory.live_kinds[0];r.array=memory.live_kinds[1];r.wrong=p.wrong;r.ok=r.live==0&&r.wrong==0;
    }catch(...){Leave(p);memory.enabled=false;h.Close();results[t].ok=false;}});
    for(auto& t:threads)t.join();for(const auto& r:results)Check(r.ok,"concurrent operation/fault/cancellation contract");
    std::printf("{\"status\":\"pass\",\"operation\":\"%s\",\"mode\":\"%s\",\"workers\":[",operation.c_str(),mode.c_str());
    for(unsigned t=0;t<T;++t){const auto& r=results[t];if(t)std::printf(",");std::printf("{\"rank\":%u,\"code\":%d,\"allocation_calls\":%zu,\"failures\":%zu,\"failed_kind\":%u,\"outstanding_scalar\":%zu,\"outstanding_array\":%zu,\"wrong_poll_owners\":%llu}",t,r.code,r.calls,r.failures,r.failed_kind,r.scalar,r.array,(unsigned long long)r.wrong);}
    Check(meter.active.load()==0,"all actual API overlap scopes released");
    std::printf("],\"peak_overlapping_api_operations\":%u,\"raw_config_reads\":%u,\"seed_scope\":\"per-call random_device+mt19937, nondeterministic seed retained\"}\n",meter.peak.load(),pq_cpu_blas_metadata_source_reads());
}
}
void* operator new(size_t n){return Allocate(n,0);}void* operator new[](size_t n){return Allocate(n,1);}
void operator delete(void* p)noexcept{Release(p);}void operator delete[](void* p)noexcept{Release(p);}
void operator delete(void* p,size_t)noexcept{Release(p);}void operator delete[](void* p,size_t)noexcept{Release(p);}
int main(int argc,char** argv)
{
    try{Check(argc>=2,"thread probe mode required");std::string mode=argv[1];if(mode=="cold-metadata")ColdMetadata();
        else if(mode=="metadata-failure"){Check(argc==3,"failure mode required");MetadataFailure((unsigned)std::stoul(argv[2]));}
        else if(mode=="raw-config-risk")RawConfigRisk();
        else{PqCpuError error{};Check(pq_cpu_blas_metadata_initialize(&error)==0,"initialize real backend before numeric threads");
            if(mode=="shared")SharedImmutableModel();else{Check(argc==3,"operation mode required");ConcurrentOperation(mode,argv[2]);}
            Check(pq_cpu_blas_metadata_source_reads()==1,"all concurrent API hashes use one immutable metadata snapshot");}
        return 0;
    }catch(const std::exception& e){memory.enabled=false;std::fprintf(stderr,"locking thread probe failed: %s\n",e.what());return 1;}
}
