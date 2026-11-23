/* Independent C-API consumer and real operator-new fault injection. */
#include "pq_cpu.h"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>
namespace {
struct Allocation {void* p;size_t n;unsigned kind;};Allocation allocations[65536]{};
bool tracking=false;size_t calls=0,fail_at=0,failures=0,live=0,bytes=0,peak=0,kinds[2]{},live_kinds[2]{};unsigned failed_kind=0;
void Track(void* p,size_t n,unsigned kind){for(auto& a:allocations)if(!a.p){a={p,n,kind};++live;++live_kinds[kind];bytes+=n;peak=std::max(peak,bytes);return;}std::abort();}
void Forget(void* p){for(auto& a:allocations)if(a.p==p){--live;--live_kinds[a.kind];bytes-=a.n;a={nullptr,0,0};return;}}
void* Allocate(size_t n,unsigned kind){if(tracking){++calls;++kinds[kind];if(fail_at&&calls==fail_at){++failures;failed_kind=kind;throw std::bad_alloc();}}
    void* p=std::malloc(n?n:1);if(!p)throw std::bad_alloc();if(tracking)Track(p,n,kind);return p;}
uint64_t checks=0;void Check(bool ok,const char* s){++checks;if(!ok)throw std::runtime_error(s);}
void Ok(int code,const PqCpuError& e){if(code)throw std::runtime_error(std::to_string(code)+":"+e.message);++checks;}
void Near(double a,double b){Check(std::isfinite(a)&&std::isfinite(b)&&std::fabs(a-b)<=0.0001*std::max(1.0,std::fabs(b)),"PQ numeric oracle mismatch");}
struct Model {PqCpuModel* p=nullptr;Model()=default;Model(const Model&)=delete;Model(Model&& x)noexcept:p(x.p){x.p=nullptr;}~Model(){pq_cpu_model_close(&p);}};
struct Codes {PqCpuCodes* p=nullptr;~Codes(){pq_cpu_codes_close(&p);}};
struct Query {PqCpuQuery* p=nullptr;~Query(){pq_cpu_query_close(&p);}};
struct Cancellation {int polls=0,after=0;};int Poll(void* p){auto* c=(Cancellation*)p;return c&&c->after>0&&++c->polls>=c->after;}
const char* CONTEXT="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
PqCpuLimits Limits(Cancellation* c=nullptr){return {sizeof(PqCpuLimits),64*1024*1024,Poll,c};}
PqCpuTrainSpec Spec(uint32_t d,uint32_t m,uint32_t metric=1,uint64_t rows=256)
{return {sizeof(PqCpuTrainSpec),1,d,m,metric,rows,CONTEXT};}
std::vector<float> Data(uint64_t n,uint32_t d,uint32_t metric=1)
{
    std::vector<float> data(n*d);for(uint64_t i=0;i<n;++i){double norm=0;
        for(uint32_t j=0;j<d;++j){float x=(float)((int)(i%256)-128)/256+(float)(j%8)/16;data[i*d+j]=x;norm+=(double)x*x;}
        if(metric==3){if(norm==0){data[i*d]=1;norm=1;}double scale=std::sqrt(norm);for(uint32_t j=0;j<d;++j)data[i*d+j]=(float)(data[i*d+j]/scale);}}
    return data;
}
PqCpuInfo Info(const Model& m){PqCpuInfo i{};i.struct_size=sizeof(i);PqCpuError e{};Ok(pq_cpu_model_info(m.p,&i,&e),e);return i;}
PqCpuInfo Info(const Codes& c){PqCpuInfo i{};i.struct_size=sizeof(i);PqCpuError e{};Ok(pq_cpu_codes_info(c.p,&i,&e),e);return i;}
PqCpuEncodeSpec EncodeSpec(const PqCpuInfo& i,uint64_t rows,int64_t file=71)
{return {sizeof(PqCpuEncodeSpec),1,file,rows,i.context_hash,i.model_hash};}
Model Train(uint32_t d,uint32_t m,uint32_t metric=1,uint64_t n=256)
{Model result;auto data=Data(n,d,metric);auto spec=Spec(d,m,metric,n);Cancellation c;auto limits=Limits(&c);PqCpuError e{};Ok(pq_cpu_train(&spec,&limits,data.data(),data.size(),&result.p,&e),e);return result;}
/* Handles deliberately remain trivially wrapped in local owners; never copy. */
void Numeric(uint32_t d,uint32_t m,uint32_t metric,uint64_t training=256)
{
    auto model=Train(d,m,metric,training);auto info=Info(model);auto input=Data(257,d,metric);
    for(uint32_t row=0;row<257;++row){double norm=0;for(uint32_t j=0;j<d;++j){input[row*d+j]+=0.0012345f;norm+=(double)input[row*d+j]*input[row*d+j];}
        if(metric==3)for(uint32_t j=0;j<d;++j)input[row*d+j]=(float)(input[row*d+j]/std::sqrt(norm));}
    auto spec=EncodeSpec(info,257);Cancellation c;auto limits=Limits(&c);PqCpuError e{};Codes codes;
    Ok(pq_cpu_encode(model.p,&spec,&limits,input.data(),input.size(),&codes.p,&e),e);auto ci=Info(codes);Check(ci.rows==257&&ci.dimension==d&&ci.chunks==m&&ci.source_file_id==71,"nodecode identity");
    Check(std::strcmp(ci.context_hash,info.context_hash)==0&&std::strcmp(ci.model_hash,info.model_hash)==0,"codebook/context binding");
    std::vector<float> centers(256*d);std::vector<uint8_t> bytes(257*m);Ok(pq_cpu_model_centers(model.p,centers.data(),centers.size(),&e),e);Ok(pq_cpu_codes_export(codes.p,bytes.data(),bytes.size(),&e),e);
    float exportCanary=991;Check(pq_cpu_model_centers(model.p,&exportCanary,0,&e)==PQ_CPU_INVALID&&exportCanary==991,"centers capacity error is atomic");
    uint8_t codeCanary=91;Check(pq_cpu_codes_export(codes.p,&codeCanary,0,&e)==PQ_CPU_INVALID&&codeCanary==91,"codes capacity error is atomic");
    std::vector<float> query(input.begin()+13*d,input.begin()+14*d);Query lut;Ok(pq_cpu_query_open(model.p,&limits,query.data(),query.size(),&lut.p,&e),e);
    std::vector<float> distances(257);Ok(pq_cpu_distances(codes.p,lut.p,&limits,0,257,distances.data(),distances.size(),&e),e);
    double mse=0,max_error=0;uint32_t chunk_size=d/m;
    for(uint32_t row=0;row<257;++row){double expected=0;
        for(uint32_t j=0;j<d;++j){float reconstructed=centers[(uint32_t)bytes[row*m+j/chunk_size]*d+j];double delta=(double)input[row*d+j]-reconstructed;mse+=delta*delta;
            if(metric==1){double difference=(double)query[j]-reconstructed;expected+=difference*difference;}else expected-=(double)query[j]*reconstructed;}
        Near(distances[row],expected);max_error=std::max(max_error,std::fabs(distances[row]-expected));}
    Query rejected;std::vector<float> bad=query;bad[0]=std::numeric_limits<float>::quiet_NaN();Check(pq_cpu_query_open(model.p,&limits,bad.data(),bad.size(),&rejected.p,&e)==PQ_CPU_INVALID&&!rejected.p,"nonfinite query rejected");
    float canary[2]={123,456};Check(pq_cpu_distances(codes.p,lut.p,&limits,257,1,canary,2,&e)==PQ_CPU_INVALID&&canary[0]==123&&canary[1]==456,"range error leaves caller output unchanged");
    c.polls=0;c.after=3;tracking=true;int cancelCode=pq_cpu_distances(codes.p,lut.p,&limits,0,2,canary,2,&e);tracking=false;
    Check(cancelCode==PQ_CPU_CANCELLED&&canary[0]==123&&canary[1]==456&&live==0,"cancellation frees temporary results and preserves caller output");c.after=0;
    Codes wrong;auto wrongspec=spec;std::string badhash(64,'b');wrongspec.model_hash=badhash.c_str();Check(pq_cpu_encode(model.p,&wrongspec,&limits,input.data(),input.size(),&wrong.p,&e)==PQ_CPU_INVALID&&!wrong.p,"model hash mismatch rejected before encoding");
    wrongspec=spec;wrongspec.context_hash=badhash.c_str();Check(pq_cpu_encode(model.p,&wrongspec,&limits,input.data(),input.size(),&wrong.p,&e)==PQ_CPU_INVALID&&!wrong.p,"source context mismatch rejected");
    wrongspec=spec;wrongspec.source_file_id=72;Ok(pq_cpu_encode(model.p,&wrongspec,&limits,input.data(),input.size(),&wrong.p,&e),e);Check(std::strcmp(Info(wrong).payload_hash,ci.payload_hash)!=0,"source file participates in code identity");
    auto poor=limits;poor.memory_bytes=1;Codes budgetRejected;Check(pq_cpu_encode(model.p,&spec,&poor,input.data(),input.size(),&budgetRejected.p,&e)==PQ_CPU_MEMORY&&!budgetRejected.p,"encode budget rejects without publishing codes");
    /* LUT/codes own primitive storage; the model may close before distance use. */
    pq_cpu_model_close(&model.p);Ok(pq_cpu_distances(codes.p,lut.p,&limits,0,257,distances.data(),distances.size(),&e),e);
    std::printf("{\"dimension\":%u,\"chunks\":%u,\"metric\":%u,\"training_rows\":%llu,\"rows\":257,\"mse\":%.12g,\"maximum_lut_error\":%.12g,\"model_hash\":\"%s\",\"codes_hash\":\"%s\"}",d,m,metric,(unsigned long long)training,mse/(257*d),max_error,info.model_hash,ci.payload_hash);
}
void Boundaries()
{
    Cancellation c;auto limits=Limits(&c);PqCpuError e{};auto data=Data(256,3);auto valid=Spec(3,1);
    for(int test=0;test<9;++test){auto s=valid;const float* values=data.data();uint64_t count=data.size();
        if(test==0)s.chunks=0;if(test==1)s.training_rows=255;if(test==2)s.training_rows=1025;if(test==3)s.dimension=0;if(test==4)s.dimension=1537;
        if(test==5)s.chunks=2;if(test==6)count=UINT64_MAX;if(test==7)values=nullptr;if(test==8)s.context_hash="bad";
        Model out;Check(pq_cpu_train(&s,&limits,values,count,&out.p,&e)!=PQ_CPU_OK&&!out.p,"invalid training contract rejected without handle");}
    Model out;auto bad=data;bad[0]=std::numeric_limits<float>::infinity();Check(pq_cpu_train(&valid,&limits,bad.data(),bad.size(),&out.p,&e)==PQ_CPU_INVALID&&!out.p,"nonfinite training rejected");
    bad[0]=std::numeric_limits<float>::max();Check(pq_cpu_train(&valid,&limits,bad.data(),bad.size(),&out.p,&e)==PQ_CPU_INVALID&&!out.p,"float accumulation overflow admission");
    auto poor=limits;poor.memory_bytes=16*1024*1024;Check(pq_cpu_train(&valid,&poor,data.data(),data.size(),&out.p,&e)==PQ_CPU_MEMORY&&!out.p,"original N*256 distance matrix is part of budget");
    c.polls=0;c.after=20;tracking=true;int cancelCode=pq_cpu_train(&valid,&limits,data.data(),data.size(),&out.p,&e);tracking=false;
    Check(cancelCode==PQ_CPU_CANCELLED&&!out.p&&live==0,"training cancellation unwinds original algorithm resources");c.after=0;
    auto model=Train(3,1);auto info=Info(model);auto spec=EncodeSpec(info,0);Codes empty;
    auto* previous=model.p;Check(pq_cpu_train(&valid,&limits,data.data(),data.size(),&model.p,&e)==PQ_CPU_INVALID&&model.p==previous,"occupied model slot rejected without losing caller ownership");
    auto many=Data(257,3);auto manySpec=EncodeSpec(info,257);Codes cancelledCodes;c.polls=0;c.after=10;tracking=true;
    cancelCode=pq_cpu_encode(model.p,&manySpec,&limits,many.data(),many.size(),&cancelledCodes.p,&e);tracking=false;
    Check(cancelCode==PQ_CPU_CANCELLED&&!cancelledCodes.p&&live==0,"encoding cancellation frees all C++ resources");c.after=0;
    Query cancelledQuery;c.polls=0;c.after=4;tracking=true;cancelCode=pq_cpu_query_open(model.p,&limits,data.data(),3,&cancelledQuery.p,&e);tracking=false;
    Check(cancelCode==PQ_CPU_CANCELLED&&!cancelledQuery.p&&live==0,"LUT cancellation frees all C++ resources");c.after=0;
    Ok(pq_cpu_encode(model.p,&spec,&limits,nullptr,0,&empty.p,&e),e);Check(Info(empty).rows==0,"empty immutable node range accepted");Ok(pq_cpu_codes_export(empty.p,nullptr,0,&e),e);
    Query query;auto q=Data(1,3);Ok(pq_cpu_query_open(model.p,&limits,q.data(),q.size(),&query.p,&e),e);Ok(pq_cpu_distances(empty.p,query.p,&limits,0,0,nullptr,0,&e),e);
    spec.node_count=PQ_CPU_MAX_NODES+1;Codes none;Check(pq_cpu_encode(model.p,&spec,&limits,nullptr,0,&none.p,&e)==PQ_CPU_UNSUPPORTED&&!none.p,"node population upper bound");
    auto wide=Train(64,64);auto wi=Info(wide);spec=EncodeSpec(wi,PQ_CPU_MAX_NODES);
    Check(pq_cpu_encode(wide.p,&spec,&limits,nullptr,(uint64_t)PQ_CPU_MAX_NODES*64,&none.p,&e)==PQ_CPU_MEMORY&&!none.p,"one-million by64 code cache exceeds64MiB admission");
    auto other=Train(3,1,2);Query ip;Ok(pq_cpu_query_open(other.p,&limits,q.data(),q.size(),&ip.p,&e),e);float sentinel=77;
    Check(pq_cpu_distances(empty.p,ip.p,&limits,0,0,&sentinel,1,&e)==PQ_CPU_INVALID&&sentinel==77,"cross-model metric mismatch rejected");
}
void Fault(const std::string& operation,size_t point)
{
    Cancellation c;auto limits=Limits(&c);PqCpuError e{};auto train=Data(256,1);auto s=Spec(1,1);Model model;Codes codes;Query query;
    if(operation!="train"){Ok(pq_cpu_train(&s,&limits,train.data(),train.size(),&model.p,&e),e);}
    auto info=operation=="train"?PqCpuInfo{}:Info(model);auto input=Data(257,1);auto spec=EncodeSpec(info,257);
    if(operation=="query"||operation=="distance"){Ok(pq_cpu_encode(model.p,&spec,&limits,input.data(),input.size(),&codes.p,&e),e);}
    if(operation=="distance")Ok(pq_cpu_query_open(model.p,&limits,train.data(),1,&query.p,&e),e);
    std::vector<float> output(257,777);fail_at=point;tracking=true;int code=PQ_CPU_STATE;
    if(operation=="train")code=pq_cpu_train(&s,&limits,train.data(),train.size(),&model.p,&e);
    else if(operation=="encode")code=pq_cpu_encode(model.p,&spec,&limits,input.data(),input.size(),&codes.p,&e);
    else if(operation=="query")code=pq_cpu_query_open(model.p,&limits,train.data(),1,&query.p,&e);
    else if(operation=="distance")code=pq_cpu_distances(codes.p,query.p,&limits,0,257,output.data(),output.size(),&e);
    tracking=false;
    if(point){Check(failures==1&&code==PQ_CPU_MEMORY&&e.code==code,"real allocation failure maps to explicit C memory status");
        if(operation=="train")Check(!model.p,"failed training publishes no model");if(operation=="encode")Check(!codes.p,"failed encode publishes no codes");if(operation=="query")Check(!query.p,"failed LUT publishes no query");
        if(operation=="distance")Check(std::all_of(output.begin(),output.end(),[](float x){return x==777;}),"failed distance leaves output unchanged");}
    else Ok(code,e);
    pq_cpu_query_close(&query.p);pq_cpu_query_close(&query.p);pq_cpu_codes_close(&codes.p);pq_cpu_codes_close(&codes.p);pq_cpu_model_close(&model.p);pq_cpu_model_close(&model.p);
    Check(live==0&&bytes==0&&live_kinds[0]==0&&live_kinds[1]==0,"all tracked scalar/array C++ allocations freed after success/failure");
    std::printf("{\"status\":\"pass\",\"operation\":\"%s\",\"calls\":%zu,\"scalar_calls\":%zu,\"array_calls\":%zu,\"failure_index\":%zu,\"failed_kind\":%u,\"failures\":%zu,\"code\":%d,\"outstanding\":%zu,\"outstanding_scalar\":%zu,\"outstanding_array\":%zu,\"peak_tracked_bytes\":%zu}\n",operation.c_str(),calls,kinds[0],kinds[1],point,failed_kind,failures,code,live,live_kinds[0],live_kinds[1],peak);
}
void Extremes()
{
    Cancellation c;auto limits=Limits(&c);PqCpuError e{};auto spec=Spec(3,1);Model rejected;
    std::vector<float> huge(256*3,1.0e30f);Check(std::isfinite(huge[0]),"1e30 is a finite input");
    Check(pq_cpu_train(&spec,&limits,huge.data(),huge.size(),&rejected.p,&e)==PQ_CPU_INVALID&&!rejected.p,"finite1e30 rejected before unsafe upstream float arithmetic");
    std::printf("{\"status\":\"pass\",\"finite_1e30_training\":\"INVALID\",\"cases\":[");bool comma=false;
    for(int pattern=0;pattern<3;++pattern)for(uint32_t metric:{(uint32_t)1,(uint32_t)2}){
        const uint64_t n=1024;std::vector<float> data(n*3,0);for(uint64_t i=0;i<n;++i)for(uint32_t j=0;j<3;++j){
            if(pattern==1)data[i*3+j]=std::numeric_limits<float>::denorm_min()*(float)(16*(j+1));
            if(pattern==2)data[i*3+j]=j==0?0.25f:j==1?-0.5f:0.125f;}
        auto s=Spec(3,1,metric,n);Model model;Ok(pq_cpu_train(&s,&limits,data.data(),data.size(),&model.p,&e),e);auto info=Info(model);
        std::vector<float> centers(256*3);Ok(pq_cpu_model_centers(model.p,centers.data(),centers.size(),&e),e);for(float x:centers)Check(std::isfinite(x),"degenerate training publishes only finite centers");
        auto es=EncodeSpec(info,n);Codes codes;Ok(pq_cpu_encode(model.p,&es,&limits,data.data(),data.size(),&codes.p,&e),e);std::vector<uint8_t> bytes(n);Ok(pq_cpu_codes_export(codes.p,bytes.data(),bytes.size(),&e),e);
        Query query;Ok(pq_cpu_query_open(model.p,&limits,data.data(),3,&query.p,&e),e);std::vector<float> distances(n,777);Ok(pq_cpu_distances(codes.p,query.p,&limits,0,n,distances.data(),distances.size(),&e),e);
        double maximum_error=0;for(uint64_t i=0;i<n;++i){double expected=0;for(uint32_t j=0;j<3;++j){double q=data[j],reconstructed=centers[(uint32_t)bytes[i]*3+j];
                if(metric==1){double delta=q-reconstructed;expected+=delta*delta;}else expected-=q*reconstructed;}
            Near(distances[i],expected);maximum_error=std::max(maximum_error,std::fabs(distances[i]-expected));}
        Query badQuery;Check(pq_cpu_query_open(model.p,&limits,huge.data(),3,&badQuery.p,&e)==PQ_CPU_INVALID&&!badQuery.p,"finite1e30 query rejected without partial LUT");
        Codes badCodes;auto one=EncodeSpec(info,1);Check(pq_cpu_encode(model.p,&one,&limits,huge.data(),3,&badCodes.p,&e)==PQ_CPU_INVALID&&!badCodes.p,"finite1e30 encode rejected without partial node codes");
        if(comma)std::printf(",");comma=true;std::printf("{\"pattern\":\"%s\",\"metric\":%u,\"training_rows\":1024,\"maximum_reconstructed_distance_error\":%.12g}",pattern==0?"all-zero":pattern==1?"subnormal":"duplicate",metric,maximum_error);
    }
    std::vector<float> zero(256*3,0);auto cosine=Spec(3,1,3);Check(pq_cpu_train(&cosine,&limits,zero.data(),zero.size(),&rejected.p,&e)==PQ_CPU_INVALID&&!rejected.p,"zero cosine input is explicitly invalid");
    std::printf("],\"zero_cosine_training\":\"INVALID\",\"checks\":%llu}\n",(unsigned long long)checks);
}
void Million()
{
    auto model=Train(1,1);auto mi=Info(model);auto data=Data(PQ_CPU_MAX_NODES,1);auto spec=EncodeSpec(mi,PQ_CPU_MAX_NODES);
    Cancellation c;auto limits=Limits(&c);PqCpuError e{};Codes codes;Ok(pq_cpu_encode(model.p,&spec,&limits,data.data(),data.size(),&codes.p,&e),e);
    auto ci=Info(codes);Check(ci.rows==PQ_CPU_MAX_NODES&&ci.resident_bytes<2*1024*1024,"one-million-node low-M cache admitted within actual budget");
    std::vector<uint8_t> bytes(PQ_CPU_MAX_NODES);std::vector<float> centers(256);Ok(pq_cpu_codes_export(codes.p,bytes.data(),bytes.size(),&e),e);Ok(pq_cpu_model_centers(model.p,centers.data(),centers.size(),&e),e);
    for(uint64_t row=0;row<PQ_CPU_MAX_NODES;++row)Check(centers[bytes[row]]==data[row],"every million-range dense node retains its encoded value");
    Query query;Ok(pq_cpu_query_open(model.p,&limits,data.data(),1,&query.p,&e),e);float tail[3]={};
    Ok(pq_cpu_distances(codes.p,query.p,&limits,PQ_CPU_MAX_NODES-3,3,tail,3,&e),e);
    for(int j=0;j<3;++j){double difference=data[PQ_CPU_MAX_NODES-3+j]-data[0];Near(tail[j],difference*difference);}
    std::printf("{\"status\":\"pass\",\"nodes\":%u,\"dimension\":1,\"chunks\":1,\"resident_code_bytes\":%llu,\"checks\":%llu,\"code_hash\":\"%s\"}\n",PQ_CPU_MAX_NODES,(unsigned long long)ci.resident_bytes,(unsigned long long)checks,ci.payload_hash);
}
}
void* operator new(size_t n){return Allocate(n,0);}void* operator new[](size_t n){return Allocate(n,1);}
void operator delete(void* p)noexcept{if(p){Forget(p);std::free(p);}}void operator delete[](void* p)noexcept{::operator delete(p);}
void operator delete(void* p,size_t)noexcept{::operator delete(p);}void operator delete[](void* p,size_t)noexcept{::operator delete(p);}
int main(int argc,char** argv)
{
    try{if(argc==4&&std::string(argv[1])=="fault"){Fault(argv[2],std::stoull(argv[3]));return 0;}
        if(argc==2&&std::string(argv[1])=="million"){Million();return 0;}
        if(argc==2&&std::string(argv[1])=="extremes"){Extremes();return 0;}
        Boundaries();std::printf("{\"status\":\"pass\",\"cases\":[");Numeric(1,1,1);std::printf(",");Numeric(3,1,1);std::printf(",");Numeric(3,1,1,1024);std::printf(",");Numeric(8,2,2);std::printf(",");Numeric(32,8,3);std::printf(",");Numeric(1536,16,1);
        std::printf("],\"checks\":%llu}\n",(unsigned long long)checks);return 0;
    }catch(const std::exception& e){tracking=false;std::fprintf(stderr,"CPU PQ C-boundary probe failed: %s\n",e.what());return 1;}
}
