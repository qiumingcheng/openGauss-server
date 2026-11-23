/* Independent primitive-persistence/range consumer. Fixture below is a TEST
 * transport, not a Native lake artifact or a product file protocol. */
#include "pq_cpu.h"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>
namespace {
struct Allocation {void* p;size_t bytes;unsigned kind;};
Allocation allocations[8192]{};
bool tracking=false;size_t calls=0,fail_at=0,failures=0,live=0,live_bytes=0,kinds[2]{},live_kinds[2]{};
void* Allocate(size_t n,unsigned kind)
{
    if(tracking){++calls;++kinds[kind];if(fail_at&&calls==fail_at){++failures;throw std::bad_alloc();}}
    void* p=std::malloc(n?n:1);if(!p)throw std::bad_alloc();
    if(tracking){bool found=false;for(auto& a:allocations)if(!a.p){a={p,n,kind};++live;++live_kinds[kind];live_bytes+=n;found=true;break;}if(!found)std::abort();}return p;
}
void Forget(void* p){if(!live)return;for(auto& a:allocations)if(a.p==p){--live;--live_kinds[a.kind];live_bytes-=a.bytes;a={};return;}}
uint64_t checks=0;
void Check(bool ok,const char* why){++checks;if(!ok)throw std::runtime_error(why);}
void Ok(int rc,const PqCpuError& e){++checks;if(rc)throw std::runtime_error(std::to_string(rc)+":"+e.message);}
void Near(double actual,double expected){Check(std::isfinite(actual)&&std::isfinite(expected)&&std::fabs(actual-expected)<=1e-4*std::max(1.0,std::fabs(expected)),"reconstructed-vector numerical mismatch");}
struct Model {PqCpuModel* p=nullptr;~Model(){pq_cpu_model_close(&p);}};
struct Codes {PqCpuCodes* p=nullptr;~Codes(){pq_cpu_codes_close(&p);}};
struct Query {PqCpuQuery* p=nullptr;~Query(){pq_cpu_query_close(&p);}};
struct Builder {PqCpuCodesBuilder* p=nullptr;~Builder(){pq_cpu_codes_builder_close(&p);}};
struct Cancellation {uint64_t polls=0,after=0;};
int Poll(void* p){auto* c=static_cast<Cancellation*>(p);if(!c)return 0;++c->polls;return c->after&&c->polls>=c->after;}
PqCpuLimits Limits(Cancellation* c=nullptr){return {sizeof(PqCpuLimits),64*1024*1024,Poll,c};}
const char CONTEXT[]="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const char OTHER[]="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
PqCpuBackendInfo Backend(){PqCpuBackendInfo b{};b.struct_size=sizeof(b);PqCpuError e{};Ok(pq_cpu_backend_info(&b,&e),e);return b;}
PqCpuInfo Info(const Model& m){PqCpuInfo i{};i.struct_size=sizeof(i);PqCpuError e{};Ok(pq_cpu_model_info(m.p,&i,&e),e);return i;}
PqCpuInfo Info(const Codes& c){PqCpuInfo i{};i.struct_size=sizeof(i);PqCpuError e{};Ok(pq_cpu_codes_info(c.p,&i,&e),e);return i;}
PqCpuCodesBuilderInfo Info(const Builder& b){PqCpuCodesBuilderInfo i{};i.struct_size=sizeof(i);PqCpuError e{};Ok(pq_cpu_codes_builder_info(b.p,&i,&e),e);return i;}
std::vector<float> Data(uint64_t first,uint64_t n,uint32_t d,uint32_t metric)
{
    std::vector<float> out(n*d);
    for(uint64_t r=0;r<n;++r){double norm=0;for(uint32_t j=0;j<d;++j){uint64_t x=(first+r)*131+j*79+((first+r)*(j+1)%29)*17;
            float v=static_cast<float>(static_cast<int>(x%2039)-1019)/1024;out[r*d+j]=v;norm+=static_cast<double>(v)*v;}
        if(metric==PQ_CPU_COSINE){if(norm==0){out[r*d]=1;norm=1;}for(uint32_t j=0;j<d;++j)out[r*d+j]=static_cast<float>(out[r*d+j]/std::sqrt(norm));}}
    return out;
}
void Train(Model& m,uint32_t d,uint32_t chunks,uint32_t metric,uint64_t n=1024,const char* context=CONTEXT)
{
    auto data=Data(0,n,d,metric);PqCpuTrainSpec s{sizeof(s),1,d,chunks,metric,n,context};auto l=Limits();PqCpuError e{};
    Ok(pq_cpu_train(&s,&l,data.data(),data.size(),&m.p,&e),e);
}
PqCpuModelRestoreSpec RestoreSpec(const PqCpuInfo& i,const PqCpuBackendInfo& backend)
{
    return {sizeof(PqCpuModelRestoreSpec),PQ_CPU_PERSIST_VERSION,PQ_CPU_VERSION,PQ_CPU_ALGORITHM_VERSION,
        i.dimension,i.chunks,256,i.metric,i.rows,i.context_hash,i.payload_hash,i.model_hash,backend.backend_hash};
}
PqCpuCodesBuilderSpec BuildSpec(const PqCpuInfo& i,const PqCpuBackendInfo& backend,uint64_t rows,uint32_t kind=PQ_CPU_INPUT_FLOATS,const char* expected=nullptr,int64_t file=71)
{
    return {sizeof(PqCpuCodesBuilderSpec),PQ_CPU_PERSIST_VERSION,PQ_CPU_VERSION,PQ_CPU_ALGORITHM_VERSION,
        i.dimension,i.chunks,256,i.metric,kind,file,rows,i.context_hash,i.model_hash,backend.backend_hash,expected};
}
std::vector<float> Centers(const Model& m){auto i=Info(m);std::vector<float> c(256*i.dimension);PqCpuError e{};Ok(pq_cpu_model_centers(m.p,c.data(),c.size(),&e),e);return c;}
std::vector<uint8_t> Bytes(const Codes& c){auto i=Info(c);std::vector<uint8_t> v(i.rows*i.chunks);PqCpuError e{};Ok(pq_cpu_codes_export(c.p,v.data(),v.size(),&e),e);return v;}
void Open(Builder& b,const Model& m,const PqCpuCodesBuilderSpec& s,const PqCpuLimits& l){PqCpuError e{};Ok(pq_cpu_codes_builder_open(m.p,&s,&l,&b.p,&e),e);}
void Append(Builder& b,const PqCpuInfo& i,uint64_t first,uint64_t rows,const PqCpuLimits& l)
{auto data=Data(first,rows,i.dimension,i.metric);PqCpuError e{};Ok(pq_cpu_codes_builder_append(b.p,&l,first,rows,data.data(),data.size(),&e),e);}
void AppendAll(Builder& b,const PqCpuInfo& i,uint64_t rows,const PqCpuLimits& l)
{for(uint64_t first=0;first<rows;first+=1024)Append(b,i,first,std::min<uint64_t>(1024,rows-first),l);}
void Finish(Builder& b,Codes& c,const PqCpuLimits& l){PqCpuError e{};Ok(pq_cpu_codes_builder_finish(b.p,&l,&c.p,&e),e);}
void RawAll(Builder& b,const std::vector<uint8_t>& bytes,uint64_t rows,uint32_t chunks,const PqCpuLimits& l)
{for(uint64_t first=0;first<rows;first+=1024){uint64_t n=std::min<uint64_t>(1024,rows-first);PqCpuError e{};
    Ok(pq_cpu_codes_builder_append_raw(b.p,&l,first,n,bytes.data()+first*chunks,n*chunks,&e),e);}}
void SameInfo(const PqCpuInfo& a,const PqCpuInfo& b)
{
    Check(a.version==b.version&&a.dimension==b.dimension&&a.chunks==b.chunks&&a.metric==b.metric&&a.rows==b.rows&&a.source_file_id==b.source_file_id,"restored primitive shape/source differs");
    Check(!std::strcmp(a.context_hash,b.context_hash)&&!std::strcmp(a.model_hash,b.model_hash)&&!std::strcmp(a.payload_hash,b.payload_hash),"restored primitive hashes differ");
}
void Write64(std::ostream& f,uint64_t x){unsigned char b[8];for(int n=0;n<8;++n){b[n]=x&255;x>>=8;}f.write(reinterpret_cast<const char*>(b),8);}
uint64_t Read64(std::istream& f){unsigned char b[8]{};f.read(reinterpret_cast<char*>(b),8);uint64_t x=0;for(int n=7;n>=0;--n)x=(x<<8)|b[n];return x;}
void WriteFloat(std::ostream& f,float x){uint32_t bits;std::memcpy(&bits,&x,4);Write64(f,bits);}
float ReadFloat(std::istream& f){uint64_t wide=Read64(f);Check(wide<=UINT32_MAX,"test float bits not canonical");uint32_t bits=static_cast<uint32_t>(wide);float x;std::memcpy(&x,&bits,4);return x;}
void WriteHash(std::ostream& f,const char* p){f.write(p,64);}
std::string ReadHash(std::istream& f){std::string s(64,'\0');f.read(&s[0],64);return s;}
struct Primitive {
    uint32_t d=0,m=0,metric=0;uint64_t train=0,nodes=0;int64_t file=0;
    std::string context,model,codebook,payload,backend;
    std::vector<float> centers,query,distances;std::vector<uint8_t> codes;
};
void Write(const std::string& path,const Primitive& p)
{
    std::ofstream f(path,std::ios::binary|std::ios::trunc);f.exceptions(std::ios::badbit|std::ios::failbit);f.write("PQTEST02",8);
    for(uint64_t x:{(uint64_t)PQ_CPU_PERSIST_VERSION,(uint64_t)PQ_CPU_VERSION,(uint64_t)PQ_CPU_ALGORITHM_VERSION,(uint64_t)256,(uint64_t)p.d,(uint64_t)p.m,(uint64_t)p.metric,p.train,p.nodes,(uint64_t)p.file})Write64(f,x);
    for(const auto* s:{&p.context,&p.model,&p.codebook,&p.payload,&p.backend})WriteHash(f,s->c_str());
    for(float x:p.centers)WriteFloat(f,x);for(float x:p.query)WriteFloat(f,x);for(float x:p.distances)WriteFloat(f,x);
    f.write(reinterpret_cast<const char*>(p.codes.data()),p.codes.size());f.close();
}
Primitive Read(const std::string& path)
{
    std::ifstream f(path,std::ios::binary);f.exceptions(std::ios::badbit|std::ios::failbit);char magic[8];f.read(magic,8);Check(!std::memcmp(magic,"PQTEST02",8),"test primitive magic mismatch");
    Check(Read64(f)==PQ_CPU_PERSIST_VERSION&&Read64(f)==PQ_CPU_VERSION&&Read64(f)==PQ_CPU_ALGORITHM_VERSION&&Read64(f)==256,"test primitive versions mismatch");
    Primitive p;p.d=Read64(f);p.m=Read64(f);p.metric=Read64(f);p.train=Read64(f);p.nodes=Read64(f);p.file=Read64(f);
    /* Expected catalog identity for this independent producer/consumer pair.
     * The original process receipt supplies expected model/code roots too. */
    Check(p.d==8&&p.m==2&&p.metric>=1&&p.metric<=3&&p.train==1024&&p.nodes==2057&&p.file==70+p.metric,"test primitive unexpected shape/source");
    p.context=ReadHash(f);p.model=ReadHash(f);p.codebook=ReadHash(f);p.payload=ReadHash(f);p.backend=ReadHash(f);
    Check(p.context==CONTEXT,"test primitive differs from catalog expected context");
    p.centers.resize(256*p.d);p.query.resize(p.d);p.distances.resize(p.nodes);p.codes.resize(p.nodes*p.m);
    for(auto& x:p.centers)x=ReadFloat(f);for(auto& x:p.query)x=ReadFloat(f);for(auto& x:p.distances)x=ReadFloat(f);
    f.read(reinterpret_cast<char*>(p.codes.data()),p.codes.size());f.exceptions(std::ios::badbit);Check(f.peek()==EOF,"test primitive has extra bytes");return p;
}
void Produce(const std::string& dir)
{
    auto backend=Backend();auto l=Limits();PqCpuError e{};std::printf("{\"status\":\"pass\",\"cases\":[");
    for(uint32_t metric=1;metric<=3;++metric){Model model;Train(model,8,2,metric);auto mi=Info(model);auto centers=Centers(model);auto data=Data(0,2057,8,metric);
        PqCpuEncodeSpec old{sizeof(old),1,70+metric,2057,mi.context_hash,mi.model_hash};Codes baseline;
        Ok(pq_cpu_encode(model.p,&old,&l,data.data(),data.size(),&baseline.p,&e),e);auto ci=Info(baseline);auto bytes=Bytes(baseline);
        auto query=Data(19,1,8,metric);Query lut;Ok(pq_cpu_query_open(model.p,&l,query.data(),query.size(),&lut.p,&e),e);
        std::vector<float> distances(2057);Ok(pq_cpu_distances(baseline.p,lut.p,&l,0,2057,distances.data(),distances.size(),&e),e);
        auto bs=BuildSpec(mi,backend,2057,PQ_CPU_INPUT_FLOATS,ci.payload_hash,70+metric);Builder builder;Open(builder,model,bs,l);
        /* Test the strong ownership contract: the original opaque model dies
         * before the first append. No full 2057*d span is passed to Builder. */
        pq_cpu_model_close(&model.p);AppendAll(builder,mi,2057,l);Codes streamed;Finish(builder,streamed,l);SameInfo(ci,Info(streamed));Check(Bytes(streamed)==bytes,"stream encoder differs from monolithic codes");
        Primitive p;p.d=8;p.m=2;p.metric=metric;p.train=1024;p.nodes=2057;p.file=70+metric;p.context=mi.context_hash;p.model=mi.model_hash;p.codebook=mi.payload_hash;
        p.payload=ci.payload_hash;p.backend=backend.backend_hash;p.centers=centers;p.query=query;p.distances=distances;p.codes=bytes;
        std::string path=dir+"/metric-"+std::to_string(metric)+".primitive";Write(path,p);
        if(metric>1)std::printf(",");std::printf("{\"metric\":%u,\"model_hash\":\"%s\",\"payload_hash\":\"%s\",\"codebook_hash\":\"%s\",\"backend_hash\":\"%s\"}",metric,mi.model_hash,ci.payload_hash,mi.payload_hash,backend.backend_hash);
    }std::printf("],\"checks\":%llu,\"maximum_input_batch_rows\":1024}\n",(unsigned long long)checks);
}
void Restore(Model& model,const Primitive& p)
{
    auto l=Limits();PqCpuError e{};PqCpuModelRestoreSpec s{sizeof(s),1,1,1,p.d,p.m,256,p.metric,p.train,p.context.c_str(),p.codebook.c_str(),p.model.c_str(),p.backend.c_str()};
    Ok(pq_cpu_model_restore(&s,&l,p.centers.data(),p.centers.size(),&model.p,&e),e);
}
void Consume(const std::string& path,const char* expected_model,const char* expected_payload)
{
    auto p=Read(path);Check(p.model==expected_model&&p.payload==expected_payload,"artifact differs from externally expected catalog hashes");Model model;Restore(model,p);
    auto mi=Info(model);auto restored_centers=Centers(model);Check(restored_centers.size()==p.centers.size()&&!std::memcmp(restored_centers.data(),p.centers.data(),p.centers.size()*sizeof(float)),"restored codebook primitive bits differ");auto backend=Backend();auto l=Limits();PqCpuError e{};
    auto s=BuildSpec(mi,backend,p.nodes,PQ_CPU_INPUT_CODES,p.payload.c_str(),p.file);Builder b;Open(b,model,s,l);RawAll(b,p.codes,p.nodes,p.m,l);Codes c;Finish(b,c,l);
    Check(Bytes(c)==p.codes,"restored code bytes differ");auto ci=Info(c);Check(ci.source_file_id==p.file&&ci.rows==p.nodes&&!std::strcmp(ci.payload_hash,p.payload.c_str()),"restored code identity differs");
    Query q;Ok(pq_cpu_query_open(model.p,&l,p.query.data(),p.query.size(),&q.p,&e),e);std::vector<float> distances(p.nodes);
    Ok(pq_cpu_distances(c.p,q.p,&l,0,p.nodes,distances.data(),distances.size(),&e),e);double maximum=0;
    for(uint64_t r=0;r<p.nodes;++r){Check(distances[r]==p.distances[r],"cross-process LUT distance differs bitwise");double oracle=0;
        for(uint32_t d=0;d<p.d;++d){float x=p.centers[(uint32_t)p.codes[r*p.m+d/(p.d/p.m)]*p.d+d];
            if(p.metric==PQ_CPU_L2){double delta=(double)p.query[d]-x;oracle+=delta*delta;}else oracle-=(double)p.query[d]*x;}
        Near(distances[r],oracle);maximum=std::max(maximum,std::fabs((double)distances[r]-oracle));}
    uint64_t nonunit=0;if(p.metric==PQ_CPU_COSINE)for(uint32_t k=0;k<256;++k){double norm=0;for(uint32_t d=0;d<p.d;++d)norm+=(double)p.centers[k*p.d+d]*p.centers[k*p.d+d];if(std::fabs(norm-1)>1e-5)++nonunit;}
    if(p.metric==PQ_CPU_COSINE)Check(nonunit>0,"cosine fixture must contain nonunit centers");
    pq_cpu_model_close(&model.p);pq_cpu_model_close(&model.p);pq_cpu_codes_close(&c.p);pq_cpu_codes_close(&c.p);pq_cpu_codes_builder_close(&b.p);pq_cpu_codes_builder_close(&b.p);
    std::printf("{\"status\":\"pass\",\"metric\":%u,\"nodes\":%llu,\"checks\":%llu,\"nonunit_cosine_centers\":%llu,\"maximum_reconstructed_error\":%.12g}\n",p.metric,(unsigned long long)p.nodes,(unsigned long long)checks,(unsigned long long)nonunit,maximum);
}
void Poisoned(Builder& b)
{
    auto i=Info(b);Check(i.state==PQ_CPU_BUILDER_FAILED,"failed mutation did not poison builder");auto l=Limits();PqCpuError e{};Codes out;
    Check(pq_cpu_codes_builder_finish(b.p,&l,&out.p,&e)==PQ_CPU_STATE&&!out.p,"failed builder published codes on retry");
}
void Negatives(const std::string& path)
{
    auto p=Read(path);Model model;Restore(model,p);auto mi=Info(model);auto backend=Backend();auto l=Limits();PqCpuError e{};
    auto valid=RestoreSpec(mi,backend);
    for(int test=0;test<24;++test){auto s=valid;auto centers=p.centers;const float* data=centers.data();uint64_t count=centers.size();
        if(test==0)s.struct_size--;if(test==1)s.version++;if(test==2)s.component_version++;if(test==3)s.algorithm_version++;
        if(test==4)s.centroids=255;if(test==5)s.dimension=0;if(test==6)s.dimension=1537;if(test==7)s.chunks=0;if(test==8)s.chunks=3;
        if(test==9)s.metric=0;if(test==10)s.metric=4;if(test==11)s.training_rows=255;if(test==12)s.training_rows=1025;
        if(test==13)s.context_hash=OTHER;if(test==14)s.codebook_hash=OTHER;if(test==15)s.model_hash=OTHER;if(test==16)s.backend_hash=OTHER;
        if(test==17)count--;if(test==18)count++;if(test==19)data=nullptr;if(test==20)centers[0]=std::nextafter(centers[0],INFINITY);
        if(test==21)centers[0]=std::numeric_limits<float>::quiet_NaN();if(test==22)centers[0]=INFINITY;if(test==23)centers[0]=1e30f;
        Model out;Check(pq_cpu_model_restore(&s,&l,data,count,&out.p,&e)!=PQ_CPU_OK&&!out.p,"invalid primitive model published");}
    for(int test=0;test<19;++test){auto s=BuildSpec(mi,backend,p.nodes,PQ_CPU_INPUT_CODES,p.payload.c_str(),p.file);
        if(test==0)s.struct_size--;if(test==1)s.version++;if(test==2)s.component_version++;if(test==3)s.algorithm_version++;
        if(test==4)s.dimension++;if(test==5)s.chunks=0;if(test==6)s.centroids=255;if(test==7)s.metric=4;
        if(test==8)s.context_hash=OTHER;if(test==9)s.model_hash=OTHER;if(test==10)s.backend_hash=OTHER;if(test==11)s.source_file_id=-1;
        if(test==12)s.node_count=PQ_CPU_MAX_NODES+1;if(test==13)s.input_kind=0;if(test==14)s.expected_payload_hash=nullptr;if(test==15)s.expected_payload_hash="bad";
        if(test==16)s.source_file_id++;if(test==17)s.node_count--;if(test==18)s.node_count++;
        Builder b;int rc=pq_cpu_codes_builder_open(model.p,&s,&l,&b.p,&e);
        if(test<16)Check(rc!=PQ_CPU_OK&&!b.p,"invalid code builder identity published");
        else{Ok(rc,e);std::vector<uint8_t> data(s.node_count*p.m,0);std::copy_n(p.codes.begin(),std::min(data.size(),p.codes.size()),data.begin());RawAll(b,data,s.node_count,p.m,l);Codes out;
            Check(pq_cpu_codes_builder_finish(b.p,&l,&out.p,&e)==PQ_CPU_INVALID&&!out.p,"wrong source/N matched original payload hash");Poisoned(b);}}
    for(int test=0;test<18;++test){auto s=BuildSpec(mi,backend,p.nodes,test>=12?PQ_CPU_INPUT_CODES:PQ_CPU_INPUT_FLOATS,p.payload.c_str(),p.file);Builder b;Open(b,model,s,l);
        auto data=Data(0,1024,p.d,p.metric);uint64_t first=0,rows=1024,count=data.size();const float* values=data.data();int rc=0;
        if(test==0)first=1;if(test==1)rows=0;if(test==2)rows=1025;if(test==3)first=UINT64_MAX;if(test==4)count--;if(test==5)count++;if(test==6)values=nullptr;
        if(test==7)data[0]=1e30f;
        if(test==8||test==9){Append(b,mi,0,1024,l);first=test==8?0:1025;}
        if(test==10){Codes out;rc=pq_cpu_codes_builder_finish(b.p,&l,&out.p,&e);Check(!out.p,"incomplete builder published");}
        else if(test==11)rc=pq_cpu_codes_builder_append_raw(b.p,&l,0,1024,p.codes.data(),1024*p.m,&e);
        else if(test==12)rc=pq_cpu_codes_builder_append(b.p,&l,0,1024,data.data(),data.size(),&e);
        else if(test>=13&&test<=15){const uint8_t* bytes=test==15?nullptr:p.codes.data();uint64_t n=1024*p.m+(test==13?-1:test==14?1:0);
            rc=pq_cpu_codes_builder_append_raw(b.p,&l,0,1024,bytes,n,&e);}
        else if(test==16){auto bytes=p.codes;bytes[17]^=1;RawAll(b,bytes,p.nodes,p.m,l);Codes out;rc=pq_cpu_codes_builder_finish(b.p,&l,&out.p,&e);Check(!out.p,"damaged bytecodes published");}
        else if(test==17){s.expected_payload_hash=OTHER;pq_cpu_codes_builder_close(&b.p);Open(b,model,s,l);RawAll(b,p.codes,p.nodes,p.m,l);Codes out;rc=pq_cpu_codes_builder_finish(b.p,&l,&out.p,&e);Check(!out.p,"wrong payload root published");}
        else rc=pq_cpu_codes_builder_append(b.p,&l,first,rows,values,count,&e);
        Check(rc!=PQ_CPU_OK,"invalid append/finish accepted");Poisoned(b);}
    /* A successful terminal state cannot publish another ownership alias. */
    {auto s=BuildSpec(mi,backend,0);Builder b;Open(b,model,s,l);Codes out;Finish(b,out,l);Check(Info(out).rows==0,"empty dense range rejected");
        Codes again;Check(pq_cpu_codes_builder_finish(b.p,&l,&again.p,&e)==PQ_CPU_STATE&&!again.p,"second finish created ownership alias");
        Check(pq_cpu_codes_builder_append(b.p,&l,0,1,p.query.data(),p.d,&e)==PQ_CPU_STATE,"finished builder accepted append");Check(Info(b).state==PQ_CPU_BUILDER_FINISHED,"finished state changed");}
    /* Exact declared open/raw/finish budget includes the private model copy. */
    {auto s=BuildSpec(mi,backend,p.nodes,PQ_CPU_INPUT_CODES,p.payload.c_str(),p.file);Builder a;Open(a,model,s,l);uint64_t need=Info(a).resident_bytes+mi.resident_bytes+65536;
        auto tight=l;tight.memory_bytes=need-1;Builder denied;Check(pq_cpu_codes_builder_open(model.p,&s,&tight,&denied.p,&e)==PQ_CPU_MEMORY&&!denied.p,"open admitted one byte below its budget");
        tight.memory_bytes=need;Open(denied,model,s,tight);auto info=Info(denied);tight.memory_bytes=info.resident_bytes+65536;RawAll(denied,p.codes,p.nodes,p.m,tight);Codes out;Finish(denied,out,tight);}
    {auto s=BuildSpec(mi,backend,p.nodes);Builder b;Open(b,model,s,l);auto poor=l;poor.memory_bytes=1;auto data=Data(0,1,p.d,p.metric);
        Check(pq_cpu_codes_builder_append(b.p,&poor,0,1,data.data(),data.size(),&e)==PQ_CPU_MEMORY,"append ignored declared budget");Poisoned(b);}
    {auto s=BuildSpec(mi,backend,p.nodes,PQ_CPU_INPUT_CODES,p.payload.c_str(),p.file);Builder b;Open(b,model,s,l);auto poor=l;poor.memory_bytes=1;
        Check(pq_cpu_codes_builder_append_raw(b.p,&poor,0,1,p.codes.data(),p.m,&e)==PQ_CPU_MEMORY,"raw append ignored declared budget");Poisoned(b);}
    {auto s=BuildSpec(mi,backend,0);Builder b;Open(b,model,s,l);auto poor=l;poor.memory_bytes=1;Codes out;
        Check(pq_cpu_codes_builder_finish(b.p,&poor,&out.p,&e)==PQ_CPU_MEMORY&&!out.p,"finish ignored declared budget");Poisoned(b);}
    /* Another actual model/context is rejected even with matching shape. */
    {Model other;Train(other,p.d,p.m,p.metric,1024,OTHER);auto s=BuildSpec(mi,backend,p.nodes,PQ_CPU_INPUT_CODES,p.payload.c_str(),p.file);Builder b;
        Check(pq_cpu_codes_builder_open(other.p,&s,&l,&b.p,&e)==PQ_CPU_INVALID&&!b.p,"cross-context actual model accepted");}
    {Model other;Train(other,p.d,p.m,p.metric,256);auto s=BuildSpec(mi,backend,p.nodes,PQ_CPU_INPUT_CODES,p.payload.c_str(),p.file);Builder b;
        Check(pq_cpu_codes_builder_open(other.p,&s,&l,&b.p,&e)==PQ_CPU_INVALID&&!b.p,"same-context different actual model accepted");}
    {auto* before=model.p;Check(pq_cpu_model_restore(&valid,&l,p.centers.data(),p.centers.size(),&model.p,&e)==PQ_CPU_INVALID&&model.p==before,"restore overwrote occupied model output");
        auto s=BuildSpec(mi,backend,0);Builder b;Open(b,model,s,l);auto* old=b.p;
        Check(pq_cpu_codes_builder_open(model.p,&s,&l,&b.p,&e)==PQ_CPU_INVALID&&b.p==old&&Info(b).state==PQ_CPU_BUILDER_ACTIVE,"open overwrote occupied builder output");
        Codes c;Finish(b,c,l);Builder next;Open(next,model,s,l);auto* prior=c.p;
        Check(pq_cpu_codes_builder_finish(next.p,&l,&c.p,&e)==PQ_CPU_INVALID&&c.p==prior,"finish overwrote occupied codes output");Poisoned(next);}
    /* Legal zero/subnormal primitive centers are restored without requiring
     * a training run or adding a spurious lower-magnitude bound at reopen. */
    for(uint32_t metric:{(uint32_t)PQ_CPU_L2,(uint32_t)PQ_CPU_NEGATIVE_IP})for(int pattern=0;pattern<2;++pattern){
        std::vector<float> values(1024*3,pattern?std::numeric_limits<float>::denorm_min()*16:0);PqCpuTrainSpec ts{sizeof(ts),1,3,1,metric,1024,CONTEXT};Model original;
        Ok(pq_cpu_train(&ts,&l,values.data(),values.size(),&original.p,&e),e);auto oi=Info(original);auto rs=RestoreSpec(oi,backend);auto c=Centers(original);Model restored;
        Ok(pq_cpu_model_restore(&rs,&l,c.data(),c.size(),&restored.p,&e),e);SameInfo(oi,Info(restored));Check(Centers(restored)==c,"zero/subnormal centers changed at restore");}
    std::printf("{\"status\":\"pass\",\"checks\":%llu,\"model_negative_cases\":24,\"codes_identity_cases\":19,\"builder_negative_cases\":18}\n",(unsigned long long)checks);
}
void Fault(const std::string& stage,uint64_t point,bool cancel)
{
    Model model;Train(model,3,1,PQ_CPU_L2,256);auto mi=Info(model);auto backend=Backend();auto centers=Centers(model);auto l=Limits();PqCpuError e{};
    auto restore=RestoreSpec(mi,backend);auto s=BuildSpec(mi,backend,2057);Builder b;Codes out;Model reopened;auto data=Data(0,1024,3,1);
    bool raw=stage.find("raw-")==0;
    if(raw){PqCpuEncodeSpec es{sizeof(es),1,71,2057,mi.context_hash,mi.model_hash};auto all=Data(0,2057,3,1);Codes encoded;
        Ok(pq_cpu_encode(model.p,&es,&l,all.data(),all.size(),&encoded.p,&e),e);auto ci=Info(encoded);auto bytes=Bytes(encoded);s.input_kind=PQ_CPU_INPUT_CODES;
        /* Keep expected hash storage alive throughout open (open copies it). */
        s.expected_payload_hash=ci.payload_hash;Open(b,model,s,l);
        uint64_t first=stage=="raw-first"?0:stage=="raw-middle"?1024:2048;
        if(first){PqCpuError x{};Ok(pq_cpu_codes_builder_append_raw(b.p,&l,0,1024,bytes.data(),1024,&x),x);}
        if(first>1024){PqCpuError x{};Ok(pq_cpu_codes_builder_append_raw(b.p,&l,1024,1024,bytes.data()+1024,1024,&x),x);}
        Cancellation c;c.after=cancel?point:0;auto limits=Limits(&c);fail_at=cancel?0:point;tracking=true;
        int rc=pq_cpu_codes_builder_append_raw(b.p,&limits,first,std::min<uint64_t>(1024,2057-first),bytes.data()+first,std::min<uint64_t>(1024,2057-first),&e);tracking=false;
        if(point){Check(rc==(cancel?PQ_CPU_CANCELLED:PQ_CPU_MEMORY),"raw append fault status");Poisoned(b);}else Ok(rc,e);
        pq_cpu_codes_builder_close(&b.p);pq_cpu_codes_builder_close(&b.p);Check(live==0&&live_bytes==0&&live_kinds[0]==0&&live_kinds[1]==0,"raw stage allocation leak");
        std::printf("{\"status\":\"pass\",\"stage\":\"%s\",\"calls\":%zu,\"polls\":%llu,\"failure_index\":%llu,\"code\":%d,\"outstanding_scalar\":%zu,\"outstanding_array\":%zu}\n",stage.c_str(),calls,(unsigned long long)c.polls,(unsigned long long)point,rc,live_kinds[0],live_kinds[1]);return;
    }
    uint64_t first=stage=="append-middle"?1024:stage=="append-last"?2048:0,rows=std::min<uint64_t>(1024,2057-first);
    if(stage!="restore"&&stage!="open"){Open(b,model,s,l);if(stage=="finish")AppendAll(b,mi,2057,l);else{if(first)Append(b,mi,0,1024,l);if(first>1024)Append(b,mi,1024,1024,l);}data=Data(first,rows,3,1);}
    Cancellation c;c.after=cancel?point:0;auto limits=Limits(&c);fail_at=cancel?0:point;tracking=true;int rc=PQ_CPU_STATE;
    if(stage=="restore")rc=pq_cpu_model_restore(&restore,&limits,centers.data(),centers.size(),&reopened.p,&e);
    else if(stage=="open")rc=pq_cpu_codes_builder_open(model.p,&s,&limits,&b.p,&e);
    else if(stage=="finish")rc=pq_cpu_codes_builder_finish(b.p,&limits,&out.p,&e);
    else rc=pq_cpu_codes_builder_append(b.p,&limits,first,rows,data.data(),data.size(),&e);
    tracking=false;
    if(point){Check(rc==(cancel?PQ_CPU_CANCELLED:PQ_CPU_MEMORY)&&e.code==rc,"failure did not stay in C boundary");
        if(stage=="restore")Check(!reopened.p,"failed restore published handle");else if(stage=="open")Check(!b.p,"failed open published handle");else{Check(!out.p,"failed finish published codes");Poisoned(b);}
        if(!cancel)Check(failures==1,"requested real allocation was not failed");}
    else Ok(rc,e);
    pq_cpu_codes_close(&out.p);pq_cpu_codes_close(&out.p);pq_cpu_codes_builder_close(&b.p);pq_cpu_codes_builder_close(&b.p);pq_cpu_model_close(&reopened.p);pq_cpu_model_close(&reopened.p);
    Check(live==0&&live_bytes==0&&live_kinds[0]==0&&live_kinds[1]==0,"range/restore scalar or array allocation leak");
    std::printf("{\"status\":\"pass\",\"stage\":\"%s\",\"calls\":%zu,\"scalar_calls\":%zu,\"array_calls\":%zu,\"polls\":%llu,\"failure_index\":%llu,\"code\":%d,\"outstanding_scalar\":%zu,\"outstanding_array\":%zu}\n",stage.c_str(),calls,kinds[0],kinds[1],(unsigned long long)c.polls,(unsigned long long)point,rc,live_kinds[0],live_kinds[1]);
}
void Million()
{
    auto backend=Backend();auto l=Limits();PqCpuError e{};Model model;Train(model,1,1,1,256);auto mi=Info(model);auto centers=Centers(model);
    auto s=BuildSpec(mi,backend,PQ_CPU_MAX_NODES);Builder b;Open(b,model,s,l);pq_cpu_model_close(&model.p);
    auto values=Data(0,256,1,1);
    for(uint64_t first=0;first<PQ_CPU_MAX_NODES;first+=1024){std::vector<float> batch(1024);for(uint64_t j=0;j<1024;++j)batch[j]=values[(first+j)%256];
        Ok(pq_cpu_codes_builder_append(b.p,&l,first,1024,batch.data(),batch.size(),&e),e);}
    Codes codes;Finish(b,codes,l);auto bytes=Bytes(codes);for(uint64_t r=0;r<PQ_CPU_MAX_NODES;++r)Check(centers[bytes[r]]==values[r%256],"million streamed dense node mismatch");
    /* Cancel after several 64KiB digest blocks have already been consumed.
     * Partial internal SHA state must never publish a restored Codes handle. */
    Model restored;auto rs=RestoreSpec(mi,backend);Ok(pq_cpu_model_restore(&rs,&l,centers.data(),centers.size(),&restored.p,&e),e);
    auto ci=Info(codes);auto raw=BuildSpec(mi,backend,PQ_CPU_MAX_NODES,PQ_CPU_INPUT_CODES,ci.payload_hash);Builder interrupted;Open(interrupted,restored,raw,l);
    RawAll(interrupted,bytes,PQ_CPU_MAX_NODES,1,l);Cancellation cancellation;cancellation.after=5;auto cancelled=Limits(&cancellation);Codes absent;
    tracking=true;int rc=pq_cpu_codes_builder_finish(interrupted.p,&cancelled,&absent.p,&e);tracking=false;
    Check(rc==PQ_CPU_CANCELLED&&!absent.p&&cancellation.polls==5&&live==0,"long digest cancellation published/leaked partial restore");Poisoned(interrupted);
    Model wide;Train(wide,64,64,1,256);auto wi=Info(wide);auto high=BuildSpec(wi,backend,PQ_CPU_MAX_NODES);Builder rejected;
    Check(pq_cpu_codes_builder_open(wide.p,&high,&l,&rejected.p,&e)==PQ_CPU_MEMORY&&!rejected.p,"high-M full codes exceed declared64MiB");
    std::printf("{\"status\":\"pass\",\"nodes\":%u,\"checks\":%llu,\"max_input_batch_rows\":1024,\"full_input_matrix_allocated\":false,\"partial_large_hash_cancelled\":true}\n",PQ_CPU_MAX_NODES,(unsigned long long)checks);
}
void Shapes()
{
    auto backend=Backend();auto l=Limits();PqCpuError e{};
    for(uint32_t d:{(uint32_t)3,(uint32_t)1536}){uint32_t m=d==3?1:16;Model original;Train(original,d,m,PQ_CPU_L2,256);auto mi=Info(original);auto centers=Centers(original);
        Model restored;auto rs=RestoreSpec(mi,backend);Ok(pq_cpu_model_restore(&rs,&l,centers.data(),centers.size(),&restored.p,&e),e);SameInfo(mi,Info(restored));
        auto copy=Centers(restored);Check(copy.size()==centers.size()&&!std::memcmp(copy.data(),centers.data(),centers.size()*sizeof(float)),"shape-boundary restore altered float bits");
        auto input=Data(0,17,d,PQ_CPU_L2);PqCpuEncodeSpec es{sizeof(es),1,71,17,mi.context_hash,mi.model_hash};Codes old;
        Ok(pq_cpu_encode(original.p,&es,&l,input.data(),input.size(),&old.p,&e),e);auto ci=Info(old);auto s=BuildSpec(mi,backend,17,PQ_CPU_INPUT_FLOATS,ci.payload_hash);Builder b;
        Open(b,restored,s,l);pq_cpu_model_close(&original.p);AppendAll(b,mi,17,l);Codes encoded;Finish(b,encoded,l);Check(Bytes(old)==Bytes(encoded),"shape-boundary streaming bytes changed");
        Query q;Ok(pq_cpu_query_open(restored.p,&l,input.data(),d,&q.p,&e),e);float a[17]{},z[17]{};
        Ok(pq_cpu_distances(old.p,q.p,&l,0,17,a,17,&e),e);Ok(pq_cpu_distances(encoded.p,q.p,&l,0,17,z,17,&e),e);
        for(int j=0;j<17;++j)Check(a[j]==z[j]&&std::isfinite(z[j]),"shape-boundary restored LUT differs");}
    std::printf("{\"status\":\"pass\",\"checks\":%llu,\"dimensions\":[3,1536],\"chunks\":[1,16]}\n",(unsigned long long)checks);
}
}
void* operator new(size_t n){return Allocate(n,0);}void* operator new[](size_t n){return Allocate(n,1);}
void operator delete(void* p)noexcept{if(p){Forget(p);std::free(p);}}void operator delete[](void* p)noexcept{::operator delete(p);}
void operator delete(void* p,size_t)noexcept{::operator delete(p);}void operator delete[](void* p,size_t)noexcept{::operator delete(p);}
int main(int argc,char** argv)
{
    try{if(argc==3&&std::string(argv[1])=="produce")Produce(argv[2]);
        else if(argc==5&&std::string(argv[1])=="consume")Consume(argv[2],argv[3],argv[4]);
        else if(argc==3&&std::string(argv[1])=="negatives")Negatives(argv[2]);
        else if(argc==4&&(std::string(argv[1])=="fault"||std::string(argv[1])=="cancel"))Fault(argv[2],std::stoull(argv[3]),std::string(argv[1])=="cancel");
        else if(argc==2&&std::string(argv[1])=="shapes")Shapes();
        else if(argc==2&&std::string(argv[1])=="million")Million();else throw std::runtime_error("invalid stream probe arguments");return 0;
    }catch(const std::exception& e){tracking=false;std::fprintf(stderr,"PQ range/persistence probe failed: %s\n",e.what());return 1;}
}
