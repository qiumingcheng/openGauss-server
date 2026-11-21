#include "pq_parquet_format.h"
#include "vendor/arrow20/abi.h"
#include <dlfcn.h>
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
struct Block {void* p;unsigned kind;};Block blocks[65536]{};bool tracking=false;size_t allocs=0,fail_at=0,failures=0,live[2]{};
void* Allocate(size_t n,unsigned kind){if(tracking&&++allocs==fail_at){++failures;throw std::bad_alloc();}void* p=std::malloc(n?n:1);if(!p)throw std::bad_alloc();if(tracking){bool found=false;for(auto& b:blocks)if(!b.p){b={p,kind};++live[kind];found=true;break;}if(!found)std::abort();}return p;}
void Forget(void* p){if(!live[0]&&!live[1])return;for(auto& b:blocks)if(b.p==p){--live[b.kind];b={};return;}}
uint64_t checks=0;void Check(bool c,const char* why){++checks;if(!c)throw std::runtime_error(why);}
void Ok(int c,const OgLakeSdkError& e){++checks;if(c)throw std::runtime_error(std::to_string(c)+":"+e.message);}
void Cpu(int c,const PqCpuError& e){++checks;if(c)throw std::runtime_error(std::to_string(c)+":"+e.message);}
const char CONTEXT[]="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const char SOURCE[]="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const char SCHEMA[]="cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
struct Cancel {uint64_t after=0,polls=0;};int Poll(void* p){auto* c=static_cast<Cancel*>(p);if(!c)return 0;++c->polls;return c->after&&c->polls>=c->after;}
uint64_t test_memory_bytes=64*1024*1024;
OgLakeSdkLimits Limits(Cancel* c=nullptr){return {sizeof(OgLakeSdkLimits),(int64_t)test_memory_bytes,1024*1024,Poll,c};}
struct Model {PqCpuModel* p=nullptr;~Model(){pq_cpu_model_close(&p);}};
struct Codes {PqCpuCodes* p=nullptr;~Codes(){pq_cpu_codes_close(&p);}};
struct CpuQuery {PqCpuQuery* p=nullptr;~CpuQuery(){pq_cpu_query_close(&p);}};
struct Writer {PqParquetWriter* p=nullptr;~Writer(){pq_parquet_writer_close(&p);}};
struct Reader {PqParquetReader* p=nullptr;~Reader(){pq_parquet_reader_close(&p);}};
struct Query {PqParquetQuery* p=nullptr;~Query(){pq_parquet_query_close(&p);}};
int cleanup_failures=0;
struct File {
    const OgLakeParquetLifecycleApi* life;OgLakeSdkWriter* p=nullptr;bool retain=false;
    explicit File(const OgLakeParquetLifecycleApi* l):life(l){}
    ~File(){if(p){OgLakeSdkError e{};int rc=retain?life->base->writer_close(&p,&e):life->writer_abort(&p,&e);if(rc)++cleanup_failures;}}
    void Open(const std::string& path,uint32_t kind,const PqParquetSpec& s,const OgLakeSdkLimits& limits){
        uint64_t a,c,d;OgLakeSdkError e{};Ok(pq_parquet_budget(&s,&limits,&a,&c,&d,&e),e);int32_t fields;const auto* f=pq_parquet_fields(kind,&fields);OgLakeSdkWriteOptions o{};
        o.struct_size=sizeof(o);o.path=path.c_str();o.limits=limits;o.limits.memory_bytes=d;o.fields=f;o.num_fields=fields;o.compression=OG_LAKE_SDK_ZSTD;o.row_group_rows=1024;Ok(life->base->writer_open(&o,&p,&e),e);}
    void Finish(uint64_t rows){OgLakeSdkWrittenFile f{};f.struct_size=sizeof(f);OgLakeSdkError e{};Ok(life->base->writer_finish(p,&f,&e),e);Check(f.num_rows==(int64_t)rows&&f.local_fsync_complete,"SDK file rows/fsync mismatch");}
    void Abort(){if(p){OgLakeSdkError e{};Ok(life->writer_abort(&p,&e),e);}}
};
std::vector<float> Data(uint64_t first,uint64_t n,uint32_t d,uint32_t metric)
{
    std::vector<float> data(n*d);for(uint64_t r=0;r<n;++r){double norm=0;for(uint32_t j=0;j<d;++j){int x=(int)(((first+r)*131+j*79+((first+r)*(j+1)%29)*17)%2039)-1019;float v=(float)x/1024;data[r*d+j]=v;norm+=(double)v*v;}
        if(metric==3){if(!norm){data[r*d]=1;norm=1;}for(uint32_t j=0;j<d;++j)data[r*d+j]=(float)(data[r*d+j]/std::sqrt(norm));}}return data;
}
PqParquetSpec Spec(uint32_t d,uint32_t m,uint32_t metric,uint64_t n)
{return {sizeof(PqParquetSpec),1,d,m,metric,1024,n,71,CONTEXT,SOURCE,SCHEMA};}
void Train(Model& m,const PqParquetSpec& s){auto data=Data(0,s.training_rows,s.dimension,s.metric);PqCpuTrainSpec t{sizeof(t),1,s.dimension,s.chunks,s.metric,s.training_rows,CONTEXT};PqCpuLimits l{sizeof(l),64*1024*1024,nullptr,nullptr};PqCpuError e{};Cpu(pq_cpu_train(&t,&l,data.data(),data.size(),&m.p,&e),e);}
PqParquetPaths Paths(const std::string& dir,const std::string& tag,std::string& m,std::string& c,std::string& h)
{m=dir+"/"+tag+"-model.parquet";c=dir+"/"+tag+"-codes.parquet";h=dir+"/"+tag+"-manifest.parquet";return {sizeof(PqParquetPaths),m.c_str(),c.c_str(),h.c_str()};}
void Oracle(const std::string& path,const std::vector<float>& centers,const std::vector<uint8_t>& codes,const std::vector<float>& query,const std::vector<float>& distances)
{
    std::ofstream f(path,std::ios::binary|std::ios::trunc);f.exceptions(std::ios::badbit|std::ios::failbit);
    f.write((const char*)centers.data(),centers.size()*4);f.write((const char*)codes.data(),codes.size());f.write((const char*)query.data(),query.size()*4);f.write((const char*)distances.data(),distances.size()*4);f.close();
}
PqParquetWritten Produce(const OgLakeParquetLifecycleApi* life,const std::string& dir,const std::string& tag,const PqParquetSpec& s)
{
    auto limits=Limits();Model model;Train(model,s);PqCpuError ce{};OgLakeSdkError e{};PqCpuInfo mi{};mi.struct_size=sizeof(mi);Cpu(pq_cpu_model_info(model.p,&mi,&ce),ce);
    auto input=Data(0,s.node_count,s.dimension,s.metric);PqCpuEncodeSpec es{sizeof(es),1,71,s.node_count,CONTEXT,mi.model_hash};PqCpuLimits cl{sizeof(cl),64*1024*1024,nullptr,nullptr};Codes codes;
    Cpu(pq_cpu_encode(model.p,&es,&cl,input.data(),input.size(),&codes.p,&ce),ce);std::vector<float> centers(256ull*s.dimension);std::vector<uint8_t> bytes(s.node_count*s.chunks);
    Cpu(pq_cpu_model_centers(model.p,centers.data(),centers.size(),&ce),ce);Cpu(pq_cpu_codes_export(codes.p,bytes.data(),bytes.size(),&ce),ce);
    auto query=Data(19,1,s.dimension,s.metric);CpuQuery q;Cpu(pq_cpu_query_open(model.p,&cl,query.data(),query.size(),&q.p,&ce),ce);std::vector<float> distances(s.node_count);
    Cpu(pq_cpu_distances(codes.p,q.p,&cl,0,s.node_count,distances.data(),distances.size(),&ce),ce);
    std::string mp,cp,hp;auto paths=Paths(dir,tag,mp,cp,hp);File mf(life),cf(life),hf(life);mf.Open(mp,1,s,limits);cf.Open(cp,2,s,limits);hf.Open(hp,3,s,limits);Writer w;
    Ok(pq_parquet_writer_open(life->base,&s,&limits,model.p,mf.p,cf.p,hf.p,&w.p,&e),e);pq_cpu_model_close(&model.p);
    for(uint64_t at=0;at<s.node_count;at+=1024){uint64_t n=std::min<uint64_t>(1024,s.node_count-at);auto part=Data(at,n,s.dimension,s.metric);Ok(pq_parquet_writer_append(w.p,&limits,at,n,part.data(),part.size(),&e),e);}
    PqParquetWritten written{};written.struct_size=sizeof(written);Ok(pq_parquet_writer_finish(w.p,&limits,&written,&e),e);
    mf.Finish(written.model_rows);cf.Finish(written.codes_rows);hf.Finish(written.manifest_rows);mf.retain=cf.retain=hf.retain=true;
    Oracle(dir+"/"+tag+".oracle",centers,bytes,query,distances);Reader r;Ok(pq_parquet_reader_open(life->base,&s,&limits,&paths,written.manifest_hash,&r.p,&e),e);
    std::vector<uint8_t> roundtrip(bytes.size());Ok(pq_parquet_codes(r.p,roundtrip.data(),roundtrip.size(),&e),e);Check(roundtrip==bytes,"Parquet batch encoding differs from original CPU encoder");return written;
}
void Consume(const OgLakeParquetLifecycleApi* life,const std::string& dir,const std::string& tag,const PqParquetSpec& s,const char* root)
{
    auto l=Limits();std::string mp,cp,hp;auto paths=Paths(dir,tag,mp,cp,hp);Reader r;OgLakeSdkError e{};Ok(pq_parquet_reader_open(life->base,&s,&l,&paths,root,&r.p,&e),e);
    PqParquetWritten w{};PqCpuInfo mi{},ci{};w.struct_size=sizeof(w);mi.struct_size=sizeof(mi);ci.struct_size=sizeof(ci);Ok(pq_parquet_reader_info(r.p,&w,&mi,&ci,&e),e);
    Check(ci.rows==s.node_count&&ci.source_file_id==s.source_file_id&&!std::strcmp(w.manifest_hash,root)&&!std::strcmp(mi.context_hash,s.context_hash),"persisted identity mismatch");
    std::ifstream f(dir+"/"+tag+".oracle",std::ios::binary);f.exceptions(std::ios::badbit|std::ios::failbit);std::vector<float> centers(256ull*s.dimension),query(s.dimension),expected(s.node_count);std::vector<uint8_t> bytes(s.node_count*s.chunks);
    f.read((char*)centers.data(),centers.size()*4);f.read((char*)bytes.data(),bytes.size());f.read((char*)query.data(),query.size()*4);f.read((char*)expected.data(),expected.size()*4);
    std::vector<float> actual_centers(centers.size()),distances(expected.size());std::vector<uint8_t> actual(bytes.size());
    Ok(pq_parquet_centers(r.p,actual_centers.data(),actual_centers.size(),&e),e);Ok(pq_parquet_codes(r.p,actual.data(),actual.size(),&e),e);
    Check(!std::memcmp(centers.data(),actual_centers.data(),centers.size()*4)&&actual==bytes,"primitive model/code bits differ after producer exit");
    Query q;Ok(pq_parquet_query_open(r.p,&l,query.data(),query.size(),&q.p,&e),e);Ok(pq_parquet_distances(r.p,q.p,&l,0,s.node_count,distances.data(),distances.size(),&e),e);
    double maximum=0;for(uint64_t at=0;at<s.node_count;++at){Check(distances[at]==expected[at],"Parquet LUT distance differs from original component");double oracle=0;
        for(uint32_t d=0;d<s.dimension;++d){double x=centers[(uint32_t)bytes[at*s.chunks+d/(s.dimension/s.chunks)]*s.dimension+d];if(s.metric==1){double delta=query[d]-x;oracle+=delta*delta;}else oracle-=(double)query[d]*x;}
        double delta=std::fabs(distances[at]-oracle);maximum=std::max(maximum,delta);Check(std::isfinite(distances[at])&&delta<=1e-4*std::max(1.0,std::fabs(oracle)),"Parquet LUT reconstructed-vector oracle mismatch");}
    float canary[2]={19,23};Check(pq_parquet_distances(r.p,q.p,&l,s.node_count,1,canary,2,&e)==OG_LAKE_SDK_INVALID&&canary[0]==19&&canary[1]==23,"invalid distance range changed output");
    pq_parquet_query_close(&q.p);pq_parquet_query_close(&q.p);pq_parquet_reader_close(&r.p);pq_parquet_reader_close(&r.p);
    std::printf("{\"status\":\"pass\",\"checks\":%llu,\"metric\":%u,\"dimension\":%u,\"nodes\":%llu,\"maximum_reconstructed_error\":%.12g}\n",(unsigned long long)checks,s.metric,s.dimension,(unsigned long long)s.node_count,maximum);
}
struct Raw {int64_t id;std::string bytes;};
std::vector<Raw> ReadRaw(const OgLakeParquetSdkApi* api,const char* path,const OgLakeSdkLimits& l)
{
    OgLakeSdkReader* reader=nullptr;OgLakeSdkBatch* batch=nullptr;OgLakeSdkError e{};std::vector<Raw> rows;OgLakeSdkReadOptions o{};o.struct_size=sizeof(o);o.path=path;o.limits=l;o.batch_rows=1024;o.num_field_ids=-1;o.num_row_groups=-1;
    Ok(api->reader_open(&o,&reader,&e),e);try{for(;;){int code=api->reader_next(reader,&batch,&e);if(code==OG_LAKE_SDK_EOF)break;Ok(code,e);
            auto a=batch->array->children[0],b=batch->array->children[1];auto ids=(const int64_t*)a->buffers[1];auto off=(const int32_t*)b->buffers[1];
            for(int64_t i=0;i<batch->num_rows;++i){int64_t at=b->offset+i;rows.push_back({ids[a->offset+i],std::string((const char*)b->buffers[2]+off[at],off[at+1]-off[at])});}api->batch_release(&batch);}
        api->reader_close(&reader);return rows;}catch(...){api->batch_release(&batch);api->reader_close(&reader);throw;}
}
void ReleaseArray(ArrowArray* a){a->release=nullptr;}void ReleaseSchema(ArrowSchema* s){s->release=nullptr;}
void WriteRaw(const OgLakeParquetLifecycleApi* life,const std::string& path,uint32_t kind,const PqParquetSpec& s,const std::vector<Raw>& rows)
{
    auto l=Limits();File file(life);file.Open(path,kind,s,l);int32_t count;auto fields=pq_parquet_fields(kind,&count);
    for(size_t first=0;first<rows.size();first+=1024){size_t n=std::min<size_t>(1024,rows.size()-first);std::vector<int64_t> ids(n);std::vector<int32_t> off(n+1);std::string bytes;
        for(size_t i=0;i<n;++i){ids[i]=rows[first+i].id;bytes+=rows[first+i].bytes;off[i+1]=bytes.size();}
        ArrowArray root{},arrays[2]{};ArrowSchema schema{},defs[2]{};ArrowArray* children[]={&arrays[0],&arrays[1]};ArrowSchema* schemas[]={&defs[0],&defs[1]};const void* rb[]={nullptr},*ib[]={nullptr,ids.data()},*bb[]={nullptr,off.data(),bytes.data()};
        root.length=n;root.n_buffers=1;root.buffers=rb;root.n_children=2;root.children=children;root.release=ReleaseArray;schema.format="+s";schema.n_children=2;schema.children=schemas;schema.release=ReleaseSchema;
        for(int i=0;i<2;++i){arrays[i].length=n;arrays[i].n_buffers=i?3:2;arrays[i].buffers=i?bb:ib;arrays[i].release=ReleaseArray;defs[i].name=fields[i].name;defs[i].format=i?"z":"l";defs[i].release=ReleaseSchema;}
        OgLakeSdkError e{};Ok(life->base->writer_append(file.p,&root,&schema,&e),e);}
    file.Finish(rows.size());file.retain=true;
}
void Negatives(const OgLakeParquetLifecycleApi* life,const std::string& dir,const std::string& tag,const PqParquetSpec& valid,const char* root)
{
    std::string mp,cp,hp;auto paths=Paths(dir,tag,mp,cp,hp);auto l=Limits();OgLakeSdkError e{};
    for(int test=0;test<13;++test){auto s=valid;auto p=paths;auto limits=l;const char* expected=root;
        if(test==0)s.context_hash=SOURCE;if(test==1)s.source_version_hash=SCHEMA;if(test==2)s.schema_hash=SOURCE;if(test==3)s.source_file_id++;
        if(test==4)s.node_count--;if(test==5)s.metric=2;if(test==6)s.dimension=3;if(test==7)s.version=2;if(test==8)expected=SOURCE;
        if(test==9)limits.memory_bytes=1024;if(test==10)p.codes_path="/nonexistent/pq-file";if(test==11)s.training_rows=256;if(test==12)s.chunks=4;
        Reader r;int rc=pq_parquet_reader_open(life->base,&s,&limits,&p,expected,&r.p,&e);Check(rc!=0&&!r.p,"wrong expected source/hash/budget published reader");}
    uint64_t malformed=0;
    for(int mode=0;mode<7;++mode){uint32_t kind=mode<4?2:mode<6?1:3;const char* original=kind==1?mp.c_str():kind==2?cp.c_str():hp.c_str();auto rows=ReadRaw(life->base,original,l);
        if(mode==0)rows[0].bytes[0]^=1;if(mode==1)std::swap(rows[0],rows[1]);if(mode==2)rows[0].bytes+='x';if(mode==3)rows.pop_back();
        if(mode==4)rows[0].bytes[0]^=1;if(mode==5){float huge=1e30f;std::memcpy(&rows[0].bytes[0],&huge,4);}if(mode==6)rows[10].bytes[0]='f';
        std::string bad=dir+"/bad-"+std::to_string(mode)+".parquet";WriteRaw(life,bad,kind,valid,rows);auto p=paths;if(kind==1)p.model_path=bad.c_str();else if(kind==2)p.codes_path=bad.c_str();else p.manifest_path=bad.c_str();
        Reader r;int rc=pq_parquet_reader_open(life->base,&valid,&l,&p,root,&r.p,&e);Check(rc==OG_LAKE_SDK_INVALID&&!r.p,"legal-schema damaged primitive published reader");++malformed;}
    {std::ifstream f(cp,std::ios::binary);std::string bytes((std::istreambuf_iterator<char>(f)),{});Check(bytes.size()>16,"source Parquet too short");bytes.resize(bytes.size()-16);std::string path=dir+"/truncated-codes.parquet";std::ofstream o(path,std::ios::binary);o.write(bytes.data(),bytes.size());o.close();auto p=paths;p.codes_path=path.c_str();Reader r;Check(pq_parquet_reader_open(life->base,&valid,&l,&p,root,&r.p,&e)!=0&&!r.p,"truncated Parquet accepted");}
    std::printf("{\"status\":\"pass\",\"checks\":%llu,\"expected_identity_cases\":13,\"legal_schema_damage_cases\":%llu}\n",(unsigned long long)checks,(unsigned long long)malformed);
}
void FaultRead(const OgLakeParquetLifecycleApi* life,const std::string& dir,const std::string& tag,const PqParquetSpec& s,const char* root,uint64_t point,bool cancel)
{
    std::string mp,cp,hp;auto paths=Paths(dir,tag,mp,cp,hp);auto l=Limits();OgLakeSdkError e{};
    /* Warm provider/type singleton initialization outside the tracked target. */
    {Reader warm;Ok(pq_parquet_reader_open(life->base,&s,&l,&paths,root,&warm.p,&e),e);}
    Cancel c;c.after=cancel?point:0;l=Limits(&c);Reader reader;fail_at=cancel?0:point;tracking=true;
    int rc=pq_parquet_reader_open(life->base,&s,&l,&paths,root,&reader.p,&e);tracking=false;
    if(point)Check(rc==(cancel?OG_LAKE_SDK_CANCELLED:OG_LAKE_SDK_MEMORY)&&!reader.p,"real reader failure was not isolated");else Ok(rc,e);
    pq_parquet_reader_close(&reader.p);pq_parquet_reader_close(&reader.p);
    Check(!live[0]&&!live[1],"reader failure retained tracked scalar/array allocations");if(point&&!cancel)Check(failures==1,"requested reader allocation was not failed");
    std::printf("{\"status\":\"pass\",\"calls\":%zu,\"polls\":%llu,\"code\":%d,\"outstanding_scalar\":%zu,\"outstanding_array\":%zu}\n",allocs,(unsigned long long)c.polls,rc,live[0],live[1]);
}
void WriterNegatives(const OgLakeParquetLifecycleApi* life,const std::string& dir,const PqParquetSpec& s)
{
    auto limits=Limits();Model model;Train(model,s);auto input=Data(0,1024,s.dimension,s.metric);OgLakeSdkError e{};
    for(int mode=0;mode<9;++mode){std::string mp,cp,hp;auto paths=Paths(dir,"writer-invalid-"+std::to_string(mode),mp,cp,hp);(void)paths;
        File mf(life),cf(life),hf(life);mf.Open(mp,1,s,limits);cf.Open(cp,2,s,limits);hf.Open(hp,3,s,limits);Writer writer;Cancel cancel;
        if(mode==7){cancel.after=1;auto l=Limits(&cancel);Check(pq_parquet_writer_open(life->base,&s,&l,model.p,mf.p,cf.p,hf.p,&writer.p,&e)==OG_LAKE_SDK_CANCELLED&&!writer.p,"cancelled writer open published");}
        else{Ok(pq_parquet_writer_open(life->base,&s,&limits,model.p,mf.p,cf.p,hf.p,&writer.p,&e),e);int rc;
            auto l=limits;if(mode==3){cancel.after=1;l=Limits(&cancel);}if(mode==4)l.memory_bytes=1;
            PqParquetWritten out{};out.struct_size=sizeof(out);out.codes_rows=9999;
            if(mode==5)Ok(pq_parquet_writer_append(writer.p,&limits,0,1024,input.data(),input.size(),&e),e);
            if(mode==8){mf.Finish(256);for(uint64_t at=0;at<s.node_count;at+=1024){uint64_t n=std::min<uint64_t>(1024,s.node_count-at);auto values=Data(at,n,s.dimension,s.metric);Ok(pq_parquet_writer_append(writer.p,&limits,at,n,values.data(),values.size(),&e),e);}cancel.after=1;l=Limits(&cancel);}
            if(mode==2||mode==5||mode==8)rc=pq_parquet_writer_finish(writer.p,&l,&out,&e);
            else rc=pq_parquet_writer_append(writer.p,&l,mode==0?1:0,mode==1?1025:1024,input.data(),mode==6?input.size()-1:input.size(),&e);
            Check(rc!=0,"invalid writer mutation accepted");Check(pq_parquet_writer_finish(writer.p,&limits,&out,&e)==OG_LAKE_SDK_STATE&&out.codes_rows==9999,"failed writer published a manifest or resumed");}
        pq_parquet_writer_close(&writer.p);pq_parquet_writer_close(&writer.p);mf.Abort();cf.Abort();hf.Abort();
    }
    std::printf("{\"status\":\"pass\",\"checks\":%llu,\"writer_error_cases\":9,\"cleanup_failures\":%d}\n",(unsigned long long)checks,cleanup_failures);
}
}
void* operator new(size_t n){return Allocate(n,0);}void* operator new[](size_t n){return Allocate(n,1);}
void operator delete(void* p)noexcept{if(p){Forget(p);std::free(p);}}void operator delete[](void* p)noexcept{::operator delete(p);}
void operator delete(void* p,size_t)noexcept{::operator delete(p);}void operator delete[](void* p,size_t)noexcept{::operator delete(p);}
int main(int argc,char** argv)
{
    try{Check(argc>=5,"provider directory mode tag required");void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);Check(library,"load real Parquet SDK");auto entry=(OgLakeParquetLifecycleEntrypoint)dlsym(library,"og_lake_parquet_sdk_v2");Check(entry,"real SDK lifecycle entrypoint");auto life=entry();Check(life&&life->abi_version==2&&life->writer_abort,"actual SDK lifecycle contract");
        std::string dir=argv[2],mode=argv[3],tag=argv[4];uint32_t metric=argc>5?std::stoul(argv[5]):1;auto spec=Spec(8,2,metric,2057);
        if(tag=="empty")spec=Spec(8,2,1,0);if(tag=="d3")spec=Spec(3,1,1,17);if(tag=="d1536"){spec=Spec(1536,16,1,17);test_memory_bytes=128*1024*1024;}
        if(mode=="produce"){auto w=Produce(life,dir,tag,spec);std::printf("{\"status\":\"pass\",\"checks\":%llu,\"metric\":%u,\"manifest\":\"%s\",\"model_hash\":\"%s\",\"codebook_hash\":\"%s\",\"codes_hash\":\"%s\",\"cleanup_failures\":%d}\n",(unsigned long long)checks,metric,w.manifest_hash,w.model_hash,w.codebook_hash,w.codes_hash,cleanup_failures);}
        else if(mode=="consume"){Check(argc==7,"consumer expected root required");Consume(life,dir,tag,spec,argv[6]);}
        else if(mode=="negative"){Check(argc==7,"negative expected root required");Negatives(life,dir,tag,spec,argv[6]);}
        else if(mode=="writer-negative")WriterNegatives(life,dir,spec);
        else if(mode=="fault"||mode=="cancel"){Check(argc==8,"failure point required");FaultRead(life,dir,tag,spec,argv[6],std::stoull(argv[7]),mode=="cancel");}
        else throw std::runtime_error("unknown PQ Parquet probe mode");Check(!cleanup_failures,"SDK candidate cleanup failed");return 0;
    }catch(const std::exception& e){tracking=false;std::fprintf(stderr,"PQ Parquet probe failed: %s\n",e.what());return 1;}
}
