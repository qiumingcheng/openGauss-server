#include "pq_parquet_format.h"
#include "vendor/arrow20/abi.h"
#include <openssl/sha.h>
#include <algorithm>
#include <climits>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
namespace {
constexpr uint64_t MIB=1024*1024,BATCH=1024;
const OgLakeSdkField MODEL[]={{0,OG_LAKE_SDK_INT64,0,"centroid_id"},{1,OG_LAKE_SDK_BINARY,0,"centers"}};
const OgLakeSdkField CODES[]={{0,OG_LAKE_SDK_INT64,0,"node_id"},{1,OG_LAKE_SDK_BINARY,0,"codes"}};
const OgLakeSdkField MANIFEST[]={{0,OG_LAKE_SDK_INT64,0,"field_id"},{1,OG_LAKE_SDK_BINARY,0,"value"}};
struct Failure:std::runtime_error {int code;Failure(int c,const char* s):std::runtime_error(s),code(c){}};
void Need(bool ok,const char* s,int code=OG_LAKE_SDK_INVALID){if(!ok)throw Failure(code,s);}
template<class F> int Guard(OgLakeSdkError* e,F f)noexcept
{
    if(e)*e={};try{f();return 0;}catch(const Failure& x){if(e){e->code=x.code;std::strncpy(e->message,x.what(),sizeof(e->message)-1);}return x.code;}
    catch(const std::bad_alloc&){if(e){e->code=OG_LAKE_SDK_MEMORY;std::strcpy(e->message,"PQ Parquet allocation failed");}return OG_LAKE_SDK_MEMORY;}
    catch(const std::exception& x){if(e){e->code=OG_LAKE_SDK_STATE;std::strncpy(e->message,x.what(),sizeof(e->message)-1);}return OG_LAKE_SDK_STATE;}
    catch(...){if(e){e->code=OG_LAKE_SDK_STATE;std::strcpy(e->message,"unexpected PQ Parquet exception");}return OG_LAKE_SDK_STATE;}
}
void Cpu(int c,const PqCpuError& e){if(c){int status=c==PQ_CPU_MEMORY?OG_LAKE_SDK_MEMORY:c==PQ_CPU_CANCELLED?OG_LAKE_SDK_CANCELLED:c==PQ_CPU_UNSUPPORTED?OG_LAKE_SDK_UNSUPPORTED:c==PQ_CPU_INVALID?OG_LAKE_SDK_INVALID:OG_LAKE_SDK_STATE;throw Failure(status,e.message);}}
void Sdk(int c,const OgLakeSdkError& e){if(c)throw Failure(c,e.message);}
void Poll(const OgLakeSdkLimits& l){if(l.should_cancel&&l.should_cancel(l.cancel_context))throw Failure(OG_LAKE_SDK_CANCELLED,"PQ Parquet cancelled");}
bool Hex(const char* p){if(!p||std::strlen(p)!=64)return false;for(int i=0;i<64;++i)if(!((p[i]>='0'&&p[i]<='9')||(p[i]>='a'&&p[i]<='f')))return false;return true;}
uint64_t Mul(uint64_t a,uint64_t b){Need(!b||a<=UINT64_MAX/b,"PQ Parquet multiplication overflow");return a*b;}
uint64_t Add(uint64_t a,uint64_t b){Need(a<=UINT64_MAX-b,"PQ Parquet addition overflow");return a+b;}
struct Budget {uint64_t adapter,cpu,sdk;OgLakeSdkLimits sdk_limits;PqCpuLimits cpu_limits;};
Budget Limits(const PqParquetSpec& s,const OgLakeSdkLimits* l)
{
    Need(s.struct_size==sizeof(s)&&s.version==1&&s.dimension>=1&&s.dimension<=PQ_CPU_MAX_DIM&&s.chunks&&s.chunks<=s.dimension&&s.dimension%s.chunks==0&&s.metric>=1&&s.metric<=3,
        "invalid PQ Parquet shape/version");
    Need(s.training_rows>=256&&s.training_rows<=1024&&s.node_count<=PQ_CPU_MAX_NODES&&s.source_file_id>=0&&Hex(s.context_hash)&&Hex(s.source_version_hash)&&Hex(s.schema_hash),"invalid PQ Parquet source identity");
    Need(l&&l->struct_size>=sizeof(*l)&&l->memory_bytes>0&&l->footer_bytes>0,"invalid PQ Parquet limits");
    uint64_t model=Add(2048,Add(Mul(513*4ull,s.dimension),Mul(s.chunks+1,4)));
    Budget b{};b.adapter=Add(8*MIB,Add(Mul(256*4ull,s.dimension),Mul(4*BATCH,s.chunks)));b.sdk=8*MIB;
    uint64_t numeric=Add(18*MIB,Add(Mul(4,model),Add(Mul(2,Mul(s.node_count,s.chunks)),Mul(8*BATCH,s.dimension))));
    Need(Add(Add(b.adapter,Mul(3,b.sdk)),numeric)<=(uint64_t)l->memory_bytes,"PQ Parquet reserved working set exceeds budget",OG_LAKE_SDK_MEMORY);
    b.cpu=(uint64_t)l->memory_bytes-b.adapter-3*b.sdk;b.sdk_limits=*l;b.sdk_limits.memory_bytes=b.sdk;
    b.cpu_limits={sizeof(PqCpuLimits),b.cpu,l->should_cancel,l->cancel_context};Poll(*l);return b;
}
void Api(const OgLakeParquetSdkApi* a)
{uint64_t flags=OG_LAKE_SDK_BINARY_VALUES|OG_LAKE_SDK_OBJECT_VERSION|OG_LAKE_SDK_ORIGINAL_POSITIONS|OG_LAKE_SDK_POOL_BUDGET;
 Need(a&&a->abi_version==1&&a->struct_size>=sizeof(*a)&&(a->capabilities&flags)==flags&&a->writer_append&&a->footer_open&&a->footer_info&&a->footer_close&&a->reader_open&&a->reader_next&&a->reader_close&&a->batch_release,"unqualified PQ Parquet SDK",OG_LAKE_SDK_UNSUPPORTED);}
struct Model {PqCpuModel* p=nullptr;~Model(){pq_cpu_model_close(&p);}};
struct Codes {PqCpuCodes* p=nullptr;~Codes(){pq_cpu_codes_close(&p);}};
struct Builder {PqCpuCodesBuilder* p=nullptr;~Builder(){pq_cpu_codes_builder_close(&p);}};
struct Spec {
    PqParquetSpec value;std::string context,source,schema;
    explicit Spec(const PqParquetSpec& s):value(s),context(s.context_hash),source(s.source_version_hash),schema(s.schema_hash)
    {value.context_hash=context.c_str();value.source_version_hash=source.c_str();value.schema_hash=schema.c_str();}
};
std::string Number(uint64_t x){std::string b(8,'\0');for(int i=7;i>=0;--i){b[i]=x&255;x>>=8;}return b;}
std::vector<std::string> Manifest(const Spec& s,const PqCpuBackendInfo& b,const PqCpuInfo& m,const char* codes)
{
    const auto& v=s.value;return {Number(1),Number(PQ_CPU_VERSION),Number(PQ_CPU_ALGORITHM_VERSION),Number(256),Number(v.dimension),Number(v.chunks),Number(v.metric),Number(v.training_rows),Number(v.node_count),Number(v.source_file_id),
        s.context,s.source,s.schema,b.backend_hash,b.source_commit,b.algorithm_hash,m.payload_hash,m.model_hash,codes,Number(PQ_CPU_PERSIST_VERSION)};
}
std::string Root(const std::vector<std::string>& rows)
{
    SHA256_CTX h;SHA256_Init(&h);const char domain[]="og_lake_pq_parquet_artifact_v1";SHA256_Update(&h,domain,sizeof(domain)-1);
    for(size_t i=0;i<rows.size();++i){auto id=Number(i+1),length=Number(rows[i].size());SHA256_Update(&h,id.data(),8);SHA256_Update(&h,length.data(),8);SHA256_Update(&h,rows[i].data(),rows[i].size());}
    unsigned char raw[32];SHA256_Final(raw,&h);const char hex[]="0123456789abcdef";std::string out(64,'0');for(int i=0;i<32;++i){out[2*i]=hex[raw[i]>>4];out[2*i+1]=hex[raw[i]&15];}return out;
}
void ReleaseArray(ArrowArray* a){a->release=nullptr;}void ReleaseSchema(ArrowSchema* s){s->release=nullptr;}
void Write(const OgLakeParquetSdkApi* api,OgLakeSdkWriter* output,const OgLakeSdkField* fields,uint64_t first,const std::vector<std::string>& data)
{
    Need(data.size()<=BATCH,"PQ Parquet batch exceeds1024 rows");std::vector<int64_t> ids(data.size());std::vector<int32_t> offsets(data.size()+1);std::string bytes;
    for(size_t i=0;i<data.size();++i){ids[i]=first+i;Need(bytes.size()+data[i].size()<=INT32_MAX,"PQ Parquet binary batch too large");bytes+=data[i];offsets[i+1]=bytes.size();}
    ArrowArray root{},columns[2]{};ArrowSchema schema{},definitions[2]{};ArrowArray* children[]={&columns[0],&columns[1]};ArrowSchema* schemas[]={&definitions[0],&definitions[1]};
    const void* rb[]={nullptr};const void* ib[]={nullptr,ids.data()};const void* bb[]={nullptr,offsets.data(),bytes.data()};
    root.length=data.size();root.n_buffers=1;root.buffers=rb;root.n_children=2;root.children=children;root.release=ReleaseArray;
    schema.format="+s";schema.n_children=2;schema.children=schemas;schema.release=ReleaseSchema;
    for(int i=0;i<2;++i){columns[i].length=data.size();columns[i].n_buffers=i?3:2;columns[i].buffers=i?bb:ib;columns[i].release=ReleaseArray;
        definitions[i].format=i?"z":"l";definitions[i].name=fields[i].name;definitions[i].release=ReleaseSchema;}
    OgLakeSdkError e{};Sdk(api->writer_append(output,&root,&schema,&e),e);
}
std::string FloatBytes(const float* values,uint32_t count)
{
    std::string out(count*4,'\0');for(uint32_t i=0;i<count;++i){uint32_t bits;std::memcpy(&bits,values+i,4);for(int j=0;j<4;++j){out[i*4+j]=bits&255;bits>>=8;}}return out;
}
void DecodeFloats(const std::string& bytes,float* out,uint32_t count)
{
    Need(bytes.size()==count*4ull,"PQ Parquet centroid byte length mismatch");for(uint32_t i=0;i<count;++i){uint32_t bits=0;for(int j=3;j>=0;--j)bits=(bits<<8)|(unsigned char)bytes[i*4+j];std::memcpy(out+i,&bits,4);}
}
struct Input {
    const OgLakeParquetSdkApi* api;OgLakeSdkReader* reader=nullptr;OgLakeSdkFooter* footer=nullptr;OgLakeSdkBatch* batch=nullptr;
    explicit Input(const OgLakeParquetSdkApi* a):api(a){}
    ~Input(){if(batch)api->batch_release(&batch);if(reader)api->reader_close(&reader);if(footer)api->footer_close(&footer);}
};
template<class F> void Read(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits& limits,const char* path,const OgLakeSdkField* fields,uint64_t rows,F visitor)
{
    Need(path&&*path,"missing PQ Parquet path");Input in(api);OgLakeSdkError e{};Sdk(api->footer_open(path,&limits,&in.footer,&e),e);auto f=api->footer_info(in.footer);
    Need(f&&f->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,object_version)&&f->object_version&&std::strlen(f->object_version)<1024&&f->num_fields==2&&f->fields&&f->num_rows==(int64_t)rows,"PQ Parquet footer population mismatch");
    for(int i=0;i<2;++i)Need(f->fields[i].field_id==fields[i].field_id&&f->fields[i].type==fields[i].type&&!f->fields[i].nullable&&f->fields[i].name&&!std::strcmp(f->fields[i].name,fields[i].name),"PQ Parquet schema mismatch");
    std::string version=f->object_version;api->footer_close(&in.footer);
    OgLakeSdkReadOptions options{};options.struct_size=sizeof(options);options.path=path;options.limits=limits;options.batch_rows=BATCH;options.projection_kind=OG_LAKE_SDK_BY_FIELD_ID;options.num_field_ids=-1;options.num_row_groups=-1;
    Sdk(api->reader_open(&options,&in.reader,&e),e);uint64_t at=0;int empty=0;
    for(;;){Poll(limits);if(in.batch)api->batch_release(&in.batch);int status=api->reader_next(in.reader,&in.batch,&e);if(status==OG_LAKE_SDK_EOF)break;Sdk(status,e);auto b=in.batch;
        Need(b&&b->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkBatch,object_version)&&b->object_version&&version==b->object_version&&b->num_rows>=0&&b->num_rows<=1024&&b->array&&b->schema&&b->array->release&&b->schema->release&&b->array->offset==0&&b->array->length==b->num_rows&&b->array->n_children==2&&b->schema->n_children==2&&b->array->children&&b->schema->children,"invalid PQ Parquet Arrow batch/object identity");
        if(!b->num_rows){Need(++empty<1024,"PQ Parquet reader made no progress",OG_LAKE_SDK_STATE);continue;}empty=0;
        const auto* ids=b->array->children[0];const auto* bytes=b->array->children[1];
        Need(ids&&bytes&&b->schema->children[0]&&b->schema->children[1]&&b->schema->children[0]->format&&b->schema->children[1]->format&&
            !std::strcmp(b->schema->children[0]->format,"l")&&!std::strcmp(b->schema->children[1]->format,"z")&&ids->offset>=0&&bytes->offset>=0&&
            ids->offset<=INT64_MAX-b->num_rows&&bytes->offset<=INT64_MAX-b->num_rows&&ids->length==b->num_rows&&bytes->length==b->num_rows&&
            !ids->dictionary&&!bytes->dictionary&&!ids->n_children&&!bytes->n_children&&ids->n_buffers==2&&bytes->n_buffers==3&&ids->buffers&&bytes->buffers&&ids->buffers[1]&&bytes->buffers[1]&&bytes->buffers[2],"invalid PQ Parquet Arrow columns");
        std::vector<std::string> values;values.reserve(b->num_rows);
        for(int64_t i=0;i<b->num_rows;++i){Poll(limits);Need(at<(uint64_t)rows,"PQ Parquet row overflow");int64_t x=ids->offset+i,y=bytes->offset+i;
            for(auto col:{ids,bytes})if(col->null_count){Need(col->buffers[0],"PQ Parquet validity bitmap missing");int64_t bit=col->offset+i;Need(((const unsigned char*)col->buffers[0])[bit>>3]&(1u<<(bit&7)),"NULL PQ Parquet primitive");}
            int64_t id=((const int64_t*)ids->buffers[1])[x];uint64_t expected=at+(fields==MANIFEST?1:0);
            Need(id>=0&&(uint64_t)id==expected&&b->original_positions&&b->original_positions[i]==(int64_t)at,"PQ Parquet dense identity/order mismatch");
            const auto* off=(const int32_t*)bytes->buffers[1];int64_t start=off[y],end=off[y+1];
            uint64_t maximum=fields==MODEL?1536*4:fields==CODES?1536:256;
            Need(start>=0&&end>=start&&(uint64_t)(end-start)<=maximum,"PQ Parquet binary primitive too long");values.emplace_back((const char*)bytes->buffers[2]+start,end-start);++at;}
        visitor(at-values.size(),values);
    }
    Need(at==rows,"PQ Parquet dense coverage incomplete");
}
PqCpuBackendInfo Backend(){PqCpuBackendInfo b{};b.struct_size=sizeof(b);PqCpuError e{};Cpu(pq_cpu_backend_info(&b,&e),e);return b;}
PqCpuInfo ModelInfo(const PqCpuModel* model){PqCpuInfo i{};i.struct_size=sizeof(i);PqCpuError e{};Cpu(pq_cpu_model_info(model,&i,&e),e);return i;}
PqCpuInfo CodesInfo(const PqCpuCodes* codes){PqCpuInfo i{};i.struct_size=sizeof(i);PqCpuError e{};Cpu(pq_cpu_codes_info(codes,&i,&e),e);return i;}
PqCpuModelRestoreSpec RestoreSpec(const Spec& s,const PqCpuBackendInfo& b,const char* codebook,const char* model)
{return {sizeof(PqCpuModelRestoreSpec),1,1,1,s.value.dimension,s.value.chunks,256,s.value.metric,s.value.training_rows,s.context.c_str(),codebook,model,b.backend_hash};}
PqCpuCodesBuilderSpec BuildSpec(const Spec& s,const PqCpuBackendInfo& b,const char* model,uint32_t mode,const char* expected)
{return {sizeof(PqCpuCodesBuilderSpec),1,1,1,s.value.dimension,s.value.chunks,256,s.value.metric,mode,s.value.source_file_id,s.value.node_count,s.context.c_str(),model,b.backend_hash,expected};}
PqParquetWritten Written(const Spec& s,const PqCpuBackendInfo& b,const PqCpuInfo& model,const char* codes,const char* root)
{
    PqParquetWritten w{};w.struct_size=sizeof(w);w.model_rows=256;w.codes_rows=s.value.node_count;w.manifest_rows=20;
    std::memcpy(w.manifest_hash,root,65);std::memcpy(w.model_hash,model.model_hash,65);std::memcpy(w.codebook_hash,model.payload_hash,65);std::memcpy(w.codes_hash,codes,65);std::memcpy(w.backend_hash,b.backend_hash,65);return w;
}
}
struct PqParquetWriter {
    const OgLakeParquetSdkApi* api;Spec spec;Model model;Builder builder;PqCpuInfo info{};PqCpuBackendInfo backend{};
    OgLakeSdkWriter* codes;OgLakeSdkWriter* manifest;uint64_t next=0;bool failed=false,finished=false;
    PqParquetWriter(const OgLakeParquetSdkApi* a,const PqParquetSpec& s):api(a),spec(s){}
};
struct PqParquetReader {Spec spec;Model model;Codes codes;PqParquetWritten written{};explicit PqParquetReader(const PqParquetSpec& s):spec(s){}};
struct PqParquetQuery {PqCpuQuery* p=nullptr;~PqParquetQuery(){pq_cpu_query_close(&p);}};
extern "C" const OgLakeSdkField* pq_parquet_fields(uint32_t file,int32_t* count)
{if(count)*count=file>=1&&file<=3?2:0;return file==1?MODEL:file==2?CODES:file==3?MANIFEST:nullptr;}
extern "C" int pq_parquet_budget(const PqParquetSpec* s,const OgLakeSdkLimits* l,uint64_t* a,uint64_t* c,uint64_t* d,OgLakeSdkError* e)
{return Guard(e,[&]{Need(s&&a&&c&&d,"missing PQ Parquet budget output");auto b=Limits(*s,l);*a=b.adapter;*c=b.cpu;*d=b.sdk;});}
extern "C" int pq_parquet_writer_open(const OgLakeParquetSdkApi* api,const PqParquetSpec* spec,const OgLakeSdkLimits* limits,const PqCpuModel* model,OgLakeSdkWriter* mf,OgLakeSdkWriter* cf,OgLakeSdkWriter* hf,PqParquetWriter** out,OgLakeSdkError* error)
{
    return Guard(error,[&]{Need(spec&&model&&mf&&cf&&hf&&mf!=cf&&mf!=hf&&cf!=hf&&out&&!*out,"invalid PQ Parquet writer request");Api(api);auto budget=Limits(*spec,limits);
        auto w=std::unique_ptr<PqParquetWriter>(new PqParquetWriter(api,*spec));w->info=ModelInfo(model);w->backend=Backend();w->codes=cf;w->manifest=hf;
        Need(w->info.dimension==spec->dimension&&w->info.chunks==spec->chunks&&w->info.metric==spec->metric&&w->info.rows==spec->training_rows&&!std::strcmp(w->info.context_hash,spec->context_hash),"PQ Parquet model/spec mismatch");
        std::vector<float> centers(256ull*spec->dimension);PqCpuError e{};Cpu(pq_cpu_model_centers(model,centers.data(),centers.size(),&e),e);
        auto rs=RestoreSpec(w->spec,w->backend,w->info.payload_hash,w->info.model_hash);Cpu(pq_cpu_model_restore(&rs,&budget.cpu_limits,centers.data(),centers.size(),&w->model.p,&e),e);
        auto bs=BuildSpec(w->spec,w->backend,w->info.model_hash,PQ_CPU_INPUT_FLOATS,nullptr);Cpu(pq_cpu_codes_builder_open(w->model.p,&bs,&budget.cpu_limits,&w->builder.p,&e),e);
        std::vector<std::string> rows;rows.reserve(256);for(uint32_t i=0;i<256;++i){Poll(*limits);rows.push_back(FloatBytes(centers.data()+i*spec->dimension,spec->dimension));}Write(api,mf,MODEL,0,rows);*out=w.release();});
}
extern "C" int pq_parquet_writer_append(PqParquetWriter* w,const OgLakeSdkLimits* limits,uint64_t first,uint64_t rows,const float* data,uint64_t count,OgLakeSdkError* error)
{
    return Guard(error,[&]{Need(w&&!w->failed&&!w->finished,"PQ Parquet writer is not active",OG_LAKE_SDK_STATE);w->failed=true;auto budget=Limits(w->spec.value,limits);
        Need(rows&&rows<=BATCH&&first==w->next&&first<=w->spec.value.node_count&&rows<=w->spec.value.node_count-first,"PQ Parquet input is not continuous bounded nodes");
        PqCpuEncodeSpec es{sizeof(es),1,w->spec.value.source_file_id,rows,w->spec.context.c_str(),w->info.model_hash};Codes part;PqCpuError e{};
        Cpu(pq_cpu_encode(w->model.p,&es,&budget.cpu_limits,data,count,&part.p,&e),e);std::vector<uint8_t> bytes(rows*w->spec.value.chunks);Cpu(pq_cpu_codes_export(part.p,bytes.data(),bytes.size(),&e),e);
        std::vector<std::string> encoded;encoded.reserve(rows);for(uint64_t i=0;i<rows;++i)encoded.emplace_back((const char*)bytes.data()+i*w->spec.value.chunks,w->spec.value.chunks);
        Cpu(pq_cpu_codes_builder_append(w->builder.p,&budget.cpu_limits,first,rows,data,count,&e),e);Write(w->api,w->codes,CODES,first,encoded);w->next+=rows;w->failed=false;});
}
extern "C" int pq_parquet_writer_finish(PqParquetWriter* w,const OgLakeSdkLimits* limits,PqParquetWritten* out,OgLakeSdkError* error)
{
    return Guard(error,[&]{Need(w&&!w->failed&&!w->finished,"PQ Parquet writer is not active",OG_LAKE_SDK_STATE);w->failed=true;Need(out&&out->struct_size>=sizeof(*out)&&w->next==w->spec.value.node_count,"PQ Parquet writer dense coverage incomplete");
        auto budget=Limits(w->spec.value,limits);Codes all;PqCpuError e{};Cpu(pq_cpu_codes_builder_finish(w->builder.p,&budget.cpu_limits,&all.p,&e),e);auto ci=CodesInfo(all.p);
        auto manifest=Manifest(w->spec,w->backend,w->info,ci.payload_hash);std::string root=Root(manifest);Poll(*limits);Write(w->api,w->manifest,MANIFEST,1,manifest);
        auto written=Written(w->spec,w->backend,w->info,ci.payload_hash,root.c_str());*out=written;w->finished=true;w->failed=false;});
}
extern "C" void pq_parquet_writer_close(PqParquetWriter** p){if(p){delete *p;*p=nullptr;}}
extern "C" int pq_parquet_reader_open(const OgLakeParquetSdkApi* api,const PqParquetSpec* spec,const OgLakeSdkLimits* limits,const PqParquetPaths* paths,const char* expected,PqParquetReader** out,OgLakeSdkError* error)
{
    return Guard(error,[&]{Need(spec&&paths&&paths->struct_size==sizeof(*paths)&&Hex(expected)&&out&&!*out,"invalid PQ Parquet reader request");Api(api);auto budget=Limits(*spec,limits);auto r=std::unique_ptr<PqParquetReader>(new PqParquetReader(*spec));auto backend=Backend();
        std::vector<std::string> manifest;Read(api,budget.sdk_limits,paths->manifest_path,MANIFEST,20,[&](uint64_t,const std::vector<std::string>& values){manifest.insert(manifest.end(),values.begin(),values.end());});
        Need(manifest.size()==20&&Root(manifest)==expected,"PQ Parquet trusted manifest root mismatch");
        for(int i:{13,15,16,17,18})Need(Hex(manifest[i].c_str())&&manifest[i].size()==64,"PQ Parquet manifest fingerprint invalid");
        PqCpuInfo claimed{};std::memcpy(claimed.payload_hash,manifest[16].c_str(),65);std::memcpy(claimed.model_hash,manifest[17].c_str(),65);
        auto check=Manifest(r->spec,backend,claimed,manifest[18].c_str());Need(check==manifest,"PQ Parquet source/version/schema/backend identity mismatch");
        std::vector<float> centers(256ull*spec->dimension);Read(api,budget.sdk_limits,paths->model_path,MODEL,256,[&](uint64_t at,const std::vector<std::string>& values){for(size_t i=0;i<values.size();++i)DecodeFloats(values[i],centers.data()+(at+i)*spec->dimension,spec->dimension);});
        auto rs=RestoreSpec(r->spec,backend,claimed.payload_hash,claimed.model_hash);PqCpuError e{};Cpu(pq_cpu_model_restore(&rs,&budget.cpu_limits,centers.data(),centers.size(),&r->model.p,&e),e);auto mi=ModelInfo(r->model.p);
        Builder builder;auto bs=BuildSpec(r->spec,backend,mi.model_hash,PQ_CPU_INPUT_CODES,manifest[18].c_str());Cpu(pq_cpu_codes_builder_open(r->model.p,&bs,&budget.cpu_limits,&builder.p,&e),e);
        Read(api,budget.sdk_limits,paths->codes_path,CODES,spec->node_count,[&](uint64_t at,const std::vector<std::string>& values){std::vector<uint8_t> bytes(values.size()*spec->chunks);for(size_t i=0;i<values.size();++i){Need(values[i].size()==spec->chunks,"PQ Parquet code width mismatch");std::memcpy(bytes.data()+i*spec->chunks,values[i].data(),spec->chunks);}Cpu(pq_cpu_codes_builder_append_raw(builder.p,&budget.cpu_limits,at,values.size(),bytes.data(),bytes.size(),&e),e);});
        Cpu(pq_cpu_codes_builder_finish(builder.p,&budget.cpu_limits,&r->codes.p,&e),e);auto ci=CodesInfo(r->codes.p);r->written=Written(r->spec,backend,mi,ci.payload_hash,expected);Poll(*limits);*out=r.release();});
}
extern "C" int pq_parquet_reader_info(const PqParquetReader* r,PqParquetWritten* w,PqCpuInfo* m,PqCpuInfo* c,OgLakeSdkError* error)
{return Guard(error,[&]{Need(r&&w&&w->struct_size>=sizeof(*w)&&m&&m->struct_size>=sizeof(*m)&&c&&c->struct_size>=sizeof(*c),"invalid PQ Parquet info output");auto mi=ModelInfo(r->model.p),ci=CodesInfo(r->codes.p);*m=mi;*c=ci;*w=r->written;});}
extern "C" int pq_parquet_centers(const PqParquetReader* r,float* out,uint64_t n,OgLakeSdkError* error)
{return Guard(error,[&]{Need(r,"missing PQ Parquet reader");PqCpuError e{};Cpu(pq_cpu_model_centers(r->model.p,out,n,&e),e);});}
extern "C" int pq_parquet_codes(const PqParquetReader* r,uint8_t* out,uint64_t n,OgLakeSdkError* error)
{return Guard(error,[&]{Need(r,"missing PQ Parquet reader");PqCpuError e{};Cpu(pq_cpu_codes_export(r->codes.p,out,n,&e),e);});}
extern "C" int pq_parquet_query_open(const PqParquetReader* r,const OgLakeSdkLimits* limits,const float* data,uint64_t n,PqParquetQuery** out,OgLakeSdkError* error)
{return Guard(error,[&]{Need(r&&out&&!*out,"invalid PQ Parquet query output");auto budget=Limits(r->spec.value,limits);auto q=std::unique_ptr<PqParquetQuery>(new PqParquetQuery);PqCpuError e{};Cpu(pq_cpu_query_open(r->model.p,&budget.cpu_limits,data,n,&q->p,&e),e);*out=q.release();});}
extern "C" int pq_parquet_distances(const PqParquetReader* r,const PqParquetQuery* q,const OgLakeSdkLimits* limits,uint64_t first,uint64_t n,float* out,uint64_t capacity,OgLakeSdkError* error)
{return Guard(error,[&]{Need(r&&q,"missing PQ Parquet query/reader");auto budget=Limits(r->spec.value,limits);PqCpuError e{};Cpu(pq_cpu_distances(r->codes.p,q->p,&budget.cpu_limits,first,n,out,capacity,&e),e);});}
extern "C" void pq_parquet_query_close(PqParquetQuery** p){if(p){delete *p;*p=nullptr;}}
extern "C" void pq_parquet_reader_close(PqParquetReader** p){if(p){delete *p;*p=nullptr;}}
