/* Checked C boundary around the locked Microsoft DiskANN numerical subset.
 * Quantization remains Microsoft's implementation. No database callbacks,
 * Parquet objects, C++ containers or exceptions cross this C API. */
#include "pq_cpu_internal.h"
#include <openssl/sha.h>
#include <cblas.h>
#include <algorithm>
#include <cfloat>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <new>

struct PqCpuModel {PqCpuInfo info{};std::vector<float> centers,transposed,centroid;std::vector<uint32_t> offsets;};
struct PqCpuCodes {PqCpuInfo info{};uint64_t model_bytes=0;std::vector<uint8_t> bytes;};
struct PqCpuQuery {PqCpuInfo info{};uint64_t model_bytes=0;std::vector<float> lut;};
struct PqCpuCodesBuilder {
    std::unique_ptr<PqCpuModel> model;
    std::unique_ptr<PqCpuCodes> pending;
    uint64_t next=0;
    uint32_t input_kind=0,state=PQ_CPU_BUILDER_ACTIVE;
    char expected_hash[65]{};
};
namespace {
constexpr uint64_t MIB=1024*1024,BATCH=1024;
constexpr char SOURCE_COMMIT[]="78256bbab4685e1774e78d331e081a153be26823";
constexpr char NUMERICAL_BACKEND[]="MicrosoftDiskANN78256bbab4685e1774e78d331e081a153be26823|plain8bit|zero_mean0|opq0|raii_poll_v1";
static_assert(sizeof(blasint)==4,"this CPU component is qualified only against LP64 CBLAS headers");
static_assert(sizeof(float)==4&&std::numeric_limits<float>::is_iec559,"CPU PQ requires IEEE float32");
struct Failure {int code;const char* text;};
void Require(bool ok,const char* text,int code=PQ_CPU_INVALID){if(!ok)throw Failure{code,text};}
thread_local const PqCpuLimits* active=nullptr;
struct Scope {
    const PqCpuLimits* previous;
    explicit Scope(const PqCpuLimits* p):previous(active){Require(!active,"recursive CPU PQ callback is unsupported",PQ_CPU_STATE);active=p;}
    ~Scope(){active=previous;}
};
template<class F> int Guard(PqCpuError* error,F f) noexcept
{
    if(error)*error={};
    try{f();return PQ_CPU_OK;}
    catch(const Failure& e){if(error){error->code=e.code;std::snprintf(error->message,sizeof(error->message),"%s",e.text);}return e.code;}
    catch(const std::bad_alloc&){if(error){error->code=PQ_CPU_MEMORY;std::snprintf(error->message,sizeof(error->message),"CPU PQ allocation failed");}return PQ_CPU_MEMORY;}
    catch(const std::exception& e){if(error){error->code=PQ_CPU_STATE;std::snprintf(error->message,sizeof(error->message),"CPU PQ exception: %.220s",e.what());}return PQ_CPU_STATE;}
    catch(...){if(error){error->code=PQ_CPU_STATE;std::snprintf(error->message,sizeof(error->message),"unexpected CPU PQ exception");}return PQ_CPU_STATE;}
}
bool Hex(const char* s){if(!s||std::strlen(s)!=64)return false;for(int i=0;i<64;++i)if(!((s[i]>='0'&&s[i]<='9')||(s[i]>='a'&&s[i]<='f')))return false;return true;}
uint64_t Mul(uint64_t a,uint64_t b){Require(!b||a<=UINT64_MAX/b,"CPU PQ population overflow");return a*b;}
uint64_t Add(uint64_t a,uint64_t b){Require(a<=UINT64_MAX-b,"CPU PQ budget overflow");return a+b;}
void Budget(const PqCpuLimits* p,uint64_t needed)
{
    Require(p&&p->struct_size==sizeof(*p),"invalid CPU PQ limits");
    Require(needed<=p->memory_bytes,"CPU PQ operation exceeds declared working budget",PQ_CPU_MEMORY);
    Require(openblas_get_parallel()==0&&openblas_get_num_threads()==1,"CPU PQ requires qualified serial OpenBLAS in this experiment",PQ_CPU_UNSUPPORTED);
}
void Shape(uint32_t d,uint32_t m,uint32_t metric)
{Require(d>=1&&d<=PQ_CPU_MAX_DIM&&m>=1&&m<=d&&d%m==0,"CPU PQ dimension/chunk shape is invalid");Require(metric>=1&&metric<=3,"invalid CPU PQ metric");}
void Data(const float* data,uint64_t rows,uint32_t d,uint32_t metric,uint64_t count)
{
    Require(count==Mul(rows,d)&&(count==0||data),"CPU PQ float span does not match shape");
    /* Also bound the float residual sum across the maximum1024-row trainer. */
    double maximum=std::sqrt((double)FLT_MAX/(16.0*PQ_CPU_MAX_TRAIN*d));
    for(uint64_t row=0;row<rows;++row){if((row&63)==0)pq_cpu_ms::Poll();double norm=0;
        for(uint32_t j=0;j<d;++j){float x=data[row*d+j];Require(std::isfinite(x)&&std::fabs((double)x)<=maximum,"CPU PQ input is nonfinite or exceeds safe float range");norm+=(double)x*x;}
        if(metric==PQ_CPU_COSINE)Require(std::fabs(norm-1.0)<=0.0001,"CPU PQ cosine input must already be normalized and nonzero");}
}
void Integer(SHA256_CTX& h,uint64_t x){unsigned char b[8];for(int i=7;i>=0;--i){b[i]=x&255;x>>=8;}SHA256_Update(&h,b,8);}
void Digest(SHA256_CTX h,char* out){unsigned char raw[32];SHA256_Final(raw,&h);const char hex[]="0123456789abcdef";for(int i=0;i<32;++i){out[i*2]=hex[raw[i]>>4];out[i*2+1]=hex[raw[i]&15];}out[64]=0;}
void Floats(SHA256_CTX& h,const std::vector<float>& values)
{for(size_t i=0;i<values.size();++i){if((i&4095)==0)pq_cpu_ms::Poll();uint32_t bits;std::memcpy(&bits,&values[i],4);Integer(h,bits);}}
void Start(SHA256_CTX& h,const char* domain,const PqCpuInfo& i)
{
    SHA256_Init(&h);SHA256_Update(&h,domain,std::strlen(domain));
    SHA256_Update(&h,NUMERICAL_BACKEND,sizeof(NUMERICAL_BACKEND)-1);SHA256_Update(&h,i.context_hash,64);
    const char* blas=openblas_get_config();SHA256_Update(&h,blas,std::strlen(blas));
    Integer(h,i.version);Integer(h,i.dimension);Integer(h,i.chunks);Integer(h,i.metric);Integer(h,256);
}
uint64_t ModelBytes(uint32_t d,uint32_t m)
{return sizeof(PqCpuModel)+Mul(2*256ull+1,Mul(d,4))+Mul((uint64_t)m+1,4);}
uint64_t Scratch(uint64_t rows,uint32_t d,uint32_t m)
{
    /* Original compute_closest_centers allocates rows*256 floats. Additional
     * factor2 on chunk rows covers training copies and encoding batch buffers.
     * Fixed allowance covers allocator rounding, picked/inverted vectors and
     * k-means bookkeeping for this <=1024-row experimental contract. */
    return Add(16*MIB,Add(Mul(Mul(rows,256),4),Add(Mul(Mul(rows,d/m),8),Mul(rows,128))));
}
pq_cpu_ms::diskann::FixedChunkPQTable Table(const PqCpuModel* model)
{
    pq_cpu_ms::diskann::FixedChunkPQTable t;t.tables_tr=const_cast<float*>(model->transposed.data());t.ndims=model->info.dimension;t.n_chunks=model->info.chunks;
    t.chunk_offsets=const_cast<uint32_t*>(model->offsets.data());t.centroid=const_cast<float*>(model->centroid.data());return t;
}
void Info(const PqCpuInfo& source,PqCpuInfo* out){Require(out&&out->struct_size>=sizeof(*out),"invalid CPU PQ info result");*out=source;}
PqCpuBackendInfo Backend()
{
    Require(openblas_get_parallel()==0&&openblas_get_num_threads()==1,
        "CPU PQ backend metadata is unavailable or unqualified",PQ_CPU_UNSUPPORTED);
    const char* config=openblas_get_config();
    Require(config,"CPU PQ backend has no BLAS configuration",PQ_CPU_UNSUPPORTED);
    size_t length=0;while(length<256&&config[length])++length;
    Require(length>0&&length<256,"CPU PQ BLAS configuration is not bounded",PQ_CPU_UNSUPPORTED);
    PqCpuBackendInfo b{};b.struct_size=sizeof(b);b.component_version=PQ_CPU_VERSION;
    b.persist_version=PQ_CPU_PERSIST_VERSION;b.algorithm_version=PQ_CPU_ALGORITHM_VERSION;b.centroids=256;
    std::memcpy(b.source_commit,SOURCE_COMMIT,sizeof(SOURCE_COMMIT));std::memcpy(b.blas_config,config,length+1);
    SHA256_CTX h;SHA256_Init(&h);SHA256_Update(&h,NUMERICAL_BACKEND,sizeof(NUMERICAL_BACKEND)-1);
    Integer(h,b.component_version);Integer(h,b.algorithm_version);Integer(h,b.centroids);Digest(h,b.algorithm_hash);
    SHA256_Init(&h);const char domain[]="og_lake_pq_cpu_backend_v1";SHA256_Update(&h,domain,sizeof(domain)-1);
    SHA256_Update(&h,b.algorithm_hash,64);SHA256_Update(&h,b.blas_config,length);Digest(h,b.backend_hash);return b;
}
void RestoreVersions(uint32_t version,uint32_t component,uint32_t algorithm,uint32_t k)
{
    Require(version==PQ_CPU_PERSIST_VERSION&&component==PQ_CPU_VERSION&&algorithm==PQ_CPU_ALGORITHM_VERSION&&k==256,
        "CPU PQ persisted version/algorithm/centroid mismatch",PQ_CPU_UNSUPPORTED);
}
void CheckBackend(const char* hash)
{
    Require(Hex(hash),"invalid CPU PQ backend fingerprint");auto backend=Backend();
    Require(std::strcmp(hash,backend.backend_hash)==0,"CPU PQ persisted backend fingerprint mismatch");
}
void ModelDerived(PqCpuModel* model)
{
    const auto& i=model->info;model->transposed.resize(model->centers.size());
    model->centroid.resize(i.dimension,0);model->offsets.resize(i.chunks+1);
    for(uint32_t d=0;d<i.dimension;++d){pq_cpu_ms::Poll();for(uint32_t c=0;c<256;++c)model->transposed[d*256+c]=model->centers[c*i.dimension+d];}
    for(uint32_t c=0;c<=i.chunks;++c)model->offsets[c]=c*(i.dimension/i.chunks);
}
void ModelHashes(PqCpuModel* model)
{
    auto& i=model->info;SHA256_CTX h;Start(h,"og_lake_pq_cpu_codebook_v1",i);Floats(h,model->centers);Digest(h,i.payload_hash);
    Start(h,"og_lake_pq_cpu_model_v1",i);Integer(h,i.rows);SHA256_Update(&h,i.payload_hash,64);Digest(h,i.model_hash);
}
void CodeHash(PqCpuCodes* codes)
{
    SHA256_CTX h;auto& i=codes->info;Start(h,"og_lake_pq_cpu_nodecodes_v1",i);SHA256_Update(&h,i.model_hash,64);
    Integer(h,i.source_file_id);Integer(h,i.rows);
    for(uint64_t at=0;at<codes->bytes.size();at+=65536){pq_cpu_ms::Poll();SHA256_Update(&h,codes->bytes.data()+at,std::min((uint64_t)65536,(uint64_t)codes->bytes.size()-at));}
    Digest(h,i.payload_hash);
}
uint64_t BuilderBytes(const PqCpuCodesBuilder* b)
{
    return Add(sizeof(PqCpuCodesBuilder),Add(b->model?b->model->info.resident_bytes:0,b->pending?b->pending->info.resident_bytes:0));
}
void BuilderRange(const PqCpuCodesBuilder* b,uint64_t first,uint64_t rows,uint32_t kind)
{
    Require(b&&b->state==PQ_CPU_BUILDER_ACTIVE,"CPU PQ builder is not active",PQ_CPU_STATE);
    Require(b->input_kind==kind,"CPU PQ builder input modes cannot be mixed");
    Require(first==b->next&&rows>0&&rows<=BATCH&&first<=b->pending->info.rows&&rows<=b->pending->info.rows-first,
        "CPU PQ builder requires continuous bounded node batches");
}
template<class F> void Mutate(PqCpuCodesBuilder* b,F f)
{
    try{Require(b&&b->state==PQ_CPU_BUILDER_ACTIVE,"CPU PQ builder is not active",PQ_CPU_STATE);f();}
    catch(...){if(b&&b->state==PQ_CPU_BUILDER_ACTIVE)b->state=PQ_CPU_BUILDER_FAILED;throw;}
}
}
namespace pq_cpu_ms {
void Poll(){if(active&&active->should_cancel&&active->should_cancel(active->cancel_context))throw Failure{PQ_CPU_CANCELLED,"CPU PQ operation cancelled"};}
}
extern "C" int pq_cpu_train(const PqCpuTrainSpec* spec,const PqCpuLimits* limits,const float* data,uint64_t count,PqCpuModel** out,PqCpuError* error)
{
    return Guard(error,[&]{Require(out&&!*out&&spec&&spec->struct_size==sizeof(*spec)&&spec->version==PQ_CPU_VERSION&&Hex(spec->context_hash),"invalid CPU PQ training request");
        Shape(spec->dimension,spec->chunks,spec->metric);Require(spec->training_rows>=PQ_CPU_MIN_TRAIN&&spec->training_rows<=PQ_CPU_MAX_TRAIN,"CPU PQ requires256..1024 training rows",PQ_CPU_UNSUPPORTED);
        Budget(limits,Add(ModelBytes(spec->dimension,spec->chunks),Scratch(spec->training_rows,spec->dimension,spec->chunks)));Scope scope(limits);pq_cpu_ms::Poll();
        Data(data,spec->training_rows,spec->dimension,spec->metric,count);auto model=std::unique_ptr<PqCpuModel>(new PqCpuModel);
        auto& i=model->info;i.struct_size=sizeof(i);i.version=PQ_CPU_VERSION;i.dimension=spec->dimension;i.chunks=spec->chunks;i.metric=spec->metric;i.rows=spec->training_rows;i.source_file_id=-1;
        std::memcpy(i.context_hash,spec->context_hash,65);i.resident_bytes=ModelBytes(i.dimension,i.chunks);
        Require(pq_cpu_ms::diskann::generate_pq_pivots_simplified(data,spec->training_rows,i.dimension,i.chunks,model->centers)==0,"Microsoft PQ trainer rejected checked input",PQ_CPU_STATE);
        pq_cpu_ms::Poll();Require(model->centers.size()==256ull*i.dimension,"trained CPU PQ codebook population changed",PQ_CPU_STATE);
        for(float x:model->centers)Require(std::isfinite(x),"trained CPU PQ codebook is nonfinite",PQ_CPU_STATE);
        model->transposed.resize(model->centers.size());model->centroid.resize(i.dimension,0);model->offsets.resize(i.chunks+1);
        for(uint32_t d=0;d<i.dimension;++d)for(uint32_t c=0;c<256;++c)model->transposed[d*256+c]=model->centers[c*i.dimension+d];
        for(uint32_t c=0;c<=i.chunks;++c)model->offsets[c]=c*(i.dimension/i.chunks);
        SHA256_CTX h;Start(h,"og_lake_pq_cpu_codebook_v1",i);Floats(h,model->centers);Digest(h,i.payload_hash);
        Start(h,"og_lake_pq_cpu_model_v1",i);Integer(h,i.rows);SHA256_Update(&h,i.payload_hash,64);Digest(h,i.model_hash);pq_cpu_ms::Poll();*out=model.release();});
}
extern "C" int pq_cpu_encode(const PqCpuModel* model,const PqCpuEncodeSpec* spec,const PqCpuLimits* limits,const float* data,uint64_t count,PqCpuCodes** out,PqCpuError* error)
{
    return Guard(error,[&]{Require(model&&spec&&out&&!*out&&spec->struct_size==sizeof(*spec)&&spec->version==PQ_CPU_VERSION&&spec->source_file_id>=0&&
            Hex(spec->context_hash)&&Hex(spec->model_hash)&&std::strcmp(spec->context_hash,model->info.context_hash)==0&&std::strcmp(spec->model_hash,model->info.model_hash)==0,"CPU PQ code source/model identity mismatch");
        Require(spec->node_count<=PQ_CPU_MAX_NODES,"CPU PQ node population exceeds existing ANN format",PQ_CPU_UNSUPPORTED);
        uint64_t bytes=Mul(spec->node_count,model->info.chunks),batch=std::min(BATCH,spec->node_count);
        Budget(limits,Add(model->info.resident_bytes,Add(sizeof(PqCpuCodes)+bytes,Add(Scratch(batch,model->info.dimension,model->info.chunks),Mul(batch,model->info.chunks)))));
        Scope scope(limits);pq_cpu_ms::Poll();Data(data,spec->node_count,model->info.dimension,model->info.metric,count);
        auto codes=std::unique_ptr<PqCpuCodes>(new PqCpuCodes);codes->info=model->info;codes->info.rows=spec->node_count;codes->info.source_file_id=spec->source_file_id;
        codes->info.resident_bytes=sizeof(PqCpuCodes)+bytes;codes->model_bytes=model->info.resident_bytes;codes->bytes.resize(bytes);
        for(uint64_t first=0;first<spec->node_count;first+=BATCH){pq_cpu_ms::Poll();uint64_t n=std::min(BATCH,spec->node_count-first);std::vector<uint8_t> part;
            Require(pq_cpu_ms::diskann::generate_pq_data_from_pivots_simplified(data+first*model->info.dimension,n,model->centers.data(),model->centers.size(),model->info.dimension,model->info.chunks,part)==0,"Microsoft PQ encoder rejected checked input",PQ_CPU_STATE);
            Require(part.size()==n*model->info.chunks,"CPU PQ encoded population changed",PQ_CPU_STATE);std::memcpy(codes->bytes.data()+first*model->info.chunks,part.data(),part.size());}
        SHA256_CTX h;Start(h,"og_lake_pq_cpu_nodecodes_v1",codes->info);SHA256_Update(&h,codes->info.model_hash,64);Integer(h,spec->source_file_id);Integer(h,spec->node_count);
        for(uint64_t first=0;first<bytes;first+=65536){pq_cpu_ms::Poll();SHA256_Update(&h,codes->bytes.data()+first,std::min((uint64_t)65536,bytes-first));}
        Digest(h,codes->info.payload_hash);pq_cpu_ms::Poll();*out=codes.release();});
}
extern "C" int pq_cpu_query_open(const PqCpuModel* model,const PqCpuLimits* limits,const float* vector,uint64_t count,PqCpuQuery** out,PqCpuError* error)
{
    return Guard(error,[&]{Require(model&&out&&!*out,"invalid CPU PQ query request");uint64_t bytes=Mul(model->info.chunks,256*4);
        Budget(limits,Add(model->info.resident_bytes,Add(sizeof(PqCpuQuery)+bytes,65536)));Scope scope(limits);pq_cpu_ms::Poll();Data(vector,1,model->info.dimension,model->info.metric,count);
        auto query=std::unique_ptr<PqCpuQuery>(new PqCpuQuery);query->info=model->info;query->info.rows=1;query->info.resident_bytes=sizeof(PqCpuQuery)+bytes;query->model_bytes=model->info.resident_bytes;query->lut.resize(256ull*model->info.chunks);
        auto table=Table(model);if(model->info.metric==PQ_CPU_L2)table.populate_chunk_distances(vector,query->lut.data());else table.populate_chunk_inner_products(vector,query->lut.data());
        for(float x:query->lut)Require(std::isfinite(x),"CPU PQ query distance table is nonfinite",PQ_CPU_STATE);
        pq_cpu_ms::Poll();*out=query.release();});
}
extern "C" int pq_cpu_distances(const PqCpuCodes* codes,const PqCpuQuery* query,const PqCpuLimits* limits,uint64_t first,uint64_t count,float* output,uint64_t capacity,PqCpuError* error)
{
    return Guard(error,[&]{Require(codes&&query&&std::strcmp(codes->info.model_hash,query->info.model_hash)==0&&std::strcmp(codes->info.context_hash,query->info.context_hash)==0&&
            codes->info.dimension==query->info.dimension&&codes->info.chunks==query->info.chunks&&codes->info.metric==query->info.metric,"CPU PQ query/codes model mismatch");
        Require(first<=codes->info.rows&&count<=codes->info.rows-first&&capacity>=count&&(count==0||output),"invalid CPU PQ node range/output capacity");
        Budget(limits,Add(std::max(codes->model_bytes,query->model_bytes),Add(codes->info.resident_bytes,Add(query->info.resident_bytes,Add(Mul(count,4),65536)))));
        Scope scope(limits);pq_cpu_ms::Poll();std::vector<float> temporary(count);
        for(uint64_t at=0;at<count;at+=BATCH){pq_cpu_ms::Poll();uint64_t n=std::min(BATCH,count-at);
            pq_cpu_ms::diskann::pq_dist_lookup(codes->bytes.data()+(first+at)*codes->info.chunks,n,codes->info.chunks,query->lut.data(),temporary.data()+at);}
        for(float x:temporary)Require(std::isfinite(x),"CPU PQ distance is nonfinite",PQ_CPU_STATE);
        pq_cpu_ms::Poll();if(count)std::memcpy(output,temporary.data(),count*sizeof(float));});
}
extern "C" int pq_cpu_model_info(const PqCpuModel* model,PqCpuInfo* out,PqCpuError* e){return Guard(e,[&]{Require(model,"missing CPU PQ model");Info(model->info,out);});}
extern "C" int pq_cpu_codes_info(const PqCpuCodes* codes,PqCpuInfo* out,PqCpuError* e){return Guard(e,[&]{Require(codes,"missing CPU PQ codes");Info(codes->info,out);});}
extern "C" int pq_cpu_model_centers(const PqCpuModel* model,float* out,uint64_t capacity,PqCpuError* e){return Guard(e,[&]{Require(model&&out&&capacity>=model->centers.size(),"CPU PQ centers output capacity too small");std::memcpy(out,model->centers.data(),model->centers.size()*sizeof(float));});}
extern "C" int pq_cpu_codes_export(const PqCpuCodes* codes,uint8_t* out,uint64_t capacity,PqCpuError* e){return Guard(e,[&]{Require(codes&&capacity>=codes->bytes.size()&&(codes->bytes.empty()||out),"CPU PQ code output capacity too small");if(!codes->bytes.empty())std::memcpy(out,codes->bytes.data(),codes->bytes.size());});}
extern "C" void pq_cpu_model_close(PqCpuModel** p){if(p){delete *p;*p=nullptr;}}
extern "C" void pq_cpu_codes_close(PqCpuCodes** p){if(p){delete *p;*p=nullptr;}}
extern "C" void pq_cpu_query_close(PqCpuQuery** p){if(p){delete *p;*p=nullptr;}}
extern "C" int pq_cpu_backend_info(PqCpuBackendInfo* out,PqCpuError* error)
{
    return Guard(error,[&]{Require(out&&out->struct_size>=sizeof(*out),"invalid CPU PQ backend info output");auto b=Backend();*out=b;});
}
extern "C" int pq_cpu_model_restore(const PqCpuModelRestoreSpec* s,const PqCpuLimits* limits,const float* centers,uint64_t count,PqCpuModel** out,PqCpuError* error)
{
    return Guard(error,[&]{Require(s&&s->struct_size==sizeof(*s)&&out&&!*out,"invalid CPU PQ model restore request");
        RestoreVersions(s->version,s->component_version,s->algorithm_version,s->centroids);Shape(s->dimension,s->chunks,s->metric);
        Require(s->training_rows>=PQ_CPU_MIN_TRAIN&&s->training_rows<=PQ_CPU_MAX_TRAIN,"invalid CPU PQ persisted training population");
        Require(Hex(s->context_hash)&&Hex(s->codebook_hash)&&Hex(s->model_hash),"invalid CPU PQ persisted model identity");
        Require(count==Mul(256,s->dimension)&&centers,"CPU PQ persisted centers span mismatch");
        Budget(limits,Add(ModelBytes(s->dimension,s->chunks),65536));Scope scope(limits);pq_cpu_ms::Poll();CheckBackend(s->backend_hash);
        /* Centers are means, not query vectors: cosine centers need not have
         * unit length. Their finite/safe-magnitude contract is still checked. */
        Data(centers,256,s->dimension,PQ_CPU_L2,count);
        auto model=std::unique_ptr<PqCpuModel>(new PqCpuModel);auto& i=model->info;
        i.struct_size=sizeof(i);i.version=PQ_CPU_VERSION;i.dimension=s->dimension;i.chunks=s->chunks;i.metric=s->metric;
        i.rows=s->training_rows;i.source_file_id=-1;i.resident_bytes=ModelBytes(i.dimension,i.chunks);std::memcpy(i.context_hash,s->context_hash,65);
        model->centers.assign(centers,centers+count);ModelDerived(model.get());ModelHashes(model.get());
        Require(std::strcmp(i.payload_hash,s->codebook_hash)==0&&std::strcmp(i.model_hash,s->model_hash)==0,"CPU PQ persisted codebook/model hash mismatch");
        pq_cpu_ms::Poll();*out=model.release();});
}
extern "C" int pq_cpu_codes_builder_open(const PqCpuModel* model,const PqCpuCodesBuilderSpec* s,const PqCpuLimits* limits,PqCpuCodesBuilder** out,PqCpuError* error)
{
    return Guard(error,[&]{Require(model&&s&&s->struct_size==sizeof(*s)&&out&&!*out,"invalid CPU PQ builder open request");
        RestoreVersions(s->version,s->component_version,s->algorithm_version,s->centroids);Shape(s->dimension,s->chunks,s->metric);
        Require(s->dimension==model->info.dimension&&s->chunks==model->info.chunks&&s->metric==model->info.metric&&s->source_file_id>=0&&
            Hex(s->context_hash)&&Hex(s->model_hash)&&std::strcmp(s->context_hash,model->info.context_hash)==0&&std::strcmp(s->model_hash,model->info.model_hash)==0,
            "CPU PQ builder source/model identity mismatch");
        Require(s->node_count<=PQ_CPU_MAX_NODES,"CPU PQ builder node population exceeds ANN format",PQ_CPU_UNSUPPORTED);
        Require(s->input_kind==PQ_CPU_INPUT_FLOATS||s->input_kind==PQ_CPU_INPUT_CODES,"invalid CPU PQ builder input kind");
        Require(!s->expected_payload_hash||Hex(s->expected_payload_hash),"invalid CPU PQ expected code payload hash");
        Require(s->input_kind!=PQ_CPU_INPUT_CODES||s->expected_payload_hash,"CPU PQ raw code restore requires a payload hash");
        uint64_t bytes=Mul(s->node_count,s->chunks),resident=Add(sizeof(PqCpuCodesBuilder),Add(model->info.resident_bytes,Add(sizeof(PqCpuCodes),bytes)));
        Budget(limits,Add(model->info.resident_bytes,Add(resident,65536)));Scope scope(limits);pq_cpu_ms::Poll();CheckBackend(s->backend_hash);
        auto b=std::unique_ptr<PqCpuCodesBuilder>(new PqCpuCodesBuilder);b->input_kind=s->input_kind;
        b->model.reset(new PqCpuModel(*model));b->pending.reset(new PqCpuCodes);auto& i=b->pending->info;i=model->info;
        i.rows=s->node_count;i.source_file_id=s->source_file_id;i.resident_bytes=sizeof(PqCpuCodes)+bytes;
        i.payload_hash[0]=0;b->pending->model_bytes=model->info.resident_bytes;b->pending->bytes.resize(bytes);
        if(s->expected_payload_hash)std::memcpy(b->expected_hash,s->expected_payload_hash,65);
        pq_cpu_ms::Poll();*out=b.release();});
}
extern "C" int pq_cpu_codes_builder_append(PqCpuCodesBuilder* b,const PqCpuLimits* limits,uint64_t first,uint64_t rows,const float* data,uint64_t count,PqCpuError* error)
{
    return Guard(error,[&]{Mutate(b,[&]{BuilderRange(b,first,rows,PQ_CPU_INPUT_FLOATS);const auto& i=b->model->info;
        Budget(limits,Add(BuilderBytes(b),Add(Scratch(rows,i.dimension,i.chunks),Mul(rows,i.chunks))));Scope scope(limits);pq_cpu_ms::Poll();
        Data(data,rows,i.dimension,i.metric,count);std::vector<uint8_t> part;
        Require(pq_cpu_ms::diskann::generate_pq_data_from_pivots_simplified(data,rows,b->model->centers.data(),b->model->centers.size(),i.dimension,i.chunks,part)==0,
            "Microsoft PQ range encoder rejected checked input",PQ_CPU_STATE);
        Require(part.size()==Mul(rows,i.chunks),"CPU PQ range encoded population changed",PQ_CPU_STATE);pq_cpu_ms::Poll();
        std::memcpy(b->pending->bytes.data()+first*i.chunks,part.data(),part.size());b->next+=rows;});});
}
extern "C" int pq_cpu_codes_builder_append_raw(PqCpuCodesBuilder* b,const PqCpuLimits* limits,uint64_t first,uint64_t rows,const uint8_t* bytes,uint64_t count,PqCpuError* error)
{
    return Guard(error,[&]{Mutate(b,[&]{BuilderRange(b,first,rows,PQ_CPU_INPUT_CODES);uint64_t needed=Mul(rows,b->model->info.chunks);
        Require(count==needed&&bytes,"CPU PQ raw code span does not match batch");Budget(limits,Add(BuilderBytes(b),65536));
        Scope scope(limits);pq_cpu_ms::Poll();std::memcpy(b->pending->bytes.data()+first*b->model->info.chunks,bytes,count);b->next+=rows;});});
}
extern "C" int pq_cpu_codes_builder_finish(PqCpuCodesBuilder* b,const PqCpuLimits* limits,PqCpuCodes** out,PqCpuError* error)
{
    return Guard(error,[&]{Mutate(b,[&]{Require(out&&!*out,"invalid CPU PQ builder finish output");
        Require(b->next==b->pending->info.rows,"CPU PQ builder has incomplete dense node coverage",PQ_CPU_STATE);
        Budget(limits,Add(BuilderBytes(b),65536));Scope scope(limits);pq_cpu_ms::Poll();CodeHash(b->pending.get());
        Require(!b->expected_hash[0]||std::strcmp(b->expected_hash,b->pending->info.payload_hash)==0,"CPU PQ restored code payload hash mismatch");
        pq_cpu_ms::Poll();*out=b->pending.release();b->state=PQ_CPU_BUILDER_FINISHED;b->model.reset();});});
}
extern "C" int pq_cpu_codes_builder_info(const PqCpuCodesBuilder* b,PqCpuCodesBuilderInfo* out,PqCpuError* error)
{
    return Guard(error,[&]{Require(b&&out&&out->struct_size>=sizeof(*out),"invalid CPU PQ builder info request");
        PqCpuCodesBuilderInfo i{};i.struct_size=sizeof(i);i.input_kind=b->input_kind;i.state=b->state;i.max_batch_rows=PQ_CPU_MAX_BATCH_ROWS;
        i.next_node=b->next;i.node_count=b->pending?b->pending->info.rows:b->next;i.resident_bytes=BuilderBytes(b);*out=i;});
}
extern "C" void pq_cpu_codes_builder_close(PqCpuCodesBuilder** p){if(p){delete *p;*p=nullptr;}}
