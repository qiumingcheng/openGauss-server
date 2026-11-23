/* Standalone real-provider bitmap component; no server or native index path. */
#include "lake_bitmap_format.h"
#include "vendor/arrow20/abi.h"
#include <roaring/roaring.h>
#include <roaring/memory.h>
#include <openssl/sha.h>
#include <dlfcn.h>
#include <sys/stat.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>
namespace {
int checks=0;void Check(bool v,const char* text){++checks;if(!v)throw std::runtime_error(text);}
void Ok(int status,const OgLakeSdkError& e){if(status)throw std::runtime_error(std::to_string(status)+": "+e.message);++checks;}
std::string Quote(const std::string& s){std::string out="\"";for(char c:s){if(c=='\\'||c=='\"')out+='\\';out+=c;}return out+'\"';}
std::string Digest(const OgLakeBitmapResult* result)
{SHA256_CTX h;SHA256_Init(&h);const auto* p=og_lake_bitmap_result_positions(result);for(int64_t i=0;i<og_lake_bitmap_result_count(result);++i){std::string line=std::to_string(p[i].file_id)+","+std::to_string(p[i].row_position)+"\n";SHA256_Update(&h,line.data(),line.size());}
unsigned char d[32];SHA256_Final(d,&h);const char digits[]="0123456789abcdef";std::string s;for(auto c:d){s+=digits[c>>4];s+=digits[c&15];}return s;}
struct Cancel {int after=0,polls=0;};int Poll(void* p){auto* c=(Cancel*)p;return c->after>0&&++c->polls>=c->after;}
struct File {
    const OgLakeParquetLifecycleApi* life;OgLakeSdkWriter* value=nullptr;bool retain=false;
    explicit File(const OgLakeParquetLifecycleApi* p):life(p){}
    ~File(){OgLakeSdkError e{};if(value){if(retain)life->base->writer_close(&value,&e);else life->writer_abort(&value,&e);}}
    void Open(const std::string& path,const OgLakeSdkLimits& limits){int32_t count;auto* fields=og_lake_bitmap_fields(&count);OgLakeSdkWriteOptions o{};
        o.struct_size=sizeof(o);o.path=path.c_str();o.limits=limits;o.num_fields=count;o.fields=fields;o.compression=OG_LAKE_SDK_ZSTD;o.row_group_rows=256;
        OgLakeSdkError e{};Ok(life->base->writer_open(&o,&value,&e),e);}
    void Finish(uint64_t rows){OgLakeSdkWrittenFile out{};out.struct_size=sizeof(out);OgLakeSdkError e{};Ok(life->base->writer_finish(value,&out,&e),e);
        Check(out.local_fsync_complete&&out.num_rows==(int64_t)rows,"SDK fsync and dictionary row count");retain=true;}
};
struct Writer {OgLakeBitmapWriter* value=nullptr;~Writer(){og_lake_bitmap_writer_close(&value);}};
struct Reader {OgLakeBitmapReader* value=nullptr;~Reader(){og_lake_bitmap_reader_close(&value);}};
struct Result {OgLakeBitmapResult* value=nullptr;~Result(){og_lake_bitmap_result_close(&value);}};
struct Artifact {std::string path,input,context,manifest;uint32_t type;uint64_t rows,keys;int64_t file;};
OgLakeBitmapFileSpec Spec(const Artifact& a){return {sizeof(OgLakeBitmapFileSpec),1,a.type,a.file,a.rows,a.context.c_str()};}
Artifact Write(const std::string& directory,const std::string& name,const std::vector<OgLakeScalarEntry>& original,
    const OgLakeParquetLifecycleApi* life,const OgLakeSdkLimits& limits,uint32_t type=OG_LAKE_SDK_INT64)
{
    Artifact a{directory+"/"+name+".parquet",directory+"/"+name+".csv",std::string(64,'a'),"",type,original.size(),0,71};
    std::ofstream csv(a.input);csv<<"key,file_id,row_position\n";for(auto row:original){if(row.is_null)csv<<"NULL";else csv<<row.key;csv<<','<<row.file_id<<','<<row.row_position<<'\n';}csv.close();
    auto rows=original;std::sort(rows.begin(),rows.end(),[](const OgLakeScalarEntry& l,const OgLakeScalarEntry& r){return std::make_tuple(l.is_null,l.is_null?0:l.key,l.row_position)<std::make_tuple(r.is_null,r.is_null?0:r.key,r.row_position);});
    File file(life);file.Open(a.path,limits);Writer writer;auto s=Spec(a);OgLakeSdkError e{};Ok(og_lake_bitmap_writer_open(life->base,&limits,&s,file.value,&writer.value,&e),e);
    for(size_t at=0;at<rows.size();){size_t n=std::min((size_t)997,rows.size()-at);Ok(og_lake_bitmap_writer_append(writer.value,rows.data()+at,n,&e),e);at+=n;}
    OgLakeBitmapWritten done{};done.struct_size=sizeof(done);Ok(og_lake_bitmap_writer_finish(writer.value,&done,&e),e);
    Check(done.physical_source_rows==a.rows,"source population separate from dictionary count");a.keys=done.dictionary_rows;a.manifest=done.manifest_hash;file.Finish(a.keys);
    Check(og_lake_bitmap_writer_finish(writer.value,&done,&e)==OG_LAKE_SDK_STATE,"double finish rejected");return a;
}
void Query(Reader& reader,int kind,bool null_argument,const std::vector<int64_t>& keys,bool comma)
{
    Result r;OgLakeSdkError e{};Ok(og_lake_bitmap_select(reader.value,kind,null_argument,keys.data(),keys.size(),&r.value,&e),e);
    if(comma)std::cout<<',';std::cout<<"{\"kind\":"<<kind<<",\"null_argument\":"<<(null_argument?"true":"false")<<",\"keys\":[";
    for(size_t i=0;i<keys.size();++i){if(i)std::cout<<',';std::cout<<keys[i];}std::cout<<"],\"rows\":"<<og_lake_bitmap_result_count(r.value)<<",\"sha256\":"<<Quote(Digest(r.value))<<'}';
}
struct Raw {int64_t key;std::string bytes;int64_t cardinality;};
std::string Portable(const std::vector<uint32_t>& positions)
{
    /* Fixture producer only; allocation-failure coverage targets the format's
     * checked builder below, never this small successful library helper. */
    auto* bitmap=roaring_bitmap_create();Check(bitmap,"fixture bitmap");
    for(auto p:positions)roaring_bitmap_add(bitmap,p);std::string bytes(roaring_bitmap_portable_size_in_bytes(bitmap),'\0');
    roaring_bitmap_portable_serialize(bitmap,&bytes[0]);roaring_bitmap_free(bitmap);return bytes;
}
void ReleaseArray(ArrowArray* p){p->release=nullptr;}void ReleaseSchema(ArrowSchema* p){p->release=nullptr;}
void WriteRaw(File& file,const std::vector<Raw>& rows)
{
    std::vector<int64_t> keys,cards;std::vector<int32_t> offsets(1,0);std::string bytes;
    for(const auto& row:rows){keys.push_back(row.key);cards.push_back(row.cardinality);bytes+=row.bytes;offsets.push_back((int32_t)bytes.size());}
    ArrowArray root{},columns[3]{};ArrowSchema schema{},fields[3]{};ArrowArray* children[]={&columns[0],&columns[1],&columns[2]};ArrowSchema* definitions[]={&fields[0],&fields[1],&fields[2]};
    const void* buffers[3][3]={{nullptr,keys.data(),nullptr},{nullptr,offsets.data(),bytes.data()},{nullptr,cards.data(),nullptr}};const void* root_buffers[]={nullptr};
    root.length=rows.size();root.n_buffers=1;root.buffers=root_buffers;root.n_children=3;root.children=children;root.release=ReleaseArray;
    schema.format="+s";schema.n_children=3;schema.children=definitions;schema.release=ReleaseSchema;int32_t count;const auto* layout=og_lake_bitmap_fields(&count);
    for(int i=0;i<3;++i){columns[i].length=rows.size();columns[i].n_buffers=i==1?3:2;columns[i].buffers=buffers[i];columns[i].release=ReleaseArray;
        fields[i].format=i==1?"z":"l";fields[i].name=layout[i].name;fields[i].flags=layout[i].nullable?ARROW_FLAG_NULLABLE:0;fields[i].release=ReleaseSchema;}
    OgLakeSdkError e{};Ok(file.life->base->writer_append(file.value,&root,&schema,&e),e);file.Finish(rows.size());
}
void Malformed(const OgLakeParquetLifecycleApi* life,const OgLakeSdkLimits& limits,const std::string& directory)
{
    Artifact source=Write(directory,"valid",{{0,71,0,0},{0,71,1,0},{1,71,2,0},{1,71,3,0}},life,limits);auto spec=Spec(source);
    const char* names[]={"key-order","duplicate-key","bad-cookie","trailing-payload","cardinality","overlap","hole","authenticated-payload"};
    std::cout<<"{\"status\":\"pass\",\"cases\":[";
    for(int mode=0;mode<8;++mode){std::vector<Raw> rows={{0,Portable({0,1}),2},{1,Portable({2,3}),2}};
        if(mode==0)std::swap(rows[0],rows[1]);if(mode==1)rows[1].key=0;if(mode==2)rows[0].bytes[0]=0;if(mode==3)rows[0].bytes+='x';
        if(mode==4)rows[0].cardinality=1;if(mode==5)rows[1].bytes=Portable({1,3});if(mode==6){rows[1].bytes=Portable({2});rows[1].cardinality=1;}
        if(mode==7)rows[1].key=2;std::string path=directory+"/"+names[mode]+".parquet";File file(life);file.Open(path,limits);WriteRaw(file,rows);
        Reader reader;OgLakeSdkError e{};int code=og_lake_bitmap_reader_open(life->base,&limits,&spec,source.manifest.c_str(),path.c_str(),&reader.value,&e);
        Check(code==OG_LAKE_SDK_INVALID&&!reader.value,"correct-schema bitmap damage rejected before reader publication");
        if(mode)std::cout<<',';std::cout<<"{\"name\":"<<Quote(names[mode])<<",\"code\":"<<code<<",\"partial\":false,\"message\":"<<Quote(e.message)<<'}';
    }
    std::cout<<"],\"checks\":"<<checks<<"}\n";
}
/* Fixed tracking storage itself cannot allocate or accidentally fail. These
 * hooks affect only this executable's hidden CRoaring copy, never the SDK. */
struct Allocation {void* p;};Allocation live[4096]{};size_t calls=0,fail_at=0,outstanding=0,failures=0;
bool Fail(){++calls;if(fail_at&&calls==fail_at){++failures;return true;}return false;}
void Track(void* p){if(!p)return;for(auto& a:live)if(!a.p){a.p=p;++outstanding;return;}std::abort();}
void Untrack(void* p){if(!p)return;for(auto& a:live)if(a.p==p){a.p=nullptr;--outstanding;return;}std::abort();}
void* Malloc(size_t n){if(Fail())return nullptr;void* p=std::malloc(n);Track(p);return p;}
void* Calloc(size_t n,size_t m){if(Fail())return nullptr;void* p=std::calloc(n,m);Track(p);return p;}
void* Realloc(void* p,size_t n){if(Fail())return nullptr;void* q=std::realloc(p,n);if(q){Untrack(p);Track(q);}return q;}
void Free(void* p){Untrack(p);std::free(p);}
void* Aligned(size_t align,size_t n){if(Fail())return nullptr;void* p=nullptr;if(posix_memalign(&p,align,n))return nullptr;Track(p);return p;}
void Fault(const OgLakeParquetLifecycleApi* life,const OgLakeSdkLimits& limits,const std::string& path,size_t at)
{
    File file(life);file.Open(path,limits);Writer writer;std::string context(64,'a');OgLakeBitmapFileSpec s{sizeof(s),1,OG_LAKE_SDK_INT64,71,1048576,context.c_str()};
    OgLakeSdkError e{};Ok(og_lake_bitmap_writer_open(life->base,&limits,&s,file.value,&writer.value,&e),e);
    std::vector<OgLakeScalarEntry> rows;rows.reserve(1048576);for(int64_t p=0;p<1048576;++p)rows.push_back({p<32?0:1,71,p,0});
    roaring_memory_t hooks={Malloc,Realloc,Calloc,Free,Aligned,Free};roaring_init_memory_hook(hooks);fail_at=at;
    int status=og_lake_bitmap_writer_append(writer.value,rows.data(),rows.size(),&e);if(!status){OgLakeBitmapWritten out{};out.struct_size=sizeof(out);status=og_lake_bitmap_writer_finish(writer.value,&out,&e);}
    if(at){Check(failures==1,"real CRoaring allocation failed exactly once");Check(status==OG_LAKE_SDK_MEMORY,"builder allocation failure maps to MEMORY");}
    else Ok(status,e);Check(!outstanding,"all partially built containers freed");
    std::cout<<"{\"status\":\"pass\",\"calls\":"<<calls<<",\"fail_at\":"<<at<<",\"failures\":"<<failures<<",\"outstanding\":"<<outstanding<<",\"code\":"<<status<<"}\n";
}
}
int main(int argc,char** argv)
{
    try{Check(argc>=4,"provider, directory and command required");void* dl=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);Check(dl,"load real SDK");
        auto entry=(OgLakeParquetLifecycleEntrypoint)dlsym(dl,"og_lake_parquet_sdk_v2");Check(entry,"actual SDK lifecycle entrypoint");const auto* life=entry();Check(life&&life->base,"provider API");
        Cancel cancel;OgLakeSdkLimits limits{sizeof(limits),128*1024*1024,32*1024*1024,Poll,&cancel};std::string command=argv[3];
        if(command=="fault"){Check(argc==5,"fault index required");Fault(life,limits,std::string(argv[2])+"/fault.parquet",std::stoull(argv[4]));return 0;}
        if(command=="malformed"){Malformed(life,limits,argv[2]);return 0;}
        if(command=="validate"){Check(argc==10,"validation source tuple required");Artifact a{argv[4],"",argv[5],argv[6],(uint32_t)std::stoul(argv[7]),std::stoull(argv[8]),0,std::stoll(argv[9])};auto s=Spec(a);Reader r;OgLakeSdkError e{};
            int code=og_lake_bitmap_reader_open(life->base,&limits,&s,a.manifest.c_str(),a.path.c_str(),&r.value,&e);std::cout<<"{\"code\":"<<code<<",\"partial\":"<<(r.value?"true":"false")<<",\"message\":"<<Quote(e.message)<<"}\n";return 0;}
        Check(command=="roundtrip","unknown command");std::vector<Artifact> artifacts;
        for(uint32_t type:{(uint32_t)OG_LAKE_SDK_INT32,(uint32_t)OG_LAKE_SDK_INT64}){std::vector<OgLakeScalarEntry> rows;
            for(int64_t p=0;p<131109;++p)rows.push_back({p%113-56,71,p,p%17==0});rows[1].key=type==OG_LAKE_SDK_INT32?INT32_MIN:INT64_MIN;rows[2].key=type==OG_LAKE_SDK_INT32?INT32_MAX:INT64_MAX;
            artifacts.push_back(Write(argv[2],type==OG_LAKE_SDK_INT32?"int32":"int64",rows,life,limits,type));}
        std::vector<OgLakeScalarEntry> dense;for(int64_t p=0;p<1048576;++p)dense.push_back({0,71,p,1});artifacts.push_back(Write(argv[2],"null-domain",dense,life,limits));
        artifacts.push_back(Write(argv[2],"empty",{},life,limits));
        std::vector<OgLakeScalarEntry> ndv;for(int64_t p=0;p<65536;++p)ndv.push_back({p,71,p,0});artifacts.push_back(Write(argv[2],"max-keys",ndv,life,limits));
        for(int mode=0;mode<8;++mode){File file(life);file.Open(std::string(argv[2])+"/invalid"+std::to_string(mode)+".parquet",limits);Writer writer;
            Artifact a{"","",std::string(64,'a'),"",OG_LAKE_SDK_INT32,3,0,71};auto s=Spec(a);OgLakeSdkError e{};Ok(og_lake_bitmap_writer_open(life->base,&limits,&s,file.value,&writer.value,&e),e);
            std::vector<OgLakeScalarEntry> rows={{1,71,0,0},{1,71,1,0},{2,71,2,0}};
            if(mode==0)rows[1].row_position=0;if(mode==1)rows[2].row_position=0;if(mode==2)rows[1].file_id=72;if(mode==3)rows[1].row_position=3;
            if(mode==4)rows[1].key=(int64_t)INT32_MAX+1;if(mode==5)rows[2].key=0;if(mode==6)rows.resize(2);
            if(mode==7){cancel.polls=0;cancel.after=1;}
            int code=og_lake_bitmap_writer_append(writer.value,rows.data(),rows.size(),&e);if(!code){OgLakeBitmapWritten out{};out.struct_size=sizeof(out);code=og_lake_bitmap_writer_finish(writer.value,&out,&e);}
            Check(code==(mode==7?OG_LAKE_SDK_CANCELLED:OG_LAKE_SDK_INVALID),"invalid/incomplete/cancelled input rejected");cancel.after=0;
            Check(og_lake_bitmap_writer_append(writer.value,nullptr,0,&e)==OG_LAKE_SDK_STATE,"failed writer remains unusable");}
        {File file(life);file.Open(std::string(argv[2])+"/over-limit.parquet",limits);Writer writer;Artifact a{"","",std::string(64,'a'),"",OG_LAKE_SDK_INT64,65537,0,71};auto s=Spec(a);OgLakeSdkError e{};
            Ok(og_lake_bitmap_writer_open(life->base,&limits,&s,file.value,&writer.value,&e),e);std::vector<OgLakeScalarEntry> rows;for(int64_t i=0;i<65537;++i)rows.push_back({i,71,i,0});
            int code=og_lake_bitmap_writer_append(writer.value,rows.data(),rows.size(),&e);if(!code){OgLakeBitmapWritten done{};done.struct_size=sizeof(done);code=og_lake_bitmap_writer_finish(writer.value,&done,&e);}
            Check(code==OG_LAKE_SDK_UNSUPPORTED,"NDV beyond prototype bound rejected");og_lake_bitmap_writer_close(&writer.value);
            s.physical_rows=1048577;Check(og_lake_bitmap_writer_open(life->base,&limits,&s,file.value,&writer.value,&e)==OG_LAKE_SDK_UNSUPPORTED&&!writer.value,"source beyond prototype bound rejected");
            s.physical_rows=3;auto low=limits;low.memory_bytes=1024;Check(og_lake_bitmap_writer_open(life->base,&low,&s,file.value,&writer.value,&e)==OG_LAKE_SDK_MEMORY&&!writer.value,"working-budget admission rejects before handle");}
        std::cout<<"{\"status\":\"pass\",\"artifacts\":[";bool comma=false;
        for(const auto& a:artifacts){Reader r;OgLakeSdkError e{};auto s=Spec(a);Ok(og_lake_bitmap_reader_open(life->base,&limits,&s,a.manifest.c_str(),a.path.c_str(),&r.value,&e),e);
            if(comma)std::cout<<',';comma=true;std::cout<<"{\"path\":"<<Quote(a.path)<<",\"input\":"<<Quote(a.input)<<",\"context\":"<<Quote(a.context)<<",\"manifest\":"<<Quote(a.manifest)
                <<",\"type\":"<<a.type<<",\"file_id\":"<<a.file<<",\"physical_rows\":"<<a.rows<<",\"dictionary_rows\":"<<a.keys<<",\"queries\":[";bool qcomma=false;
            for(int kind=1;kind<=5;++kind)for(int64_t key:{INT64_MIN,(int64_t)-57,(int64_t)0,(int64_t)7,(int64_t)57,INT64_MAX}){Query(r,kind,false,{key},qcomma);qcomma=true;}
            for(int kind=1;kind<=5;++kind)Query(r,kind,true,{0},true);Query(r,6,false,{},true);Query(r,7,false,{},true);Query(r,8,false,{},true);Query(r,8,false,{7,7,-2},true);Query(r,8,true,{7},true);std::cout<<"]}";
            Reader wrong;std::string bad(64,'0');Check(og_lake_bitmap_reader_open(life->base,&limits,&s,bad.c_str(),a.path.c_str(),&wrong.value,&e)==OG_LAKE_SDK_INVALID&&!wrong.value,"root mismatch yields no reader");
            auto altered=s;altered.file_id++;Check(og_lake_bitmap_reader_open(life->base,&limits,&altered,a.manifest.c_str(),a.path.c_str(),&wrong.value,&e)==OG_LAKE_SDK_INVALID&&!wrong.value,"source identity mismatch yields no reader");
            Result out;int64_t key=1;Check(og_lake_bitmap_select(r.value,0,0,nullptr,0,&out.value,&e)==OG_LAKE_SDK_INVALID&&!out.value,"unknown predicate rejected");
            Check(og_lake_bitmap_select(r.value,6,1,nullptr,0,&out.value,&e)==OG_LAKE_SDK_INVALID&&!out.value,"NULL-test arity matches scalar protocol");
            Check(og_lake_bitmap_select(r.value,3,0,&key,0,&out.value,&e)==OG_LAKE_SDK_INVALID&&!out.value,"comparison arity rejected");
            cancel.polls=0;cancel.after=1;Check(og_lake_bitmap_select(r.value,7,0,nullptr,0,&out.value,&e)==OG_LAKE_SDK_CANCELLED&&!out.value,"cancel yields no partial positions");
            Check(og_lake_bitmap_reader_open(life->base,&limits,&s,a.manifest.c_str(),a.path.c_str(),&wrong.value,&e)==OG_LAKE_SDK_CANCELLED&&!wrong.value,"cancel interrupts open validation");cancel.after=0;
        }
        std::cout<<"],\"checks\":"<<checks<<"}\n";return 0;
    }catch(const std::exception& e){std::cerr<<"bitmap format probe failed: "<<e.what()<<'\n';return 1;}
}
