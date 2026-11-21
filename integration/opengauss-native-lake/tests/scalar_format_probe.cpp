/* Standalone actual-provider consumer. SQL oracle lives in the Python driver. */
#include "lake_scalar_format.h"
#include <openssl/sha.h>
#include <dlfcn.h>
#include <sys/stat.h>
#include <dirent.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

static int checks=0;
static void Check(bool value,const char* text){++checks;if(!value)throw std::runtime_error(text);}
static void Ok(int code,const OgLakeSdkError& error){if(code)throw std::runtime_error(std::string("format/provider error ")+std::to_string(code)+": "+error.message);++checks;}
static std::string Quote(const std::string& value)
{
    static const char digits[]="0123456789abcdef";std::string out="\"";
    for(unsigned char c:value){
        if(c=='\\'||c=='\"'){out+='\\';out+=(char)c;}
        else if(c<32){out+="\\u00";out+=digits[c>>4];out+=digits[c&15];}
        else out+=(char)c;
    }
    return out+'\"';
}

static std::string Digest(const OgLakeScalarResult* result)
{
    SHA256_CTX hash;SHA256_Init(&hash);const auto* rows=og_lake_scalar_result_positions(result);
    for(int64_t i=0;i<og_lake_scalar_result_count(result);++i){std::string row=std::to_string(rows[i].file_id)+","+std::to_string(rows[i].row_position)+"\n";SHA256_Update(&hash,row.data(),row.size());}
    unsigned char raw[32];SHA256_Final(raw,&hash);static const char digits[]="0123456789abcdef";std::string out;
    for(unsigned char c:raw){out+=digits[c>>4];out+=digits[c&15];}return out;
}
struct Cancel {int polls=0,after=0;};
static int Poll(void* pointer){auto* value=(Cancel*)pointer;return value->after>0&&++value->polls>=value->after;}
struct Pair {
    const OgLakeParquetSdkApi* api;const OgLakeParquetLifecycleApi* life;
    OgLakeSdkWriter* data=nullptr;OgLakeSdkWriter* lookup=nullptr;bool retain=false;
    Pair(const OgLakeParquetLifecycleApi* value):api(value->base),life(value){}
    ~Pair(){OgLakeSdkError e{};for(auto** p:{&data,&lookup})if(*p){if(retain)api->writer_close(p,&e);else life->writer_abort(p,&e);}}
    void Open(const std::string& directory,const OgLakeSdkLimits& limits)
    {
        Check(::mkdir(directory.c_str(),0700)==0,"test directory must be new");
        for(int i=0;i<2;++i){int32_t count;const auto* fields=og_lake_scalar_fields(i,&count);
            std::string path=directory+"/"+(i?OG_LAKE_SCALAR_LOOKUP_FILE:OG_LAKE_SCALAR_DATA_FILE);
            OgLakeSdkWriteOptions options{};options.struct_size=sizeof(options);options.path=path.c_str();options.limits=limits;
            options.num_fields=count;options.fields=fields;options.compression=OG_LAKE_SDK_ZSTD;options.row_group_rows=OG_LAKE_SCALAR_PAGE_ROWS;
            OgLakeSdkError error{};Ok(api->writer_open(&options,i?&lookup:&data,&error),error);}
    }
    void Finish(int64_t rows,int64_t pages)
    {
        for(int i=0;i<2;++i){OgLakeSdkWrittenFile result{};result.struct_size=sizeof(result);OgLakeSdkError error{};
            Ok(api->writer_finish(i?lookup:data,&result,&error),error);Check(result.local_fsync_complete==1&&result.num_rows==(i?pages:rows),"durable Parquet row count");}
        retain=true;
    }
};
struct Reader {OgLakeScalarReader* value=nullptr;~Reader(){og_lake_scalar_reader_close(&value);}};
struct Result {OgLakeScalarResult* value=nullptr;~Result(){og_lake_scalar_result_close(&value);}};
struct Artifact {std::string directory,context,manifest,input;uint32_t type;};

static Artifact Write(const std::string& root,const std::string& name,uint32_t type,std::vector<OgLakeScalarEntry> rows,
    const OgLakeParquetLifecycleApi* life,const OgLakeSdkLimits& limits)
{
    Artifact artifact{root+"/"+name,std::string(64,name[0]),"",root+"/"+name+"-input.csv",type};
    std::ofstream csv(artifact.input);csv<<"values,file_id,row_position\n";
    for(const auto& row:rows){if(row.is_null)csv<<"NULL";else csv<<row.key;csv<<','<<row.file_id<<','<<row.row_position<<'\n';}csv.close();
    std::sort(rows.begin(),rows.end(),[](const OgLakeScalarEntry& a,const OgLakeScalarEntry& b){
        if(a.is_null!=b.is_null)return a.is_null<b.is_null;
        return std::make_tuple(a.is_null?0:a.key,a.file_id,a.row_position)<std::make_tuple(b.is_null?0:b.key,b.file_id,b.row_position);});
    Pair files(life);files.Open(artifact.directory,limits);OgLakeScalarWriter* writer=nullptr;OgLakeSdkError error{};
    Ok(og_lake_scalar_writer_open(files.api,&limits,type,artifact.context.c_str(),files.data,files.lookup,&writer,&error),error);
    for(size_t offset=0;offset<rows.size();){size_t count=std::min(rows.size()-offset,(size_t)(offset%4099+1));
        Ok(og_lake_scalar_writer_append(writer,rows.data()+offset,count,&error),error);offset+=count;}
    OgLakeScalarWritten written{};written.struct_size=sizeof(written);Ok(og_lake_scalar_writer_finish(writer,&written,&error),error);
    Check(written.num_rows==(int64_t)rows.size()&&written.num_pages==(int64_t)((rows.size()+4095)/4096),"format physical completeness");
    Check(og_lake_scalar_writer_finish(writer,&written,&error)==OG_LAKE_SDK_STATE,"double finish rejected");
    artifact.manifest=written.manifest_hash;og_lake_scalar_writer_close(&writer);files.Finish(written.num_rows,written.num_pages);return artifact;
}
static void Query(Reader& reader,int kind,bool null_argument,const std::vector<int64_t>& keys,bool comma)
{
    OgLakeSdkError error{};Result result;Ok(og_lake_scalar_select(reader.value,kind,null_argument,keys.data(),keys.size(),&result.value,&error),error);
    if(comma)std::cout<<',';std::cout<<"{\"kind\":"<<kind<<",\"null_argument\":"<<(null_argument?"true":"false")<<",\"keys\":[";
    for(size_t i=0;i<keys.size();++i){if(i)std::cout<<',';std::cout<<keys[i];}
    std::cout<<"],\"rows\":"<<og_lake_scalar_result_count(result.value)<<",\"sha256\":"<<Quote(Digest(result.value))<<'}';
}
static int Fds(){int count=0;DIR* dir=opendir("/proc/self/fd");Check(dir!=nullptr,"FD inspection");while(readdir(dir))++count;closedir(dir);return count;}
int main(int argc,char** argv)
{
    try{
        Check(argc==3||argc==6||argc==9||argc==10,"provider/output or explicit validation tuple required");void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);Check(library!=nullptr,"load genuine provider");
        auto entry=(OgLakeParquetLifecycleEntrypoint)dlsym(library,"og_lake_parquet_sdk_v2");Check(entry!=nullptr,"actual lifecycle ABI");
        const auto* life=entry();Check(life&&life->abi_version==2&&life->base,"genuine provider lifecycle");
        Cancel cancel;OgLakeSdkLimits limits{sizeof(limits),64*1024*1024,32*1024*1024,Poll,&cancel};
        if(argc==9||argc==10) {
            Check(std::string(argv[3])=="validate","unknown probe command");
            if(argc==10){limits.memory_bytes=std::stoll(argv[9]);limits.footer_bytes=std::min(limits.footer_bytes,limits.memory_bytes);}
            Reader reader;OgLakeSdkError error{};
            int code=og_lake_scalar_reader_open(life->base,&limits,(uint32_t)std::stoul(argv[6]),argv[4],argv[5],argv[7],argv[8],&reader.value,&error);
            int verified=-1;if(code==0){verified=og_lake_scalar_verify(reader.value,&error);code=verified;}
            bool partial=false;
            if(code==0)for(int kind:{OG_LAKE_SCALAR_IS_NULL,OG_LAKE_SCALAR_NOT_NULL}) {
                Result result;code=og_lake_scalar_select(reader.value,kind,0,nullptr,0,&result.value,&error);
                if(code){partial=result.value!=nullptr;break;}
            }
            std::cout<<"{\"code\":"<<code<<",\"verify_code\":"<<verified<<",\"partial_result\":"<<(partial?"true":"false")<<",\"message\":"<<Quote(error.message)<<"}\n";
            return 0;
        }
        if(argc==6) {
            Check(std::string(argv[3])=="rewrite","unknown rewrite command");
            std::ifstream input(argv[4]);Check(input.good(),"open original independent CSV");
            std::string line;std::getline(input,line);std::vector<OgLakeScalarEntry> rows;
            while(std::getline(input,line)) {
                auto first=line.find(','),second=line.find(',',first+1);Check(first!=std::string::npos&&second!=std::string::npos,"numeric CSV record");
                std::string key=line.substr(0,first);rows.push_back({key=="NULL"?0:std::stoll(key),
                    std::stoll(line.substr(first+1,second-first-1))+10000,std::stoll(line.substr(second+1)),key=="NULL"?1:0});
            }
            Artifact altered=Write(argv[2],"aaaa",(uint32_t)std::stoul(argv[5]),rows,life,limits);
            std::cout<<"{\"data\":"<<Quote(altered.directory+"/"+OG_LAKE_SCALAR_DATA_FILE)<<",\"lookup\":"<<Quote(altered.directory+"/"+OG_LAKE_SCALAR_LOOKUP_FILE)<<"}\n";
            return 0;
        }
        std::vector<Artifact> artifacts;
        for(int type:{OG_LAKE_SDK_INT64,OG_LAKE_SDK_INT32}){
            std::vector<OgLakeScalarEntry> rows;
            for(int64_t i=0;i<17000;++i)rows.push_back({i%97-48,i%3,i/3,(int32_t)(i%13==0)});
            for(int64_t i=0;i<8500;++i)rows.push_back({7,9,i,0});
            rows.push_back({type==OG_LAKE_SDK_INT64?INT64_MIN:INT32_MIN,4,0,0});
            rows.push_back({type==OG_LAKE_SDK_INT64?INT64_MAX:INT32_MAX,4,1,0});
            artifacts.push_back(Write(argv[2],type==OG_LAKE_SDK_INT64?"bbbb":"cccc",type,rows,life,limits));
        }
        std::vector<OgLakeScalarEntry> nulls;for(int64_t i=0;i<8193;++i)nulls.push_back({INT64_MAX,1,i,1});
        artifacts.push_back(Write(argv[2],"dddd",OG_LAKE_SDK_INT32,nulls,life,limits));
        artifacts.push_back(Write(argv[2],"eeee",OG_LAKE_SDK_INT64,{},life,limits));
        for(int mode=0;mode<4;++mode){Pair files(life);files.Open(std::string(argv[2])+"/bad"+std::to_string(mode),limits);
            OgLakeScalarWriter* writer=nullptr;OgLakeSdkError error{};std::string context(64,'a');
            Ok(og_lake_scalar_writer_open(files.api,&limits,OG_LAKE_SDK_INT32,context.c_str(),files.data,files.lookup,&writer,&error),error);
            std::vector<OgLakeScalarEntry> rows;
            if(mode==0)rows={{2,0,0,0},{1,0,1,0}};
            if(mode==1)rows={{1,0,0,0},{1,0,0,0}};
            if(mode==2)rows={{(int64_t)INT32_MAX+1,0,0,0}};
            if(mode==3)rows={{0,-1,0,0}};
            Check(og_lake_scalar_writer_append(writer,rows.data(),rows.size(),&error)==OG_LAKE_SDK_INVALID,"invalid sorted input rejected");
            Check(og_lake_scalar_writer_append(writer,nullptr,0,&error)==OG_LAKE_SDK_STATE,"failed writer remains unusable");og_lake_scalar_writer_close(&writer);}
        std::cout<<"{\"status\":\"pass\",\"artifacts\":[";bool first=true;
        for(const auto& artifact:artifacts){Reader reader;OgLakeSdkError error{};
            std::string data=artifact.directory+"/"+OG_LAKE_SCALAR_DATA_FILE,lookup=artifact.directory+"/"+OG_LAKE_SCALAR_LOOKUP_FILE;
            Ok(og_lake_scalar_reader_open(life->base,&limits,artifact.type,artifact.context.c_str(),artifact.manifest.c_str(),data.c_str(),lookup.c_str(),&reader.value,&error),error);
            Ok(og_lake_scalar_verify(reader.value,&error),error);
            if(!first)std::cout<<',';first=false;
            std::cout<<"{\"input\":"<<Quote(artifact.input)<<",\"data\":"<<Quote(data)<<",\"lookup\":"<<Quote(lookup)<<",\"manifest\":"<<Quote(artifact.manifest)<<",\"context\":"<<Quote(artifact.context)<<",\"key_type\":"<<artifact.type<<",\"queries\":[";bool comma=false;
            for(int kind=1;kind<=5;++kind)for(int64_t key:{INT64_MIN,(int64_t)-49,(int64_t)0,(int64_t)7,(int64_t)49,INT64_MAX}){Query(reader,kind,false,{key},comma);comma=true;}
            for(int kind=1;kind<=5;++kind)Query(reader,kind,true,{0},true);
            Query(reader,6,false,{},true);Query(reader,7,false,{},true);Query(reader,8,false,{},true);
            Query(reader,8,false,{7,7,2},true);Query(reader,8,false,{INT64_MIN,INT64_MAX,7},true);Query(reader,8,true,{},true);
            std::cout<<"]}";
            Reader wrong;std::string bad(64,'0');Check(og_lake_scalar_reader_open(life->base,&limits,artifact.type,artifact.context.c_str(),bad.c_str(),data.c_str(),lookup.c_str(),&wrong.value,&error)==OG_LAKE_SDK_INVALID&&!wrong.value,"manifest mismatch rejected");
            Check(og_lake_scalar_reader_open(life->base,&limits,artifact.type,bad.c_str(),artifact.manifest.c_str(),data.c_str(),lookup.c_str(),&wrong.value,&error)==OG_LAKE_SDK_INVALID&&!wrong.value,"context mismatch rejected");
            Result result;Check(og_lake_scalar_select(reader.value,0,0,nullptr,0,&result.value,&error)==OG_LAKE_SDK_INVALID&&!result.value,"unknown predicate rejected");
            cancel.polls=0;cancel.after=1;Check(og_lake_scalar_select(reader.value,6,0,nullptr,0,&result.value,&error)==OG_LAKE_SDK_CANCELLED&&!result.value,"cancel returns no partial result");
            cancel.polls=0;Check(og_lake_scalar_verify(reader.value,&error)==OG_LAKE_SDK_CANCELLED,"cancel interrupts streaming verification");cancel.after=0;
            Ok(og_lake_scalar_select(reader.value,6,0,nullptr,0,&result.value,&error),error);og_lake_scalar_result_close(&result.value);
            int before=Fds();for(int i=0;i<25;++i){int64_t key=7;Ok(og_lake_scalar_select(reader.value,3,0,&key,1,&result.value,&error),error);og_lake_scalar_result_close(&result.value);}Check(before==Fds(),"repeated selections release file descriptors");
        }
        std::cout<<"],\"checks\":"<<checks<<"}\n";return 0;
    }catch(const std::exception& e){std::cerr<<"scalar format probe failed: "<<e.what()<<'\n';return 1;}
}
