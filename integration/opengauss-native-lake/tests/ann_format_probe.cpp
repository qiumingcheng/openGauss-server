/* Real SDK transport/format consumer. This deliberately does not claim ANN. */
#include "lake_ann_format.h"
#include <dlfcn.h>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>

static unsigned checks;
static void check(bool good,const char* name)
{++checks;if(!good){std::cerr<<"ANN format check failed: "<<name<<"\n";std::exit(1);}}
static void ok(int code,const OgLakeSdkError& error)
{if(code){std::cerr<<"ANN format status "<<code<<": "<<error.message<<"\n";std::exit(1);}++checks;}
static const char* context="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
static int cancel(void* value){return *static_cast<int*>(value);}
static OgLakeSdkLimits limits={sizeof(OgLakeSdkLimits),64*1024*1024,16*1024*1024,nullptr,nullptr};
static OgLakeAnnSpec spec(int metric,uint32_t count=143)
{return {sizeof(OgLakeAnnSpec),OG_LAKE_ANN_FORMAT_VERSION,3,(uint32_t)metric,count,count?2u:0u,(int64_t)count*2,context};}
struct Row {
    OgLakeAnnNode node{};float vector[3];uint32_t edges[2];OgLakeAnnAddress addresses[2];
    Row(uint32_t id,uint32_t count){
        vector[0]=(float)id+.25f;vector[1]=-0.0f;vector[2]=std::numeric_limits<float>::denorm_min();
        edges[0]=(id+1)%count;edges[1]=(id+count-1)%count;
        addresses[0]={(int64_t)(id/100),id,(int64_t)id+100000,17};
        addresses[1]={(int64_t)(id/100)+2,id,(int64_t)id+100000,18};
        node={id,(id==0||id==count-1)?1u:0u,0,2,2,vector,edges,addresses};}
};
static void compare(const OgLakeAnnNode* actual,uint32_t id,uint32_t count)
{
    Row expected(id,count);check(actual&&actual->node_id==id&&actual->entry==expected.node.entry,"node identity/entry");
    check(actual->deleted==0&&actual->neighbor_count==2&&actual->posting_count==2,"deletion/edge/posting populations");
    check(std::memcmp(actual->vector,expected.vector,sizeof(expected.vector))==0,"exact IEEE bits including negative zero and subnormal");
    for(int i=0;i<2;++i){check(actual->neighbors[i]==expected.edges[i],"directed topology unchanged");
        check(actual->postings[i].file_id==expected.addresses[i].file_id&&actual->postings[i].row_position==expected.addresses[i].row_position&&
            actual->postings[i].row_id==expected.addresses[i].row_id&&actual->postings[i].birth_snapshot==expected.addresses[i].birth_snapshot,
            "original physical/logical/version address unchanged");}
}
struct Writers {
    const OgLakeParquetSdkApi* api;OgLakeSdkWriter* nodes=nullptr;OgLakeSdkWriter* lookup=nullptr;
    Writers(const OgLakeParquetSdkApi* a,const std::string& directory):api(a){
        check(::mkdir(directory.c_str(),0700)==0,"new owned artifact directory");
        for(int i=0;i<2;++i){std::string path=directory+"/"+(i?OG_LAKE_ANN_LOOKUP_FILE:OG_LAKE_ANN_NODES_FILE);int32_t count;
            auto fields=og_lake_ann_fields(i,&count);OgLakeSdkWriteOptions options={sizeof(options),path.c_str(),limits,count,fields,OG_LAKE_SDK_ZSTD,OG_LAKE_ANN_PAGE_ROWS};
            OgLakeSdkError error{};ok(api->writer_open(&options,i?&lookup:&nodes,&error),error);}}
    ~Writers(){OgLakeSdkError error{};ok(api->writer_close(&nodes,&error),error);ok(api->writer_close(&lookup,&error),error);}
    void Finish(){for(auto writer:{nodes,lookup}){OgLakeSdkWrittenFile result{};result.struct_size=sizeof(result);OgLakeSdkError error{};
        ok(api->writer_finish(writer,&result,&error),error);check(result.local_fsync_complete,"local artifact durably finished");}}
};
static std::string build(const OgLakeParquetSdkApi* api,const std::string& directory,int metric,uint32_t count,bool alter=false)
{
    Writers sdk(api,directory);auto definition=spec(metric,count);OgLakeAnnWriter* writer=nullptr;OgLakeSdkError error{};
    ok(og_lake_ann_writer_open(api,&limits,&definition,sdk.nodes,sdk.lookup,&writer,&error),error);
    for(uint32_t id=0;id<count;++id){Row row(id,count);if(alter&&id==100)row.vector[0]+=1;
        ok(og_lake_ann_writer_append(writer,&row.node,1,&error),error);}
    OgLakeAnnWritten written{};written.struct_size=sizeof(written);ok(og_lake_ann_writer_finish(writer,&written,&error),error);
    check(written.page_count==(count+63)/64&&std::strlen(written.manifest_hash)==64,"page count/root digest");
    check(og_lake_ann_writer_append(writer,nullptr,0,&error)==OG_LAKE_SDK_STATE,"finished format writer is not reusable");
    og_lake_ann_writer_close(&writer);check(!writer,"format writer clears owner");sdk.Finish();return written.manifest_hash;
}
static void read(const OgLakeParquetSdkApi* api,const std::string& directory,const char* manifest,int metric,uint32_t count)
{
    auto definition=spec(metric,count);OgLakeSdkError error{};OgLakeAnnReader* reader=nullptr;
    std::string data=directory+"/"+OG_LAKE_ANN_NODES_FILE,lookup=directory+"/"+OG_LAKE_ANN_LOOKUP_FILE;
    OgLakeAnnSpec inspected={};inspected.struct_size=sizeof(inspected);
    ok(og_lake_ann_inspect(api,&limits,definition.dimension,definition.metric,context,data.c_str(),lookup.c_str(),&inspected,&error),error);
    check(inspected.node_count==definition.node_count&&inspected.entry_count==definition.entry_count&&
        inspected.posting_count==definition.posting_count,"populations derived from Parquet/directory without catalog extensions");
    ok(og_lake_ann_reader_open(api,&limits,&definition,manifest,data.c_str(),lookup.c_str(),&reader,&error),error);
    const auto* stored=og_lake_ann_reader_spec(reader);
    check(stored&&stored->dimension==3&&stored->metric==(uint32_t)metric&&stored->node_count==count&&
        std::strcmp(stored->context_hash,context)==0,"reader owns its immutable specification/context");
    ok(og_lake_ann_verify(reader,&error),error);
    const uint32_t* entries=nullptr;uint32_t entry_count=0;
    ok(og_lake_ann_reader_entries(reader,&entries,&entry_count,&error),error);
    check(entry_count==(count?2u:0u)&&(!count||(entries[0]==0&&entries[1]==count-1)),"entry identities come from verified pages");
    for(uint32_t i=0;i<count;++i){uint32_t id=(i*37)%count;const OgLakeAnnNode* node=nullptr;
        ok(og_lake_ann_reader_node(reader,id,&node,&error),error);compare(node,id,count);
        const OgLakeAnnNode* hit=nullptr;ok(og_lake_ann_reader_node(reader,id,&hit,&error),error);check(hit==node,"one-page cache reuses verified node storage");}
    const OgLakeAnnNode* node=nullptr;check(og_lake_ann_reader_node(reader,count,&node,&error)==OG_LAKE_SDK_INVALID&&!node,"out-of-range node is refused");
    check(og_lake_ann_verify(reader,&error)==OG_LAKE_SDK_STATE,"read error poisons format reader");og_lake_ann_reader_close(&reader);check(!reader,"reader clears owner");
    auto wrong=definition;wrong.metric=metric==1?2:1;
    check(og_lake_ann_reader_open(api,&limits,&wrong,manifest,data.c_str(),lookup.c_str(),&reader,&error)==OG_LAKE_SDK_INVALID&&!reader,"wrong metric/root rejected");
    wrong=definition;wrong.context_hash="1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    check(og_lake_ann_reader_open(api,&limits,&wrong,manifest,data.c_str(),lookup.c_str(),&reader,&error)==OG_LAKE_SDK_INVALID&&!reader,"wrong source/definition context rejected");
    auto old=*api;old.capabilities&=~(uint64_t)OG_LAKE_SDK_BINARY_VALUES;
    check(og_lake_ann_reader_open(&old,&limits,&definition,manifest,data.c_str(),lookup.c_str(),&reader,&error)==OG_LAKE_SDK_UNSUPPORTED&&!reader,"missing binary capability rejected before I/O");
    auto tiny=limits;tiny.memory_bytes=1024;tiny.footer_bytes=1024;
    check(og_lake_ann_reader_open(api,&tiny,&definition,manifest,data.c_str(),lookup.c_str(),&reader,&error)==OG_LAKE_SDK_MEMORY&&!reader,"small working budget rejected");
    int state=0;auto interruptible=limits;interruptible.should_cancel=cancel;interruptible.cancel_context=&state;
    ok(og_lake_ann_reader_open(api,&interruptible,&definition,manifest,data.c_str(),lookup.c_str(),&reader,&error),error);state=1;
    check(og_lake_ann_verify(reader,&error)==OG_LAKE_SDK_CANCELLED,"format scan cancellation");og_lake_ann_reader_close(&reader);
}
static void invalid(const OgLakeParquetSdkApi* api,const std::string& directory,int kind)
{
    Writers sdk(api,directory);auto definition=spec(3);OgLakeAnnWriter* writer=nullptr;OgLakeSdkError error{};
    ok(og_lake_ann_writer_open(api,&limits,&definition,sdk.nodes,sdk.lookup,&writer,&error),error);
    Row row(0,143);
    if(kind==0)row.node.node_id=1;
    if(kind==1)row.vector[0]=std::numeric_limits<float>::quiet_NaN();
    if(kind==2)row.vector[0]=row.vector[1]=row.vector[2]=0;
    if(kind==3)row.edges[0]=143;
    if(kind==4)row.addresses[1]=row.addresses[0];
    if(kind==5)row.addresses[0].birth_snapshot=-1;
    if(kind==6)row.node.neighbor_count=97;
    if(kind==7)row.node.posting_count=11;
    if(kind<8){check(og_lake_ann_writer_append(writer,&row.node,1,&error)==OG_LAKE_SDK_INVALID,"malformed node refused");
        check(og_lake_ann_writer_append(writer,nullptr,0,&error)==OG_LAKE_SDK_STATE,"invalid append poisons writer");}
    else{ok(og_lake_ann_writer_append(writer,&row.node,1,&error),error);OgLakeAnnWritten result{};result.struct_size=sizeof(result);
        check(og_lake_ann_writer_finish(writer,&result,&error)==OG_LAKE_SDK_INVALID,"incomplete physical coverage cannot finish");}
    og_lake_ann_writer_close(&writer);
}
static void navigation(const OgLakeParquetSdkApi* api,const std::string& directory)
{
    Writers sdk(api,directory);auto definition=spec(1,2);definition.posting_count=0;OgLakeAnnWriter* writer=nullptr;OgLakeSdkError error{};
    ok(og_lake_ann_writer_open(api,&limits,&definition,sdk.nodes,sdk.lookup,&writer,&error),error);
    for(uint32_t i=0;i<2;++i){Row row(i,2);row.node.deleted=1;row.node.posting_count=0;row.node.postings=nullptr;
        ok(og_lake_ann_writer_append(writer,&row.node,1,&error),error);}
    OgLakeAnnWritten written{};written.struct_size=sizeof(written);ok(og_lake_ann_writer_finish(writer,&written,&error),error);
    og_lake_ann_writer_close(&writer);sdk.Finish();
    std::string data=directory+"/"+OG_LAKE_ANN_NODES_FILE,lookup=directory+"/"+OG_LAKE_ANN_LOOKUP_FILE;OgLakeAnnReader* reader=nullptr;
    ok(og_lake_ann_reader_open(api,&limits,&definition,written.manifest_hash,data.c_str(),lookup.c_str(),&reader,&error),error);
    ok(og_lake_ann_verify(reader,&error),error);const uint32_t* entries=nullptr;uint32_t count=0;
    ok(og_lake_ann_reader_entries(reader,&entries,&count,&error),error);check(count==2&&entries[0]==0&&entries[1]==1,"navigation-only entries retained");
    for(uint32_t id=0;id<2;++id){const OgLakeAnnNode* node=nullptr;ok(og_lake_ann_reader_node(reader,id,&node,&error),error);
        check(node->deleted==1&&node->posting_count==0&&node->neighbor_count==2,"deleted navigation nodes preserve edges without inventing result rows");}
    og_lake_ann_reader_close(&reader);
}
int main(int argc,char** argv)
{
    check(argc==3||argc==7,"provider/output or reopen arguments");void* lib=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);check(lib,"real SDK loaded");
    auto entry=(OgLakeParquetSdkEntrypoint)dlsym(lib,"og_lake_parquet_sdk_v1");check(entry,"base SDK entry");auto api=entry();
    if(argc==7){uint32_t count=(uint32_t)std::stoul(argv[5]);int metric=std::stoi(argv[4]);
        if(std::strcmp(argv[6],"bad")==0){auto definition=spec(metric,count);OgLakeAnnReader* reader=nullptr;OgLakeSdkError error{};
            std::string data=std::string(argv[2])+"/"+OG_LAKE_ANN_NODES_FILE,lookup=std::string(argv[2])+"/"+OG_LAKE_ANN_LOOKUP_FILE;
            int code=og_lake_ann_reader_open(api,&limits,&definition,argv[3],data.c_str(),lookup.c_str(),&reader,&error);
            if(!code)code=og_lake_ann_verify(reader,&error);check(code!=OG_LAKE_SDK_OK,"corrupt graph/directory is never accepted");og_lake_ann_reader_close(&reader);
        }else read(api,argv[2],argv[3],metric,count);
        std::cout<<"{\"status\":\"pass\",\"checks\":"<<checks<<"}\n";return 0;}
    std::vector<std::string> roots;
    for(int metric=1;metric<=3;++metric){std::string directory=std::string(argv[2])+"/metric"+std::to_string(metric);auto root=build(api,directory,metric,143);roots.push_back(root);read(api,directory,root.c_str(),metric,143);}
    auto empty=std::string(argv[2])+"/empty";auto root=build(api,empty,1,0);roots.push_back(root);read(api,empty,root.c_str(),1,0);
    auto altered=build(api,std::string(argv[2])+"/altered",1,143,true);check(altered!=roots[0],"node changes alter the canonical page/root hash");
    navigation(api,std::string(argv[2])+"/navigation");
    for(int kind=0;kind<9;++kind)invalid(api,std::string(argv[2])+"/invalid"+std::to_string(kind),kind);
    std::cout<<"{\"status\":\"pass\",\"checks\":"<<checks<<",\"roots\":[";
    for(size_t i=0;i<roots.size();++i){if(i)std::cout<<",";std::cout<<"\""<<roots[i]<<"\"";}
    std::cout<<"]}\n";return 0;
}
