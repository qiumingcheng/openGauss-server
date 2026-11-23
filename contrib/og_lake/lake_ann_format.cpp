/* Page directory + immutable graph records over real Parquet. This transports
 * existing DiskANN output; it contains no graph build or search algorithm. */
#include "lake_ann_format.h"
#include "vendor/arrow20/abi.h"
#include <openssl/sha.h>
#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
const OgLakeSdkField NODES[]={{0,OG_LAKE_SDK_INT64,0,"node_id"},
    {1,OG_LAKE_SDK_INT64,0,"flags"},{2,OG_LAKE_SDK_BINARY,0,"vector_le"},
    {3,OG_LAKE_SDK_BINARY,0,"neighbors_le"},{4,OG_LAKE_SDK_BINARY,0,"postings_le"}};
const OgLakeSdkField DIRECTORY[]={{0,OG_LAKE_SDK_INT64,0,"page_id"},
    {1,OG_LAKE_SDK_INT64,0,"row_count"},{2,OG_LAKE_SDK_INT64,0,"entry_count"},
    {3,OG_LAKE_SDK_INT64,0,"posting_count"},{4,OG_LAKE_SDK_UTF8,0,"page_hash"}};
struct Failure:std::runtime_error {
    int code,number;
    Failure(int c,const char* m,int n=0):std::runtime_error(m),code(c),number(n){}
};
void Require(bool yes,const char* message,int code=OG_LAKE_SDK_INVALID)
{if(!yes)throw Failure(code,message);}
void Check(int code,const OgLakeSdkError& error)
{if(code!=OG_LAKE_SDK_OK)throw Failure(code,error.message,error.system_errno);}
void Poll(const OgLakeSdkLimits& limits)
{if(limits.should_cancel&&limits.should_cancel(limits.cancel_context))throw Failure(OG_LAKE_SDK_CANCELLED,"ANN format operation cancelled");}
template<class F>int Guard(OgLakeSdkError* error,F fn)noexcept
{
    if(error)std::memset(error,0,sizeof(*error));
    try{fn();return OG_LAKE_SDK_OK;}
    catch(const Failure& x){if(error){error->code=x.code;error->system_errno=x.number;std::strncpy(error->message,x.what(),sizeof(error->message)-1);}return x.code;}
    catch(const std::bad_alloc&){if(error){error->code=OG_LAKE_SDK_MEMORY;std::strcpy(error->message,"ANN format allocation failed");}return OG_LAKE_SDK_MEMORY;}
    catch(...){if(error){error->code=OG_LAKE_SDK_STATE;std::strcpy(error->message,"unexpected ANN format failure");}return OG_LAKE_SDK_STATE;}
}
bool Hex(const char* text)
{
    if(!text||std::strlen(text)!=64)return false;
    for(int i=0;i<64;++i)if(!((text[i]>='0'&&text[i]<='9')||(text[i]>='a'&&text[i]<='f')))return false;
    return true;
}
std::string Digest(SHA256_CTX hash)
{
    unsigned char raw[32];SHA256_Final(raw,&hash);const char* digits="0123456789abcdef";
    std::string result(64,'0');for(int i=0;i<32;++i){result[i*2]=digits[raw[i]>>4];result[i*2+1]=digits[raw[i]&15];}return result;
}
void Integer(SHA256_CTX& hash,uint64_t value)
{
    unsigned char bytes[8];for(int i=0;i<8;++i){bytes[i]=(unsigned char)(value&255);value>>=8;}SHA256_Update(&hash,bytes,8);
}
void Put(std::vector<unsigned char>& bytes,uint64_t value,int width)
{for(int i=0;i<width;++i){bytes.push_back((unsigned char)(value&255));value>>=8;}}
uint64_t Get(const unsigned char* bytes,int width)
{uint64_t value=0;for(int i=width-1;i>=0;--i)value=(value<<8)|bytes[i];return value;}
void Validate(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,const OgLakeAnnSpec* spec)
{
    Require(api&&api->abi_version==1&&api->struct_size>=sizeof(*api)&&
        (api->capabilities&OG_LAKE_SDK_BINARY_VALUES)&&api->writer_append&&api->reader_open&&
        api->reader_next&&api->reader_close&&api->batch_release&&api->footer_open&&api->footer_info&&api->footer_close,
        "ANN graph storage requires the binary provider capability",OG_LAKE_SDK_UNSUPPORTED);
    Require(limits&&limits->struct_size>=sizeof(*limits)&&limits->memory_bytes>=8*1024*1024&&
        limits->footer_bytes>0&&limits->footer_bytes<=limits->memory_bytes,
        "ANN graph storage requires an explicit SDK budget of at least 8 MiB",OG_LAKE_SDK_MEMORY);
    Require(sizeof(float)==4&&std::numeric_limits<float>::is_iec559,"ANN graph requires IEEE float32",OG_LAKE_SDK_UNSUPPORTED);
    Require(spec&&spec->struct_size>=sizeof(*spec)&&spec->format_version==OG_LAKE_ANN_FORMAT_VERSION&&
        spec->dimension>=1&&spec->dimension<=OG_LAKE_ANN_MAX_DIM&&spec->metric>=1&&spec->metric<=3&&
        spec->node_count<=OG_LAKE_ANN_MAX_PAGES*OG_LAKE_ANN_PAGE_ROWS&&
        spec->entry_count<=spec->node_count&&((spec->node_count==0)==(spec->entry_count==0))&&
        spec->posting_count>=0&&spec->posting_count<=(int64_t)spec->node_count*OG_LAKE_ANN_MAX_POSTINGS&&
        Hex(spec->context_hash),"invalid ANN graph format specification");Poll(*limits);
}
SHA256_CTX Start(const OgLakeAnnSpec& spec,const std::string& context,const char* domain)
{
    SHA256_CTX hash;SHA256_Init(&hash);SHA256_Update(&hash,domain,std::strlen(domain));SHA256_Update(&hash,context.data(),context.size());
    Integer(hash,spec.format_version);Integer(hash,spec.dimension);Integer(hash,spec.metric);
    Integer(hash,spec.node_count);Integer(hash,spec.entry_count);Integer(hash,spec.posting_count);Integer(hash,OG_LAKE_ANN_PAGE_ROWS);return hash;
}
struct Node {
    OgLakeAnnNode view{};
    std::vector<float> vector;
    std::vector<uint32_t> edges;
    std::vector<OgLakeAnnAddress> postings;
    void Refresh(){view.vector=vector.data();view.neighbors=edges.data();view.postings=postings.data();}
};
Node Copy(const OgLakeAnnSpec& spec,const OgLakeAnnNode& in)
{
    Require(in.node_id<spec.node_count&&in.entry<=1&&in.deleted<=1&&in.vector&&in.neighbor_count<=OG_LAKE_ANN_MAX_DEGREE&&
        (in.neighbor_count==0||in.neighbors)&&in.posting_count<=OG_LAKE_ANN_MAX_POSTINGS&&(in.posting_count==0||in.postings),
        "invalid ANN graph node");
    Node row;row.view=in;row.vector.assign(in.vector,in.vector+spec.dimension);double norm=0;
    for(float value:row.vector){Require(std::isfinite(value),"nonfinite ANN graph vector");norm+=(double)value*value;}
    Require(spec.metric!=3||norm>0,"zero cosine vector cannot be a graph vertex");
    if(in.neighbor_count)row.edges.assign(in.neighbors,in.neighbors+in.neighbor_count);
    for(uint32_t id:row.edges)Require(id<spec.node_count,"ANN neighbor is outside this segment");
    if(in.posting_count)row.postings.assign(in.postings,in.postings+in.posting_count);
    for(size_t i=0;i<row.postings.size();++i){const auto& address=row.postings[i];
        Require(address.file_id>=0&&address.row_position>=0&&address.row_id>=0&&address.birth_snapshot>=0,"invalid ANN source address");
        for(size_t j=0;j<i;++j)Require(address.file_id!=row.postings[j].file_id||address.row_position!=row.postings[j].row_position,
            "duplicate physical ANN posting in a node");}
    row.Refresh();return row;
}
struct Page {uint32_t id=0,rows=0,entries=0;int64_t postings=0;std::string hash;};
std::array<std::vector<unsigned char>,3> Encode(const Node& node)
{
    std::array<std::vector<unsigned char>,3> bytes;
    for(float value:node.vector){uint32_t bits;std::memcpy(&bits,&value,4);Put(bytes[0],bits,4);}
    for(uint32_t id:node.edges)Put(bytes[1],id,4);
    for(const auto& row:node.postings){Put(bytes[2],row.file_id,8);Put(bytes[2],row.row_position,8);Put(bytes[2],row.row_id,8);Put(bytes[2],row.birth_snapshot,8);}return bytes;
}
Page Describe(const OgLakeAnnSpec& spec,const std::string& context,uint32_t id,const std::vector<Node>& nodes)
{
    Page page;page.id=id;page.rows=nodes.size();auto hash=Start(spec,context,"og_lake_ann_page_v1");Integer(hash,id);Integer(hash,nodes.size());
    for(const auto& row:nodes){Integer(hash,row.view.node_id);Integer(hash,row.view.entry);Integer(hash,row.view.deleted);page.entries+=row.view.entry;page.postings+=row.postings.size();
        for(const auto& data:Encode(row)){Integer(hash,data.size());if(!data.empty())SHA256_Update(&hash,data.data(),data.size());}}
    page.hash=Digest(hash);return page;
}
std::string Root(const OgLakeAnnSpec& spec,const std::string& context,const std::vector<Page>& pages)
{
    auto hash=Start(spec,context,"og_lake_ann_directory_v1");Integer(hash,pages.size());
    for(const auto& page:pages){Integer(hash,page.id);Integer(hash,page.rows);Integer(hash,page.entries);Integer(hash,page.postings);SHA256_Update(&hash,page.hash.data(),page.hash.size());}return Digest(hash);
}
void ReleaseArray(ArrowArray* a){a->release=nullptr;}
void ReleaseSchema(ArrowSchema* s){s->release=nullptr;}
struct Batch {
    ArrowArray array{};ArrowSchema schema{};std::array<ArrowArray,5> arrays{};std::array<ArrowSchema,5> schemas{};
    std::array<ArrowArray*,5> children;std::array<ArrowSchema*,5> fields;
    std::array<std::array<const void*,3>,5> buffers{};const void* root_buffers[1]={nullptr};
    Batch(const OgLakeSdkField* definitions,int64_t count){
        array.length=count;array.n_buffers=1;array.buffers=root_buffers;array.n_children=5;array.children=children.data();array.release=ReleaseArray;
        schema.format="+s";schema.n_children=5;schema.children=fields.data();schema.release=ReleaseSchema;
        for(size_t i=0;i<5;++i){bool binary=definitions[i].type==OG_LAKE_SDK_BINARY,text=definitions[i].type==OG_LAKE_SDK_UTF8;
            children[i]=&arrays[i];fields[i]=&schemas[i];arrays[i].length=count;arrays[i].n_buffers=binary||text?3:2;
            arrays[i].buffers=buffers[i].data();arrays[i].release=ReleaseArray;
            schemas[i].format=binary?"z":text?"u":"l";schemas[i].name=definitions[i].name;schemas[i].release=ReleaseSchema;}}
};
void WriteNodes(const OgLakeParquetSdkApi* api,OgLakeSdkWriter* writer,const std::vector<Node>& nodes)
{
    Batch batch(NODES,nodes.size());std::vector<int64_t> ids,entries;std::array<std::vector<unsigned char>,3> values;
    std::array<std::vector<int32_t>,3> offsets;for(auto& v:offsets)v.push_back(0);
    for(const auto& row:nodes){ids.push_back(row.view.node_id);entries.push_back(row.view.entry|(row.view.deleted<<1));auto bytes=Encode(row);
        for(size_t i=0;i<3;++i){values[i].insert(values[i].end(),bytes[i].begin(),bytes[i].end());offsets[i].push_back(values[i].size());}}
    batch.buffers[0][1]=ids.data();batch.buffers[1][1]=entries.data();
    for(size_t i=0;i<3;++i){batch.buffers[i+2][1]=offsets[i].data();batch.buffers[i+2][2]=values[i].data();}
    OgLakeSdkError error{};Check(api->writer_append(writer,&batch.array,&batch.schema,&error),error);
}
void WriteDirectory(const OgLakeParquetSdkApi* api,OgLakeSdkWriter* writer,const std::vector<Page>& pages)
{
    if(pages.empty())return;Batch batch(DIRECTORY,pages.size());std::array<std::vector<int64_t>,4> columns;
    std::vector<int32_t> offsets(1,0);std::string text;
    for(const auto& page:pages){columns[0].push_back(page.id);columns[1].push_back(page.rows);columns[2].push_back(page.entries);columns[3].push_back(page.postings);
        text+=page.hash;offsets.push_back(text.size());}
    for(size_t i=0;i<4;++i)batch.buffers[i][1]=columns[i].data();batch.buffers[4][1]=offsets.data();batch.buffers[4][2]=text.data();
    OgLakeSdkError error{};Check(api->writer_append(writer,&batch.array,&batch.schema,&error),error);
}
struct Input {
    const OgLakeParquetSdkApi* api;OgLakeSdkReader* reader=nullptr;OgLakeSdkBatch* batch=nullptr;
    Input(const OgLakeParquetSdkApi* a):api(a){}
    ~Input(){api->batch_release(&batch);api->reader_close(&reader);}
    void Open(const char* path,const OgLakeSdkLimits& limits,const int32_t* group=nullptr){
        OgLakeSdkReadOptions options={};options.struct_size=sizeof(options);options.path=path;options.limits=limits;
        options.batch_rows=OG_LAKE_ANN_PAGE_ROWS;options.num_field_ids=-1;options.num_row_groups=group?1:-1;options.row_groups=group;
        OgLakeSdkError error{};Check(api->reader_open(&options,&reader,&error),error);}
    bool Next(){api->batch_release(&batch);OgLakeSdkError error{};int status=api->reader_next(reader,&batch,&error);
        if(status==OG_LAKE_SDK_EOF)return false;Check(status,error);
        Require(batch&&batch->array&&batch->schema&&batch->num_rows>0&&batch->num_rows<=OG_LAKE_ANN_PAGE_ROWS&&
            batch->array->n_children==5&&batch->schema->n_children==5&&batch->array->children&&batch->schema->children,
            "invalid ANN Arrow batch");return true;}
};
const ArrowArray* Column(const OgLakeSdkBatch* batch,int field,const char* format,int64_t row)
{
    const auto* value=batch->array->children[field];const auto* schema=batch->schema->children[field];
    Require(value&&schema&&schema->format&&std::strcmp(schema->format,format)==0&&value->offset>=0&&
        value->length==batch->num_rows&&row>=0&&row<value->length&&!value->dictionary&&!schema->dictionary&&
        value->n_children==0&&value->buffers&&value->n_buffers>1,"invalid ANN Arrow column");
    if(value->null_count!=0){Require(value->buffers[0],"missing ANN validity bitmap");int64_t at=value->offset+row;
        Require(((const unsigned char*)value->buffers[0])[at/8]&(1u<<(at%8)),"NULL in ANN structural field");}return value;
}
int64_t Long(const OgLakeSdkBatch* batch,int field,int64_t row)
{const auto* value=Column(batch,field,"l",row);Require(value->buffers[1],"missing ANN integer buffer");return ((const int64_t*)value->buffers[1])[value->offset+row];}
std::vector<unsigned char> Bytes(const OgLakeSdkBatch* batch,int field,int64_t row,const char* format,size_t maximum)
{
    const auto* value=Column(batch,field,format,row);Require(value->n_buffers==3&&value->buffers[1],"missing ANN binary offsets");
    const auto* offsets=(const int32_t*)value->buffers[1];int64_t at=value->offset+row,start=offsets[at],end=offsets[at+1];
    Require(start>=0&&end>=start&&(uint64_t)(end-start)<=maximum&&(end==start||value->buffers[2]),"invalid ANN binary length");
    if(end==start)return {};const auto* bytes=(const unsigned char*)value->buffers[2];return {bytes+start,bytes+end};
}
Node Decode(const OgLakeAnnSpec& spec,const OgLakeSdkBatch* batch,int64_t row,uint32_t expected)
{
    int64_t id=Long(batch,0,row),flags=Long(batch,1,row);Require(id==expected&&flags>=0&&flags<=3,"ANN node ordering or flags changed");
    auto vector=Bytes(batch,2,row,"z",spec.dimension*4);auto edges=Bytes(batch,3,row,"z",OG_LAKE_ANN_MAX_DEGREE*4);
    auto postings=Bytes(batch,4,row,"z",OG_LAKE_ANN_MAX_POSTINGS*32);
    Require(vector.size()==spec.dimension*4&&edges.size()%4==0&&postings.size()%32==0,"malformed ANN byte field width");
    Node result;result.view.node_id=id;result.view.entry=flags&1;result.view.deleted=(flags>>1)&1;
    result.view.neighbor_count=edges.size()/4;result.view.posting_count=postings.size()/32;
    result.vector.reserve(spec.dimension);
    double norm=0;
    for(size_t i=0;i<vector.size();i+=4){
        uint32_t bits=Get(vector.data()+i,4);float value;std::memcpy(&value,&bits,4);
        Require(std::isfinite(value),"nonfinite ANN graph vector");
        norm+=(double)value*value;
        result.vector.push_back(value);
    }
    Require(spec.metric!=3||norm>0,"zero cosine vector cannot be a graph vertex");
    result.edges.reserve(result.view.neighbor_count);
    for(size_t i=0;i<edges.size();i+=4){
        uint32_t edgeId=Get(edges.data()+i,4);
        Require(edgeId<spec.node_count,"ANN neighbor is outside this segment");
        result.edges.push_back(edgeId);
    }
    result.postings.reserve(result.view.posting_count);
    for(size_t i=0;i<postings.size();i+=32){OgLakeAnnAddress address{};int64_t* fields[]={&address.file_id,&address.row_position,&address.row_id,&address.birth_snapshot};
        for(size_t k=0;k<4;++k){uint64_t value=Get(postings.data()+i+k*8,8);Require(value<=(uint64_t)INT64_MAX,"ANN address overflows int64");*fields[k]=value;}
        Require(address.file_id>=0&&address.row_position>=0&&address.row_id>=0&&address.birth_snapshot>=0,"invalid ANN source address");
        for(size_t j=0;j<result.postings.size();++j)Require(address.file_id!=result.postings[j].file_id||address.row_position!=result.postings[j].row_position,
            "duplicate physical ANN posting in a node");
        result.postings.push_back(address);
    }
    result.Refresh();return result;
}
void Footer(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits& limits,const char* path,const OgLakeSdkField* fields,
    uint32_t population,bool nodes)
{
    OgLakeSdkFooter* footer=nullptr;OgLakeSdkError error{};Check(api->footer_open(path,&limits,&footer,&error),error);
    try{const auto* info=api->footer_info(footer);Require(info&&info->num_fields==5&&info->num_rows==population,"ANN footer population changed");
        for(size_t i=0;i<5;++i)Require(info->fields[i].field_id==fields[i].field_id&&info->fields[i].type==fields[i].type&&
            info->fields[i].nullable==0&&std::strcmp(info->fields[i].name,fields[i].name)==0,"ANN structural schema changed");
        if(nodes){uint32_t pages=(population+OG_LAKE_ANN_PAGE_ROWS-1)/OG_LAKE_ANN_PAGE_ROWS;
            Require(info->num_row_groups==(int)pages,"ANN node row-group directory changed");
            for(uint32_t i=0;i<pages;++i)Require(info->row_groups[i].first_row==(int64_t)i*OG_LAKE_ANN_PAGE_ROWS&&
                info->row_groups[i].num_rows==std::min<uint32_t>(OG_LAKE_ANN_PAGE_ROWS,population-i*OG_LAKE_ANN_PAGE_ROWS),"ANN node page layout changed");}
        api->footer_close(&footer);
    }catch(...){api->footer_close(&footer);throw;}
}
} // namespace

struct OgLakeAnnWriter {
    const OgLakeParquetSdkApi* api;OgLakeSdkLimits limits;OgLakeAnnSpec spec;std::string context;
    OgLakeSdkWriter* nodes;OgLakeSdkWriter* lookup;std::vector<Node> buffer;std::vector<Page> pages;
    uint32_t count=0,entries=0;int64_t postings=0;bool failed=false,finished=false;
    void Flush(){if(buffer.empty())return;Poll(limits);auto page=Describe(spec,context,pages.size(),buffer);WriteNodes(api,nodes,buffer);pages.push_back(page);buffer.clear();}
};
struct OgLakeAnnReader {
    struct CachedPage {
        uint32_t id=0;
        uint64_t stamp=0;
        size_t bytes=0;
        std::vector<Node> rows;
    };
    const OgLakeParquetSdkApi* api;OgLakeSdkLimits limits;OgLakeAnnSpec spec;std::string context,nodes;
    std::vector<Page> pages;std::vector<std::unique_ptr<CachedPage>> cache;std::vector<uint32_t> entries;
    uint64_t clock=0;size_t cached_bytes=0;bool failed=false,entries_ready=false;
    CachedPage* Load(uint32_t page){
        Poll(limits);
        for(auto& entry:cache)if(entry&&entry->id==page){entry->stamp=++clock;return entry.get();}
        const size_t maxPages=64;
        const size_t maxBytes=std::max<size_t>(2ULL*1024*1024,(size_t)(limits.memory_bytes/4));
        auto loaded=std::unique_ptr<CachedPage>(new CachedPage);
        loaded->id=page;
        loaded->rows.reserve(pages[page].rows);
        Input input(api);int32_t group=page;input.Open(nodes.c_str(),limits,&group);
        while(input.Next())for(int64_t i=0;i<input.batch->num_rows;++i){Require(loaded->rows.size()<pages[page].rows,"too many ANN node rows");
            loaded->rows.push_back(Decode(spec,input.batch,i,page*OG_LAKE_ANN_PAGE_ROWS+loaded->rows.size()));}
        Require(loaded->rows.size()==pages[page].rows,"ANN node page ended early");auto actual=Describe(spec,context,page,loaded->rows);
        Require(actual.hash==pages[page].hash&&actual.entries==pages[page].entries&&actual.postings==pages[page].postings,
            "ANN node page digest or population mismatch");
        size_t bytes=sizeof(CachedPage)+loaded->rows.size()*sizeof(Node);
        for(auto& row:loaded->rows){
            row.Refresh();
            bytes+=row.vector.capacity()*sizeof(float)+row.edges.capacity()*sizeof(uint32_t)+row.postings.capacity()*sizeof(OgLakeAnnAddress);
        }
        loaded->bytes=bytes;
        loaded->stamp=++clock;
        while(!cache.empty()&&(cache.size()>=maxPages||cached_bytes+bytes>maxBytes)){
            size_t oldest=0;
            for(size_t i=1;i<cache.size();++i)if(cache[i]->stamp<cache[oldest]->stamp)oldest=i;
            cached_bytes-=cache[oldest]->bytes;
            cache.erase(cache.begin()+oldest);
        }
        cached_bytes+=bytes;
        CachedPage* ptr=loaded.get();
        cache.push_back(std::move(loaded));
        return ptr;
    }
};

extern "C" const OgLakeSdkField* og_lake_ann_fields(int lookup,int32_t* count)
{if(count)*count=5;return lookup?DIRECTORY:NODES;}
extern "C" int og_lake_ann_writer_open(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,const OgLakeAnnSpec* spec,
    OgLakeSdkWriter* nodes,OgLakeSdkWriter* lookup,OgLakeAnnWriter** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;return Guard(error,[&]{Validate(api,limits,spec);Require(out&&nodes&&lookup,"missing ANN candidate writer");
        auto writer=std::unique_ptr<OgLakeAnnWriter>(new OgLakeAnnWriter);writer->api=api;writer->limits=*limits;writer->spec=*spec;
        writer->context=spec->context_hash;writer->spec.context_hash=nullptr;writer->nodes=nodes;writer->lookup=lookup;
        writer->buffer.reserve(OG_LAKE_ANN_PAGE_ROWS);*out=writer.release();});
}
extern "C" int og_lake_ann_writer_append(OgLakeAnnWriter* writer,const OgLakeAnnNode* nodes,int64_t count,OgLakeSdkError* error)
{
    return Guard(error,[&]{Require(writer&&!writer->failed&&!writer->finished,"ANN writer is not appendable",OG_LAKE_SDK_STATE);
        writer->failed=true;Require(count>=0&&(count==0||nodes)&&count<=writer->spec.node_count-writer->count,"invalid ANN append population");
        for(int64_t i=0;i<count;++i){Poll(writer->limits);Require(nodes[i].node_id==writer->count,"ANN node IDs must be dense and ordered");
            auto row=Copy(writer->spec,nodes[i]);writer->entries+=row.view.entry;writer->postings+=row.postings.size();
            writer->buffer.push_back(std::move(row));++writer->count;if(writer->buffer.size()==OG_LAKE_ANN_PAGE_ROWS)writer->Flush();}
        writer->failed=false;});
}
extern "C" int og_lake_ann_writer_finish(OgLakeAnnWriter* writer,OgLakeAnnWritten* out,OgLakeSdkError* error)
{
    return Guard(error,[&]{Require(writer&&!writer->failed&&!writer->finished,"ANN writer is not finishable",OG_LAKE_SDK_STATE);writer->failed=true;
        Poll(writer->limits);
        Require(out&&out->struct_size>=sizeof(*out)&&writer->count==writer->spec.node_count&&writer->entries==writer->spec.entry_count&&
            writer->postings==writer->spec.posting_count,"ANN graph lacks declared node/entry/posting coverage");
        writer->Flush();WriteDirectory(writer->api,writer->lookup,writer->pages);auto root=Root(writer->spec,writer->context,writer->pages);
        out->page_count=writer->pages.size();std::memcpy(out->manifest_hash,root.c_str(),65);writer->finished=true;writer->failed=false;});
}
extern "C" void og_lake_ann_writer_close(OgLakeAnnWriter** writer)
{if(writer){delete *writer;*writer=nullptr;}}
extern "C" int og_lake_ann_reader_open(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,const OgLakeAnnSpec* spec,
    const char* manifest,const char* nodes,const char* lookup,OgLakeAnnReader** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;return Guard(error,[&]{Validate(api,limits,spec);Require(out&&nodes&&lookup&&Hex(manifest),"invalid ANN graph read input");
        uint32_t pages=(spec->node_count+OG_LAKE_ANN_PAGE_ROWS-1)/OG_LAKE_ANN_PAGE_ROWS;
        Footer(api,*limits,nodes,NODES,spec->node_count,true);Footer(api,*limits,lookup,DIRECTORY,pages,false);
        auto reader=std::unique_ptr<OgLakeAnnReader>(new OgLakeAnnReader);reader->api=api;reader->limits=*limits;reader->spec=*spec;
        reader->context=spec->context_hash;reader->spec.context_hash=reader->context.c_str();reader->nodes=nodes;reader->pages.reserve(pages);
        Input input(api);input.Open(lookup,*limits);uint32_t entries=0;int64_t postings=0;
        while(input.Next())for(int64_t i=0;i<input.batch->num_rows;++i){Poll(*limits);Require(reader->pages.size()<pages,"extra ANN directory page");
            Page page;int64_t id=Long(input.batch,0,i),rows=Long(input.batch,1,i),ec=Long(input.batch,2,i),pc=Long(input.batch,3,i);
            Require(id==(int64_t)reader->pages.size()&&rows==std::min<uint32_t>(OG_LAKE_ANN_PAGE_ROWS,spec->node_count-id*OG_LAKE_ANN_PAGE_ROWS)&&
                ec>=0&&ec<=rows&&pc>=0&&pc<=rows*OG_LAKE_ANN_MAX_POSTINGS,"invalid ANN page directory population");
            page.id=id;page.rows=rows;page.entries=ec;page.postings=pc;auto bytes=Bytes(input.batch,4,i,"u",64);
            page.hash.assign(bytes.begin(),bytes.end());Require(Hex(page.hash.c_str()),"invalid ANN page digest");
            entries+=page.entries;postings+=page.postings;reader->pages.push_back(page);}
        Require(reader->pages.size()==pages&&entries==spec->entry_count&&postings==spec->posting_count&&
            Root(*spec,reader->context,reader->pages)==manifest,"ANN manifest/definition/source mismatch");
        *out=reader.release();});
}
extern "C" int og_lake_ann_inspect(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,uint32_t dimension,
    uint32_t metric,const char* context,const char* nodes,const char* lookup,OgLakeAnnSpec* out,OgLakeSdkError* error)
{
    return Guard(error,[&]{OgLakeAnnSpec spec={sizeof(spec),OG_LAKE_ANN_FORMAT_VERSION,dimension,metric,0,0,0,context};
        Validate(api,limits,&spec);Require(out&&out->struct_size>=sizeof(*out)&&nodes&&lookup,"invalid ANN inspect output");
        OgLakeSdkFooter* footer=nullptr;OgLakeSdkError status{};Check(api->footer_open(nodes,limits,&footer,&status),status);
        int64_t rows=-1;try{const auto* info=api->footer_info(footer);
            Require(info&&info->num_rows>=0&&info->num_rows<=OG_LAKE_ANN_MAX_PAGES*OG_LAKE_ANN_PAGE_ROWS,"ANN graph population exceeds segment bounds");
            rows=info->num_rows;api->footer_close(&footer);
        }catch(...){api->footer_close(&footer);throw;}
        spec.node_count=rows;uint32_t pages=(spec.node_count+OG_LAKE_ANN_PAGE_ROWS-1)/OG_LAKE_ANN_PAGE_ROWS;
        Footer(api,*limits,nodes,NODES,spec.node_count,true);Footer(api,*limits,lookup,DIRECTORY,pages,false);
        Input input(api);input.Open(lookup,*limits);uint32_t seen=0;
        while(input.Next())for(int64_t i=0;i<input.batch->num_rows;++i){Poll(*limits);Require(seen<pages,"extra ANN directory page");
            int64_t id=Long(input.batch,0,i),count=Long(input.batch,1,i),entries=Long(input.batch,2,i),postings=Long(input.batch,3,i);
            Require(id==seen&&count==std::min<uint32_t>(OG_LAKE_ANN_PAGE_ROWS,spec.node_count-seen*OG_LAKE_ANN_PAGE_ROWS)&&
                entries>=0&&entries<=count&&postings>=0&&postings<=count*OG_LAKE_ANN_MAX_POSTINGS,"invalid ANN inspection population");
            spec.entry_count+=entries;spec.posting_count+=postings;++seen;}
        Require(seen==pages,"incomplete ANN inspection directory");Validate(api,limits,&spec);*out=spec;});
}
extern "C" int og_lake_ann_reader_open_inspect(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,
    uint32_t dimension,uint32_t metric,const char* context,const char* manifest,const char* nodes,const char* lookup,
    OgLakeAnnSpec* spec_out,OgLakeAnnReader** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;
    return Guard(error,[&]{
        OgLakeAnnSpec spec={sizeof(spec),OG_LAKE_ANN_FORMAT_VERSION,dimension,metric,0,0,0,context};
        Validate(api,limits,&spec);Require(spec_out&&spec_out->struct_size>=sizeof(*spec_out)&&out&&nodes&&lookup&&Hex(manifest),"invalid ANN graph inspect/open input");
        {
            OgLakeSdkFooter* footer=nullptr;OgLakeSdkError status{};Check(api->footer_open(nodes,limits,&footer,&status),status);
            int64_t rows=-1;
            try{const auto* info=api->footer_info(footer);
                Require(info&&info->num_fields==5&&info->num_rows>=0&&info->num_rows<=OG_LAKE_ANN_MAX_PAGES*OG_LAKE_ANN_PAGE_ROWS,"ANN graph population exceeds segment bounds");
                for(size_t i=0;i<5;++i)Require(info->fields[i].field_id==NODES[i].field_id&&info->fields[i].type==NODES[i].type&&
                    info->fields[i].nullable==0&&std::strcmp(info->fields[i].name,NODES[i].name)==0,"ANN structural schema changed");
                rows=info->num_rows;
                uint32_t pages=(uint32_t)((rows+OG_LAKE_ANN_PAGE_ROWS-1)/OG_LAKE_ANN_PAGE_ROWS);
                Require(info->num_row_groups==(int)pages,"ANN node row-group directory changed");
                for(uint32_t i=0;i<pages;++i)Require(info->row_groups[i].first_row==(int64_t)i*OG_LAKE_ANN_PAGE_ROWS&&
                    info->row_groups[i].num_rows==std::min<uint32_t>(OG_LAKE_ANN_PAGE_ROWS,(uint32_t)rows-i*OG_LAKE_ANN_PAGE_ROWS),"ANN node page layout changed");
                api->footer_close(&footer);
            }catch(...){api->footer_close(&footer);throw;}
            spec.node_count=(uint32_t)rows;
        }
        uint32_t pages=(spec.node_count+OG_LAKE_ANN_PAGE_ROWS-1)/OG_LAKE_ANN_PAGE_ROWS;
        Footer(api,*limits,lookup,DIRECTORY,pages,false);
        auto reader=std::unique_ptr<OgLakeAnnReader>(new OgLakeAnnReader);reader->api=api;reader->limits=*limits;reader->nodes=nodes;reader->pages.reserve(pages);
        reader->context=context;
        Input input(api);input.Open(lookup,*limits);uint32_t entries=0;int64_t postings=0;
        while(input.Next())for(int64_t i=0;i<input.batch->num_rows;++i){Poll(*limits);Require(reader->pages.size()<pages,"extra ANN directory page");
            Page page;int64_t id=Long(input.batch,0,i),rows=Long(input.batch,1,i),ec=Long(input.batch,2,i),pc=Long(input.batch,3,i);
            Require(id==(int64_t)reader->pages.size()&&rows==std::min<uint32_t>(OG_LAKE_ANN_PAGE_ROWS,spec.node_count-id*OG_LAKE_ANN_PAGE_ROWS)&&
                ec>=0&&ec<=rows&&pc>=0&&pc<=rows*OG_LAKE_ANN_MAX_POSTINGS,"invalid ANN page directory population");
            page.id=id;page.rows=rows;page.entries=ec;page.postings=pc;auto bytes=Bytes(input.batch,4,i,"u",64);
            page.hash.assign(bytes.begin(),bytes.end());Require(Hex(page.hash.c_str()),"invalid ANN page digest");
            entries+=page.entries;postings+=page.postings;reader->pages.push_back(page);}
        spec.entry_count=entries;spec.posting_count=postings;
        Validate(api,limits,&spec);
        Require(reader->pages.size()==pages&&Root(spec,reader->context,reader->pages)==manifest,"ANN manifest/definition/source mismatch");
        reader->spec=spec;reader->spec.context_hash=reader->context.c_str();
        *spec_out=reader->spec;*out=reader.release();
    });
}
extern "C" int og_lake_ann_reader_node(OgLakeAnnReader* reader,uint32_t id,const OgLakeAnnNode** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;return Guard(error,[&]{Require(reader&&!reader->failed&&out,"ANN reader is unavailable",OG_LAKE_SDK_STATE);reader->failed=true;
        Require(id<reader->spec.node_count,"ANN requested node is outside the segment");
        auto* cachedPage=reader->Load(id/OG_LAKE_ANN_PAGE_ROWS);
        *out=&cachedPage->rows[id%OG_LAKE_ANN_PAGE_ROWS].view;reader->failed=false;});
}
extern "C" const OgLakeAnnSpec* og_lake_ann_reader_spec(const OgLakeAnnReader* reader)
{return reader?&reader->spec:nullptr;}
extern "C" int og_lake_ann_verify(OgLakeAnnReader* reader,OgLakeSdkError* error)
{
    return Guard(error,[&]{Require(reader&&!reader->failed,"ANN reader is unavailable",OG_LAKE_SDK_STATE);reader->failed=true;
        Poll(reader->limits);
        for(size_t i=0;i<reader->pages.size();++i)reader->Load(i);reader->failed=false;});
}
extern "C" int og_lake_ann_reader_entries(OgLakeAnnReader* reader,const uint32_t** out,uint32_t* count,OgLakeSdkError* error)
{
    if(out)*out=nullptr;if(count)*count=0;
    return Guard(error,[&]{Require(reader&&!reader->failed&&out&&count,"ANN entry reader is unavailable",OG_LAKE_SDK_STATE);reader->failed=true;
        Poll(reader->limits);
        if(!reader->entries_ready){
            reader->entries.clear();reader->entries.reserve(reader->spec.entry_count);
            for(size_t i=0;i<reader->pages.size();++i)if(reader->pages[i].entries){
                auto* cachedPage=reader->Load(i);
                for(const auto& row:cachedPage->rows)if(row.view.entry)reader->entries.push_back(row.view.node_id);}
            Require(reader->entries.size()==reader->spec.entry_count,"ANN entry identities do not match the manifest");reader->entries_ready=true;}
        *count=reader->entries.size();*out=reader->entries.data();reader->failed=false;});
}
extern "C" void og_lake_ann_reader_close(OgLakeAnnReader** reader)
{if(reader){delete *reader;*reader=nullptr;}}
