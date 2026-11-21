/* Two-level sorted pages, following Lance's lookup/data organization.
 * Parquet is supplied by the existing storage provider. The host supplies
 * already sorted, complete physical input and owns publication/rollback.
 * Hashes use an explicit byte order, never serialized C/C++ structures. */
#include "lake_scalar_format.h"
#include "vendor/arrow20/abi.h"
#include <openssl/sha.h>
#include <algorithm>
#include <array>
#include <climits>
#include <cstddef>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
constexpr size_t PAGE=OG_LAKE_SCALAR_PAGE_ROWS;
constexpr int64_t MIN_MEMORY=1024*1024;
const OgLakeSdkField DATA_FIELDS[]={{0,OG_LAKE_SDK_INT64,1,"values"},
    {1,OG_LAKE_SDK_INT64,0,"file_id"},{2,OG_LAKE_SDK_INT64,0,"row_position"}};
const OgLakeSdkField LOOKUP_FIELDS[]={{0,OG_LAKE_SDK_INT64,1,"min"},
    {1,OG_LAKE_SDK_INT64,1,"max"},{2,OG_LAKE_SDK_INT64,0,"null_count"},
    {3,OG_LAKE_SDK_INT64,0,"page_idx"},{4,OG_LAKE_SDK_INT64,0,"row_count"},
    {5,OG_LAKE_SDK_UTF8,0,"page_hash"}};
struct Failure:std::runtime_error {
    int code,number;
    Failure(int c,const std::string& text,int n=0):std::runtime_error(text),code(c),number(n){}
};
void Require(bool condition,const char* message,int code=OG_LAKE_SDK_INVALID)
{if(!condition)throw Failure(code,message);}
void Poll(const OgLakeSdkLimits& limits)
{if(limits.should_cancel && limits.should_cancel(limits.cancel_context))throw Failure(OG_LAKE_SDK_CANCELLED,"scalar index operation cancelled");}
void Check(int status,const OgLakeSdkError& error)
{if(status!=OG_LAKE_SDK_OK)throw Failure(status,error.message,error.system_errno);}
template<class F> int Guard(OgLakeSdkError* error,F fn) noexcept
{
    if(error)std::memset(error,0,sizeof(*error));
    try{fn();return OG_LAKE_SDK_OK;}
    catch(const Failure& e){if(error){error->code=e.code;error->system_errno=e.number;std::strncpy(error->message,e.what(),sizeof(error->message)-1);}return e.code;}
    catch(const std::bad_alloc&){if(error){error->code=OG_LAKE_SDK_MEMORY;std::strcpy(error->message,"scalar index allocation failed");}return OG_LAKE_SDK_MEMORY;}
    catch(const std::exception& e){if(error){error->code=OG_LAKE_SDK_STATE;std::strncpy(error->message,e.what(),sizeof(error->message)-1);}return OG_LAKE_SDK_STATE;}
    catch(...){if(error){error->code=OG_LAKE_SDK_STATE;std::strcpy(error->message,"unexpected scalar index failure");}return OG_LAKE_SDK_STATE;}
}
bool Hex(const char* value)
{
    if(!value || std::strlen(value)!=64)return false;
    for(int i=0;i<64;++i)if(!((value[i]>='0'&&value[i]<='9')||(value[i]>='a'&&value[i]<='f')))return false;
    return true;
}
std::string HexDigest(const unsigned char* bytes)
{
    static const char digits[]="0123456789abcdef";std::string result(64,'0');
    for(size_t i=0;i<32;++i){result[i*2]=digits[bytes[i]>>4];result[i*2+1]=digits[bytes[i]&15];}return result;
}
std::array<unsigned char,32> ParseDigest(const std::string& text)
{
    Require(Hex(text.c_str()),"invalid scalar index digest");std::array<unsigned char,32> result{};
    auto digit=[](char c){return c<='9'?c-'0':c-'a'+10;};
    for(size_t i=0;i<32;++i)result[i]=(digit(text[i*2])<<4)|digit(text[i*2+1]);return result;
}
void Integer(SHA256_CTX& hash,uint64_t value)
{
    unsigned char bytes[8];for(int i=7;i>=0;--i){bytes[i]=value&255;value>>=8;}SHA256_Update(&hash,bytes,8);
}
void StartRoot(SHA256_CTX& hash,uint32_t type,const std::string& context)
{
    static const char domain[]="og_lake_scalar_parquet_v1";SHA256_Init(&hash);
    SHA256_Update(&hash,domain,sizeof(domain)-1);SHA256_Update(&hash,context.data(),context.size());
    Integer(hash,type);Integer(hash,PAGE);
}
struct Page {
    int64_t minimum=0,maximum=0,nulls=0,index=0,rows=0;
    bool bounds=false;
    std::array<unsigned char,32> digest{};
};
void HashPage(SHA256_CTX& hash,const Page& page)
{
    Integer(hash,page.index);Integer(hash,page.rows);Integer(hash,page.nulls);Integer(hash,page.bounds?1:0);
    Integer(hash,page.bounds?(uint64_t)page.minimum:0);Integer(hash,page.bounds?(uint64_t)page.maximum:0);
    SHA256_Update(&hash,page.digest.data(),page.digest.size());
}
std::string FinishRoot(SHA256_CTX hash,int64_t rows,int64_t pages)
{
    Integer(hash,rows);Integer(hash,pages);unsigned char digest[32];SHA256_Final(digest,&hash);return HexDigest(digest);
}
int Compare(const OgLakeScalarEntry& a,const OgLakeScalarEntry& b)
{
    if(a.is_null!=b.is_null)return a.is_null?1:-1;
    if(!a.is_null && a.key!=b.key)return a.key<b.key?-1:1;
    if(a.file_id!=b.file_id)return a.file_id<b.file_id?-1:1;
    return a.row_position==b.row_position?0:(a.row_position<b.row_position?-1:1);
}
void ValidateEntry(const OgLakeScalarEntry& row,uint32_t type)
{
    Require((row.is_null==0||row.is_null==1)&&row.file_id>=0&&row.row_position>=0,"invalid scalar posting identity");
    Require(row.is_null || type!=OG_LAKE_SDK_INT16 || (row.key>=INT16_MIN&&row.key<=INT16_MAX),"scalar index key exceeds INT16 domain");
    Require(row.is_null || type!=OG_LAKE_SDK_INT32 || (row.key>=INT32_MIN&&row.key<=INT32_MAX),"scalar index key exceeds declared integer type");
}
Page Describe(const std::vector<OgLakeScalarEntry>& rows,int64_t index,const std::string& context)
{
    Page page;page.index=index;page.rows=rows.size();SHA256_CTX hash;SHA256_Init(&hash);
    static const char domain[]="og_lake_scalar_leaf_v1";SHA256_Update(&hash,domain,sizeof(domain)-1);
    SHA256_Update(&hash,context.data(),context.size());Integer(hash,index);Integer(hash,rows.size());
    for(const auto& row:rows){
        Integer(hash,row.is_null?1:0);Integer(hash,row.is_null?0:(uint64_t)row.key);
        Integer(hash,row.file_id);Integer(hash,row.row_position);
        if(row.is_null)++page.nulls;
        else{if(!page.bounds){page.minimum=row.key;page.bounds=true;}page.maximum=row.key;}
    }
    SHA256_Final(page.digest.data(),&hash);return page;
}
void ReleaseArray(ArrowArray* array){array->release=nullptr;}
void ReleaseSchema(ArrowSchema* schema){schema->release=nullptr;}
/* Synchronous borrowed C Data shells. SDK may neither keep nor release input. */
struct Batch {
    ArrowArray array{};ArrowSchema schema{};
    std::vector<ArrowArray> arrays;std::vector<ArrowArray*> children;
    std::vector<ArrowSchema> schemas;std::vector<ArrowSchema*> fields;
    std::vector<std::array<const void*,3>> buffers;
    const void* root_buffers[1]={nullptr};
    Batch(const OgLakeSdkField* definitions,int count,int64_t rows):arrays(count),children(count),schemas(count),fields(count),buffers(count)
    {
        array.length=rows;array.n_buffers=1;array.buffers=root_buffers;array.n_children=count;array.children=children.data();array.release=ReleaseArray;
        schema.format="+s";schema.n_children=count;schema.children=fields.data();schema.release=ReleaseSchema;
        for(int i=0;i<count;++i){
            children[i]=&arrays[i];fields[i]=&schemas[i];arrays[i].length=rows;
            arrays[i].n_buffers=definitions[i].type==OG_LAKE_SDK_UTF8?3:2;
            arrays[i].buffers=buffers[i].data();arrays[i].release=ReleaseArray;
            schemas[i].format=definitions[i].type==OG_LAKE_SDK_UTF8?"u":"l";
            schemas[i].name=definitions[i].name;schemas[i].flags=definitions[i].nullable?ARROW_FLAG_NULLABLE:0;
            schemas[i].release=ReleaseSchema;
        }
    }
};
void WriteRows(const OgLakeParquetSdkApi* api,OgLakeSdkWriter* writer,const std::vector<OgLakeScalarEntry>& rows)
{
    Batch batch(DATA_FIELDS,3,rows.size());std::vector<int64_t> key(rows.size()),file(rows.size()),position(rows.size());
    std::vector<unsigned char> valid((rows.size()+7)/8,0);int64_t nulls=0;
    for(size_t i=0;i<rows.size();++i){key[i]=rows[i].key;file[i]=rows[i].file_id;position[i]=rows[i].row_position;
        if(rows[i].is_null)++nulls;else valid[i>>3]|=1u<<(i&7);}
    batch.buffers[0][0]=valid.data();batch.buffers[0][1]=key.data();batch.arrays[0].null_count=nulls;
    batch.buffers[1][1]=file.data();batch.buffers[2][1]=position.data();OgLakeSdkError error{};
    Check(api->writer_append(writer,&batch.array,&batch.schema,&error),error);
}
void WritePages(const OgLakeParquetSdkApi* api,OgLakeSdkWriter* writer,const std::vector<Page>& pages)
{
    Batch batch(LOOKUP_FIELDS,6,pages.size());std::array<std::vector<int64_t>,5> values;
    for(auto& v:values)v.resize(pages.size());std::vector<unsigned char> valid((pages.size()+7)/8,0);
    std::vector<int32_t> offsets(pages.size()+1,0);std::string text;text.reserve(pages.size()*64);int64_t nulls=0;
    for(size_t i=0;i<pages.size();++i){const auto& p=pages[i];values[0][i]=p.minimum;values[1][i]=p.maximum;
        values[2][i]=p.nulls;values[3][i]=p.index;values[4][i]=p.rows;
        if(p.bounds)valid[i>>3]|=1u<<(i&7);else ++nulls;
        text+=HexDigest(p.digest.data());offsets[i+1]=text.size();}
    for(int i=0;i<5;++i){batch.buffers[i][1]=values[i].data();if(i<2){batch.buffers[i][0]=valid.data();batch.arrays[i].null_count=nulls;}}
    batch.buffers[5][1]=offsets.data();batch.buffers[5][2]=text.data();OgLakeSdkError error{};
    Check(api->writer_append(writer,&batch.array,&batch.schema,&error),error);
}
void ValidateApi(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,uint32_t type,const char* context)
{
    Require(api&&api->abi_version==OG_LAKE_PARQUET_ABI&&api->struct_size>=sizeof(*api),"unsupported scalar storage provider");
    Require(limits&&limits->struct_size>=sizeof(*limits)&&limits->memory_bytes>=MIN_MEMORY,"scalar format requires at least one MiB working budget",OG_LAKE_SDK_MEMORY);
    Require(type==OG_LAKE_SDK_INT16||type==OG_LAKE_SDK_INT32||type==OG_LAKE_SDK_INT64,"scalar index key type is not supported",OG_LAKE_SDK_UNSUPPORTED);
    Require(Hex(context),"scalar index context requires a canonical SHA256 digest");Poll(*limits);
}
} // namespace

struct OgLakeScalarWriter {
    const OgLakeParquetSdkApi* api;OgLakeSdkLimits limits;uint32_t type;
    std::string context;OgLakeSdkWriter* data;OgLakeSdkWriter* lookup;
    SHA256_CTX root;std::vector<OgLakeScalarEntry> rows;std::vector<Page> pages;
    OgLakeScalarEntry previous{};bool have_previous=false,failed=false,finished=false;
    int64_t total=0,page_count=0;
    void FlushPages(){if(!pages.empty()){WritePages(api,lookup,pages);pages.clear();}}
    void FlushRows(){if(rows.empty())return;Require(page_count<INT32_MAX,"scalar index has too many Parquet pages",OG_LAKE_SDK_MEMORY);
        Poll(limits);Page page=Describe(rows,page_count,context);WriteRows(api,data,rows);HashPage(root,page);
        pages.push_back(page);++page_count;rows.clear();if(pages.size()==PAGE)FlushPages();}
};

extern "C" const OgLakeSdkField* og_lake_scalar_fields(int lookup,int32_t* count)
{if(count)*count=lookup?6:3;return lookup?LOOKUP_FIELDS:DATA_FIELDS;}
extern "C" int og_lake_scalar_writer_open(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,
    uint32_t type,const char* context,OgLakeSdkWriter* data,OgLakeSdkWriter* lookup,
    OgLakeScalarWriter** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;return Guard(error,[&]{ValidateApi(api,limits,type,context);
        Require(out&&data&&lookup&&data!=lookup&&api->writer_append,"scalar writer requires two caller-owned candidates");
        auto result=std::unique_ptr<OgLakeScalarWriter>(new OgLakeScalarWriter);
        result->api=api;result->limits=*limits;result->type=type;result->context=context;result->data=data;result->lookup=lookup;
        result->rows.reserve(PAGE);result->pages.reserve(PAGE);StartRoot(result->root,type,context);*out=result.release();});
}
extern "C" int og_lake_scalar_writer_append(OgLakeScalarWriter* writer,const OgLakeScalarEntry* rows,int64_t count,OgLakeSdkError* error)
{
    return Guard(error,[&]{Require(writer&&!writer->failed&&!writer->finished,"scalar writer is not appendable",OG_LAKE_SDK_STATE);
        writer->failed=true;Require(count>=0&&(count==0||rows),"missing scalar sorted input");
        Require(count<=INT64_MAX-writer->total,"scalar row population overflow");
        for(int64_t i=0;i<count;++i){Poll(writer->limits);auto row=rows[i];ValidateEntry(row,writer->type);if(row.is_null)row.key=0;
            Require(!writer->have_previous||Compare(writer->previous,row)<0,"scalar input is unsorted or repeats a posting");
            writer->previous=row;writer->have_previous=true;writer->rows.push_back(row);++writer->total;
            if(writer->rows.size()==PAGE)writer->FlushRows();}
        writer->failed=false;});
}
extern "C" int og_lake_scalar_writer_finish(OgLakeScalarWriter* writer,OgLakeScalarWritten* out,OgLakeSdkError* error)
{
    return Guard(error,[&]{Require(writer&&out&&out->struct_size>=sizeof(*out),"invalid scalar finish result");
        Require(!writer->failed&&!writer->finished,"scalar writer cannot finish",OG_LAKE_SDK_STATE);writer->failed=true;
        Poll(writer->limits);writer->FlushRows();writer->FlushPages();std::string hash=FinishRoot(writer->root,writer->total,writer->page_count);
        out->num_rows=writer->total;out->num_pages=writer->page_count;std::memcpy(out->manifest_hash,hash.c_str(),65);
        writer->finished=true;writer->failed=false;});
}
extern "C" void og_lake_scalar_writer_close(OgLakeScalarWriter** writer)
{if(writer){delete *writer;*writer=nullptr;}}

namespace {
struct Input {
    const OgLakeParquetSdkApi* api;
    OgLakeSdkFooter* footer=nullptr;OgLakeSdkReader* reader=nullptr;OgLakeSdkBatch* batch=nullptr;
    explicit Input(const OgLakeParquetSdkApi* value):api(value){}
    ~Input(){if(batch)api->batch_release(&batch);if(reader)api->reader_close(&reader);if(footer)api->footer_close(&footer);}
    Input(const Input&)=delete;Input& operator=(const Input&)=delete;
    const OgLakeSdkFooterInfo* Footer(const char* path,const OgLakeSdkLimits& limits)
    {
        OgLakeSdkError error{};Check(api->footer_open(path,&limits,&footer,&error),error);
        const auto* info=api->footer_info(footer);Require(info&&info->struct_size>=offsetof(OgLakeSdkFooterInfo,footer_bytes),"missing scalar Parquet footer");return info;
    }
    void Read(const char* path,const OgLakeSdkLimits& limits,const std::vector<int32_t>* groups=nullptr)
    {
        OgLakeSdkReadOptions options{};options.struct_size=sizeof(options);options.path=path;options.limits=limits;
        options.batch_rows=PAGE;options.projection_kind=OG_LAKE_SDK_BY_FIELD_ID;options.num_field_ids=-1;
        options.num_row_groups=groups?(int32_t)groups->size():-1;options.row_groups=groups?groups->data():nullptr;
        OgLakeSdkError error{};Check(api->reader_open(&options,&reader,&error),error);
    }
    bool Next(const OgLakeSdkLimits& limits,int columns)
    {
        for(int empty=0;;){
            Poll(limits);if(batch)api->batch_release(&batch);OgLakeSdkError error{};
            int status=api->reader_next(reader,&batch,&error);if(status==OG_LAKE_SDK_EOF)return false;Check(status,error);
            Require(batch&&batch->struct_size>=offsetof(OgLakeSdkBatch,object_version)&&batch->num_rows>=0&&batch->num_rows<=(int64_t)PAGE,
                "invalid scalar index batch");
            Require(batch->array&&batch->schema&&batch->array->release&&batch->schema->release&&
                batch->array->length==batch->num_rows&&batch->array->offset==0&&
                batch->array->n_children==columns&&batch->schema->n_children==columns&&
                batch->array->children&&batch->schema->children,"invalid scalar index C Data layout");
            if(batch->num_rows)return true;
            Require(++empty<1024,"scalar storage reader made no row progress",OG_LAKE_SDK_STATE);
        }
    }
};
void Schema(const OgLakeSdkFooterInfo* info,const OgLakeSdkField* expected,int count)
{
    Require(info->num_fields==count&&info->fields&&info->num_rows>=0&&info->num_row_groups>=0,
        "scalar index Parquet field population changed");
    for(int i=0;i<count;++i)Require(info->fields[i].field_id==expected[i].field_id&&
        info->fields[i].type==expected[i].type&&info->fields[i].nullable==expected[i].nullable&&
        info->fields[i].name&&std::strcmp(info->fields[i].name,expected[i].name)==0,
        "scalar index Parquet schema changed");
}
const ArrowArray* Column(const OgLakeSdkBatch* batch,int field,const char* format)
{
    const auto* value=batch->array->children[field];const auto* schema=batch->schema->children[field];
    Require(value&&schema&&schema->format&&std::strcmp(schema->format,format)==0&&
        value->length==batch->num_rows&&value->offset>=0&&value->offset<=INT64_MAX-value->length&&
        !value->dictionary&&!schema->dictionary&&value->n_children==0&&value->buffers,
        "scalar index Arrow field changed");return value;
}
bool IsNull(const ArrowArray* value,int64_t row)
{
    Require(value->n_buffers>=2,"scalar index Arrow field has no value buffers");
    if(value->null_count==0)return false;
    Require(value->buffers[0],"scalar index Arrow validity bitmap is missing");
    int64_t index=value->offset+row;return !(static_cast<const unsigned char*>(value->buffers[0])[index>>3]&(1u<<(index&7)));
}
int64_t Long(const OgLakeSdkBatch* batch,int field,int64_t row,bool* absent=nullptr)
{
    const auto* value=Column(batch,field,"l");bool missing=IsNull(value,row);if(absent)*absent=missing;
    else Require(!missing,"NULL in required scalar index field");
    if(missing)return 0;Require(value->buffers[1],"scalar index integer values are missing");
    return static_cast<const int64_t*>(value->buffers[1])[value->offset+row];
}
std::string Text(const OgLakeSdkBatch* batch,int field,int64_t row)
{
    const auto* value=Column(batch,field,"u");Require(value->n_buffers==3&&!IsNull(value,row)&&value->buffers[1]&&value->buffers[2],
        "missing scalar index digest field");
    const auto* offsets=static_cast<const int32_t*>(value->buffers[1]);int64_t index=value->offset+row;
    int64_t start=offsets[index],end=offsets[index+1];Require(start>=0&&end-start==64,"invalid scalar digest length");
    return std::string(static_cast<const char*>(value->buffers[2])+start,64);
}
bool SamePage(const Page& a,const Page& b)
{
    return a.minimum==b.minimum&&a.maximum==b.maximum&&a.nulls==b.nulls&&a.index==b.index&&a.rows==b.rows&&
        a.bounds==b.bounds&&a.digest==b.digest;
}
struct Predicate {
    int kind;bool null_argument;std::vector<int64_t> values;
    bool PageMatches(const Page& p)const
    {
        if(kind==OG_LAKE_SCALAR_IS_NULL)return p.nulls>0;
        if(kind==OG_LAKE_SCALAR_NOT_NULL)return p.rows>p.nulls;
        if(null_argument||!p.bounds||values.empty())return false;
        int64_t key=values[0];
        switch(kind){
            case OG_LAKE_SCALAR_LT:return p.minimum<key;
            case OG_LAKE_SCALAR_LE:return p.minimum<=key;
            case OG_LAKE_SCALAR_EQ:return p.minimum<=key&&p.maximum>=key;
            case OG_LAKE_SCALAR_GE:return p.maximum>=key;
            case OG_LAKE_SCALAR_GT:return p.maximum>key;
            default:{auto at=std::lower_bound(values.begin(),values.end(),p.minimum);return at!=values.end()&&*at<=p.maximum;}
        }
    }
    bool Matches(const OgLakeScalarEntry& row)const
    {
        if(kind==OG_LAKE_SCALAR_IS_NULL)return row.is_null;
        if(kind==OG_LAKE_SCALAR_NOT_NULL)return !row.is_null;
        if(null_argument||row.is_null||values.empty())return false;
        int64_t key=values[0];
        switch(kind){
            case OG_LAKE_SCALAR_LT:return row.key<key;
            case OG_LAKE_SCALAR_LE:return row.key<=key;
            case OG_LAKE_SCALAR_EQ:return row.key==key;
            case OG_LAKE_SCALAR_GE:return row.key>=key;
            case OG_LAKE_SCALAR_GT:return row.key>key;
            default:return std::binary_search(values.begin(),values.end(),row.key);
        }
    }
};
} // namespace

struct OgLakeScalarReader {
    const OgLakeParquetSdkApi* api;OgLakeSdkLimits limits;uint32_t type;
    std::string context,data_path;std::vector<Page> pages;
    int64_t rows=0;
};
struct OgLakeScalarResult {std::vector<OgLakeScalarPosition> positions;};

extern "C" int og_lake_scalar_reader_open(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,
    uint32_t type,const char* context,const char* manifest,const char* data_path,const char* lookup_path,
    OgLakeScalarReader** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;return Guard(error,[&]{ValidateApi(api,limits,type,context);
        Require(out&&data_path&&lookup_path&&Hex(manifest)&&api->footer_open&&api->footer_info&&api->footer_close&&
            api->reader_open&&api->reader_next&&api->reader_close&&api->batch_release&&
            (api->capabilities&OG_LAKE_SDK_ORIGINAL_POSITIONS),"incomplete scalar storage reader");
        auto result=std::unique_ptr<OgLakeScalarReader>(new OgLakeScalarReader);
        result->api=api;result->limits=*limits;result->type=type;result->context=context;result->data_path=data_path;
        Input lookup(api),data(api);const auto* info=lookup.Footer(lookup_path,*limits);Schema(info,LOOKUP_FIELDS,6);
        Require((uint64_t)info->num_rows<=(uint64_t)limits->memory_bytes/(4*sizeof(Page))&&info->num_rows<=INT32_MAX,
            "scalar lookup exceeds its working budget",OG_LAKE_SDK_MEMORY);
        result->pages.reserve(info->num_rows);lookup.Read(lookup_path,*limits);SHA256_CTX root;StartRoot(root,type,context);
        while(lookup.Next(*limits,6))for(int64_t i=0;i<lookup.batch->num_rows;++i){
            Page p;bool min_null=false,max_null=false;p.minimum=Long(lookup.batch,0,i,&min_null);p.maximum=Long(lookup.batch,1,i,&max_null);
            p.nulls=Long(lookup.batch,2,i);p.index=Long(lookup.batch,3,i);p.rows=Long(lookup.batch,4,i);p.bounds=!min_null;
            p.digest=ParseDigest(Text(lookup.batch,5,i));
            Require(p.index==(int64_t)result->pages.size()&&p.index<info->num_rows&&p.rows>0&&p.rows<=(int64_t)PAGE&&
                p.nulls>=0&&p.nulls<=p.rows&&min_null==max_null&&p.bounds==(p.nulls<p.rows),"invalid scalar lookup page");
            Require(!p.bounds||p.minimum<=p.maximum,"scalar lookup has inverted bounds");
            Require(!p.bounds||type!=OG_LAKE_SDK_INT16||(p.minimum>=INT16_MIN&&p.maximum<=INT16_MAX),"scalar lookup exceeds INT16 domain");
            Require(!p.bounds||type!=OG_LAKE_SDK_INT32||(p.minimum>=INT32_MIN&&p.maximum<=INT32_MAX),"scalar lookup key type changed");
            if(!result->pages.empty()){const auto& before=result->pages.back();
                Require(before.rows==(int64_t)PAGE&&(!before.nulls||!p.bounds)&&
                    (!before.bounds||!p.bounds||before.maximum<=p.minimum),"scalar lookup pages are not ordered");}
            Require(p.rows<=INT64_MAX-result->rows,"scalar lookup row population overflow");result->rows+=p.rows;
            HashPage(root,p);result->pages.push_back(p);
        }
        Require((int64_t)result->pages.size()==info->num_rows&&FinishRoot(root,result->rows,result->pages.size())==manifest,
            "scalar lookup manifest checksum mismatch");
        const auto* physical=data.Footer(data_path,*limits);Schema(physical,DATA_FIELDS,3);
        Require(physical->num_rows==result->rows&&physical->num_row_groups==(int32_t)result->pages.size()&&
            (physical->num_row_groups==0||physical->row_groups),"scalar data page population changed");
        for(size_t i=0;i<result->pages.size();++i)Require(physical->row_groups[i].first_row==(int64_t)(i*PAGE)&&
            physical->row_groups[i].num_rows==result->pages[i].rows,"scalar data row groups changed");
        *out=result.release();});
}

namespace {
template<class Visitor> void VisitScalarPages(OgLakeScalarReader* reader,
    const std::vector<int32_t>& groups,Visitor visit)
{
    if(groups.empty())return;
        Input input(reader->api);input.Read(reader->data_path.c_str(),reader->limits,&groups);
        size_t current=0;std::vector<OgLakeScalarEntry> rows;rows.reserve(PAGE);
        while(input.Next(reader->limits,3))for(int64_t i=0;i<input.batch->num_rows;++i){
            Require(current<groups.size()&&input.batch->original_positions,"scalar storage lost original positions");
            const auto& expected=reader->pages[groups[current]];
            Require(input.batch->original_positions[i]==expected.index*(int64_t)PAGE+(int64_t)rows.size(),
                "scalar index page position changed");
            bool absent=false;OgLakeScalarEntry row{};row.key=Long(input.batch,0,i,&absent);row.is_null=absent;
            row.file_id=Long(input.batch,1,i);row.row_position=Long(input.batch,2,i);ValidateEntry(row,reader->type);
            Require(rows.empty()||Compare(rows.back(),row)<0,"scalar leaf is not sorted or repeats a posting");rows.push_back(row);
            if((int64_t)rows.size()==expected.rows){
                Require(SamePage(Describe(rows,expected.index,reader->context),expected),"scalar leaf checksum or bounds mismatch");
                for(const auto& value:rows)visit(value);
                rows.clear();++current;
            }
        }
        Require(current==groups.size()&&rows.empty(),"scalar storage returned incomplete pages");
}
} // namespace

extern "C" int og_lake_scalar_verify(OgLakeScalarReader* reader,OgLakeSdkError* error)
{
    return Guard(error,[&]{Require(reader,"scalar reader is required");Poll(reader->limits);
        std::vector<int32_t> groups;groups.reserve(reader->pages.size());
        for(const auto& page:reader->pages){Poll(reader->limits);groups.push_back(page.index);}
        /* No candidate vector: verify leaf populations one page at a time.
         * The normal directory and page working budgets still apply. */
        VisitScalarPages(reader,groups,[](const OgLakeScalarEntry&){});
    });
}

extern "C" int og_lake_scalar_select(OgLakeScalarReader* reader,int kind,int argument_is_null,
    const int64_t* values,int64_t count,OgLakeScalarResult** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;return Guard(error,[&]{Require(reader&&out&&kind>=1&&kind<=8&&
        (argument_is_null==0||argument_is_null==1)&&count>=0&&(count==0||values),"invalid scalar predicate");
        Poll(reader->limits);Predicate predicate{kind,argument_is_null!=0,{}};
        bool null_test=kind==OG_LAKE_SCALAR_IS_NULL||kind==OG_LAKE_SCALAR_NOT_NULL;
        Require(null_test?(count==0&&!argument_is_null):(kind==OG_LAKE_SCALAR_IN||count==1),"invalid scalar predicate arity");
        Require(count<=reader->limits.memory_bytes/(16*(int64_t)sizeof(int64_t)),"scalar predicate exceeds its working budget",OG_LAKE_SDK_MEMORY);
        if(count)predicate.values.assign(values,values+count);
        if(kind==OG_LAKE_SCALAR_IN){std::sort(predicate.values.begin(),predicate.values.end());predicate.values.erase(
            std::unique(predicate.values.begin(),predicate.values.end()),predicate.values.end());}
        auto result=std::unique_ptr<OgLakeScalarResult>(new OgLakeScalarResult);std::vector<int32_t> groups;
        for(const auto& page:reader->pages){Poll(reader->limits);if(predicate.PageMatches(page))groups.push_back(page.index);}
        if(groups.empty()){*out=result.release();return;}
        const size_t maximum=(size_t)(reader->limits.memory_bytes/(4*(int64_t)sizeof(OgLakeScalarPosition)));
        VisitScalarPages(reader,groups,[&](const OgLakeScalarEntry& value){
            if(!predicate.Matches(value))return;
            Require(result->positions.size()<maximum,"scalar postings exceed their working budget",OG_LAKE_SDK_MEMORY);
            if(result->positions.size()==result->positions.capacity())result->positions.reserve(
                std::min(maximum,std::max((size_t)1024,result->positions.capacity()*2)));
            result->positions.push_back({value.file_id,value.row_position});
        });
        auto less=[](const OgLakeScalarPosition& a,const OgLakeScalarPosition& b){return a.file_id<b.file_id||
            (a.file_id==b.file_id&&a.row_position<b.row_position);};
        std::sort(result->positions.begin(),result->positions.end(),less);
        for(size_t i=1;i<result->positions.size();++i)Require(less(result->positions[i-1],result->positions[i]),"duplicate scalar source posting");
        *out=result.release();});
}
extern "C" int64_t og_lake_scalar_result_count(const OgLakeScalarResult* result)
{return result?(int64_t)result->positions.size():0;}
extern "C" const OgLakeScalarPosition* og_lake_scalar_result_positions(const OgLakeScalarResult* result)
{return result&&!result->positions.empty()?result->positions.data():nullptr;}
extern "C" void og_lake_scalar_result_close(OgLakeScalarResult** result)
{if(result){delete *result;*result=nullptr;}}
extern "C" void og_lake_scalar_reader_close(OgLakeScalarReader** reader)
{if(reader){delete *reader;*reader=nullptr;}}
