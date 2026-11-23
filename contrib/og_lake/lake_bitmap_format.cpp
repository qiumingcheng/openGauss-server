/* Lance-style value dictionary, standard CRoaring portable payloads and the
 * existing Parquet C ABI. This prototype deliberately reads the full bounded
 * dictionary; it is not a new bitmap algorithm or an ANN/PQ implementation. */
#include "lake_bitmap_format.h"
#include "vendor/arrow20/abi.h"
#include <roaring/roaring.h>
#include <roaring/containers/array.h>
#include <roaring/containers/bitset.h>
#include <roaring/roaring_array.h>
#include <roaring/roaring_version.h>
#include <openssl/sha.h>
#include <algorithm>
#include <array>
#include <climits>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
static_assert(ROARING_VERSION_MAJOR==4&&ROARING_VERSION_MINOR==7&&ROARING_VERSION_REVISION==2,
    "checked container construction must be re-reviewed for another CRoaring version");
constexpr uint64_t MIB=1024*1024;
constexpr int64_t BATCH_ROWS=256;
const OgLakeSdkField FIELDS[]={{0,OG_LAKE_SDK_INT64,1,"key"},
    {1,OG_LAKE_SDK_BINARY,0,"bitmap"},{2,OG_LAKE_SDK_INT64,0,"cardinality"}};
struct Failure:std::runtime_error {
    int code,number;
    Failure(int c,const std::string& s,int n=0):std::runtime_error(s),code(c),number(n){}
};
void Require(bool ok,const char* text,int code=OG_LAKE_SDK_INVALID)
{if(!ok)throw Failure(code,text);}
void Poll(const OgLakeSdkLimits& limits)
{if(limits.should_cancel&&limits.should_cancel(limits.cancel_context))throw Failure(OG_LAKE_SDK_CANCELLED,"bitmap dictionary operation cancelled");}
void Check(int status,const OgLakeSdkError& e)
{if(status!=OG_LAKE_SDK_OK)throw Failure(status,e.message,e.system_errno);}
template<class F> int Guard(OgLakeSdkError* error,F fn) noexcept
{
    if(error)*error={};try{fn();return OG_LAKE_SDK_OK;}
    catch(const Failure& e){if(error){error->code=e.code;error->system_errno=e.number;std::strncpy(error->message,e.what(),sizeof(error->message)-1);}return e.code;}
    catch(const std::bad_alloc&){if(error){error->code=OG_LAKE_SDK_MEMORY;std::strcpy(error->message,"bitmap dictionary allocation failed");}return OG_LAKE_SDK_MEMORY;}
    catch(const std::exception& e){if(error){error->code=OG_LAKE_SDK_STATE;std::strncpy(error->message,e.what(),sizeof(error->message)-1);}return OG_LAKE_SDK_STATE;}
    catch(...){if(error){error->code=OG_LAKE_SDK_STATE;std::strcpy(error->message,"unexpected bitmap dictionary failure");}return OG_LAKE_SDK_STATE;}
}
bool Hex(const char* p)
{if(!p||std::strlen(p)!=64)return false;for(int i=0;i<64;++i)if(!((p[i]>='0'&&p[i]<='9')||(p[i]>='a'&&p[i]<='f')))return false;return true;}
void Integer(SHA256_CTX& hash,uint64_t n)
{unsigned char b[8];for(int i=7;i>=0;--i){b[i]=n&255;n>>=8;}SHA256_Update(&hash,b,8);}
void Start(SHA256_CTX& hash,const OgLakeBitmapFileSpec& s)
{
    const char domain[]="og_lake_bitmap_parquet_v1";SHA256_Init(&hash);SHA256_Update(&hash,domain,sizeof(domain)-1);
    SHA256_Update(&hash,s.context_hash,64);Integer(hash,s.format_version);Integer(hash,s.key_type);
    Integer(hash,s.file_id);Integer(hash,s.physical_rows);Integer(hash,OG_LAKE_BITMAP_CODEC_VERSION);
}
void GroupHash(SHA256_CTX& hash,int64_t key,bool null_key,uint64_t count,const std::string& bytes)
{Integer(hash,null_key);Integer(hash,null_key?0:(uint64_t)key);Integer(hash,count);Integer(hash,bytes.size());SHA256_Update(&hash,bytes.data(),bytes.size());}
std::string Finish(SHA256_CTX hash,uint64_t rows,uint64_t keys)
{
    Integer(hash,rows);Integer(hash,keys);unsigned char digest[32];SHA256_Final(digest,&hash);
    const char digits[]="0123456789abcdef";std::string text(64,'0');for(int i=0;i<32;++i){text[i*2]=digits[digest[i]>>4];text[i*2+1]=digits[digest[i]&15];}return text;
}
void Validate(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,const OgLakeBitmapFileSpec* s)
{
    Require(api&&api->abi_version==OG_LAKE_PARQUET_ABI&&api->struct_size>=sizeof(*api),"unsupported bitmap Parquet provider");
    Require((api->capabilities&OG_LAKE_SDK_BINARY_VALUES)!=0,"bitmap format requires genuine BINARY-capable provider",OG_LAKE_SDK_UNSUPPORTED);
    Require(s&&s->struct_size==sizeof(*s)&&s->format_version==OG_LAKE_BITMAP_FORMAT_VERSION&&s->file_id>=0&&Hex(s->context_hash),"invalid bitmap source specification");
    Require(s->key_type==OG_LAKE_SDK_INT16||s->key_type==OG_LAKE_SDK_INT32||s->key_type==OG_LAKE_SDK_INT64,"unsupported encoded bitmap key",OG_LAKE_SDK_UNSUPPORTED);
    Require(s->physical_rows<=OG_LAKE_BITMAP_MAX_ROWS,"bitmap prototype source exceeds 1048576 physical rows",OG_LAKE_SDK_UNSUPPORTED);
    /* Admission bound includes retained postings/results, coverage, dictionary
     * overhead and one worst-case decoder. SDK enforces its own pool budget. */
    uint64_t needed=8*MIB+s->physical_rows*24+OG_LAKE_BITMAP_MAX_KEYS*128ull;
    Require(limits&&limits->struct_size>=sizeof(*limits)&&limits->memory_bytes>=0&&
        (uint64_t)limits->memory_bytes>=needed,"bitmap dictionary exceeds bounded working budget",OG_LAKE_SDK_MEMORY);Poll(*limits);
}
int KeyCompare(int64_t a,bool an,int64_t b,bool bn)
{if(an!=bn)return an?1:-1;return an||a==b?0:(a<b?-1:1);}
void Mark(std::vector<uint64_t>& coverage,uint32_t position)
{uint64_t bit=1ull<<(position&63);Require(!(coverage[position>>6]&bit),"bitmap physical positions overlap or repeat");coverage[position>>6]|=bit;}
struct FreeRoaring {void operator()(roaring_bitmap_t* p)const{if(p)roaring_bitmap_free(p);}};
/* CRoaring's general add/of_ptr path has unchecked allocation sites in 4.7.2.
 * Use its documented array/bitset containers with exact preallocation instead.
 * ra_append cannot allocate because the full high16 directory is reserved.
 * Encoding, validation and serialization remain CRoaring's implementation. */
std::string Encode(const std::vector<uint32_t>& positions)
{
    using namespace roaring::internal;
    Require(!positions.empty(),"empty bitmap dictionary group");
    std::unique_ptr<roaring_bitmap_t,FreeRoaring> bitmap(roaring_bitmap_create_with_capacity(16));
    Require(bitmap!=nullptr,"CRoaring bitmap allocation failed",OG_LAKE_SDK_MEMORY);
    for(size_t at=0;at<positions.size();){size_t end=at+1;uint16_t high=positions[at]>>16;
        while(end<positions.size()&&(positions[end]>>16)==high)++end;
        Require(bitmap->high_low_container.size<bitmap->high_low_container.allocation_size,
            "preallocated CRoaring directory exhausted",OG_LAKE_SDK_STATE);
        size_t count=end-at;container_t* container=nullptr;uint8_t type;
        if(count<=DEFAULT_MAX_SIZE){auto* array=array_container_create_given_capacity((int32_t)count);
            Require(array!=nullptr,"CRoaring array allocation failed",OG_LAKE_SDK_MEMORY);
            for(size_t i=0;i<count;++i)array->array[i]=(uint16_t)positions[at+i];array->cardinality=(int32_t)count;
            container=(container_t*)array;type=ARRAY_CONTAINER_TYPE;
        }else{auto* bits=bitset_container_create();Require(bits!=nullptr,"CRoaring bitset allocation failed",OG_LAKE_SDK_MEMORY);
            for(size_t i=at;i<end;++i)bitset_container_set(bits,(uint16_t)positions[i]);
            container=(container_t*)bits;type=BITSET_CONTAINER_TYPE;}
        ra_append(&bitmap->high_low_container,high,container,type);at=end;
    }
    const char* reason=nullptr;Require(roaring_bitmap_internal_validate(bitmap.get(),&reason),"CRoaring builder produced invalid containers",OG_LAKE_SDK_STATE);
    Require(roaring_bitmap_get_cardinality(bitmap.get())==positions.size(),"CRoaring builder cardinality changed",OG_LAKE_SDK_STATE);
    size_t length=roaring_bitmap_portable_size_in_bytes(bitmap.get());Require(length<=2*MIB,"bitmap payload exceeds prototype bound");
    std::string bytes(length,'\0');Require(roaring_bitmap_portable_serialize(bitmap.get(),&bytes[0])==length,"CRoaring serialized size changed",OG_LAKE_SDK_STATE);return bytes;
}
void ReleaseArray(ArrowArray* p){p->release=nullptr;}
void ReleaseSchema(ArrowSchema* p){p->release=nullptr;}
struct Encoded {int64_t key;bool null_key;int64_t cardinality;std::string bytes;};
void Write(const OgLakeParquetSdkApi* api,OgLakeSdkWriter* writer,const std::vector<Encoded>& rows)
{
    ArrowArray root{},columns[3]{};ArrowSchema schema{},fields[3]{};ArrowArray* children[]={&columns[0],&columns[1],&columns[2]};
    ArrowSchema* definitions[]={&fields[0],&fields[1],&fields[2]};std::vector<unsigned char> valid((rows.size()+7)/8);
    std::vector<int64_t> keys(rows.size()),cards(rows.size());std::vector<int32_t> offsets(rows.size()+1);std::string bytes;int64_t nulls=0;
    for(size_t i=0;i<rows.size();++i){keys[i]=rows[i].key;cards[i]=rows[i].cardinality;
        if(rows[i].null_key)++nulls;else valid[i>>3]|=1u<<(i&7);
        Require(bytes.size()+rows[i].bytes.size()<=INT32_MAX,"bitmap batch exceeds Arrow BINARY bounds",OG_LAKE_SDK_MEMORY);
        bytes+=rows[i].bytes;offsets[i+1]=(int32_t)bytes.size();}
    const void* buffers[3][3]={{valid.data(),keys.data(),nullptr},{nullptr,offsets.data(),bytes.data()},{nullptr,cards.data(),nullptr}};
    const void* root_buffers[]={nullptr};root.length=rows.size();root.n_buffers=1;root.buffers=root_buffers;root.n_children=3;root.children=children;root.release=ReleaseArray;
    schema.format="+s";schema.n_children=3;schema.children=definitions;schema.release=ReleaseSchema;
    for(int i=0;i<3;++i){columns[i].length=rows.size();columns[i].n_buffers=i==1?3:2;columns[i].buffers=buffers[i];columns[i].release=ReleaseArray;
        fields[i].format=i==1?"z":"l";fields[i].name=FIELDS[i].name;fields[i].flags=i==0?ARROW_FLAG_NULLABLE:0;fields[i].release=ReleaseSchema;}
    columns[0].null_count=nulls;OgLakeSdkError error{};Check(api->writer_append(writer,&root,&schema,&error),error);
}
struct Input {
    const OgLakeParquetSdkApi* api;OgLakeSdkFooter* footer=nullptr;OgLakeSdkReader* reader=nullptr;OgLakeSdkBatch* batch=nullptr;
    explicit Input(const OgLakeParquetSdkApi* p):api(p){}
    ~Input(){if(batch)api->batch_release(&batch);if(reader)api->reader_close(&reader);if(footer)api->footer_close(&footer);}
};
const ArrowArray* Column(const OgLakeSdkBatch* batch,int i,const char* format)
{
    const auto* a=batch->array->children[i];const auto* s=batch->schema->children[i];
    Require(a&&s&&s->format&&std::strcmp(s->format,format)==0&&a->length==batch->num_rows&&a->offset>=0&&
        a->offset<=INT64_MAX-a->length&&!a->dictionary&&!s->dictionary&&a->n_children==0&&a->buffers,"bitmap Arrow schema changed");return a;
}
bool IsNull(const ArrowArray* a,int64_t row)
{
    Require(a->n_buffers>=2,"bitmap Arrow buffers missing");if(a->null_count==0)return false;
    Require(a->buffers[0],"bitmap Arrow validity missing");int64_t i=a->offset+row;
    return !(static_cast<const unsigned char*>(a->buffers[0])[i>>3]&(1u<<(i&7)));
}
int64_t Long(const OgLakeSdkBatch* batch,int i,int64_t row,bool* missing=nullptr)
{
    const auto* a=Column(batch,i,"l");bool absent=IsNull(a,row);if(missing)*missing=absent;else Require(!absent,"NULL bitmap cardinality");
    if(absent)return 0;Require(a->buffers[1],"bitmap Arrow integer values missing");return static_cast<const int64_t*>(a->buffers[1])[a->offset+row];
}
std::string Binary(const OgLakeSdkBatch* batch,int64_t row)
{
    const auto* a=Column(batch,1,"z");Require(a->n_buffers==3&&!IsNull(a,row)&&a->buffers[1]&&a->buffers[2],"bitmap BINARY payload missing");
    const auto* offsets=static_cast<const int32_t*>(a->buffers[1]);int64_t i=a->offset+row,start=offsets[i],end=offsets[i+1];
    Require(start>=0&&end>start&&end-start<=2*(int64_t)MIB,"bitmap BINARY payload length invalid");
    return std::string(static_cast<const char*>(a->buffers[2])+start,(size_t)(end-start));
}
struct Group {int64_t key;bool null_key;std::vector<uint32_t> positions;};
struct BitmapHandle {OgLakeBitmap* value=nullptr;~BitmapHandle(){og_lake_bitmap_close(&value);}};
}

struct OgLakeBitmapWriter {
    const OgLakeParquetSdkApi* api;OgLakeSdkLimits limits;OgLakeBitmapFileSpec spec;std::string context;
    OgLakeSdkWriter* output;SHA256_CTX root;std::vector<uint64_t> coverage;std::vector<uint32_t> positions;std::vector<Encoded> pending;
    int64_t key=0;bool null_key=false,have_key=false,failed=false,finished=false;uint64_t rows=0,keys=0;
    void Flush(){if(!have_key)return;Poll(limits);Require(keys<OG_LAKE_BITMAP_MAX_KEYS,"bitmap prototype exceeds 65536 keys",OG_LAKE_SDK_UNSUPPORTED);
        std::string bytes=Encode(positions);GroupHash(root,key,null_key,positions.size(),bytes);
        pending.push_back({key,null_key,(int64_t)positions.size(),std::move(bytes)});++keys;positions.clear();
        if(pending.size()==BATCH_ROWS)FlushPending();}
    void FlushPending(){if(!pending.empty()){Poll(limits);Write(api,output,pending);pending.clear();}}
};
struct OgLakeBitmapReader {
    OgLakeSdkLimits limits;int64_t file_id;uint64_t physical_rows;std::vector<Group> groups;
};
struct OgLakeBitmapResult {std::vector<OgLakeScalarPosition> positions;};
extern "C" const OgLakeSdkField* og_lake_bitmap_fields(int32_t* count)
{if(count)*count=3;return FIELDS;}
extern "C" int og_lake_bitmap_writer_open(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,
    const OgLakeBitmapFileSpec* spec,OgLakeSdkWriter* output,OgLakeBitmapWriter** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;return Guard(error,[&]{Validate(api,limits,spec);Require(out&&output&&api->writer_append,"bitmap writer requires caller-owned SDK candidate");
        auto w=std::unique_ptr<OgLakeBitmapWriter>(new OgLakeBitmapWriter);w->api=api;w->limits=*limits;w->spec=*spec;w->context=spec->context_hash;
        w->spec.context_hash=w->context.c_str();w->output=output;w->coverage.resize((spec->physical_rows+63)/64);Start(w->root,w->spec);*out=w.release();});
}
extern "C" int og_lake_bitmap_writer_append(OgLakeBitmapWriter* w,const OgLakeScalarEntry* rows,int64_t count,OgLakeSdkError* error)
{
    return Guard(error,[&]{Require(w&&!w->failed&&!w->finished,"bitmap writer is not appendable",OG_LAKE_SDK_STATE);w->failed=true;
        Require(count>=0&&(count==0||rows),"missing bitmap sorted input");Require((uint64_t)count<=w->spec.physical_rows-w->rows,"bitmap input exceeds physical population");
        for(int64_t i=0;i<count;++i){Poll(w->limits);auto row=rows[i];Require((row.is_null==0||row.is_null==1)&&row.file_id==w->spec.file_id&&
                row.row_position>=0&&(uint64_t)row.row_position<w->spec.physical_rows,"bitmap posting outside immutable source");
            Require(row.is_null||w->spec.key_type!=OG_LAKE_SDK_INT16||(row.key>=INT16_MIN&&row.key<=INT16_MAX),"bitmap key exceeds INT16 domain");
            Require(row.is_null||w->spec.key_type!=OG_LAKE_SDK_INT32||(row.key>=INT32_MIN&&row.key<=INT32_MAX),"bitmap key exceeds INT32 domain");
            if(row.is_null)row.key=0;int compare=w->have_key?KeyCompare(w->key,w->null_key,row.key,row.is_null):-1;
            Require(compare<=0,"bitmap keys are not ordered NULLS LAST");
            if(compare<0){w->Flush();w->key=row.key;w->null_key=row.is_null;w->have_key=true;}
            Require(w->positions.empty()||w->positions.back()<(uint32_t)row.row_position,"bitmap positions are not strictly ordered within key");
            Mark(w->coverage,(uint32_t)row.row_position);w->positions.push_back((uint32_t)row.row_position);++w->rows;}
        w->failed=false;});
}
extern "C" int og_lake_bitmap_writer_finish(OgLakeBitmapWriter* w,OgLakeBitmapWritten* out,OgLakeSdkError* error)
{
    return Guard(error,[&]{Require(w&&out&&out->struct_size>=sizeof(*out),"invalid bitmap finish result");
        Require(!w->failed&&!w->finished,"bitmap writer cannot finish",OG_LAKE_SDK_STATE);w->failed=true;Poll(w->limits);
        Require(w->rows==w->spec.physical_rows,"bitmap input omits physical source rows");w->Flush();w->FlushPending();std::string hash=Finish(w->root,w->rows,w->keys);
        out->physical_source_rows=w->rows;out->dictionary_rows=w->keys;std::memcpy(out->manifest_hash,hash.c_str(),65);w->finished=true;w->failed=false;});
}
extern "C" void og_lake_bitmap_writer_close(OgLakeBitmapWriter** w){if(w){delete *w;*w=nullptr;}}
extern "C" int og_lake_bitmap_reader_open(const OgLakeParquetSdkApi* api,const OgLakeSdkLimits* limits,
    const OgLakeBitmapFileSpec* spec,const char* expected,const char* path,OgLakeBitmapReader** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;return Guard(error,[&]{Validate(api,limits,spec);Require(out&&path&&Hex(expected)&&api->footer_open&&api->footer_info&&api->footer_close&&
        api->reader_open&&api->reader_next&&api->reader_close&&api->batch_release,"invalid bitmap reader request");
        Input input(api);OgLakeSdkError e{};Check(api->footer_open(path,limits,&input.footer,&e),e);const auto* info=api->footer_info(input.footer);
        Require(info&&info->struct_size>=sizeof(*info)&&info->num_fields==3&&info->fields&&info->num_rows>=0&&
            info->num_rows<=OG_LAKE_BITMAP_MAX_KEYS,"bitmap dictionary footer population invalid");
        for(int i=0;i<3;++i)Require(info->fields[i].field_id==FIELDS[i].field_id&&info->fields[i].type==FIELDS[i].type&&
            info->fields[i].nullable==FIELDS[i].nullable&&info->fields[i].name&&std::strcmp(info->fields[i].name,FIELDS[i].name)==0,"bitmap Parquet schema changed");
        int64_t dictionary_rows=info->num_rows;
        auto r=std::unique_ptr<OgLakeBitmapReader>(new OgLakeBitmapReader);r->limits=*limits;r->file_id=spec->file_id;r->physical_rows=spec->physical_rows;
        r->groups.reserve((size_t)dictionary_rows);std::vector<uint64_t> coverage((spec->physical_rows+63)/64);uint64_t rows=0;
        SHA256_CTX hash;Start(hash,*spec);OgLakeSdkReadOptions options{};options.struct_size=sizeof(options);options.path=path;options.limits=*limits;
        options.batch_rows=BATCH_ROWS;options.projection_kind=OG_LAKE_SDK_BY_FIELD_ID;options.num_field_ids=-1;options.num_row_groups=-1;
        Check(api->reader_open(&options,&input.reader,&e),e);int empty=0;
        for(;;){Poll(*limits);if(input.batch)api->batch_release(&input.batch);int status=api->reader_next(input.reader,&input.batch,&e);if(status==OG_LAKE_SDK_EOF)break;Check(status,e);
            const auto* b=input.batch;Require(b&&b->struct_size>=sizeof(*b)&&b->num_rows>=0&&b->num_rows<=BATCH_ROWS&&b->array&&b->schema&&
                b->array->release&&b->schema->release&&b->array->length==b->num_rows&&b->array->offset==0&&b->array->n_children==3&&b->schema->n_children==3&&
                b->array->children&&b->schema->children,"invalid bitmap dictionary Arrow batch");
            if(!b->num_rows){Require(++empty<1024,"bitmap reader made no progress",OG_LAKE_SDK_STATE);continue;}empty=0;
            for(int64_t i=0;i<b->num_rows;++i){Poll(*limits);Require(r->groups.size()<(size_t)dictionary_rows,"bitmap dictionary row population changed");
                bool null_key=false;int64_t key=Long(b,0,i,&null_key),card=Long(b,2,i);Require(card>0&&(uint64_t)card<=spec->physical_rows-rows,"invalid bitmap cardinality population");
                Require(null_key||spec->key_type!=OG_LAKE_SDK_INT16||(key>=INT16_MIN&&key<=INT16_MAX),"bitmap key exceeds INT16 domain");
                Require(null_key||spec->key_type!=OG_LAKE_SDK_INT32||(key>=INT32_MIN&&key<=INT32_MAX),"bitmap key exceeds INT32 domain");
                if(!r->groups.empty()){const auto& last=r->groups.back();Require(KeyCompare(last.key,last.null_key,key,null_key)<0,"bitmap dictionary key order or uniqueness changed");}
                std::string bytes=Binary(b,i);BitmapHandle bitmap;OgLakeBitmapDecodeSpec decode{sizeof(decode),OG_LAKE_BITMAP_CODEC_VERSION,spec->physical_rows,(uint64_t)card,(uint64_t)limits->memory_bytes};
                Check(og_lake_bitmap_decode(&decode,bytes.data(),bytes.size(),&bitmap.value,&e),e);Group group{key,null_key,{}};group.positions.resize((size_t)card);
                Check(og_lake_bitmap_export(bitmap.value,group.positions.data(),group.positions.size(),&e),e);
                for(size_t j=0;j<group.positions.size();++j){if((j&4095)==0)Poll(*limits);uint32_t p=group.positions[j];Require(p<spec->physical_rows&&(!j||group.positions[j-1]<p),"bitmap export outside source or unordered");Mark(coverage,p);}
                GroupHash(hash,key,null_key,card,bytes);rows+=(uint64_t)card;r->groups.push_back(std::move(group));}
        }
        Require(r->groups.size()==(size_t)dictionary_rows&&rows==spec->physical_rows,"bitmap dictionary lacks full source coverage");
        Require(Finish(hash,rows,r->groups.size())==expected,"bitmap source/context/payload manifest mismatch");*out=r.release();});
}
extern "C" int og_lake_bitmap_select(OgLakeBitmapReader* r,int kind,int null_argument,const int64_t* values,int64_t count,OgLakeBitmapResult** out,OgLakeSdkError* error)
{
    if(out)*out=nullptr;return Guard(error,[&]{Require(r&&out&&kind>=1&&kind<=8&&(null_argument==0||null_argument==1)&&count>=0&&(count==0||values),"invalid bitmap predicate");
        Require((uint64_t)count<=OG_LAKE_BITMAP_MAX_KEYS,"bitmap IN list exceeds prototype bound",OG_LAKE_SDK_UNSUPPORTED);
        bool null_test=kind==6||kind==7;Require(null_test?(count==0&&!null_argument):(kind==8||count==1),"invalid bitmap predicate arity");
        Poll(r->limits);std::vector<int64_t> keys;if(count)keys.assign(values,values+count);
        if(kind==8){std::sort(keys.begin(),keys.end());keys.erase(std::unique(keys.begin(),keys.end()),keys.end());}
        auto result=std::unique_ptr<OgLakeBitmapResult>(new OgLakeBitmapResult);
        for(const auto& g:r->groups){Poll(r->limits);bool match=false;
            if(kind==6)match=g.null_key;else if(kind==7)match=!g.null_key;
            else if(!null_argument&&!g.null_key&&!keys.empty()){int64_t v=keys[0];switch(kind){case 1:match=g.key<v;break;case 2:match=g.key<=v;break;case 3:match=g.key==v;break;case 4:match=g.key>=v;break;case 5:match=g.key>v;break;default:match=std::binary_search(keys.begin(),keys.end(),g.key);}}
            if(match)for(size_t j=0;j<g.positions.size();++j){if((j&4095)==0)Poll(r->limits);result->positions.push_back({r->file_id,g.positions[j]});}}
        std::sort(result->positions.begin(),result->positions.end(),[](const OgLakeScalarPosition& a,const OgLakeScalarPosition& b){return a.row_position<b.row_position;});
        Poll(r->limits);*out=result.release();});
}
extern "C" int64_t og_lake_bitmap_result_count(const OgLakeBitmapResult* r){return r?(int64_t)r->positions.size():0;}
extern "C" const OgLakeScalarPosition* og_lake_bitmap_result_positions(const OgLakeBitmapResult* r){return r&&!r->positions.empty()?r->positions.data():nullptr;}
extern "C" void og_lake_bitmap_result_close(OgLakeBitmapResult** r){if(r){delete *r;*r=nullptr;}}
extern "C" void og_lake_bitmap_reader_close(OgLakeBitmapReader** r){if(r){delete *r;*r=nullptr;}}
