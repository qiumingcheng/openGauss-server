/* Source-domain validation around CRoaring's standard portable format. This
 * module does not implement a bitmap algorithm or an index publication path. */
#include "lake_bitmap_codec.h"
#include <roaring/roaring.h>
#include <cstdio>
#include <memory>
#include <new>
#include <limits>

struct OgLakeBitmap {uint64_t rows;roaring_bitmap_t* bitmap;};
namespace {
struct FreeBitmap {void operator()(roaring_bitmap_t* value)const{if(value)roaring_bitmap_free(value);}};
static uint32_t U32(const unsigned char* p)
{return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);}
static uint16_t U16(const unsigned char* p)
{return (uint16_t)((uint16_t)p[0]|((uint16_t)p[1]<<8));}
static int Error(OgLakeSdkError* error,int code,const char* message)
{if(error){*error={};error->code=code;std::snprintf(error->message,sizeof(error->message),"%s",message);}return code;}
}
extern "C" int og_lake_bitmap_decode(const OgLakeBitmapDecodeSpec* spec,const void* input,size_t length,
    OgLakeBitmap** output,OgLakeSdkError* error)
{
    if(!output||*output||!spec||spec->struct_size!=sizeof(*spec)||!input||length<4||
        spec->format_version!=OG_LAKE_BITMAP_CODEC_VERSION||spec->physical_rows>OG_LAKE_BITMAP_MAX_ROWS||
        spec->expected_cardinality>spec->physical_rows)
        return Error(error,OG_LAKE_SDK_INVALID,"invalid bounded bitmap decode request");
    const auto* bytes=(const unsigned char*)input;uint32_t cookie=U32(bytes),containers=0;size_t offset=0;
    if(cookie==12346u){
        if(length<8)return Error(error,OG_LAKE_SDK_INVALID,"truncated bitmap cookie");
        containers=U32(bytes+4);offset=8;
    }else if((cookie&65535u)==12347u){containers=(cookie>>16)+1;offset=4+(containers+7)/8;}
    else return Error(error,OG_LAKE_SDK_INVALID,"unknown portable bitmap cookie");
    uint64_t domain=(spec->physical_rows+65535)/65536;
    if(containers>domain||offset>length||containers>(length-offset)/4)
        return Error(error,OG_LAKE_SDK_INVALID,"bitmap containers exceed physical source domain");
    uint32_t previous=0;
    for(uint32_t i=0;i<containers;++i){uint32_t key=U16(bytes+offset+i*4);
        if(key>=domain||(i&&key<=previous))return Error(error,OG_LAKE_SDK_INVALID,"invalid bitmap container domain or order");
        previous=key;}
    /* A malformed run container can request65535 run pairs before semantic
     * validation. Bound that allocation from the domain BEFORE decoding.
     * Encoded storage and conservative per-container overhead are charged too.
     * This is a codec admission bound, not whole-query allocator accounting. */
    uint64_t working=(uint64_t)containers*(4u*65536u+1024u)+4096;
    if(length>spec->memory_bytes||working>spec->memory_bytes-length||
        length>spec->memory_bytes-length-working)
        return Error(error,OG_LAKE_SDK_MEMORY,"bitmap decode exceeds its bounded working budget");
    if(roaring_bitmap_portable_deserialize_size((const char*)input,length)!=length)
        return Error(error,OG_LAKE_SDK_INVALID,"invalid portable bitmap length or trailing bytes");
    try{
        std::unique_ptr<roaring_bitmap_t,FreeBitmap> bitmap(roaring_bitmap_portable_deserialize_safe((const char*)input,length));
        /* For the qualified run-preallocation variant, the nonallocating
         * length pass above covers every decode bounds/cookie rejection.
         * With immutable input, NULL here therefore denotes allocation failure. */
        if(!bitmap)return Error(error,OG_LAKE_SDK_MEMORY,"portable bitmap allocation failed");
        const char* reason=nullptr;
        if(!roaring_bitmap_internal_validate(bitmap.get(),&reason))
            return Error(error,OG_LAKE_SDK_INVALID,"portable bitmap failed internal semantic validation");
        uint64_t count=roaring_bitmap_get_cardinality(bitmap.get());
        if(count!=spec->expected_cardinality||(count&&roaring_bitmap_maximum(bitmap.get())>=spec->physical_rows))
            return Error(error,OG_LAKE_SDK_INVALID,"bitmap cardinality or position exceeds immutable physical source");
        auto result=std::unique_ptr<OgLakeBitmap>(new OgLakeBitmap{spec->physical_rows,bitmap.get()});
        bitmap.release();*output=result.release();if(error)*error={};return OG_LAKE_SDK_OK;
    }catch(const std::bad_alloc&){return Error(error,OG_LAKE_SDK_MEMORY,"bitmap allocation failed");}
    catch(...){return Error(error,OG_LAKE_SDK_INVALID,"unexpected bitmap decode failure");}
}
extern "C" uint64_t og_lake_bitmap_count(const OgLakeBitmap* value)
{return value?roaring_bitmap_get_cardinality(value->bitmap):0;}
extern "C" int og_lake_bitmap_contains(const OgLakeBitmap* value,uint32_t position)
{return value&&position<value->rows&&roaring_bitmap_contains(value->bitmap,position)?1:0;}
extern "C" int og_lake_bitmap_export(const OgLakeBitmap* value,uint32_t* positions,
    uint64_t capacity,OgLakeSdkError* error)
{
    if(!value||capacity>std::numeric_limits<size_t>::max()/sizeof(uint32_t))
        return Error(error,OG_LAKE_SDK_INVALID,"invalid bitmap export request");
    uint64_t count=roaring_bitmap_get_cardinality(value->bitmap);
    if(capacity<count||(count&&!positions))
        return Error(error,OG_LAKE_SDK_INVALID,"bitmap export capacity is insufficient");
    if(count)roaring_bitmap_to_uint32_array(value->bitmap,positions);
    if(error)*error={};return OG_LAKE_SDK_OK;
}
extern "C" void og_lake_bitmap_close(OgLakeBitmap** value)
{if(value&&*value){roaring_bitmap_free((*value)->bitmap);delete *value;*value=nullptr;}}
