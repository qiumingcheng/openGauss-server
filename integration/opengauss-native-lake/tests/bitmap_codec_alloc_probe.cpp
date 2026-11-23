/* Isolated-process allocator fault injection over the real CRoaring decoder.
 * Not installed in gaussdb; memory hooks are never changed in a server. */
#include "lake_bitmap_codec.h"
#include <roaring/roaring.h>
#include <roaring/memory.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <new>
#include <vector>
#include <string>

namespace {
struct Allocation {void* pointer;size_t bytes;};
Allocation live[1024]={};
size_t live_count=0,live_bytes=0,peak_bytes=0,calls=0,fail_at=0,failures=0;
size_t kinds[4]={};
bool inside_cpp=false,fail_cpp=false;size_t cpp_calls=0,cpp_live=0;void* cpp_pointer=nullptr;
void Check(bool ok,const char* what){if(!ok){std::fprintf(stderr,"FAIL %s\n",what);std::exit(2);}}
bool Fail(unsigned kind){++calls;++kinds[kind];if(fail_at&&calls==fail_at){++failures;return true;}return false;}
void Track(void* p,size_t size){if(!p)return;Check(live_count<1024,"tracker capacity");for(size_t i=0;i<1024;++i)if(!live[i].pointer){live[i]={p,size};++live_count;live_bytes+=size;if(live_bytes>peak_bytes)peak_bytes=live_bytes;return;}std::abort();}
void Untrack(void* p){if(!p)return;for(size_t i=0;i<1024;++i)if(live[i].pointer==p){live_bytes-=live[i].bytes;live[i]={};--live_count;return;}Check(false,"unknown or double C-library free");}
void* Malloc(size_t n){if(Fail(0))return nullptr;void* p=std::malloc(n);Track(p,n);return p;}
void* Calloc(size_t n,size_t s){if(Fail(1))return nullptr;void* p=std::calloc(n,s);Track(p,n*s);return p;}
void* Realloc(void* p,size_t n){if(Fail(2))return nullptr;if(!n){Untrack(p);std::free(p);return nullptr;}void* q=std::realloc(p,n);if(q){Untrack(p);Track(q,n);}return q;}
void Free(void* p){Untrack(p);std::free(p);}
void* Aligned(size_t alignment,size_t n){if(Fail(3))return nullptr;void* p=nullptr;if(posix_memalign(&p,alignment,n))return nullptr;Track(p,n);return p;}
struct Fixture {std::vector<char> bytes;uint64_t rows;uint64_t count;bool valid;};
Fixture Make(const std::string& kind){
 roaring_bitmap_t* bitmap=roaring_bitmap_create();Check(bitmap!=nullptr,"producer allocation");uint64_t rows=1048576;
 if(kind=="empty"){rows=0;}
 else if(kind=="array"||kind=="bad_array"){rows=143;for(uint32_t i=0;i<143;i+=7)roaring_bitmap_add(bitmap,i);}
 else if(kind=="multi_array"){for(uint32_t block=0;block<16;++block){roaring_bitmap_add(bitmap,block*65536+1);roaring_bitmap_add(bitmap,block*65536+997);}}
 else if(kind=="bitset"){for(uint32_t i=0;i<1048576;i+=2)roaring_bitmap_add(bitmap,i);}
 else if(kind=="run"||kind=="many_runs"){for(uint32_t block=0;block<16;++block){if(kind=="run")roaring_bitmap_add_range(bitmap,block*65536+10,block*65536+60000);else for(uint32_t i=0;i<65536;i+=64)roaring_bitmap_add_range(bitmap,block*65536+i,block*65536+i+32);}roaring_bitmap_run_optimize(bitmap);}
 else if(kind=="mixed"){for(uint32_t i=0;i<65536;i+=2)roaring_bitmap_add(bitmap,i);roaring_bitmap_add_range(bitmap,65536+10,65536+60000);roaring_bitmap_add(bitmap,2*65536+11);roaring_bitmap_add(bitmap,15*65536+999);roaring_bitmap_run_optimize(bitmap);}
 else if(kind=="bad_run"){rows=65536;roaring_bitmap_add_range(bitmap,2,100);roaring_bitmap_add_range(bitmap,200,400);roaring_bitmap_run_optimize(bitmap);}
 else Check(false,"unknown fixture");
 uint64_t count=roaring_bitmap_get_cardinality(bitmap);std::vector<char> bytes(roaring_bitmap_portable_size_in_bytes(bitmap));roaring_bitmap_portable_serialize(bitmap,bytes.data());roaring_bitmap_free(bitmap);
 bool valid=true;
 if(kind=="bad_array"){Check(bytes.size()>20,"known array layout");bytes[18]=bytes[16];bytes[19]=bytes[17];valid=false;}
 if(kind=="bad_run"){Check(bytes.size()==19,"known run layout");bytes[15]=bytes[11];bytes[16]=bytes[12];valid=false;}
 return {std::move(bytes),rows,count,valid};
}
}
// The only operator-new call inside decode is its output wrapper allocation.
// Fixtures and logs are built before/after the injection window.
void* operator new(std::size_t n){if(inside_cpp){++cpp_calls;if(fail_cpp)throw std::bad_alloc();}void* p=std::malloc(n);if(!p)throw std::bad_alloc();if(inside_cpp){++cpp_live;cpp_pointer=p;}return p;}
void operator delete(void* p)noexcept{if(p&&p==cpp_pointer){--cpp_live;cpp_pointer=nullptr;}std::free(p);}
void operator delete(void* p,std::size_t)noexcept{::operator delete(p);}

int main(int argc,char** argv){
 Check(argc==4,"arguments fixture/failure-index/cpp-failure");Fixture fixture=Make(argv[1]);fail_at=std::strtoull(argv[2],nullptr,10);fail_cpp=std::strcmp(argv[3],"1")==0;
 roaring_memory_t hooks={Malloc,Realloc,Calloc,Free,Aligned,Free};roaring_init_memory_hook(hooks);
 OgLakeBitmapDecodeSpec spec={sizeof(spec),1,fixture.rows,fixture.count,64u*1024u*1024u};OgLakeBitmap* output=nullptr;OgLakeSdkError error={};
 inside_cpp=true;int status=og_lake_bitmap_decode(&spec,fixture.bytes.data(),fixture.bytes.size(),&output,&error);inside_cpp=false;
 Check(error.code==status,"status/error agree");
 if(fail_at){Check(failures==1,"requested real C-library allocation failed exactly once");Check(status==OG_LAKE_SDK_MEMORY,"real C-library allocation failure has memory status");Check(!output,"C failure publishes no partial handle");}
 else if(fail_cpp&&fixture.valid){Check(cpp_calls==1,"wrapper new was reached");Check(status==OG_LAKE_SDK_MEMORY,"wrapper new failure has memory status");Check(!output,"wrapper failure publishes no partial handle");}
 else if(fixture.valid){Check(status==0&&output,"valid baseline succeeds");Check(og_lake_bitmap_count(output)==fixture.count,"baseline cardinality");}
 else {Check(status==OG_LAKE_SDK_INVALID&&!output,"semantic invalid baseline rejects without a handle");}
 og_lake_bitmap_close(&output);og_lake_bitmap_close(&output);
 Check(!output&&live_count==0&&live_bytes==0&&cpp_live==0,"all tracked C/C++ allocations freed");
 std::printf("{\"status\":\"pass\",\"fixture\":\"%s\",\"failure_index\":%zu,\"cpp_failure\":%s,\"allocation_calls\":%zu,\"malloc_calls\":%zu,\"calloc_calls\":%zu,\"realloc_calls\":%zu,\"aligned_calls\":%zu,\"injected_failures\":%zu,\"decode_status\":%d,\"peak_C_bytes\":%zu,\"outstanding_C_allocations\":%zu,\"outstanding_cpp_allocations\":%zu}\n",argv[1],fail_at,fail_cpp?"true":"false",calls,kinds[0],kinds[1],kinds[2],kinds[3],failures,status,peak_bytes,live_count,cpp_live);
}
