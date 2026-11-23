#include "lake_bitmap_codec.h"
#include <roaring/roaring.h>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
static unsigned checks=0;
static void check(bool ok,const char* label){++checks;if(!ok){std::fprintf(stderr,"FAIL %s\n",label);std::exit(1);}}
static int decode(const std::vector<char>& bytes,uint64_t rows,uint64_t count,OgLakeBitmap** result,uint64_t budget=64u*1024u*1024u)
{OgLakeBitmapDecodeSpec spec={sizeof(spec),1,rows,count,budget};OgLakeSdkError error={};int status=og_lake_bitmap_decode(&spec,bytes.data(),bytes.size(),result,&error);if(status)check(!*result,"failed decode publishes no handle");return status;}
int main(){
 for(unsigned mode=0;mode<5;++mode){
  uint32_t rows=mode==0?0:mode==1?143:1048576;roaring_bitmap_t* native=roaring_bitmap_create();check(native!=nullptr,"native producer allocation");
  std::vector<bool> expected(rows);uint64_t count=0;
  for(uint32_t i=0;i<rows;++i){bool selected=mode==1?i%7==0:mode==2?i%3==0:mode==3?i>=70000&&i<900000:i==rows-1;
   if(selected){roaring_bitmap_add(native,i);expected[i]=true;++count;}}
  if(mode==3)roaring_bitmap_run_optimize(native);
  std::vector<char> bytes(roaring_bitmap_portable_size_in_bytes(native));check(roaring_bitmap_portable_serialize(native,bytes.data())==bytes.size(),"actual portable producer length");roaring_bitmap_free(native);
  OgLakeBitmap* result=nullptr;check(decode(bytes,rows,count,&result)==0,"portable decode");check(og_lake_bitmap_count(result)==count,"expected cardinality");
  for(uint32_t i=0;i<rows;++i)check((og_lake_bitmap_contains(result,i)!=0)==expected[i],"independent full-domain membership");
  std::vector<uint32_t> positions(count+2,0xdededeu);OgLakeSdkError error={};
  check(og_lake_bitmap_export(result,positions.data()+1,count,&error)==0,"bounded export");
  check(positions.front()==0xdededeu&&positions.back()==0xdededeu,"export preserves boundary canaries");
  uint64_t offset=1;for(uint32_t i=0;i<rows;++i)if(expected[i])check(positions[offset++]==i,"independent sorted export positions");
  if(count){auto before=positions;check(og_lake_bitmap_export(result,positions.data()+1,count-1,&error)!=0,"undersized export rejected");check(positions==before,"undersized export writes nothing");
   check(og_lake_bitmap_export(result,nullptr,count,&error)!=0,"nonempty NULL export rejected");}
  else check(og_lake_bitmap_export(result,nullptr,0,&error)==0,"empty NULL export accepted");
  auto before=positions;check(og_lake_bitmap_export(result,positions.data(),UINT64_MAX,&error)!=0,"overflow capacity rejected");check(positions==before,"overflow capacity writes nothing");
  check(!og_lake_bitmap_contains(result,rows),"source upper bound");og_lake_bitmap_close(&result);og_lake_bitmap_close(&result);check(!result,"idempotent release");
  if(rows){check(decode(bytes,rows,count+1,&result)!=0,"wrong cardinality rejected");check(decode(bytes,rows,count,&result,4096)==OG_LAKE_SDK_MEMORY,"memory admission before decode");}
  auto trailing=bytes;trailing.push_back(0);check(decode(trailing,rows,count,&result)!=0,"trailing bytes rejected");
  auto shortBytes=bytes;shortBytes.pop_back();check(decode(shortBytes,rows,count,&result)!=0,"truncated payload rejected");
  if(bytes.size()>8){auto bad=bytes;bad[4]=(char)0xff;bad[5]=(char)0xff;bad[6]=(char)0xff;bad[7]=(char)0xff;
   check(decode(bad,rows,count,&result)!=0,"malformed container domain rejected");}
 }
 // Target semantic corruption that length-safe decoding alone cannot reject.
 {
  roaring_bitmap_t* native=roaring_bitmap_create();roaring_bitmap_add_range(native,2,100);roaring_bitmap_add_range(native,200,400);roaring_bitmap_run_optimize(native);
  std::vector<char> bytes(roaring_bitmap_portable_size_in_bytes(native));roaring_bitmap_portable_serialize(native,bytes.data());roaring_bitmap_free(native);
  check(bytes.size()==19&&(unsigned char)bytes[0]==0x3b&&(unsigned char)bytes[1]==0x30,"known standard two-run wire fixture");
  OgLakeBitmap* result=nullptr;check(decode(bytes,65536,298,&result)==0,"valid two-run fixture");og_lake_bitmap_close(&result);
  auto overlap=bytes;overlap[15]=overlap[11];overlap[16]=overlap[12];check(decode(overlap,65536,298,&result)!=0,"overlapping runs fail semantic validation");
 }
 {
  roaring_bitmap_t* native=roaring_bitmap_create();roaring_bitmap_add(native,2);roaring_bitmap_add(native,14);
  std::vector<char> bytes(roaring_bitmap_portable_size_in_bytes(native));roaring_bitmap_portable_serialize(native,bytes.data());roaring_bitmap_free(native);
  check(bytes.size()==20&&(unsigned char)bytes[0]==0x3a&&(unsigned char)bytes[1]==0x30,"known standard two-array-value wire fixture");
  OgLakeBitmap* result=nullptr;auto duplicate=bytes;duplicate[18]=duplicate[16];duplicate[19]=duplicate[17];
  check(decode(duplicate,143,2,&result)!=0,"duplicate array positions fail semantic validation");
  check(decode(bytes,10,2,&result)!=0,"last position respects partial-container source bound");
  for(size_t length=0;length<bytes.size();++length){auto truncated=bytes;truncated.resize(length);check(decode(truncated,143,2,&result)!=0,"each truncated array prefix rejected");}
 }
 uint64_t seed=0x1415926535ULL;
 for(unsigned trial=0;trial<20000;++trial){seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;std::vector<char> raw(seed%257);
  for(char& b:raw){seed^=seed<<13;seed^=seed>>7;seed^=seed<<17;b=(char)seed;}
  OgLakeBitmap* result=nullptr;int status=decode(raw,1048576,0,&result);if(!status){check(og_lake_bitmap_count(result)==0,"arbitrary accepted payload respects expected cardinality");og_lake_bitmap_close(&result);}}
 std::printf("{\"status\":\"pass\",\"checks\":%u}\n",checks);
}
