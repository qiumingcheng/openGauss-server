/* Owned-test artifact mutator. Real SDK reader/writer preserves exact required
 * field IDs/types/names while changing one primitive payload byte. */
#include "sdk/lake_parquet_sdk.h"
#include "vendor/arrow20/abi.h"
#include <dlfcn.h>
#include <vector>
#include <string>
#include <stdexcept>
#include <cstdio>
#include <cstring>
#include <algorithm>
static void Need(bool b,const char*m){if(!b)throw std::runtime_error(m);}
static void Ok(int s,const OgLakeSdkError&e){if(s)throw std::runtime_error(e.message);}
static void ArrayRelease(ArrowArray*a){a->release=nullptr;}static void SchemaRelease(ArrowSchema*s){s->release=nullptr;}
struct Row{int64_t id;std::string bytes;};
struct Scope{const OgLakeParquetLifecycleApi* life;OgLakeSdkReader* reader=nullptr;OgLakeSdkBatch* batch=nullptr;OgLakeSdkWriter* writer=nullptr;bool keep=false;
 ~Scope(){if(batch)life->base->batch_release(&batch);if(reader)life->base->reader_close(&reader);if(writer){OgLakeSdkError e{};if(keep)life->base->writer_close(&writer,&e);else life->writer_abort(&writer,&e);}}};
int main(int argc,char**argv){try{
 Need(argc==5,"provider original new-output kind required");void*lib=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);Need(lib,"real SDK load failed");auto entry=(OgLakeParquetLifecycleEntrypoint)dlsym(lib,"og_lake_parquet_sdk_v2");Need(entry,"SDK lifecycle absent");auto life=entry();Need(life&&life->abi_version==2&&life->base&&life->writer_abort,"SDK lifecycle contract");Scope s{life};OgLakeSdkError e{};
 int kind=std::stoi(argv[4]);Need(kind>=1&&kind<=3,"invalid sidecar kind");const char*names[][2]={{"centroid_id","centers"},{"node_id","codes"},{"field_id","value"}};
 OgLakeSdkLimits limits{sizeof(limits),16*1024*1024,1024*1024,nullptr,nullptr};OgLakeSdkReadOptions ro{};ro.struct_size=sizeof(ro);ro.path=argv[2];ro.limits=limits;ro.batch_rows=1024;ro.num_field_ids=-1;ro.num_row_groups=-1;ro.projection_kind=OG_LAKE_SDK_BY_FIELD_ID;
 Ok(life->base->reader_open(&ro,&s.reader,&e),e);std::vector<Row>rows;
 for(;;){int code=life->base->reader_next(s.reader,&s.batch,&e);if(code==OG_LAKE_SDK_EOF)break;Ok(code,e);auto*b=s.batch;
 Need(b&&b->num_rows>=0&&b->num_rows<=1024&&b->array&&b->array->n_children==2,"invalid real SDK batch");auto*a=b->array->children[0];auto*z=b->array->children[1];Need(a&&z&&a->buffers[1]&&z->buffers[1]&&z->buffers[2],"missing Arrow columns");
 auto ids=(const int64_t*)a->buffers[1];auto offsets=(const int32_t*)z->buffers[1];for(int64_t i=0;i<b->num_rows;i++){int64_t at=z->offset+i;Need(offsets[at]>=0&&offsets[at+1]>=offsets[at],"bad binary offsets");Need(rows.size()<1048576,"test row bound");rows.push_back({ids[a->offset+i],std::string((const char*)z->buffers[2]+offsets[at],offsets[at+1]-offsets[at])});}life->base->batch_release(&s.batch);}
 life->base->reader_close(&s.reader);Need(!rows.empty()&&!rows[0].bytes.empty(),"empty mutation input");unsigned old=(unsigned char)rows[0].bytes[0];rows[0].bytes[0]^=1;
 OgLakeSdkField fields[]={{0,OG_LAKE_SDK_INT64,0,names[kind-1][0]},{1,OG_LAKE_SDK_BINARY,0,names[kind-1][1]}};OgLakeSdkWriteOptions wo{};wo.struct_size=sizeof(wo);wo.path=argv[3];wo.limits=limits;wo.num_fields=2;wo.fields=fields;wo.compression=OG_LAKE_SDK_ZSTD;wo.row_group_rows=1024;Ok(life->base->writer_open(&wo,&s.writer,&e),e);
 for(size_t first=0;first<rows.size();first+=1024){size_t n=std::min<size_t>(1024,rows.size()-first);std::vector<int64_t>ids(n);std::vector<int32_t>offsets(n+1);std::string bytes;
 for(size_t i=0;i<n;i++){ids[i]=rows[first+i].id;bytes+=rows[first+i].bytes;offsets[i+1]=bytes.size();}
 ArrowArray root{},a[2]{};ArrowSchema schema{},defs[2]{};ArrowArray*children[]={&a[0],&a[1]};ArrowSchema*schemas[]={&defs[0],&defs[1]};const void*rb[]={nullptr},*ib[]={nullptr,ids.data()},*bb[]={nullptr,offsets.data(),bytes.data()};
 root.length=n;root.n_buffers=1;root.buffers=rb;root.n_children=2;root.children=children;root.release=ArrayRelease;schema.format="+s";schema.n_children=2;schema.children=schemas;schema.release=SchemaRelease;
 for(int i=0;i<2;i++){a[i].length=n;a[i].n_buffers=i?3:2;a[i].buffers=i?bb:ib;a[i].release=ArrayRelease;defs[i].name=fields[i].name;defs[i].format=i?"z":"l";defs[i].release=SchemaRelease;}Ok(life->base->writer_append(s.writer,&root,&schema,&e),e);}
 OgLakeSdkWrittenFile written{};written.struct_size=sizeof(written);Ok(life->base->writer_finish(s.writer,&written,&e),e);Need(written.local_fsync_complete&&written.num_rows==(int64_t)rows.size(),"real SDK mutation durability mismatch");s.keep=true;
 printf("{\"status\":\"mutated_real_sdk_required_schema\",\"rows\":%zu,\"before\":%u,\"after\":%u}\n",rows.size(),old,(unsigned char)rows[0].bytes[0]);return 0;
 }catch(const std::exception&e){fprintf(stderr,"%s\n",e.what());return 1;}}
