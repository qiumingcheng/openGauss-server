/* Real isolated provider codec test; no database or metadata simulation. */
#include "sdk/lake_parquet_sdk.h"
#include "vendor/arrow20/abi.h"
#include <dirent.h>
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static unsigned long checks;
static void check(int condition,const char* name)
{++checks;if(!condition){fprintf(stderr,"float codec check failed: %s\n",name);exit(1);}}
static void ok(int code,const OgLakeSdkError* error)
{if(code){fprintf(stderr,"float codec status %d: %s\n",code,error->message);exit(1);}++checks;}
static void array_release(struct ArrowArray* array){array->release=NULL;}
static void schema_release(struct ArrowSchema* schema){schema->release=NULL;}
static int fds(void)
{int count=0;DIR* dir=opendir("/proc/self/fd");check(dir!=NULL,"open fd inventory");while(readdir(dir))++count;closedir(dir);return count;}
static const uint32_t bits32[]={0,UINT32_C(0x80000000),UINT32_C(0x3f800000),UINT32_C(0xbf800000),
    1,UINT32_C(0x80000001),UINT32_C(0x007fffff),UINT32_C(0x00800000),UINT32_C(0x7f7fffff),
    UINT32_C(0xff7fffff),UINT32_C(0x7f800000),UINT32_C(0xff800000),UINT32_C(0x7fc12345),
    UINT32_C(0xffc01234),UINT32_C(0x3eaaaaab),UINT32_C(0xbeaaaaab)};
static const uint64_t bits64[]={0,UINT64_C(0x8000000000000000),UINT64_C(0x3ff0000000000000),
    UINT64_C(0xbff0000000000000),1,UINT64_C(0x8000000000000001),UINT64_C(0x000fffffffffffff),
    UINT64_C(0x0010000000000000),UINT64_C(0x7fefffffffffffff),UINT64_C(0xffefffffffffffff),
    UINT64_C(0x7ff0000000000000),UINT64_C(0xfff0000000000000),UINT64_C(0x7ff8123456789abc),
    UINT64_C(0xfff8fedcba987654),UINT64_C(0x3fd5555555555555),UINT64_C(0xbfd5555555555555)};
static int absent(int64_t row,int wide){return row%(wide?19:17)==(wide?18:16);}
static const char* names[]={"f32.bits","f64_值"};

static void append(const OgLakeParquetSdkApi* api,OgLakeSdkWriter* writer,int64_t start,int64_t count)
{
    enum {OFFSET=3,MAX=4093};check(count<=MAX,"bounded input batch");
    float values32[MAX+OFFSET];double values64[MAX+OFFSET];
    unsigned char valid32[(MAX+OFFSET+7)/8],valid64[(MAX+OFFSET+7)/8];
    memset(valid32,0,sizeof(valid32));memset(valid64,0,sizeof(valid64));int64_t null32=0,null64=0;
    for(int64_t i=0;i<count;++i){
        int64_t row=start+i;memcpy(&values32[i+OFFSET],&bits32[row%16],4);memcpy(&values64[i+OFFSET],&bits64[row%16],8);
        if(absent(row,0))++null32;else valid32[(i+OFFSET)/8]|=(unsigned char)(1u<<((i+OFFSET)%8));
        if(absent(row,1))++null64;else valid64[(i+OFFSET)/8]|=(unsigned char)(1u<<((i+OFFSET)%8));
    }
    const void* buffers32[]={valid32,values32};const void* buffers64[]={valid64,values64};const void* root_buffers[]={NULL};
    struct ArrowArray a32={0},a64={0},root={0};struct ArrowSchema s32={0},s64={0},schema={0};
    a32.length=count;a32.offset=OFFSET;a32.null_count=null32;a32.n_buffers=2;a32.buffers=buffers32;a32.release=array_release;
    a64.length=count;a64.offset=OFFSET;a64.null_count=null64;a64.n_buffers=2;a64.buffers=buffers64;a64.release=array_release;
    s32.format="f";s32.name=names[0];s32.flags=2;s32.release=schema_release;
    s64.format="g";s64.name=names[1];s64.flags=2;s64.release=schema_release;
    struct ArrowArray* arrays[]={&a32,&a64};struct ArrowSchema* fields[]={&s32,&s64};
    root.length=count;root.n_buffers=1;root.buffers=root_buffers;root.n_children=2;root.children=arrays;root.release=array_release;
    schema.format="+s";schema.n_children=2;schema.children=fields;schema.release=schema_release;
    OgLakeSdkError error={0};ok(api->writer_append(writer,&root,&schema,&error),&error);
    check(root.release&&schema.release&&a32.release&&a64.release&&s32.release&&s64.release,"borrowed callbacks not consumed");
}

static void inspect(const struct ArrowArray* column,int64_t index,int64_t row,int wide)
{
    int64_t position=column->offset+index;int is_null=0;
    if(column->null_count!=0){check(column->buffers[0]!=NULL,"validity exists");
        const unsigned char* bitmap=(const unsigned char*)column->buffers[0];is_null=!(bitmap[position/8]&(1u<<(position%8)));}
    check(is_null==absent(row,wide),"independent NULL pattern");if(is_null)return;
    check(column->buffers[1]!=NULL,"float value buffer exists");
    if(wide){uint64_t actual;memcpy(&actual,(const unsigned char*)column->buffers[1]+position*8,8);check(actual==bits64[row%16],"exact float64 IEEE bits");}
    else{uint32_t actual;memcpy(&actual,(const unsigned char*)column->buffers[1]+position*4,4);check(actual==bits32[row%16],"exact float32 IEEE bits");}
}

int main(int argc,char** argv)
{
    check(argc==3||argc==4,"provider/output arguments");check(sizeof(float)==4&&sizeof(double)==8,"IEEE host sizes");
    void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);check(library!=NULL,"load real provider");
    OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(library,"og_lake_parquet_sdk_v1");check(entry!=NULL,"real base entry");
    const OgLakeParquetSdkApi* api=entry();check(api&&api->abi_version==1&&api->struct_size==sizeof(*api),"unchanged base ABI");
    OgLakeSdkLimits limits={sizeof(limits),64*1024*1024,16*1024*1024,NULL,NULL};
    OgLakeSdkField fields[]={{101,OG_LAKE_SDK_FLOAT32,1,names[0]},{103,OG_LAKE_SDK_FLOAT64,1,names[1]}};
    OgLakeSdkError error={0};char path[4096];OgLakeSdkWriter* writer=NULL;
    if(argc==4){
        check(strcmp(argv[3],"unsupported")==0&&!(api->capabilities&OG_LAKE_SDK_FLOAT_VALUES),"old provider advertises no floating codec");
        snprintf(path,sizeof(path),"%s/unsupported.parquet",argv[2]);
        OgLakeSdkWriteOptions options={sizeof(options),path,limits,2,fields,OG_LAKE_SDK_ZSTD,1024};
        check(api->writer_open(&options,&writer,&error)==OG_LAKE_SDK_UNSUPPORTED&&writer==NULL,"old provider rejects new field codes");
        check(access(path,F_OK)!=0,"unsupported type creates no output");
        printf("{\"status\":\"pass\",\"checks\":%lu,\"unsupported\":true}\n",checks);return 0;
    }
    check(api->capabilities&OG_LAKE_SDK_FLOAT_VALUES,"floating capability present");int before=fds();
    const uint32_t codecs[]={OG_LAKE_SDK_UNCOMPRESSED,OG_LAKE_SDK_ZSTD,OG_LAKE_SDK_SNAPPY};
    for(int test=0;test<3;++test){
        snprintf(path,sizeof(path),"%s/floating_%d.parquet",argv[2],test);
        OgLakeSdkWriteOptions options={sizeof(options),path,limits,2,fields,codecs[test],1024};
        ok(api->writer_open(&options,&writer,&error),&error);
        append(api,writer,0,4093);append(api,writer,4093,4093);append(api,writer,8186,7);
        OgLakeSdkWrittenFile written={0};written.struct_size=sizeof(written);ok(api->writer_finish(writer,&written,&error),&error);
        check(written.num_rows==8193&&written.local_fsync_complete,"finished exact local population");ok(api->writer_close(&writer,&error),&error);
        OgLakeSdkFooter* footer=NULL;ok(api->footer_open(path,&limits,&footer,&error),&error);
        const OgLakeSdkFooterInfo* info=api->footer_info(footer);check(info->num_rows==8193&&info->num_fields==2,"real Parquet population/schema");
        for(int i=0;i<2;++i)check(info->fields[i].type==fields[i].type&&info->fields[i].field_id==fields[i].field_id&&strcmp(info->fields[i].name,names[i])==0,"field type/ID/name preserved");
        api->footer_close(&footer);int64_t projection[]={103,101};OgLakeSdkReader* reader=NULL;
        OgLakeSdkReadOptions read={sizeof(read),path,limits,257,OG_LAKE_SDK_BY_FIELD_ID,2,projection,-1,NULL};
        ok(api->reader_open(&read,&reader,&error),&error);int64_t total=0;
        for(;;){OgLakeSdkBatch* batch=NULL;int code=api->reader_next(reader,&batch,&error);if(code==OG_LAKE_SDK_EOF)break;ok(code,&error);
            check(batch&&batch->original_positions&&batch->array->n_children==2&&batch->schema->n_children==2,"owned projected C Data");
            check(strcmp(batch->schema->children[0]->format,"g")==0&&strcmp(batch->schema->children[1]->format,"f")==0,"reversed float projection formats");
            for(int64_t i=0;i<batch->num_rows;++i){int64_t row=total+i;check(batch->original_positions[i]==row,"exact original source address");
                inspect(batch->array->children[0],i,row,1);inspect(batch->array->children[1],i,row,0);}
            total+=batch->num_rows;api->batch_release(&batch);
        }
        check(total==8193,"every row read once");api->reader_close(&reader);
    }
    check(fds()==before,"all owned float file descriptors released");
    printf("{\"status\":\"pass\",\"checks\":%lu,\"rows\":8193,\"files\":3}\n",checks);return 0;
}
