/* Real Parquet BINARY transport, independent of database and index formats. */
#include "sdk/lake_parquet_sdk.h"
#include "vendor/arrow20/abi.h"
#include <dlfcn.h>
#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static unsigned long checks;
static void check(int good,const char* name)
{++checks;if(!good){fprintf(stderr,"binary check failed: %s\n",name);exit(1);}}
static void ok(int status,const OgLakeSdkError* error)
{if(status){fprintf(stderr,"binary status %d: %s\n",status,error->message);exit(1);}++checks;}
static void release_array(struct ArrowArray* a){a->release=NULL;}
static void release_schema(struct ArrowSchema* s){s->release=NULL;}
static int fd_count(void)
{int n=0;DIR* d=opendir("/proc/self/fd");check(d!=NULL,"fd directory");while(readdir(d))++n;closedir(d);return n;}
static int absent(int64_t row){return row%17==16;}
static int length(int64_t row){return row%257==0?8192:(int)(row%8==0?0:row%256);}
static unsigned char value(int64_t row,int offset){return (unsigned char)(row*13+offset*31);}
static int cancelled(void* state){return *(int*)state;}

static void append(const OgLakeParquetSdkApi* api,OgLakeSdkWriter* writer,int64_t start,int count)
{
    enum {OFFSET=3,MAX=257,CAPACITY=MAX*8192};
    check(count<=MAX,"bounded input population");
    unsigned char* data=(unsigned char*)malloc(CAPACITY);check(data!=NULL,"bounded input bytes");
    int32_t offsets[MAX+OFFSET+1];unsigned char valid[(MAX+OFFSET+7)/8];
    memset(offsets,0,sizeof(offsets));memset(valid,0,sizeof(valid));int32_t used=0;int64_t nulls=0;
    for(int i=0;i<count;++i){
        int64_t row=start+i;int n=absent(row)?0:length(row);
        for(int j=0;j<n;++j)data[used++]=value(row,j);
        offsets[i+OFFSET+1]=used;
        if(absent(row))++nulls;else valid[(i+OFFSET)/8]|=(unsigned char)(1u<<((i+OFFSET)%8));
    }
    const void* buffers[]={valid,offsets,data};const void* root_buffers[]={NULL};
    struct ArrowArray column={0},root={0};struct ArrowSchema field={0},schema={0};
    column.length=count;column.offset=OFFSET;column.null_count=nulls;column.n_buffers=3;
    column.buffers=buffers;column.release=release_array;
    field.format="z";field.name="node.bytes";field.flags=2;field.release=release_schema;
    struct ArrowArray* children[]={&column};struct ArrowSchema* fields[]={&field};
    root.length=count;root.n_buffers=1;root.buffers=root_buffers;root.n_children=1;root.children=children;root.release=release_array;
    schema.format="+s";schema.n_children=1;schema.children=fields;schema.release=release_schema;
    OgLakeSdkError error={0};ok(api->writer_append(writer,&root,&schema,&error),&error);
    check(root.release&&column.release&&schema.release&&field.release,"borrowed ownership retained");free(data);
}

static void inspect(const OgLakeSdkBatch* batch)
{
    check(batch&&batch->array&&batch->schema&&batch->array->n_children==1&&batch->original_positions,"owned projected batch");
    const struct ArrowArray* a=batch->array->children[0];const char* f=batch->schema->children[0]->format;
    check(strcmp(f,"z")==0||strcmp(f,"Z")==0,"binary format is not UTF8");
    for(int64_t i=0;i<batch->num_rows;++i){
        int64_t row=batch->original_positions[i],at=a->offset+i;int isnull=0;
        if(a->null_count!=0){check(a->buffers[0]!=NULL,"nullable bitmap");
            isnull=!(((const unsigned char*)a->buffers[0])[at/8]&(1u<<(at%8)));}
        check(isnull==absent(row),"NULL pattern retained");if(isnull)continue;
        int64_t begin=strcmp(f,"z")==0?((const int32_t*)a->buffers[1])[at]:((const int64_t*)a->buffers[1])[at];
        int64_t end=strcmp(f,"z")==0?((const int32_t*)a->buffers[1])[at+1]:((const int64_t*)a->buffers[1])[at+1];
        check(end-begin==length(row),"exact byte length including empty");
        for(int j=0;j<end-begin;++j)check(((const unsigned char*)a->buffers[2])[begin+j]==value(row,j),"all byte values including NUL/invalid UTF8");
    }
}

static void reject_utf8(const OgLakeParquetSdkApi* api,const char* directory,const OgLakeSdkLimits* limits)
{
    OgLakeSdkError error={0};char path[4096];
    for(int mismatch=0;mismatch<2;++mismatch){
        snprintf(path,sizeof(path),"%s/rejected_%d.parquet",directory,mismatch);
        OgLakeSdkField field={101,mismatch?OG_LAKE_SDK_BINARY:OG_LAKE_SDK_UTF8,1,"node.bytes"};
        OgLakeSdkWriteOptions options={sizeof(options),path,*limits,1,&field,OG_LAKE_SDK_ZSTD,1024};
        OgLakeSdkWriter* writer=NULL;ok(api->writer_open(&options,&writer,&error),&error);
        const unsigned char data[]={255,0,254};const int32_t offsets[]={0,3};
        const void* buffers[]={NULL,offsets,data};const void* root_buffers[]={NULL};
        struct ArrowArray column={0},root={0};struct ArrowSchema schema={0},child={0};
        column.length=1;column.n_buffers=3;column.buffers=buffers;column.release=release_array;
        child.format="u";child.name="node.bytes";child.flags=2;child.release=release_schema;
        struct ArrowArray* arrays[]={&column};struct ArrowSchema* fields[]={&child};
        root.length=1;root.n_buffers=1;root.buffers=root_buffers;root.n_children=1;root.children=arrays;root.release=release_array;
        schema.format="+s";schema.n_children=1;schema.children=fields;schema.release=release_schema;
        check(api->writer_append(writer,&root,&schema,&error)==OG_LAKE_SDK_INVALID,
            mismatch?"BINARY does not accept an Arrow UTF8 array":"UTF8 still rejects invalid encoded bytes");
        check(root.release&&column.release&&schema.release&&child.release,"error retains caller ownership");
        ok(api->writer_close(&writer,&error),&error);check(access(path,F_OK)!=0,"failed output discarded");
    }
}

static void read_file(const OgLakeParquetSdkApi* api,const char* path,const OgLakeSdkLimits* limits)
{
    OgLakeSdkError error={0};OgLakeSdkFooter* footer=NULL;
    ok(api->footer_open(path,limits,&footer,&error),&error);const OgLakeSdkFooterInfo* info=api->footer_info(footer);
    check(info->num_rows==4097&&info->num_fields==1&&info->num_row_groups>=2,"footer population");
    check(info->fields[0].field_id==101&&info->fields[0].type==OG_LAKE_SDK_BINARY,"footer type and permanent field ID");
    int last=info->num_row_groups-1;int64_t last_first=info->row_groups[last].first_row,last_count=info->row_groups[last].num_rows;
    api->footer_close(&footer);int64_t field=101;OgLakeSdkReader* reader=NULL;
    OgLakeSdkReadOptions options={sizeof(options),path,*limits,127,OG_LAKE_SDK_BY_FIELD_ID,1,&field,-1,NULL};
    ok(api->reader_open(&options,&reader,&error),&error);int64_t count=0;
    for(;;){OgLakeSdkBatch* b=NULL;int status=api->reader_next(reader,&b,&error);if(status==OG_LAKE_SDK_EOF)break;
        ok(status,&error);inspect(b);for(int64_t i=0;i<b->num_rows;++i)check(b->original_positions[i]==count+i,"absolute physical row position");
        count+=b->num_rows;api->batch_release(&b);check(b==NULL,"batch release clears ownership");}
    check(count==4097,"complete read count");api->reader_close(&reader);
    options.num_row_groups=1;options.row_groups=&last;
    ok(api->reader_open(&options,&reader,&error),&error);count=0;
    for(;;){OgLakeSdkBatch* b=NULL;int status=api->reader_next(reader,&b,&error);if(status==OG_LAKE_SDK_EOF)break;
        ok(status,&error);inspect(b);for(int64_t i=0;i<b->num_rows;++i)check(b->original_positions[i]==last_first+count+i,"selected row group keeps source positions");
        count+=b->num_rows;api->batch_release(&b);}
    check(count==last_count,"selected row-group population");api->reader_close(&reader);
    options.num_row_groups=-1;options.row_groups=NULL;ok(api->reader_open(&options,&reader,&error),&error);
    OgLakeSdkBatch* retained=NULL;ok(api->reader_next(reader,&retained,&error),&error);api->reader_close(&reader);inspect(retained);api->batch_release(&retained);
    int cancel=1;options.limits.should_cancel=cancelled;options.limits.cancel_context=&cancel;
    check(api->reader_open(&options,&reader,&error)==OG_LAKE_SDK_CANCELLED&&reader==NULL,"cancelled open leaves no reader");
    options.limits=*limits;options.limits.memory_bytes=64;options.limits.footer_bytes=64;
    check(api->reader_open(&options,&reader,&error)!=OG_LAKE_SDK_OK&&reader==NULL,"tiny budget cannot create an unbounded reader");
}

int main(int argc,char** argv)
{
    check(argc==3||argc==4,"provider/output arguments");void* lib=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);check(lib!=NULL,"load real provider");
    OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(lib,"og_lake_parquet_sdk_v1");check(entry!=NULL,"base entry");
    const OgLakeParquetSdkApi* api=entry();check(api&&api->abi_version==1&&api->struct_size==sizeof(*api),"unchanged base ABI");
    OgLakeSdkLimits limits={sizeof(limits),64*1024*1024,16*1024*1024,NULL,NULL};OgLakeSdkError error={0};
    OgLakeSdkField field={101,OG_LAKE_SDK_BINARY,1,"node.bytes"};OgLakeSdkWriter* writer=NULL;char path[4096];
    if(argc==4&&strcmp(argv[3],"unsupported")==0){
        check(!(api->capabilities&OG_LAKE_SDK_BINARY_VALUES),"old provider does not advertise binary");
        snprintf(path,sizeof(path),"%s/unsupported.parquet",argv[2]);
        OgLakeSdkWriteOptions options={sizeof(options),path,limits,1,&field,OG_LAKE_SDK_ZSTD,1024};
        check(api->writer_open(&options,&writer,&error)==OG_LAKE_SDK_UNSUPPORTED&&writer==NULL,"old provider rejects type8 before output");
        check(access(path,F_OK)!=0,"no unsupported artifact");
    }else{
        check(api->capabilities&OG_LAKE_SDK_BINARY_VALUES,"binary capability present");int before=fd_count();
        if(argc==4){check(strcmp(argv[3],"read")==0,"external read mode");read_file(api,argv[2],&limits);}
        else {reject_utf8(api,argv[2],&limits);for(int codec=0;codec<3;++codec){
            snprintf(path,sizeof(path),"%s/binary_%d.parquet",argv[2],codec);
            OgLakeSdkWriteOptions options={sizeof(options),path,limits,1,&field,(uint32_t)codec,1024};
            ok(api->writer_open(&options,&writer,&error),&error);
            for(int start=0;start<4097;start+=257)append(api,writer,start,4097-start>257?257:4097-start);
            OgLakeSdkWrittenFile result={0};result.struct_size=sizeof(result);ok(api->writer_finish(writer,&result,&error),&error);
            check(result.local_fsync_complete&&result.num_rows==4097,"durable full file");ok(api->writer_close(&writer,&error),&error);
            read_file(api,path,&limits);
        }}
        check(fd_count()==before,"all binary FDs released");
    }
    printf("{\"status\":\"pass\",\"checks\":%lu}\n",checks);return 0;
}
