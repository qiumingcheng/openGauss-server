/* Independent C11 consumer: dynamically loads only the public C ABI. */
#include "lake_parquet_sdk.h"
#include <arrow/c/abi.h>
#include <dlfcn.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static const OgLakeParquetSdkApi* api;
static unsigned long checks;
#define REQUIRE(x) do { ++checks; if (!(x)) { \
    fprintf(stderr,"check failed at %s:%d: %s\n",__FILE__,__LINE__,#x); exit(1); } } while(0)
static void ok(int status,OgLakeSdkError* error)
{
    if(status!=OG_LAKE_SDK_OK)fprintf(stderr,"SDK error %d: %s\n",status,error->message);
    REQUIRE(status==OG_LAKE_SDK_OK);
}
static OgLakeSdkLimits limits(void)
{
    OgLakeSdkLimits result={sizeof(OgLakeSdkLimits),64*1024*1024,16*1024*1024,NULL,NULL};
    return result;
}
static OgLakeSdkReadOptions options(const char* path)
{
    OgLakeSdkReadOptions result={0};result.struct_size=sizeof(result);
    result.path=path;result.limits=limits();result.batch_rows=137;
    result.num_field_ids=-1;result.num_row_groups=-1;return result;
}
static int valid(const struct ArrowArray* column,int64_t row)
{
    int64_t index=column->offset+row;
    return !column->buffers[0]||(((const unsigned char*)column->buffers[0])[index/8]>>(index%8)&1);
}
static void verify_value(const struct ArrowArray* column,const struct ArrowSchema* schema,
                         int64_t row,int field,int64_t position)
{
    int expected_null=field==1?position%5==0:field==2?position>1&&position%7==0:field==3?position%13==0:0;
    REQUIRE(valid(column,row)==!expected_null);if(expected_null)return;
    int64_t index=column->offset+row;
    if(field==0){
        REQUIRE(strcmp(schema->format,"i")==0);
        REQUIRE(((const int32_t*)column->buffers[1])[index]==position);
    }else if(field==1){
        REQUIRE(strcmp(schema->format,"b")==0);
        REQUIRE((((const unsigned char*)column->buffers[1])[index/8]>>(index%8)&1)==(position%2==0));
    }else if(field==2){
        int64_t expected=position==0?INT64_MIN:position==1?INT64_MAX:position*10000000000LL;
        REQUIRE(strcmp(schema->format,"l")==0);
        REQUIRE(((const int64_t*)column->buffers[1])[index]==expected);
    }else{
        int64_t start,end;
        if(strcmp(schema->format,"u")==0){start=((const int32_t*)column->buffers[1])[index];end=((const int32_t*)column->buffers[1])[index+1];}
        else{REQUIRE(strcmp(schema->format,"U")==0);start=((const int64_t*)column->buffers[1])[index];end=((const int64_t*)column->buffers[1])[index+1];}
        if(position%11==0)REQUIRE(end==start);
        else{
            char prefix[64];int length=snprintf(prefix,sizeof(prefix),"湖/%" PRId64 "/",position);
            REQUIRE(end-start==length+1024);
            REQUIRE(memcmp((const char*)column->buffers[2]+start,prefix,(size_t)length)==0);
            /* Every byte is checked, including the UTF8 prefix and long tail. */
            for(int n=0;n<1024;++n)REQUIRE(((const char*)column->buffers[2])[start+length+n]=='x');
        }
    }
}
static uint64_t scan(const char* path,const int64_t* ids,int count,const int32_t* groups,int ngroups,
                     const OgLakeSdkFooterInfo* info)
{
    OgLakeSdkReadOptions config=options(path);config.field_ids=ids;config.num_field_ids=count;
    config.row_groups=groups;config.num_row_groups=ngroups;
    OgLakeSdkReader* reader=NULL;OgLakeSdkError error;
    ok(api->reader_open(&config,&reader,&error),&error);
    int64_t expected=0,group_offset=0;int group_index=0;OgLakeSdkBatch* batch=NULL;
    for(;;){
        int status=api->reader_next(reader,&batch,&error);
        if(status==OG_LAKE_SDK_EOF){REQUIRE(batch==NULL);break;}
        ok(status,&error);REQUIRE(batch&&batch->num_rows>0&&batch->num_rows<=137);
        REQUIRE(batch->array->length==batch->num_rows);
        REQUIRE(batch->array->n_children==(count<0?4:count));
        for(int64_t row=0;row<batch->num_rows;++row){
            int64_t position=expected;
            if(ngroups>=0){
                while(group_offset==info->row_groups[groups[group_index]].num_rows){++group_index;group_offset=0;}
                position=info->row_groups[groups[group_index]].first_row+group_offset++;
            }
            REQUIRE(batch->original_positions[row]==position);
            for(int field=0;field<batch->array->n_children;++field){
                int original=count<0?field:(int)((ids[field]-11)/10);
                verify_value(batch->array->children[field],batch->schema->children[field],row,original,position);
            }
            ++expected;
        }
        api->batch_release(&batch);REQUIRE(batch==NULL);api->batch_release(&batch);
    }
    int64_t population=info->num_rows;
    if(ngroups>=0){population=0;for(int n=0;n<ngroups;++n)population+=info->row_groups[groups[n]].num_rows;}
    REQUIRE(expected==population);
    OgLakeSdkIoStats stats;api->reader_stats(reader,&stats);
    REQUIRE(stats.read_calls>0&&stats.bytes_read>0&&stats.peak_pool_bytes<=64*1024*1024);
    REQUIRE(stats.decoded_rows==(count==0?0:(uint64_t)population));
    api->reader_close(&reader);REQUIRE(reader==NULL);api->reader_close(&reader);
    return stats.bytes_read;
}
static int cancelled(void* context){return *(int*)context;}
int main(int argc,char** argv)
{
    REQUIRE(argc==7);
    void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);
    if(!library)fprintf(stderr,"%s\n",dlerror());
    REQUIRE(library!=NULL);
    OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(library,"og_lake_parquet_sdk_v1");
    REQUIRE(entry!=NULL);api=entry();REQUIRE(api->abi_version==1&&api->struct_size==sizeof(*api));
    REQUIRE((api->capabilities&OG_LAKE_SDK_ORIGINAL_POSITIONS)!=0);
    OgLakeSdkLimits budget=limits();OgLakeSdkError error;OgLakeSdkFooter* footer=NULL;
    ok(api->footer_open(argv[2],&budget,&footer,&error),&error);
    const OgLakeSdkFooterInfo* info=api->footer_info(footer);
    REQUIRE(info->num_fields==4&&info->num_rows==12288&&info->num_row_groups>=6);
    for(int n=0;n<4;++n)REQUIRE(info->fields[n].field_id==11+10*n);
    uint64_t all=scan(argv[2],NULL,-1,NULL,-1,info);
    int64_t id_only[]={11},reordered[]={41,11,21,31};
    uint64_t projected=scan(argv[2],id_only,1,NULL,-1,info);
    REQUIRE(projected<all);
    int32_t groups[]={info->num_row_groups-1,0,2};
    scan(argv[2],reordered,4,groups,3,info);
    uint64_t metadata_only=scan(argv[2],NULL,0,NULL,-1,info);REQUIRE(metadata_only<all);
    scan(argv[2],NULL,-1,NULL,0,info);
    /* Releasing a batch after closing its reader must keep the pool alive. */
    OgLakeSdkReadOptions config=options(argv[2]);OgLakeSdkReader* reader=NULL;OgLakeSdkBatch* batch=NULL;
    ok(api->reader_open(&config,&reader,&error),&error);ok(api->reader_next(reader,&batch,&error),&error);
    OgLakeSdkBatch* rejected=NULL;
    REQUIRE(api->reader_next(reader,&rejected,&error)==OG_LAKE_SDK_STATE&&rejected==NULL);
    api->reader_close(&reader);REQUIRE(batch->num_rows>0);
    verify_value(batch->array->children[0],batch->schema->children[0],1,0,1);
    api->batch_release(&batch);api->batch_release(NULL);
    /* A successful admission followed by cancellation is an explicit error. */
    int cancel=0;config=options(argv[2]);config.limits.should_cancel=cancelled;config.limits.cancel_context=&cancel;
    ok(api->reader_open(&config,&reader,&error),&error);cancel=1;
    REQUIRE(api->reader_next(reader,&batch,&error)==OG_LAKE_SDK_CANCELLED&&batch==NULL);
    REQUIRE(api->reader_next(reader,&batch,&error)==OG_LAKE_SDK_STATE);api->reader_close(&reader);
    cancel=1;REQUIRE(api->reader_open(&config,&reader,&error)==OG_LAKE_SDK_CANCELLED&&reader==NULL);
    config=options(argv[2]);int64_t absent[]={999};config.num_field_ids=1;config.field_ids=absent;
    REQUIRE(api->reader_open(&config,&reader,&error)==OG_LAKE_SDK_UNSUPPORTED&&reader==NULL);
    int64_t duplicate[]={11,11};config.num_field_ids=2;config.field_ids=duplicate;
    REQUIRE(api->reader_open(&config,&reader,&error)==OG_LAKE_SDK_INVALID&&reader==NULL);
    config=options(argv[3]);REQUIRE(api->reader_open(&config,&reader,&error)==OG_LAKE_SDK_UNSUPPORTED&&reader==NULL);
    config.projection_kind=OG_LAKE_SDK_BY_ORDINAL;
    ok(api->reader_open(&config,&reader,&error),&error);api->reader_close(&reader);
    OgLakeSdkFooter* invalid=NULL;
    REQUIRE(api->footer_open(argv[4],&budget,&invalid,&error)!=OG_LAKE_SDK_OK&&invalid==NULL);
    api->footer_close(&footer);api->footer_close(&footer);api->footer_close(NULL);
    footer=NULL;REQUIRE(api->footer_open(argv[5],&budget,&footer,&error)!=OG_LAKE_SDK_OK&&footer==NULL);
    REQUIRE(api->footer_open(argv[6],&budget,&footer,&error)==OG_LAKE_SDK_IO&&footer==NULL);
    config=options(argv[2]);config.batch_rows=12288;config.limits.memory_bytes=65536;config.limits.footer_bytes=65536;
    int code=api->reader_open(&config,&reader,&error);
    if(code==OG_LAKE_SDK_OK)code=api->reader_next(reader,&batch,&error);
    REQUIRE(code==OG_LAKE_SDK_MEMORY&&batch==NULL);api->reader_close(&reader);
    printf("{\"status\":\"passed\",\"assertions\":%lu,\"all_bytes\":%" PRIu64 ",\"projected_bytes\":%" PRIu64 ",\"metadata_only_bytes\":%" PRIu64 "}\n",checks,all,projected,metadata_only);
    REQUIRE(dlclose(library)==0);return 0;
}
