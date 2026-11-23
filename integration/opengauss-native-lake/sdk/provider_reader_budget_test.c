/* Regression for migrated SDK54's missing bounded row-group retry.
 * Fixture: 320 rows, id=i, payload="row-%06d-" + (i<65 ? "" : 65536
 * copies of 'A'+i%26), field IDs 11/21. A 65-row initial batch is intentional:
 * after memory pressure halves it, the delivered prefix is not divisible by 32.
 */
#include "lake_parquet_sdk.h"
#include <arrow/c/abi.h>
#include <dlfcn.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const OgLakeParquetSdkApi* api;
static unsigned long checks;
static int cancelled;
#define REQUIRE(x) do { ++checks; if (!(x)) { \
    fprintf(stderr,"check failed at %s:%d: %s\n",__FILE__,__LINE__,#x); exit(1); } } while(0)
static int cancel(void* ignored) { (void)ignored; return cancelled; }
static OgLakeSdkReadOptions options(const char* path, int64_t bytes)
{
    OgLakeSdkReadOptions o={0};
    o.struct_size=sizeof(o);o.path=path;o.batch_rows=65;
    o.limits=(OgLakeSdkLimits){sizeof(OgLakeSdkLimits),bytes,65536,cancel,NULL};
    o.num_field_ids=-1;o.num_row_groups=-1;
    return o;
}
static void good(int status, const OgLakeSdkError* error)
{
    if(status!=OG_LAKE_SDK_OK)fprintf(stderr,"SDK error %d: %s\n",status,error->message);
    REQUIRE(status==OG_LAKE_SDK_OK);
}
static void value(const OgLakeSdkBatch* b, int64_t row, int64_t id)
{
    REQUIRE(b->original_positions[row]==id);
    const struct ArrowArray* a=b->array->children[0];
    REQUIRE(strcmp(b->schema->children[0]->format,"l")==0);
    REQUIRE(((const int64_t*)a->buffers[1])[a->offset+row]==id);
    a=b->array->children[1];int64_t start,end,index=a->offset+row;
    if(strcmp(b->schema->children[1]->format,"u")==0){
        start=((const int32_t*)a->buffers[1])[index];end=((const int32_t*)a->buffers[1])[index+1];
    }else{
        REQUIRE(strcmp(b->schema->children[1]->format,"U")==0);
        start=((const int64_t*)a->buffers[1])[index];end=((const int64_t*)a->buffers[1])[index+1];
    }
    char prefix[32];int n=snprintf(prefix,sizeof(prefix),"row-%06" PRId64 "-",id);
    REQUIRE(end-start==n+(id<65?0:65536));
    const char* text=(const char*)a->buffers[2]+start;
    REQUIRE(memcmp(text,prefix,(size_t)n)==0);
    int correct=1;
    for(int k=n;k<end-start;++k)if(text[k]!='A'+id%26)correct=0;
    REQUIRE(correct);
}
int main(int argc,char** argv)
{
    REQUIRE(argc==3);
    void* so=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);
    if(!so)fprintf(stderr,"dlopen: %s\n",dlerror());
    REQUIRE(so!=NULL);
    const OgLakeParquetSdkApi* (*entry)(void)=dlsym(so,"og_lake_parquet_sdk_v1");
    REQUIRE(entry!=NULL);api=entry();
    OgLakeSdkReadOptions o=options(argv[2],8*1024*1024);
    OgLakeSdkReader* r=NULL;OgLakeSdkBatch* b=NULL;OgLakeSdkError e;
    good(api->reader_open(&o,&r,&e),&e);
    int64_t seen=0;int batches=0,short_after_prefix=0;
    for(;;){
        int status=api->reader_next(r,&b,&e);
        if(status==OG_LAKE_SDK_EOF){REQUIRE(b==NULL);break;}
        if(status!=OG_LAKE_SDK_OK)fprintf(stderr,"delivered before failure: %" PRId64 "\n",seen);
        good(status,&e);REQUIRE(b&&b->num_rows>0&&b->num_rows<=65);
        if(batches==0)REQUIRE(b->num_rows==65);
        if(seen==65&&b->num_rows<65)short_after_prefix=1;
        REQUIRE(b->array->n_children==2);
        for(int64_t row=0;row<b->num_rows;++row)value(b,row,seen+row);
        if(batches==0){
            OgLakeSdkBatch* blocked=NULL;
            REQUIRE(api->reader_next(r,&blocked,&e)==OG_LAKE_SDK_STATE&&blocked==NULL);
        }
        seen+=b->num_rows;++batches;api->batch_release(&b);REQUIRE(b==NULL);
    }
    OgLakeSdkIoStats stats={0};api->reader_stats(r,&stats);
    REQUIRE(seen==320&&short_after_prefix);
    REQUIRE(stats.decoded_rows==320&&stats.peak_pool_bytes<=o.limits.memory_bytes);
    api->reader_close(&r);REQUIRE(r==NULL);

    /* Cancellation must remain terminal; it must never trigger memory retry. */
    good(api->reader_open(&o,&r,&e),&e);good(api->reader_next(r,&b,&e),&e);
    api->batch_release(&b);cancelled=1;
    REQUIRE(api->reader_next(r,&b,&e)==OG_LAKE_SDK_CANCELLED&&b==NULL);
    REQUIRE(api->reader_next(r,&b,&e)==OG_LAKE_SDK_STATE&&b==NULL);
    cancelled=0;api->reader_close(&r);

    /* Even a single wide value cannot fit in this budget: retain OOM. */
    o=options(argv[2],65536);o.batch_rows=1;
    int status=api->reader_open(&o,&r,&e);
    if(status==OG_LAKE_SDK_OK){
        do{
            status=api->reader_next(r,&b,&e);
            if(status==OG_LAKE_SDK_OK)api->batch_release(&b);
        }while(status==OG_LAKE_SDK_OK);
        REQUIRE(status==OG_LAKE_SDK_MEMORY&&b==NULL);
        REQUIRE(api->reader_next(r,&b,&e)==OG_LAKE_SDK_STATE&&b==NULL);
    }else REQUIRE(status==OG_LAKE_SDK_MEMORY);
    api->reader_close(&r);
    printf("{\"status\":\"passed\",\"rows\":%" PRId64 ",\"batches\":%d,\"peak_pool_bytes\":%" PRIu64 ",\"checks\":%lu}\n",
           seen,batches,(uint64_t)stats.peak_pool_bytes,checks);
    REQUIRE(dlclose(so)==0);return 0;
}
