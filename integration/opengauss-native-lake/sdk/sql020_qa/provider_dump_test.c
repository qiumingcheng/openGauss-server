#define main standalone_reader_suite
#include "provider_reader_test.c"
#undef main

static void quoted(const char* data,int64_t count)
{
    putchar('"');
    for(int64_t n=0;n<count;++n){
        unsigned char value=(unsigned char)data[n];
        if(value=='"'||value=='\\'){putchar('\\');putchar(value);}
        else if(value<32)printf("\\u%04x",value);
        else putchar(value);
    }
    putchar('"');
}
static void value(const struct ArrowArray* column,const struct ArrowSchema* schema,int64_t row)
{
    if(!valid(column,row)){fputs("null",stdout);return;}
    int64_t index=row+column->offset;
    if(strcmp(schema->format,"b")==0)
        fputs(((const unsigned char*)column->buffers[1])[index/8]>>(index%8)&1?"true":"false",stdout);
    else if(strcmp(schema->format,"i")==0)printf("%" PRId32,((const int32_t*)column->buffers[1])[index]);
    else if(strcmp(schema->format,"l")==0)printf("%" PRId64,((const int64_t*)column->buffers[1])[index]);
    else{
        int64_t start,end;
        if(strcmp(schema->format,"u")==0){start=((const int32_t*)column->buffers[1])[index];end=((const int32_t*)column->buffers[1])[index+1];}
        else{REQUIRE(strcmp(schema->format,"U")==0);start=((const int64_t*)column->buffers[1])[index];end=((const int64_t*)column->buffers[1])[index+1];}
        quoted((const char*)column->buffers[2]+start,end-start);
    }
}
int main(int argc,char** argv)
{
    REQUIRE(argc==3);void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);REQUIRE(library!=NULL);
    OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(library,"og_lake_parquet_sdk_v1");
    REQUIRE(entry!=NULL);api=entry();OgLakeSdkFooter* footer=NULL;OgLakeSdkLimits budget=limits();OgLakeSdkError error;
    ok(api->footer_open(argv[2],&budget,&footer,&error),&error);const OgLakeSdkFooterInfo* info=api->footer_info(footer);
    fputs("{\"fields\":[",stdout);
    for(int n=0;n<info->num_fields;++n){
        if(n)putchar(',');
        printf("{\"id\":%" PRId64 ",\"type\":%u,\"name\":",info->fields[n].field_id,info->fields[n].type);
        quoted(info->fields[n].name,(int64_t)strlen(info->fields[n].name));putchar('}');
    }
    printf("],\"num_rows\":%" PRId64 ",\"rows\":[",info->num_rows);
    OgLakeSdkReadOptions config=options(argv[2]);config.batch_rows=2;
    OgLakeSdkReader* reader=NULL;OgLakeSdkBatch* batch=NULL;int64_t position=0;
    ok(api->reader_open(&config,&reader,&error),&error);
    for(;;){
        int result=api->reader_next(reader,&batch,&error);if(result==OG_LAKE_SDK_EOF)break;
        ok(result,&error);
        for(int64_t row=0;row<batch->num_rows;++row){
            REQUIRE(batch->original_positions[row]==position);
            if(position++)putchar(',');
            putchar('[');
            for(int n=0;n<batch->array->n_children;++n){if(n)putchar(',');value(batch->array->children[n],batch->schema->children[n],row);}
            putchar(']');
        }
        api->batch_release(&batch);
    }
    REQUIRE(position==info->num_rows);api->reader_close(&reader);api->footer_close(&footer);
    printf("],\"assertions\":%lu}\n",checks);REQUIRE(dlclose(library)==0);return 0;
}
