#define main standalone_reader_suite
#include "provider_reader_test.c"
#undef main
int main(int argc,char** argv)
{
    REQUIRE(argc==5);void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);REQUIRE(library!=NULL);
    OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(library,"og_lake_parquet_sdk_v1");
    REQUIRE(entry!=NULL);api=entry();int codec=atoi(argv[4]);REQUIRE(codec>=0&&codec<=4);
    OgLakeSdkError error;OgLakeSdkFooter* footer=NULL;OgLakeSdkLimits budget=limits();
    ok(api->footer_open(argv[2],&budget,&footer,&error),&error);
    const OgLakeSdkFooterInfo* info=api->footer_info(footer);
    OgLakeSdkWriteOptions config={0};config.struct_size=sizeof(config);config.path=argv[3];config.limits=limits();
    config.num_fields=info->num_fields;config.fields=info->fields;config.row_group_rows=2048;config.compression=(uint32_t)codec;
    OgLakeSdkWriter* writer=NULL;ok(api->writer_open(&config,&writer,&error),&error);
    OgLakeSdkReadOptions input=options(argv[2]);OgLakeSdkReader* reader=NULL;OgLakeSdkBatch* batch=NULL;
    ok(api->reader_open(&input,&reader,&error),&error);
    for(;;){
        int status=api->reader_next(reader,&batch,&error);if(status==OG_LAKE_SDK_EOF)break;
        ok(status,&error);ok(api->writer_append(writer,batch->array,batch->schema,&error),&error);api->batch_release(&batch);
    }
    api->reader_close(&reader);OgLakeSdkWrittenFile written={sizeof(written),0,0,0};
    ok(api->writer_finish(writer,&written,&error),&error);REQUIRE(written.num_rows==12288&&written.local_fsync_complete==1);
    ok(api->writer_close(&writer,&error),&error);api->footer_close(&footer);
    ok(api->footer_open(argv[3],&budget,&footer,&error),&error);
    scan(argv[3],NULL,-1,NULL,-1,api->footer_info(footer));api->footer_close(&footer);
    printf("{\"status\":\"passed\",\"codec\":%d,\"assertions\":%lu}\n",codec,checks);
    REQUIRE(dlclose(library)==0);return 0;
}
