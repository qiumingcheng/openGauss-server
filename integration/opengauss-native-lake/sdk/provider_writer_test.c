/* Reuse the independent C Arrow value decoder, never provider internals. */
#define main standalone_reader_suite
#include "provider_reader_test.c"
#undef main
#include <fcntl.h>
#include <sys/stat.h>

static OgLakeSdkWriteOptions write_options(const char* path,const OgLakeSdkFooterInfo* info)
{
    OgLakeSdkWriteOptions config={0};config.struct_size=sizeof(config);config.path=path;
    config.limits=limits();config.num_fields=info->num_fields;config.fields=info->fields;
    config.compression=OG_LAKE_SDK_ZSTD;config.row_group_rows=2048;return config;
}
static void copy_rows(const char* input,OgLakeSdkWriter* writer)
{
    OgLakeSdkError error;OgLakeSdkReadOptions config=options(input);
    OgLakeSdkReader* reader=NULL;OgLakeSdkBatch* batch=NULL;
    ok(api->reader_open(&config,&reader,&error),&error);
    for(;;){
        int result=api->reader_next(reader,&batch,&error);
        if(result==OG_LAKE_SDK_EOF)break;
        ok(result,&error);
        void (*array_release)(struct ArrowArray*)=batch->array->release;
        void (*schema_release)(struct ArrowSchema*)=batch->schema->release;
        ok(api->writer_append(writer,batch->array,batch->schema,&error),&error);
        REQUIRE(batch->array->release==array_release&&batch->schema->release==schema_release);
        api->batch_release(&batch);
    }
    api->reader_close(&reader);
}
int main(int argc,char** argv)
{
    REQUIRE(argc==8);
    void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);
    if(!library)fprintf(stderr,"%s\n",dlerror());
    REQUIRE(library!=NULL);
    OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(library,"og_lake_parquet_sdk_v1");
    REQUIRE(entry!=NULL);api=entry();REQUIRE((api->capabilities&OG_LAKE_SDK_WRITER)!=0);
    OgLakeSdkError error;OgLakeSdkLimits budget=limits();OgLakeSdkFooter* footer=NULL;
    ok(api->footer_open(argv[2],&budget,&footer,&error),&error);
    const OgLakeSdkFooterInfo* info=api->footer_info(footer);
    OgLakeSdkWriteOptions config=write_options(argv[3],info);OgLakeSdkWriter* writer=NULL;
    ok(api->writer_open(&config,&writer,&error),&error);copy_rows(argv[2],writer);
    OgLakeSdkWrittenFile result={sizeof(result),0,0,0};
    ok(api->writer_finish(writer,&result,&error),&error);
    REQUIRE(result.local_fsync_complete==1&&result.num_rows==12288&&result.file_bytes>12);
    struct stat output;REQUIRE(stat(argv[3],&output)==0&&output.st_size==result.file_bytes);
    REQUIRE(api->writer_finish(writer,&result,&error)==OG_LAKE_SDK_STATE&&result.local_fsync_complete==0);
    ok(api->writer_close(&writer,&error),&error);REQUIRE(writer==NULL);
    ok(api->writer_close(&writer,&error),&error);ok(api->writer_close(NULL,&error),&error);
    OgLakeSdkFooter* written=NULL;ok(api->footer_open(argv[3],&budget,&written,&error),&error);
    const OgLakeSdkFooterInfo* written_info=api->footer_info(written);
    REQUIRE(written_info->num_rows==info->num_rows&&written_info->num_fields==4);
    for(int field=0;field<4;++field)REQUIRE(written_info->fields[field].field_id==info->fields[field].field_id);
    scan(argv[3],NULL,-1,NULL,-1,written_info);api->footer_close(&written);
    /* O_EXCL must leave existing input bytes untouched. */
    config=write_options(argv[2],info);
    REQUIRE(api->writer_open(&config,&writer,&error)==OG_LAKE_SDK_IO&&writer==NULL);
    config=write_options(argv[4],info);ok(api->writer_open(&config,&writer,&error),&error);
    copy_rows(argv[2],writer);ok(api->writer_close(&writer,&error),&error);
    REQUIRE(access(argv[4],F_OK)!=0);
    /* Cancellation poisons the writer; close still cleans its own candidate. */
    int cancel=0;config=write_options(argv[5],info);
    config.limits.should_cancel=cancelled;config.limits.cancel_context=&cancel;
    ok(api->writer_open(&config,&writer,&error),&error);cancel=1;
    REQUIRE(api->writer_append(writer,NULL,NULL,&error)==OG_LAKE_SDK_CANCELLED);
    REQUIRE(api->writer_finish(writer,&result,&error)==OG_LAKE_SDK_STATE);
    ok(api->writer_close(&writer,&error),&error);REQUIRE(access(argv[5],F_OK)!=0);
    /* An external replacement is preserved and reported, never unlinked. */
    config=write_options(argv[6],info);ok(api->writer_open(&config,&writer,&error),&error);
    REQUIRE(unlink(argv[6])==0);
    int fd=open(argv[6],O_WRONLY|O_CREAT|O_EXCL,0600);REQUIRE(fd>=0);
    REQUIRE(write(fd,"replacement",11)==11);REQUIRE(close(fd)==0);
    REQUIRE(api->writer_close(&writer,&error)==OG_LAKE_SDK_IO&&writer==NULL);
    REQUIRE(stat(argv[6],&output)==0&&output.st_size==11);
    config=write_options(argv[7],info);ok(api->writer_open(&config,&writer,&error),&error);
    result.struct_size=sizeof(result);ok(api->writer_finish(writer,&result,&error),&error);
    REQUIRE(result.num_rows==0&&result.local_fsync_complete==1);ok(api->writer_close(&writer,&error),&error);
    ok(api->footer_open(argv[7],&budget,&written,&error),&error);
    REQUIRE(api->footer_info(written)->num_rows==0);api->footer_close(&written);
    api->footer_close(&footer);
    printf("{\"status\":\"passed\",\"assertions\":%lu}\n",checks);
    REQUIRE(dlclose(library)==0);return 0;
}
