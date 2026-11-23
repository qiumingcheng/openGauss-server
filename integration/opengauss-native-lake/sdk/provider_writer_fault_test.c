#define _POSIX_C_SOURCE 200809L
#define main standalone_reader_suite
#include "provider_reader_test.c"
#undef main
#include <fcntl.h>
#include <sys/stat.h>

static unsigned releases;
static void array_done(struct ArrowArray* array)
{
    for(int64_t n=0;n<array->n_children;++n)if(array->children[n]->release)array->children[n]->release(array->children[n]);
    array->release=NULL;++releases;
}
static void schema_done(struct ArrowSchema* schema)
{
    for(int64_t n=0;n<schema->n_children;++n)if(schema->children[n]->release)schema->children[n]->release(schema->children[n]);
    schema->release=NULL;++releases;
}
static OgLakeSdkWriteOptions configuration(const char* path,const OgLakeSdkFooterInfo* info)
{
    OgLakeSdkWriteOptions result={0};result.struct_size=sizeof(result);result.path=path;
    result.limits=limits();result.fields=info->fields;result.num_fields=4;
    result.compression=OG_LAKE_SDK_ZSTD;result.row_group_rows=2048;return result;
}
static void join(char* result,size_t size,const char* directory,const char* name)
{
    int length=snprintf(result,size,"%s/%s.parquet",directory,name);REQUIRE(length>0&&(size_t)length<size);
}
static void borrowed_input(OgLakeSdkWriter* writer,const OgLakeSdkFooterInfo* info)
{
    int32_t ids[]={0,1,2};int64_t big[]={INT64_MIN,INT64_MAX,0};
    unsigned char flags=1,flag_valid=5,big_valid=3,text_valid=3;
    char text[]="湖/borrow";int32_t offsets[]={0,sizeof(text)-1,sizeof(text)-1,sizeof(text)-1};
    const void* root_buffers[]={NULL};
    const void* buffers[4][3]={{NULL,ids,NULL},{&flag_valid,&flags,NULL},
                             {&big_valid,big,NULL},{&text_valid,offsets,text}};
    struct ArrowArray columns[4]={{0}};struct ArrowArray* pointers[4];
    struct ArrowSchema fields[4]={{0}};struct ArrowSchema* field_pointers[4];
    const char* names[]={"id","flag","big","txt"};const char* formats[]={"i","b","l","u"};
    for(int n=0;n<4;++n){
        columns[n].length=3;columns[n].null_count=n?1:0;columns[n].n_buffers=n==3?3:2;
        columns[n].buffers=buffers[n];columns[n].release=array_done;pointers[n]=&columns[n];
        fields[n].name=names[n];fields[n].format=formats[n];fields[n].flags=info->fields[n].nullable?ARROW_FLAG_NULLABLE:0;
        fields[n].release=schema_done;field_pointers[n]=&fields[n];
    }
    struct ArrowArray root={0};root.length=3;root.n_buffers=1;root.buffers=root_buffers;
    root.n_children=4;root.children=pointers;root.release=array_done;
    struct ArrowSchema schema={0};schema.format="+s";schema.n_children=4;schema.children=field_pointers;schema.release=schema_done;
    OgLakeSdkError error;ok(api->writer_append(writer,&root,&schema,&error),&error);
    REQUIRE(root.release==array_done&&schema.release==schema_done&&releases==0);
    for(int n=0;n<4;++n)REQUIRE(columns[n].release==array_done&&fields[n].release==schema_done);
    root.release(&root);schema.release(&schema);REQUIRE(releases==10);
    /* Inputs go out of scope before Finish; overwrite all payloads first. */
    memset(ids,0xee,sizeof(ids));memset(big,0xee,sizeof(big));memset(text,'X',sizeof(text));
    flags=flag_valid=big_valid=text_valid=0;memset(offsets,0,sizeof(offsets));
}
int main(int argc,char** argv)
{
    REQUIRE(argc==4);void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);REQUIRE(library!=NULL);
    OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(library,"og_lake_parquet_sdk_v1");
    REQUIRE(entry!=NULL);api=entry();OgLakeSdkLimits budget=limits();OgLakeSdkFooter* footer=NULL;OgLakeSdkError error;
    ok(api->footer_open(argv[2],&budget,&footer,&error),&error);const OgLakeSdkFooterInfo* info=api->footer_info(footer);
    OgLakeSdkReadOptions read_options=options(argv[2]);OgLakeSdkReader* reader=NULL;OgLakeSdkBatch* batch=NULL;
    ok(api->reader_open(&read_options,&reader,&error),&error);ok(api->reader_next(reader,&batch,&error),&error);
    const char* modes[]={"file_fsync","directory_fsync","close","partial_write"};
    for(int n=0;n<4;++n){
        char path[4096];join(path,sizeof(path),argv[3],modes[n]);
        OgLakeSdkWriteOptions config=configuration(path,info);OgLakeSdkWriter* writer=NULL;
        ok(api->writer_open(&config,&writer,&error),&error);
        if(n!=3)ok(api->writer_append(writer,batch->array,batch->schema,&error),&error);
        REQUIRE(setenv("OG_LAKE_TEST_FAULT_PATH",path,1)==0&&setenv("OG_LAKE_TEST_FAULT_MODE",modes[n],1)==0);
        OgLakeSdkWrittenFile result={sizeof(result),0,0,0};
        if(n==3){
            REQUIRE(api->writer_append(writer,batch->array,batch->schema,&error)==OG_LAKE_SDK_IO);
            struct stat partial;REQUIRE(stat(path,&partial)==0&&partial.st_size>4);
            REQUIRE(api->writer_finish(writer,&result,&error)==OG_LAKE_SDK_STATE);
        }else REQUIRE(api->writer_finish(writer,&result,&error)==OG_LAKE_SDK_IO);
        REQUIRE(result.local_fsync_complete==0&&result.num_rows==0&&result.file_bytes==0);
        /* Directory fsync also fails during discard: report it to the caller. */
        int closed=api->writer_close(&writer,&error);
        REQUIRE(closed==(n==1?OG_LAKE_SDK_IO:OG_LAKE_SDK_OK)&&writer==NULL);
        REQUIRE(unsetenv("OG_LAKE_TEST_FAULT_MODE")==0&&unsetenv("OG_LAKE_TEST_FAULT_PATH")==0);
        REQUIRE(access(path,F_OK)!=0);
    }
    api->batch_release(&batch);api->reader_close(&reader);
    char small[4096];join(small,sizeof(small),argv[3],"borrowed");
    OgLakeSdkWriteOptions config=configuration(small,info);OgLakeSdkWriter* writer=NULL;
    ok(api->writer_open(&config,&writer,&error),&error);borrowed_input(writer,info);
    OgLakeSdkWrittenFile result={sizeof(result),0,0,0};ok(api->writer_finish(writer,&result,&error),&error);
    REQUIRE(result.num_rows==3&&result.local_fsync_complete==1);ok(api->writer_close(&writer,&error),&error);
    api->footer_close(&footer);
    printf("{\"status\":\"passed\",\"assertions\":%lu,\"fault_modes\":4,\"caller_releases\":%u}\n",checks,releases);
    REQUIRE(dlclose(library)==0);return 0;
}
