#define _POSIX_C_SOURCE 200809L
#include "lake_parquet_sdk.h"
#include <dlfcn.h>
#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#define REQUIRE(x) do { if(!(x)){fprintf(stderr,"failed: %s, line %d\n",#x,__LINE__);return 1;} } while(0)
static const OgLakeParquetSdkApi* api;
static int cancelled(void* data){return *(int*)data;}
static int descriptors(void)
{
    DIR* directory=opendir("/proc/self/fd");if(!directory)return -1;
    int count=0;struct dirent* item;
    while((item=readdir(directory))!=NULL)if(item->d_name[0]!='.')++count;
    closedir(directory);return count;
}

static int copy(const char* source,OgLakeSdkWriter* writer)
{
    OgLakeSdkReadOptions options={0};options.struct_size=sizeof(options);options.path=source;
    options.limits=(OgLakeSdkLimits){sizeof(OgLakeSdkLimits),64*1024*1024,16*1024*1024,NULL,NULL};
    options.batch_rows=4096;options.num_field_ids=-1;options.num_row_groups=-1;
    OgLakeSdkReader* reader=NULL;OgLakeSdkError error;
    REQUIRE(api->reader_open(&options,&reader,&error)==0);
    for(;;){
        OgLakeSdkBatch* batch=NULL;int status=api->reader_next(reader,&batch,&error);
        if(status==OG_LAKE_SDK_EOF)break;
        REQUIRE(status==0&&batch!=NULL&&api->writer_append(writer,batch->array,batch->schema,&error)==0);
        api->batch_release(&batch);
    }
    api->reader_close(&reader);return 0;
}
int main(int argc,char** argv)
{
    REQUIRE(argc==4);void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);REQUIRE(library!=NULL);
    OgLakeParquetSdkEntrypoint get=(OgLakeParquetSdkEntrypoint)dlsym(library,"og_lake_parquet_sdk_v1");
    OgLakeParquetLifecycleEntrypoint get2=(OgLakeParquetLifecycleEntrypoint)dlsym(library,"og_lake_parquet_sdk_v2");
    REQUIRE(get!=NULL&&get2!=NULL);api=get();const OgLakeParquetLifecycleApi* life=get2();
    REQUIRE(api->abi_version==1&&life->abi_version==2&&life->struct_size>=sizeof(*life)&&life->base==api&&
            (life->capabilities&OG_LAKE_SDK_ABORT_FINISHED)&&life->writer_abort!=NULL);
    OgLakeSdkLimits limits={sizeof(limits),64*1024*1024,16*1024*1024,NULL,NULL};OgLakeSdkError error;
    OgLakeSdkFooter* footer=NULL;REQUIRE(api->footer_open(argv[2],&limits,&footer,&error)==0);
    const OgLakeSdkFooterInfo* info=api->footer_info(footer);
    const char* modes[]={"close-retains","abort-finished","abort-unfinished","replace-finished","abort-fsync-error","abort-cancel-pending"};
    int baseline=descriptors();REQUIRE(baseline>=0);
    for(int i=0;i<6;++i){
        int cancel=0;
        char path[4096];int length=snprintf(path,sizeof(path),"%s/%s.parquet",argv[3],modes[i]);
        REQUIRE(length>0&&length<(int)sizeof(path));
        OgLakeSdkWriteOptions options={0};options.struct_size=sizeof(options);options.path=path;options.limits=limits;
        options.limits.should_cancel=cancelled;options.limits.cancel_context=&cancel;
        options.num_fields=info->num_fields;options.fields=info->fields;options.compression=OG_LAKE_SDK_ZSTD;options.row_group_rows=4096;
        OgLakeSdkWriter* writer=NULL;REQUIRE(api->writer_open(&options,&writer,&error)==0);
        REQUIRE(copy(argv[2],writer)==0);
        if(i!=2){
            OgLakeSdkWrittenFile written={sizeof(written),0,0,0};
            REQUIRE(api->writer_finish(writer,&written,&error)==0&&written.local_fsync_complete==1&&written.num_rows==info->num_rows);
        }
        if(i==0){
            REQUIRE(api->writer_close(&writer,&error)==0&&writer==NULL&&access(path,F_OK)==0);
        }else if(i==4){
            REQUIRE(setenv("OG_LAKE_TEST_FAULT_PATH",path,1)==0);
            REQUIRE(setenv("OG_LAKE_TEST_FAULT_MODE","directory_fsync",1)==0);
            REQUIRE(life->writer_abort(&writer,&error)==OG_LAKE_SDK_IO&&writer==NULL);
            REQUIRE(access(path,F_OK)!=0);
            REQUIRE(unsetenv("OG_LAKE_TEST_FAULT_PATH")==0&&unsetenv("OG_LAKE_TEST_FAULT_MODE")==0);
        }else if(i==3){
            REQUIRE(unlink(path)==0);int fd=open(path,O_WRONLY|O_CREAT|O_EXCL,0600);
            REQUIRE(fd>=0&&write(fd,"replacement",11)==11&&close(fd)==0);
            REQUIRE(life->writer_abort(&writer,&error)==OG_LAKE_SDK_IO&&writer==NULL);
            struct stat found;REQUIRE(stat(path,&found)==0&&found.st_size==11);
            char data[11];fd=open(path,O_RDONLY);REQUIRE(fd>=0&&read(fd,data,11)==11&&close(fd)==0&&memcmp(data,"replacement",11)==0);
        }else{
            if(i==5)cancel=1;
            REQUIRE(life->writer_abort(&writer,&error)==0&&writer==NULL);
            REQUIRE(access(path,F_OK)!=0);
            REQUIRE(life->writer_abort(&writer,&error)==0&&life->writer_abort(NULL,&error)==0);
        }
        REQUIRE(descriptors()==baseline);
        printf("{\"mode\":\"%s\",\"status\":\"pass\",\"fds\":%d}\n",modes[i],baseline);
    }
    api->footer_close(&footer);dlclose(library);return 0;
}
