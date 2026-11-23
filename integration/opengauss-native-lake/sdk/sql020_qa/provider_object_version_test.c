/* Independent C ABI consumer. Version identities are opaque, not hashes. */
#define _POSIX_C_SOURCE 200809L
#include "lake_parquet_sdk.h"
#include <dlfcn.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static unsigned checks;
#define REQUIRE(x) do { ++checks; if (!(x)) { \
    fprintf(stderr,"check failed at %s:%d: %s\n",__FILE__,__LINE__,#x); exit(1); } } while(0)
static const OgLakeParquetSdkApi* api;
static OgLakeSdkLimits limits(void)
{
    OgLakeSdkLimits value={sizeof(value),64*1024*1024,16*1024*1024,NULL,NULL};return value;
}
static char* version(const char* path)
{
    OgLakeSdkFooter* footer=NULL;OgLakeSdkError error;OgLakeSdkLimits budget=limits();
    REQUIRE(api->footer_open(path,&budget,&footer,&error)==OG_LAKE_SDK_OK);
    const OgLakeSdkFooterInfo* info=api->footer_info(footer);
    REQUIRE(info&&info->struct_size>=offsetof(OgLakeSdkFooterInfo,object_version)+sizeof(info->object_version)&&info->object_version&&*info->object_version);
    char* value=strdup(info->object_version);REQUIRE(value!=NULL);
    api->footer_close(&footer);REQUIRE(footer==NULL);return value;
}
static OgLakeSdkReader* reader(const char* path)
{
    OgLakeSdkReadOptions options={0};options.struct_size=sizeof(options);options.path=path;
    options.limits=limits();options.batch_rows=137;options.num_field_ids=-1;options.num_row_groups=-1;
    OgLakeSdkReader* value=NULL;OgLakeSdkError error;
    REQUIRE(api->reader_open(&options,&value,&error)==OG_LAKE_SDK_OK);return value;
}
static void batch_version(OgLakeSdkReader** input,const char* expected)
{
    OgLakeSdkBatch* batch=NULL;OgLakeSdkError error;
    REQUIRE(api->reader_next(*input,&batch,&error)==OG_LAKE_SDK_OK);
    REQUIRE(batch&&batch->num_rows>0&&batch->struct_size>=sizeof(*batch));
    REQUIRE(batch->object_version&&strcmp(batch->object_version,expected)==0);
    api->reader_close(input);REQUIRE(*input==NULL);
    /* The suffix belongs to the outstanding batch, not the closed reader. */
    REQUIRE(strcmp(batch->object_version,expected)==0);
    api->batch_release(&batch);REQUIRE(batch==NULL);
}
int main(int argc,char** argv)
{
    REQUIRE(argc==5);void* library=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);
    if(!library)fprintf(stderr,"%s\n",dlerror());REQUIRE(library!=NULL);
    OgLakeParquetSdkEntrypoint entry=(OgLakeParquetSdkEntrypoint)dlsym(library,"og_lake_parquet_sdk_v1");
    REQUIRE(entry!=NULL);api=entry();REQUIRE(api&&api->abi_version==1&&api->struct_size==sizeof(*api));
    if(strcmp(argv[4],"legacy")==0){
        REQUIRE(!(api->capabilities&OG_LAKE_SDK_OBJECT_VERSION));
        OgLakeSdkFooter* footer=NULL;OgLakeSdkLimits budget=limits();OgLakeSdkError error;
        REQUIRE(api->footer_open(argv[2],&budget,&footer,&error)==OG_LAKE_SDK_OK);
        const OgLakeSdkFooterInfo* info=api->footer_info(footer);
        REQUIRE(info&&info->struct_size>=offsetof(OgLakeSdkFooterInfo,object_version));
        REQUIRE(info->num_rows==12288);api->footer_close(&footer);
        OgLakeSdkReader* value=reader(argv[2]);OgLakeSdkBatch* batch=NULL;
        REQUIRE(api->reader_next(value,&batch,&error)==OG_LAKE_SDK_OK);
        REQUIRE(batch&&batch->struct_size>=offsetof(OgLakeSdkBatch,object_version)&&batch->num_rows>0);
        api->reader_close(&value);api->batch_release(&batch);
    }else{
        REQUIRE(api->capabilities&OG_LAKE_SDK_OBJECT_VERSION);
        char* initial=version(argv[2]);char* again=version(argv[2]);
        REQUIRE(strcmp(initial,again)==0);free(again);
        OgLakeSdkReader* value=reader(argv[2]);batch_version(&value,initial);

        struct stat original;REQUIRE(stat(argv[2],&original)==0);
        int fd=open(argv[2],O_RDWR);REQUIRE(fd>=0);char byte;
        REQUIRE(pread(fd,&byte,1,4)==1);char changed=(char)(byte^1);
        REQUIRE(pwrite(fd,&changed,1,4)==1);REQUIRE(fsync(fd)==0);
        REQUIRE(pwrite(fd,&byte,1,4)==1);REQUIRE(fsync(fd)==0);
        struct timespec times[2]={original.st_atim,original.st_mtim};
        REQUIRE(futimens(fd,times)==0);REQUIRE(close(fd)==0);
        struct stat restored;REQUIRE(stat(argv[2],&restored)==0);
        REQUIRE(restored.st_size==original.st_size&&restored.st_ino==original.st_ino);
        REQUIRE(restored.st_mtim.tv_sec==original.st_mtim.tv_sec&&restored.st_mtim.tv_nsec==original.st_mtim.tv_nsec);
        char* modified=version(argv[2]);REQUIRE(strcmp(initial,modified)!=0);
        value=reader(argv[2]);batch_version(&value,modified);

        /* Replacing the name changes the old inode's ctime. An already open
         * reader must reject that mutation rather than label it as new data. */
        value=reader(argv[2]);REQUIRE(rename(argv[3],argv[2])==0);
        char* replaced=version(argv[2]);REQUIRE(strcmp(modified,replaced)!=0);
        OgLakeSdkBatch* rejected=NULL;OgLakeSdkError error;
        REQUIRE(api->reader_next(value,&rejected,&error)==OG_LAKE_SDK_IO&&rejected==NULL);
        api->reader_close(&value);REQUIRE(value==NULL);
        value=reader(argv[2]);batch_version(&value,replaced);
        again=version(argv[2]);REQUIRE(strcmp(again,replaced)==0);
        free(again);free(replaced);free(modified);free(initial);
    }
    REQUIRE(dlclose(library)==0);
    printf("{\"status\":\"pass\",\"checks\":%u}\n",checks);return 0;
}
