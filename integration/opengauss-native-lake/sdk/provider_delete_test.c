#define _GNU_SOURCE
#include "lake_parquet_sdk.h"
#include <dlfcn.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
static int checks;
#define CHECK(c) do{++checks;if(!(c)){fprintf(stderr,"check %d line %d failed: %s\n",checks,__LINE__,#c);exit(1);}}while(0)
static int count_fds(void){DIR*d=opendir("/proc/self/fd");CHECK(d!=NULL);int n=0;while(readdir(d))++n;closedir(d);return n;}
static int cancel(void*p){(void)p;return 1;}
static void make(const char* root,const char* relative){char p[PATH_MAX];CHECK(snprintf(p,sizeof p,"%s/%s",root,relative)<(int)sizeof p);int f=open(p,O_WRONLY|O_CREAT|O_EXCL,0600);CHECK(f>=0);CHECK(write(f,"PAR1owned",9)==9);CHECK(close(f)==0);}
static int exists(const char* root,const char* relative){char p[PATH_MAX];CHECK(snprintf(p,sizeof p,"%s/%s",root,relative)<(int)sizeof p);struct stat s;return lstat(p,&s)==0;}
static void copy_input(const char* input,const char* output){int a=open(input,O_RDONLY);CHECK(a>=0);int b=open(output,O_WRONLY|O_CREAT|O_EXCL,0600);CHECK(b>=0);char block[4096];ssize_t n;while((n=read(a,block,sizeof block))>0){ssize_t written=0;while(written<n){ssize_t step=write(b,block+written,(size_t)(n-written));CHECK(step>0);written+=step;}}CHECK(n==0);CHECK(close(a)==0);CHECK(close(b)==0);}
int main(int argc,char**argv){
 CHECK(argc==4);void* lib=dlopen(argv[1],RTLD_NOW|RTLD_LOCAL);CHECK(lib!=NULL);
 OgLakeParquetMaintenanceEntrypoint entry=(OgLakeParquetMaintenanceEntrypoint)dlsym(lib,"og_lake_parquet_sdk_v3");CHECK(entry!=NULL);
 const OgLakeParquetMaintenanceApi*api=entry();CHECK(api&&api->abi_version==3&&api->struct_size==sizeof(*api)&&api->base&&api->base->abi_version==1&&api->delete_file&&(api->capabilities&OG_LAKE_SDK_LOCAL_DURABLE_DELETE));
 OgLakeSdkLimits limits={0};limits.struct_size=sizeof limits;limits.memory_bytes=1024*1024;limits.footer_bytes=1024*1024;OgLakeSdkError error;uint32_t removed;const char*root=argv[2];int before=count_fds();
 char genuine[PATH_MAX];CHECK(snprintf(genuine,sizeof genuine,"%s/genuine.parquet",root)<(int)sizeof genuine);copy_input(argv[3],genuine);OgLakeSdkFooter*footer=NULL;CHECK(api->base->footer_open(genuine,&limits,&footer,&error)==0);CHECK(api->base->footer_info(footer)->num_rows>0);api->base->footer_close(&footer);CHECK(footer==NULL);CHECK(api->delete_file(root,"genuine.parquet",&limits,&removed,&error)==0&&removed==1);CHECK(!exists(root,"genuine.parquet"));
 make(root,"regular.parquet");CHECK(api->delete_file(root,"regular.parquet",&limits,&removed,&error)==0&&removed==1);CHECK(!exists(root,"regular.parquet"));CHECK(api->delete_file(root,"regular.parquet",&limits,&removed,&error)==0&&removed==0);
 const char*bad[]={"../outside.parquet","/outside.parquet","","./bad.parquet","bad//file.parquet","bad/../file.parquet","bad/./file.parquet","bad/","note.txt"};
 for(unsigned i=0;i<sizeof bad/sizeof*bad;++i){CHECK(api->delete_file(root,bad[i],&limits,&removed,&error)==OG_LAKE_SDK_INVALID);CHECK(removed==0);}
 CHECK(api->delete_file("/","bad.parquet",&limits,&removed,&error)==OG_LAKE_SDK_INVALID);CHECK(api->delete_file("relative","bad.parquet",&limits,&removed,&error)==OG_LAKE_SDK_INVALID);CHECK(api->delete_file(root,"bad.parquet",&limits,NULL,&error)==OG_LAKE_SDK_INVALID);
 make(root,"cancel.parquet");limits.should_cancel=cancel;CHECK(api->delete_file(root,"cancel.parquet",&limits,&removed,&error)==OG_LAKE_SDK_CANCELLED&&removed==0);CHECK(exists(root,"cancel.parquet"));limits.should_cancel=NULL;CHECK(api->delete_file(root,"cancel.parquet",&limits,&removed,&error)==0&&removed==1);
 char path[PATH_MAX],target[PATH_MAX];CHECK(snprintf(path,sizeof path,"%s/leaf.parquet",root)<(int)sizeof path);CHECK(snprintf(target,sizeof target,"%s/survivor.parquet",root)<(int)sizeof target);make(root,"survivor.parquet");CHECK(symlink(target,path)==0);CHECK(api->delete_file(root,"leaf.parquet",&limits,&removed,&error)==OG_LAKE_SDK_INVALID);CHECK(exists(root,"leaf.parquet")&&exists(root,"survivor.parquet"));
 CHECK(snprintf(path,sizeof path,"%s/alias",root)<(int)sizeof path);CHECK(symlink(root,path)==0);CHECK(api->delete_file(root,"alias/survivor.parquet",&limits,&removed,&error)==OG_LAKE_SDK_IO);CHECK(exists(root,"survivor.parquet"));CHECK(api->delete_file(path,"survivor.parquet",&limits,&removed,&error)==OG_LAKE_SDK_IO);
 CHECK(snprintf(path,sizeof path,"%s/directory.parquet",root)<(int)sizeof path);CHECK(mkdir(path,0700)==0);CHECK(api->delete_file(root,"directory.parquet",&limits,&removed,&error)==OG_LAKE_SDK_INVALID);CHECK(exists(root,"directory.parquet"));
 CHECK(snprintf(path,sizeof path,"%s/nested",root)<(int)sizeof path);CHECK(mkdir(path,0700)==0);make(root,"nested/data.parquet");CHECK(api->delete_file(root,"nested/data.parquet",&limits,&removed,&error)==0&&removed==1);
 CHECK(snprintf(path,sizeof path,"%s/readonly",root)<(int)sizeof path);CHECK(mkdir(path,0700)==0);make(root,"readonly/kept.parquet");CHECK(chmod(path,0500)==0);CHECK(api->delete_file(root,"readonly/kept.parquet",&limits,&removed,&error)==OG_LAKE_SDK_IO&&removed==0);CHECK(chmod(path,0700)==0);CHECK(exists(root,"readonly/kept.parquet"));
 make(root,"fsync.parquet");CHECK(snprintf(path,sizeof path,"%s/fsync.parquet",root)<(int)sizeof path);CHECK(setenv("OG_LAKE_TEST_FAULT_PATH",path,1)==0);CHECK(setenv("OG_LAKE_TEST_FAULT_MODE","directory_fsync",1)==0);CHECK(api->delete_file(root,"fsync.parquet",&limits,&removed,&error)==OG_LAKE_SDK_IO&&removed==1);CHECK(!exists(root,"fsync.parquet"));CHECK(unsetenv("OG_LAKE_TEST_FAULT_MODE")==0);CHECK(unsetenv("OG_LAKE_TEST_FAULT_PATH")==0);CHECK(api->delete_file(root,"fsync.parquet",&limits,&removed,&error)==0&&removed==0);
 for(int i=0;i<100;++i){CHECK(api->delete_file(root,"alias/survivor.parquet",&limits,&removed,&error)==OG_LAKE_SDK_IO);CHECK(api->delete_file(root,"missing.parquet",&limits,&removed,&error)==0&&removed==0);}
 CHECK(count_fds()==before);CHECK(exists(root,"survivor.parquet"));CHECK(dlclose(lib)==0);printf("{\"status\":\"pass\",\"checks\":%d,\"scope\":\"standalone local SDK deletion; no catalog or native GC qualification\"}\n",checks);return 0;
}
