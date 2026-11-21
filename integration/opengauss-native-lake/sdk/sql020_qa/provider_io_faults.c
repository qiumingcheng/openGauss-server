/* Test-only syscall failure injection, limited to the owned candidate path. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int owned(int fd,int directory)
{
    const char* path=getenv("OG_LAKE_TEST_FAULT_PATH");
    if(!path||!*path)return 0;
    char entry[64],actual[PATH_MAX];snprintf(entry,sizeof(entry),"/proc/self/fd/%d",fd);
    ssize_t count=readlink(entry,actual,sizeof(actual)-1);
    if(count<0)return 0;
    actual[count]=0;
    if(!directory)return strcmp(actual,path)==0;
    const char* slash=strrchr(path,'/');
    return slash&&strlen(actual)==(size_t)(slash-path)&&memcmp(actual,path,(size_t)(slash-path))==0;
}
static int mode(const char* expected)
{
    const char* active=getenv("OG_LAKE_TEST_FAULT_MODE");return active&&strcmp(active,expected)==0;
}
int fsync(int fd)
{
    int (*real_fsync)(int)=(int(*)(int))dlsym(RTLD_NEXT,"fsync");
    if((mode("file_fsync")&&owned(fd,0))||(mode("directory_fsync")&&owned(fd,1))){errno=EIO;return -1;}
    return real_fsync(fd);
}
int close(int fd)
{
    int (*real_close)(int)=(int(*)(int))dlsym(RTLD_NEXT,"close");
    int fail=mode("close")&&owned(fd,0);int result=real_close(fd);
    if(fail&&result==0){errno=EIO;return -1;}
    return result;
}
ssize_t write(int fd,const void* data,size_t bytes)
{
    ssize_t (*real_write)(int,const void*,size_t)=(ssize_t(*)(int,const void*,size_t))dlsym(RTLD_NEXT,"write");
    static unsigned calls;
    if(mode("partial_write")&&owned(fd,0)){
        if(calls++==0)return real_write(fd,data,bytes<7?bytes:7);
        errno=ENOSPC;return -1;
    }
    return real_write(fd,data,bytes);
}
