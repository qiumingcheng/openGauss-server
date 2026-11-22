#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "lake_external_path.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/syscall.h>
#if defined(__has_include)
#if __has_include(<linux/openat2.h>)
#include <linux/openat2.h>
#define LAKE_EXTERNAL_HAS_OPENAT2_HEADER 1
#endif
#endif
#endif

#if defined(LAKE_EXTERNAL_HAS_OPENAT2_HEADER) && defined(SYS_openat2)
static int valid_components(const char* path)
{
    if(!path||!*path||strnlen(path,PATH_MAX)==PATH_MAX)
        return 0;
    while(*path){
        size_t n=strcspn(path,"/");
        if(n==0||n>NAME_MAX||(n==1&&path[0]=='.')||(n==2&&path[0]=='.'&&path[1]=='.'))
            return 0;
        for(size_t i=0;i<n;++i){
            unsigned char c=(unsigned char)path[i];
            if(c<0x20||c==0x7f||c==':'||c=='\\')
                return 0;
        }
        path+=n;
        if(!*path)return 1;
        ++path;
    }
    return 0;
}

static int open_beneath(int directory,const char* path,unsigned long flags,unsigned long resolve)
{
    struct open_how how={.flags=flags,.resolve=resolve};
    int fd;
    do{fd=(int)syscall(SYS_openat2,directory,path,&how,sizeof(how));}while(fd<0&&errno==EINTR);
    return fd;
}

int og_lake_open_external_parquet(const char* root,const char* relative)
{
    if(!root||root[0]!='/'||!valid_components(root+1)||!valid_components(relative)){
        errno=EINVAL;
        return -1;
    }
    size_t n=strlen(relative);
    if(n<=8||memcmp(relative+n-8,".parquet",8)){
        errno=EINVAL;
        return -1;
    }
    /* The configured root may itself be an explicit mount. Do not follow any
     * symlink along the configured path, or cross a further mount below it. */
    int filesystem=open("/",O_PATH|O_DIRECTORY|O_CLOEXEC);
    if(filesystem<0)return -1;
    int anchor=open_beneath(filesystem,root+1,O_PATH|O_DIRECTORY|O_CLOEXEC,
        RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS);
    int failure=errno;
    close(filesystem);
    if(anchor<0){errno=failure;return -1;}
    int file=open_beneath(anchor,relative,O_RDONLY|O_NONBLOCK|O_CLOEXEC|O_NOFOLLOW,
        RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_XDEV);
    failure=errno;
    close(anchor);
    if(file<0){errno=failure;return -1;}
    struct stat st;
    if(fstat(file,&st)<0){failure=errno;close(file);errno=failure;return -1;}
    if(!S_ISREG(st.st_mode)){close(file);errno=EINVAL;return -1;}
    return file;
}
#else
int og_lake_open_external_parquet(const char* root,const char* relative)
{
    (void)root;
    (void)relative;
    errno=ENOSYS;
    return -1;
}
#endif