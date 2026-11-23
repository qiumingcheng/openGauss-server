/* Linux-only route C substrate probe. No SDK/product callers or fallback. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <linux/openat2.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "line %d: %s (errno=%d)\n", __LINE__, #x, errno); exit(1); } } while (0)

static int permitted_relative_path(const char *path)
{
    while (*path) {
        size_t length = strcspn(path, "/");
        if (!length || (length == 1 && path[0] == '.') || (length == 2 && !memcmp(path, "..", 2)))
            return 0;
        path += length;
        if (!*path)
            return 1;
        ++path;
    }
    return 0;
}

static int open_relative(int root, const char *path)
{
    if (!permitted_relative_path(path)) {
        errno = EINVAL;
        return -1;
    }
    struct open_how request = {
        .flags = O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW,
        .resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_XDEV,
    };
    int fd;
    do {
        fd = syscall(SYS_openat2, root, path, &request, sizeof(request));
    } while (fd < 0 && errno == EINTR);
    if (fd < 0)
        return fd;
    struct stat data;
    if (fstat(fd, &data) != 0) {
        const int saved = errno;
        close(fd);
        errno = saved;
        return -1;
    }
    if (!S_ISREG(data.st_mode)) {
        close(fd);
        errno = EINVAL;
        return -1;
    }
    return fd;
}

int main(int argc, char **argv)
{
    CHECK(argc == 2);
    const int root = open(argv[1], O_PATH | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    CHECK(root >= 0);
    const int file = open_relative(root, "nested/data");
    CHECK(file >= 0 && fcntl(file, F_GETFD) == FD_CLOEXEC);
    char bytes[8];
    CHECK(read(file, bytes, sizeof(bytes)) == 4 && memcmp(bytes, "safe", 4) == 0);
    const char *rejected[] = {"", "/nested/data", "nested/../nested/data", "./nested/data",
        "nested//data", "nested/data/", "missing", "symbolic-file", "binary-dir/data", "pipe", "nested"};
    for (unsigned int i = 0; i < sizeof(rejected) / sizeof(rejected[0]); ++i)
        CHECK(open_relative(root, rejected[i]) < 0);
    /* Replacing the file name after open must not retarget the validated fd. */
    CHECK(renameat(root, "nested/data", root, "nested/original") == 0);
    CHECK(symlinkat("original", root, "nested/data") == 0);
    CHECK(open_relative(root, "nested/data") < 0);
    CHECK(pread(file, bytes, sizeof(bytes), 0) == 4 && memcmp(bytes, "safe", 4) == 0);
    CHECK(close(file) == 0 && close(root) == 0);
    puts("{\"status\":\"pass\",\"negative_paths\":11,\"replaced_path_rejected\":true,\"original_fd_retained_identity\":true}");
    return 0;
}