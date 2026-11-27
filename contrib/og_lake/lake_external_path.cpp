/* Kernel openat2 anchor-fd path admission for read-only external files. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_external_path.h"
#include "commands/copy.h"
#include "catalog/pg_authid.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#if defined(__linux__) && defined(__has_include)
#if __has_include(<linux/openat2.h>)
#include <linux/openat2.h>
#define LAKE_EXT_HAVE_OPENAT2 1
#endif
#endif

static bool LakeHasValidSegments(const char* path)
{
    if (path == NULL || path[0] == '\0' || strnlen(path, MAXPGPATH) >= MAXPGPATH)
        return false;
    while (*path != '\0') {
        size_t len = strcspn(path, "/");
        if (len == 0 || len > NAME_MAX || path[0] == '.' ||
            (len == 2 && path[0] == '.' && path[1] == '.'))
            return false;
        for (size_t i = 0; i < len; ++i) {
            unsigned char ch = (unsigned char)path[i];
            if (ch < 0x20 || ch == 0x7f || ch == ':' || ch == '\\')
                return false;
        }
        path += len;
        if (*path == '\0')
            return true;
        ++path;
    }
    return false;
}

static bool LakeEndsWithInsensitive(const char* value, const char* suffix)
{
    if (value == NULL || suffix == NULL)
        return false;
    Size vlen = strlen(value);
    Size slen = strlen(suffix);
    return vlen >= slen && pg_strcasecmp(value + (vlen - slen), suffix) == 0;
}

static int LakeOpenBeneath(int dirfd, const char* path, uint64 flags, uint64 resolve)
{
#if defined(LAKE_EXT_HAVE_OPENAT2) && defined(SYS_openat2)
    struct open_how how;
    errno_t rc = memset_s(&how, sizeof(how), 0, sizeof(how));
    securec_check(rc, "", "");
    how.flags = flags;
    how.resolve = resolve;
    int fd;
    do {
        fd = (int)syscall(SYS_openat2, dirfd, path, &how, sizeof(how));
    } while (fd < 0 && errno == EINTR);
    return fd;
#else
    (void)dirfd;
    (void)path;
    (void)flags;
    (void)resolve;
    errno = ENOSYS;
    return -1;
#endif
}

bool LakeExternalIsRemoteUrl(const char* input)
{
    if (input == NULL)
        return false;
    const char* p = input;
    if (!isalpha((unsigned char)*p))
        return false;
    ++p;
    while (*p != '\0' && (isalnum((unsigned char)*p) || *p == '+' || *p == '-' || *p == '.'))
        ++p;
    if (strncmp(p, "://", 3) != 0)
        return false;
    return pg_strncasecmp(input, "file://", 7) != 0;
}

char* LakeExternalValidateRoot(const char* root)
{
    if (root == NULL || root[0] == '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external lake root directory cannot be empty")));
    if (LakeExternalIsRemoteUrl(root) || pg_strncasecmp(root, "file://", 7) == 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external lake root requires a local filesystem directory path")));
    if (root[0] != '/')
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external lake root directory must be an absolute path")));
    Size len = strlen(root);
    if (len >= MAXPGPATH - 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external lake root directory exceeds path limit")));
    char canonical[MAXPGPATH];
    errno_t rc = strcpy_s(canonical, sizeof(canonical), root);
    securec_check(rc, "", "");
    while (len > 1 && canonical[len - 1] == '/')
        canonical[--len] = '\0';
    if (len == 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("filesystem root cannot be used as external lake root")));
    if (!LakeHasValidSegments(canonical + 1))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external lake root contains invalid or dot path components")));
    struct stat lst;
    if (lstat(canonical, &lst) != 0)
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("cannot inspect external lake root \"%s\": %m", canonical)));
    if (S_ISLNK(lst.st_mode) || !S_ISDIR(lst.st_mode))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("external lake root \"%s\" must be a non-symlink directory", canonical)));
    char* resolved = realpath(canonical, NULL);
    if (resolved == NULL)
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("cannot resolve external lake root \"%s\": %m", canonical)));
    bool exactMatch = strcmp(canonical, resolved) == 0;
    free(resolved);
    if (!exactMatch)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("external lake root \"%s\" must not contain symbolic-link path components", canonical)));

    int fsFd = open("/", O_PATH | O_DIRECTORY | O_CLOEXEC);
    if (fsFd < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot open filesystem root: %m")));
    int dirFd = LakeOpenBeneath(fsFd, canonical + 1, O_PATH | O_DIRECTORY | O_CLOEXEC,
        RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS);
    int savedErrno = errno;
    (void)close(fsFd);
    if (dirFd < 0) {
        errno = savedErrno;
        ereport(ERROR, (errcode(savedErrno == ENOSYS ? ERRCODE_FEATURE_NOT_SUPPORTED : ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("external lake root \"%s\" failed kernel symlink-free validation: %m", canonical)));
    }
    struct stat st;
    int fstatRc = fstat(dirFd, &st);
    savedErrno = errno;
    (void)close(dirFd);
    if (fstatRc != 0) {
        errno = savedErrno;
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot stat external lake root \"%s\": %m", canonical)));
    }
    if (!S_ISDIR(st.st_mode))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("external lake root \"%s\" is not a directory", canonical)));
    return pstrdup(canonical);
}

char* LakeExternalNormalizeFormat(const char* explicitFormat, const char* path)
{
    if (explicitFormat == NULL || explicitFormat[0] == '\0') {
        if (LakeEndsWithInsensitive(path, ".parquet"))
            return pstrdup("parquet");
        if (LakeEndsWithInsensitive(path, ".csv"))
            return pstrdup("csv");
        if (LakeEndsWithInsensitive(path, ".tsv") || LakeEndsWithInsensitive(path, ".txt"))
            return pstrdup("text");
        if (LakeEndsWithInsensitive(path, ".json") || LakeEndsWithInsensitive(path, ".jsonl") ||
            LakeEndsWithInsensitive(path, ".ndjson"))
            return pstrdup("json");
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("cannot infer external file format for \"%s\"; specify format ('parquet', 'csv', 'text', or 'json')",
                path ? path : "")));
    }
    if (pg_strcasecmp(explicitFormat, "parquet") == 0)
        return pstrdup("parquet");
    if (pg_strcasecmp(explicitFormat, "csv") == 0)
        return pstrdup("csv");
    if (pg_strcasecmp(explicitFormat, "text") == 0)
        return pstrdup("text");
    if (pg_strcasecmp(explicitFormat, "json") == 0 || pg_strcasecmp(explicitFormat, "jsonl") == 0 ||
        pg_strcasecmp(explicitFormat, "ndjson") == 0)
        return pstrdup("json");
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("external lake file format \"%s\" is not supported", explicitFormat),
        errhint("Supported external file formats are parquet, csv, text, and json (jsonl/ndjson).")));
    return NULL;
}

bool LakeExternalValidRelativePath(const char* relative, const char* requiredFormat)
{
    if (relative == NULL || relative[0] == '\0' || relative[0] == '/')
        return false;
    if (LakeExternalIsRemoteUrl(relative) || pg_strncasecmp(relative, "file://", 7) == 0)
        return false;
    if (!LakeHasValidSegments(relative))
        return false;
    /* Reject hidden dot segments or sensitive key/config suffixes at the basename. */
    const char* base = strrchr(relative, '/');
    base = (base != NULL) ? base + 1 : relative;
    if (base[0] == '\0' || base[0] == '.' || strcmp(base, "openssl.cnf") == 0 ||
        LakeEndsWithInsensitive(base, ".key.cipher") ||
        LakeEndsWithInsensitive(base, ".key.rand") ||
        LakeEndsWithInsensitive(base, "_adt"))
        return false;
    if (requiredFormat != NULL) {
        if (pg_strcasecmp(requiredFormat, "parquet") == 0)
            return LakeEndsWithInsensitive(relative, ".parquet");
        if (pg_strcasecmp(requiredFormat, "json") == 0 || pg_strcasecmp(requiredFormat, "jsonl") == 0 ||
            pg_strcasecmp(requiredFormat, "ndjson") == 0)
            return LakeEndsWithInsensitive(relative, ".json") || LakeEndsWithInsensitive(relative, ".jsonl") ||
                LakeEndsWithInsensitive(relative, ".ndjson");
        if (pg_strcasecmp(requiredFormat, "csv") == 0 || pg_strcasecmp(requiredFormat, "text") == 0)
            return !LakeEndsWithInsensitive(relative, ".parquet");
    }
    return true;
}

void LakeExternalCloseOpenedFile(LakeExternalOpenedFile* opened) noexcept
{
    if (opened != NULL && opened->fd >= 0) {
        int fd = opened->fd;
        opened->fd = -1;
        (void)close(fd);
    }
}

LakeExternalOpenedFile LakeExternalOpenReadonly(const char* root, const char* relative,
    const char* requiredFormat)
{
    if (root == NULL || root[0] != '/' || !LakeHasValidSegments(root + 1))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external file root directory is invalid")));
    if (LakeExternalIsRemoteUrl(relative))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("remote external file URLs are not implemented"),
            errdetail("Only local files inside an authorized root directory are currently supported.")));
    if (!LakeExternalValidRelativePath(relative, requiredFormat))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid or disallowed external relative file path \"%s\"", relative ? relative : "")));

    volatile int fsFd = -1;
    volatile int anchorFd = -1;
    volatile int fileFd = -1;
    struct stat st;
    PG_TRY();
    {
        fsFd = open("/", O_PATH | O_DIRECTORY | O_CLOEXEC);
        if (fsFd < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot open filesystem root: %m")));
        anchorFd = LakeOpenBeneath(fsFd, root + 1, O_PATH | O_DIRECTORY | O_CLOEXEC,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS);
        int anchorErrno = errno;
        (void)close(fsFd);
        fsFd = -1;
        if (anchorFd < 0) {
            errno = anchorErrno;
            ereport(ERROR, (errcode(anchorErrno == ENOENT ? ERRCODE_UNDEFINED_FILE :
                (anchorErrno == ENOSYS ? ERRCODE_FEATURE_NOT_SUPPORTED : ERRCODE_INSUFFICIENT_PRIVILEGE)),
                errmsg("cannot open external root directory \"%s\": %m", root)));
        }
        fileFd = LakeOpenBeneath(anchorFd, relative,
            O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_XDEV);
        int fileErrno = errno;
        (void)close(anchorFd);
        anchorFd = -1;
        if (fileFd < 0) {
            errno = fileErrno;
            ereport(ERROR, (errcode(fileErrno == ENOENT ? ERRCODE_UNDEFINED_FILE :
                (fileErrno == ELOOP || fileErrno == EXDEV ? ERRCODE_INSUFFICIENT_PRIVILEGE :
                (fileErrno == ENOSYS ? ERRCODE_FEATURE_NOT_SUPPORTED : ERRCODE_FDW_ERROR))),
                errmsg("cannot safely open external file \"%s\" beneath \"%s\": %m", relative, root)));
        }
        if (fstat(fileFd, &st) != 0)
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("cannot stat external file \"%s\": %m", relative)));
        if (!S_ISREG(st.st_mode))
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("external file \"%s\" must be a regular file", relative)));
    }
    PG_CATCH();
    {
        if (fileFd >= 0)
            (void)close(fileFd);
        if (anchorFd >= 0)
            (void)close(anchorFd);
        if (fsFd >= 0)
            (void)close(fsFd);
        PG_RE_THROW();
    }
    PG_END_TRY();

    LakeExternalOpenedFile result;
    result.fd = fileFd;
    result.file_bytes = (int64)st.st_size;
    result.display_name = pstrdup(relative);
    return result;
}

static const char* LakeExternalAuthorizeLocalPath(const char* rawPath, char** safeRootOut)
{
    if (safeRootOut != NULL)
        *safeRootOut = NULL;
    if (rawPath == NULL || rawPath[0] == '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external file path cannot be empty")));
    if (LakeExternalIsRemoteUrl(rawPath))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("remote external file URLs are not implemented"),
            errdetail("External storage adapters for object-store URLs are closed in this release.")));
    const char* path = rawPath;
    if (pg_strncasecmp(path, "file://", 7) == 0) {
        path += 7;
        if (path[0] != '/')
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("file:// external URI must specify an absolute local path")));
    }
    if (path[0] != '/')
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("standalone external file path must be absolute")));
    if (strlen(path) >= MAXPGPATH)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external file path exceeds maximum host limit")));

    /* Follow openGauss server file COPY privilege boundaries before touching filesystem. */
    if (!initialuser()) {
        if (!u_sess->attr.attr_storage.enable_copy_server_files)
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("direct server file access requires enable_copy_server_files or a configured external server")));
        if (!superuser() && !is_member_of_role(GetUserId(), DEFAULT_ROLE_COPY_FILES))
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin or a member of gs_role_copy_files to inspect server files directly")));
    }

    const char* safePaths = u_sess->attr.attr_common.safe_data_path;
    if (safePaths != NULL && safePaths[0] != '\0') {
        char* copy = pstrdup(safePaths);
        char* saveptr = NULL;
        char* bestRoot = NULL;
        Size bestLen = 0;
        for (char* tok = strtok_r(copy, ",", &saveptr); tok != NULL; tok = strtok_r(NULL, ",", &saveptr)) {
            while (*tok == ' ' || *tok == '\t')
                ++tok;
            Size tlen = strlen(tok);
            while (tlen > 0 && (tok[tlen - 1] == ' ' || tok[tlen - 1] == '\t'))
                tok[--tlen] = '\0';
            if (tlen == 0)
                continue;
            char* candRoot = LakeExternalValidateRoot(tok);
            Size rlen = strlen(candRoot);
            if (strncmp(path, candRoot, rlen) == 0 && path[rlen] == '/' && rlen > bestLen) {
                bestRoot = candRoot;
                bestLen = rlen;
            }
        }
        pfree(copy);
        if (bestRoot == NULL || path[bestLen + 1] == '\0')
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("external file path must reside beneath safe_data_path")));
        if (safeRootOut != NULL)
            *safeRootOut = bestRoot;
        return path;
    }

    if (!initialuser() && !superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("direct file path access without safe_data_path requires superuser or a configured external server")));
    return path;
}

void LakeExternalAuthorizePath(const char* rawPath, char** rootOut, char** relOut)
{
    char* bestRoot = NULL;
    const char* path = LakeExternalAuthorizeLocalPath(rawPath, &bestRoot);
    if (bestRoot != NULL) {
        *rootOut = bestRoot;
        *relOut = pstrdup(path + strlen(bestRoot) + 1);
        return;
    }

    const char* slash = strrchr(path, '/');
    if (slash == NULL || slash == path || slash[1] == '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external file path must be inside a non-root parent directory")));
    char* parent = pnstrdup(path, (Size)(slash - path));
    *rootOut = LakeExternalValidateRoot(parent);
    *relOut = pstrdup(slash + 1);
    pfree(parent);
}

char* LakeExternalAuthorizeWritePath(const char* rawPath, int* parentOut)
{
    char* bestRoot = NULL;
    const char* path = LakeExternalAuthorizeLocalPath(rawPath, &bestRoot);
    const char* slash = strrchr(path, '/');
    if (slash == NULL || slash == path || slash[1] == '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external COPY TO path must specify a non-root directory and file name")));
    const char* fileName = slash + 1;
    if (!LakeHasValidSegments(fileName) || strchr(fileName, '/') != NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid external COPY TO file name \"%s\"", fileName)));
    if (bestRoot != NULL) {
        const char* rel = path + strlen(bestRoot) + 1;
        if (!LakeHasValidSegments(rel))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid external COPY TO relative path \"%s\"", rel)));
    }
    char* rawParent = pnstrdup(path, (Size)(slash - path));
    char* validParent = LakeExternalValidateRoot(rawParent);
    pfree(rawParent);

    char* canonical = psprintf("%s/%s", validParent, fileName);
    /* Reject symlinks along parent hierarchy and refuse overwriting existing entries. */
    volatile int fsFd = -1;
    volatile int parentFd = -1;
    PG_TRY();
    {
        fsFd = open("/", O_PATH | O_DIRECTORY | O_CLOEXEC);
        if (fsFd < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot open filesystem root: %m")));
        parentFd = LakeOpenBeneath(fsFd, validParent + 1, O_PATH | O_DIRECTORY | O_CLOEXEC,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS);
        int pErr = errno;
        (void)close(fsFd);
        fsFd = -1;
        if (parentFd < 0) {
            errno = pErr;
            ereport(ERROR, (errcode(pErr == ENOENT ? ERRCODE_UNDEFINED_FILE :
                (pErr == ELOOP || pErr == EXDEV ? ERRCODE_INSUFFICIENT_PRIVILEGE : ERRCODE_IO_ERROR)),
                errmsg("cannot safely open target parent directory \"%s\": %m", validParent)));
        }
        struct stat st;
        if (fstatat(parentFd, fileName, &st, AT_SYMLINK_NOFOLLOW) == 0)
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_FILE),
                errmsg("external COPY TO target file \"%s\" already exists", path)));
        int sErr = errno;
        if (sErr != ENOENT) {
            errno = sErr;
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("cannot inspect external COPY TO target path \"%s\": %m", path)));
        }
        if (parentOut != NULL)
            *parentOut = parentFd;
        else
            (void)close(parentFd);
        parentFd = -1;
    }
    PG_CATCH();
    {
        if (parentFd >= 0)
            (void)close(parentFd);
        if (fsFd >= 0)
            (void)close(fsFd);
        PG_RE_THROW();
    }
    PG_END_TRY();

    pfree(validParent);
    return canonical;
}
