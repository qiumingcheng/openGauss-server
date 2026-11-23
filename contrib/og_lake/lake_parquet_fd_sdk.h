/* Optional local descriptor input; the existing five SDK ABIs are unchanged. */
#ifndef OG_LAKE_PARQUET_FD_SDK_H
#define OG_LAKE_PARQUET_FD_SDK_H
#include "sdk/lake_parquet_sdk.h"

#ifdef __cplusplus
extern "C" {
#endif

#define OG_LAKE_PARQUET_FD_ABI 6u
#define OG_LAKE_SDK_READONLY_FD (1ull << 0)

/* fd is a borrowed, authorized POSIX descriptor for a regular read-only file.
 * The caller keeps it valid until the call returns. Success owns a private
 * close-on-exec duplicate. Failure does not close or change the supplied fd.
 * reader_open_fd requires options->path == NULL and exact object_version from
 * footer_open_fd (copied if the footer handle has already been released).
 * These operations never resolve a path or change the shared file offset. */
typedef struct OgLakeParquetFdApi {
    uint32_t abi_version;
    uint32_t struct_size;
    const OgLakeParquetSdkApi* base;
    uint64_t capabilities;
    int (*footer_open_fd)(int fd,const OgLakeSdkLimits*,OgLakeSdkFooter**,OgLakeSdkError*);
    int (*reader_open_fd)(int fd,const char* object_version,const OgLakeSdkReadOptions*,
        OgLakeSdkReader**,OgLakeSdkError*);
} OgLakeParquetFdApi;
typedef const OgLakeParquetFdApi* (*OgLakeParquetFdEntrypoint)(void);
const OgLakeParquetFdApi* og_lake_parquet_sdk_v6(void);

#ifdef __cplusplus
}
#endif
#endif