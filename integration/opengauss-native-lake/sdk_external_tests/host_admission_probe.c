/* Exercise the actual candidate host path admission together with SDK6. */
#define _GNU_SOURCE
#define LAKE_FD_PROBE_ENTRY fd_probe_entry
#include "fd_probe.c"
#undef LAKE_FD_PROBE_ENTRY
#include "lake_external_path.h"
#include <errno.h>
#include <sys/stat.h>

int main(int argc, char **argv)
{
    CHECK(argc == 4);
    const unsigned int base = open_count();
    const char *bad[] = {"", "/outside.parquet", "../outside.parquet", "nested/../data.parquet",
        "nested//data.parquet", "nested/data.parquet/", "nested/file.txt", "nested/missing.parquet",
        "symbol.parquet", "alias/outside.parquet", "pipe.parquet", "directory.parquet", "nested\\x.parquet",
        "scheme:object.parquet", "nested/\n.parquet"};
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); ++i) {
        CHECK(og_lake_open_external_parquet(argv[2], bad[i]) < 0);
        CHECK(open_count() == base);
    }
    CHECK(og_lake_open_external_parquet(argv[3], "shadow/nested/data.parquet") < 0);
    CHECK(og_lake_open_external_parquet("/", "not-allowed.parquet") < 0);
    CHECK(og_lake_open_external_parquet("relative-root", "data.parquet") < 0);
    CHECK(open_count() == base);
    const int fd = og_lake_open_external_parquet(argv[2], "nested/data.parquet");
    CHECK(fd >= 0 && (fcntl(fd, F_GETFL) & O_ACCMODE) == O_RDONLY && fcntl(fd, F_GETFD) == FD_CLOEXEC);
    CHECK(open_count() == base + 1);
    const int root = open(argv[2], O_PATH | O_DIRECTORY | O_CLOEXEC);
    CHECK(root >= 0);
    CHECK(renameat(root, "nested/data.parquet", root, "nested/retained.parquet") == 0);
    CHECK(symlinkat("retained.parquet", root, "nested/data.parquet") == 0);
    CHECK(close(root) == 0 && og_lake_open_external_parquet(argv[2], "nested/data.parquet") < 0);

    void *library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    CHECK(library);
    OgLakeParquetFdEntrypoint entry = (OgLakeParquetFdEntrypoint)dlsym(library, "og_lake_parquet_sdk_v6");
    CHECK(entry);
    const OgLakeParquetFdApi *fd_api = entry();
    CHECK(fd_api && fd_api->abi_version == 6);
    const OgLakeParquetSdkApi *api = fd_api->base;
    const OgLakeSdkLimits limits = {.struct_size = sizeof(limits), .memory_bytes = (64 << 20), .footer_bytes = (16 << 20)};
    OgLakeSdkFooter *footer = NULL;
    OgLakeSdkError error;
    CHECK(fd_api->footer_open_fd(fd, &limits, &footer, &error) == OG_LAKE_SDK_OK);
    const OgLakeSdkFooterInfo *info = api->footer_info(footer);
    CHECK(info && info->num_rows == 5000 && info->num_fields == 2 && info->num_row_groups == 3);
    const char *fields[] = {"text_col", "id"};
    const uint32_t types[] = {OG_LAKE_SDK_UTF8, OG_LAKE_SDK_INT32};
    int64_t ordinals[] = {-1, -1};
    for (int c = 0; c < 2; ++c) {
        for (int f = 0; f < info->num_fields; ++f) {
            if (strcmp(info->fields[f].name, fields[c]) == 0) {
                CHECK(ordinals[c] == -1 && info->fields[f].type == types[c]);
                ordinals[c] = f;
            }
        }
        CHECK(ordinals[c] >= 0);
    }
    char *version = strdup(info->object_version);
    CHECK(version);
    api->footer_close(&footer);
    OgLakeSdkReadOptions options = {.struct_size = sizeof(options), .limits = limits, .batch_rows = 128,
        .projection_kind = OG_LAKE_SDK_BY_ORDINAL, .num_field_ids = 2, .field_ids = ordinals, .num_row_groups = -1};
    OgLakeSdkReader *reader = NULL;
    CHECK(fd_api->reader_open_fd(fd, version, &options, &reader, &error) == OG_LAKE_SDK_OK);
    CHECK(close(fd) == 0);
    read_all(api, reader, version, 2);
    api->reader_close(&reader);
    CHECK(open_count() == base);
    free(version);
    CHECK(dlclose(library) == 0);
    puts("{\"status\":\"pass\",\"rows\":5000,\"negative_paths\":18,\"exact_named_projection\":true,"
         "\"sdk_consumes_same_authorized_inode\":true,\"no_fd_leaks\":true}");
    return 0;
}