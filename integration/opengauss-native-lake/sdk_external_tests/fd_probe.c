/* Reuse the actual value/order checks for the new POSIX descriptor lifecycle. */
#define _GNU_SOURCE
#define main old_path_probe_main
#include "reader_probe.c"
#undef main
#include "lake_parquet_fd_sdk.h"
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

static unsigned int open_count(void)
{
    DIR *dir = opendir("/proc/self/fd");
    CHECK(dir);
    unsigned int count = 0;
    while (readdir(dir))
        ++count;
    CHECK(closedir(dir) == 0);
    return count;
}

static int cancel(void *context)
{
    return *(const int *)context;
}

static void read_all(const OgLakeParquetSdkApi *api, OgLakeSdkReader *reader, const char *version,
                     int children)
{
    OgLakeSdkError error;
    int64_t position = 0;
    for (;;) {
        OgLakeSdkBatch *batch = NULL;
        int status = api->reader_next(reader, &batch, &error);
        if (status == OG_LAKE_SDK_EOF) {
            CHECK(!batch);
            break;
        }
        if (status != OG_LAKE_SDK_OK)
            fprintf(stderr, "read error %d: %s\n", status, error.message);
        CHECK(status == OG_LAKE_SDK_OK && batch && batch->num_rows > 0 && batch->num_rows <= 128);
        CHECK(batch->object_version && strcmp(batch->object_version, version) == 0);
        CHECK(batch->array->n_children == children && batch->schema->n_children == children);
        for (int64_t row = 0; row < batch->num_rows; ++row, ++position) {
            CHECK(batch->original_positions[row] == position);
            if (!children)
                continue;
            CHECK(strcmp(batch->schema->children[0]->name, "text_col") == 0);
            CHECK(strcmp(batch->schema->children[1]->name, "id") == 0);
            value(batch->array->children[0], batch->schema->children[0], row, position);
            const struct ArrowArray *id = batch->array->children[1];
            CHECK(strcmp(batch->schema->children[1]->format, "i") == 0 && nonnull(id, row));
            CHECK(((const int32_t *)id->buffers[1])[id->offset + row] == position);
        }
        api->batch_release(&batch);
        CHECK(!batch);
    }
    CHECK(position == 5000);
}

#ifndef LAKE_FD_PROBE_ENTRY
#define LAKE_FD_PROBE_ENTRY main
#endif
int LAKE_FD_PROBE_ENTRY(int argc, char **argv)
{
    CHECK(argc == 5);
    void *library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    if (!library)
        fprintf(stderr, "%s\n", dlerror());
    CHECK(library);
    for (unsigned int n = 1; n <= 6; ++n) {
        char name[64];
        snprintf(name, sizeof(name), "og_lake_parquet_sdk_v%u", n);
        CHECK(dlsym(library, name));
    }
    OgLakeParquetFdEntrypoint entry = (OgLakeParquetFdEntrypoint)dlsym(library, "og_lake_parquet_sdk_v6");
    const OgLakeParquetFdApi *fd_api = entry();
    CHECK(fd_api && fd_api->abi_version == 6 && fd_api->struct_size == sizeof(*fd_api));
    CHECK(fd_api->capabilities == OG_LAKE_SDK_READONLY_FD);
    const OgLakeParquetSdkApi *api = fd_api->base;
    CHECK(api && api->abi_version == 1 && api->struct_size == sizeof(*api));

    const OgLakeSdkLimits limits = {.struct_size = sizeof(limits), .memory_bytes = (64 << 20), .footer_bytes = (16 << 20)};
    const int64_t ordinals[] = {1, 0};
    OgLakeSdkReadOptions opts = {.struct_size = sizeof(opts), .limits = limits, .batch_rows = 128,
        .projection_kind = OG_LAKE_SDK_BY_ORDINAL, .num_field_ids = 2, .field_ids = ordinals, .num_row_groups = -1};
    OgLakeSdkError error;
    OgLakeSdkFooter *footer = NULL;
    const int caller = open(argv[2], O_RDONLY | O_CLOEXEC);
    const int same_object = open(argv[2], O_RDONLY | O_CLOEXEC);
    CHECK(caller >= 0 && same_object >= 0 && lseek(caller, 7, SEEK_SET) == 7);
    CHECK(rename(argv[2], argv[4]) == 0 && symlink(argv[3], argv[2]) == 0);
    const unsigned int base_fds = open_count();
    CHECK(fd_api->footer_open_fd(caller, &limits, &footer, &error) == OG_LAKE_SDK_OK && open_count() == base_fds + 1);
    const OgLakeSdkFooterInfo *info = api->footer_info(footer);
    CHECK(info && info->object_version && info->num_rows == 5000 && info->num_row_groups == 3);
    char *version = strdup(info->object_version);
    CHECK(version);
    api->footer_close(&footer);
    CHECK(!footer && open_count() == base_fds && lseek(caller, 0, SEEK_CUR) == 7);

    OgLakeSdkReader *reader = NULL;
    const int different = open(argv[3], O_RDONLY | O_CLOEXEC);
    const int writable = open(argv[4], O_RDWR | O_CLOEXEC);
    const int path_only = open(argv[4], O_PATH | O_CLOEXEC);
    const int nonfile = open("/dev/null", O_RDONLY | O_CLOEXEC);
    CHECK(different >= 0 && writable >= 0 && path_only >= 0 && nonfile >= 0);
    CHECK(fd_api->reader_open_fd(different, version, &opts, &reader, &error) == OG_LAKE_SDK_STATE && !reader);
    CHECK(fd_api->reader_open_fd(caller, NULL, &opts, &reader, &error) == OG_LAKE_SDK_INVALID && !reader);
    opts.path = argv[4];
    CHECK(fd_api->reader_open_fd(caller, version, &opts, &reader, &error) == OG_LAKE_SDK_INVALID && !reader);
    opts.path = NULL;
    CHECK(fd_api->footer_open_fd(writable, &limits, &footer, &error) == OG_LAKE_SDK_INVALID && !footer);
    CHECK(fd_api->footer_open_fd(path_only, &limits, &footer, &error) == OG_LAKE_SDK_INVALID && !footer);
    CHECK(fd_api->footer_open_fd(nonfile, &limits, &footer, &error) == OG_LAKE_SDK_INVALID && !footer);
    CHECK(fd_api->footer_open_fd(-1, &limits, &footer, &error) == OG_LAKE_SDK_INVALID && !footer);
    int cancelled = 1;
    OgLakeSdkLimits cancelled_limits = limits;
    cancelled_limits.should_cancel = cancel;
    cancelled_limits.cancel_context = &cancelled;
    CHECK(fd_api->footer_open_fd(caller, &cancelled_limits, &footer, &error) == OG_LAKE_SDK_CANCELLED && !footer);
    CHECK(close(nonfile) == 0 && close(path_only) == 0 && close(writable) == 0 && close(different) == 0);
    CHECK(open_count() == base_fds && lseek(caller, 0, SEEK_CUR) == 7);

    CHECK(fd_api->reader_open_fd(caller, version, &opts, &reader, &error) == OG_LAKE_SDK_OK);
    CHECK(open_count() == base_fds + 1 && lseek(caller, 0, SEEK_CUR) == 7 && close(caller) == 0);
    read_all(api, reader, version, 2);
    api->reader_close(&reader);
    CHECK(!reader && open_count() == base_fds - 1);
    opts.num_field_ids = 0;
    opts.field_ids = NULL;
    CHECK(fd_api->reader_open_fd(same_object, version, &opts, &reader, &error) == OG_LAKE_SDK_OK);
    read_all(api, reader, version, 0);
    api->reader_close(&reader);
    CHECK(close(same_object) == 0 && open_count() == base_fds - 2);
    free(version);
    CHECK(dlclose(library) == 0);
    puts("{\"status\":\"pass\",\"abi\":6,\"legacy_exports\":5,\"rows\":5000,\"empty_projection\":true,"
         "\"rejected_wrong_descriptors_or_options\":8,\"caller_offset_preserved\":true,\"original_fd_pins_object\":true,\"no_fd_leaks\":true}");
    return 0;
}