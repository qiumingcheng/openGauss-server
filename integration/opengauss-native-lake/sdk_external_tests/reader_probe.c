/* Independent legacy-entry consumer of SDK6 Parquet without DuckLake field IDs. */
#include "lake_parquet_sdk.h"
#include "vendor/arrow20/abi.h"
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "line %d: %s\n", __LINE__, #x); exit(1); } } while (0)

static int nonnull(const struct ArrowArray *column, int64_t row)
{
    const int64_t absolute = column->offset + row;
    const unsigned char *validity = column->buffers[0];
    return !validity || ((validity[absolute / 8] >> (absolute % 8)) & 1);
}

static void value(const struct ArrowArray *column, const struct ArrowSchema *schema, int64_t row, int64_t position)
{
    const int64_t absolute = column->offset + row;
    const char *text = position % 7 == 0 ? "" : "湖/csv";
    CHECK(nonnull(column, row) == (position % 5 != 0));
    if (!nonnull(column, row))
        return;
    CHECK(strcmp(schema->format, "u") == 0 || strcmp(schema->format, "U") == 0);
    const int large = strcmp(schema->format, "U") == 0;
    const int64_t start = large ? ((const int64_t *)column->buffers[1])[absolute] :
                                 ((const int32_t *)column->buffers[1])[absolute];
    const int64_t stop = large ? ((const int64_t *)column->buffers[1])[absolute + 1] :
                                ((const int32_t *)column->buffers[1])[absolute + 1];
    CHECK(start >= 0 && stop - start == (int64_t)strlen(text));
    CHECK(stop == start || memcmp((const char *)column->buffers[2] + start, text, (size_t)(stop - start)) == 0);
}

static int64_t scan(const OgLakeParquetSdkApi *api, OgLakeSdkReadOptions opts,
                    const char *version, int projected)
{
    OgLakeSdkError error;
    OgLakeSdkReader *reader = NULL;
    CHECK(api->reader_open(&opts, &reader, &error) == OG_LAKE_SDK_OK);
    int64_t current = 0;
    for (;;) {
        OgLakeSdkBatch *batch = NULL;
        const int status = api->reader_next(reader, &batch, &error);
        if (status == OG_LAKE_SDK_EOF) {
            CHECK(batch == NULL);
            break;
        }
        CHECK(status == OG_LAKE_SDK_OK && batch && batch->num_rows > 0 && batch->num_rows <= opts.batch_rows);
        CHECK(batch->struct_size >= OG_LAKE_SDK_FIELD_END(OgLakeSdkBatch, object_version));
        CHECK(batch->object_version && strcmp(batch->object_version, version) == 0);
        CHECK(batch->array->n_children == projected && batch->schema->n_children == projected);
        for (int64_t row = 0; row < batch->num_rows; ++row, ++current) {
            CHECK(batch->original_positions[row] == current);
            if (projected == 0)
                continue;
            CHECK(strcmp(batch->schema->children[0]->name, "text_col") == 0);
            CHECK(strcmp(batch->schema->children[1]->name, "id") == 0);
            value(batch->array->children[0], batch->schema->children[0], row, current);
            const struct ArrowArray *id = batch->array->children[1];
            CHECK(strcmp(batch->schema->children[1]->format, "i") == 0 && nonnull(id, row));
            CHECK(((const int32_t *)id->buffers[1])[id->offset + row] == current);
        }
        api->batch_release(&batch);
        CHECK(batch == NULL);
    }
    api->reader_close(&reader);
    CHECK(reader == NULL);
    return current;
}

int main(int argc, char **argv)
{
    CHECK(argc == 3);
    void *library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    if (!library)
        fprintf(stderr, "%s\n", dlerror());
    CHECK(library);
    OgLakeParquetSdkEntrypoint entry = (OgLakeParquetSdkEntrypoint)dlsym(library, "og_lake_parquet_sdk_v1");
    CHECK(entry);
    const OgLakeParquetSdkApi *api = entry();
    CHECK(api && api->abi_version == 1 && api->struct_size == sizeof(*api));
    CHECK((api->capabilities & OG_LAKE_SDK_ORIGINAL_POSITIONS) != 0);
    const OgLakeSdkLimits limits = {.struct_size = sizeof(limits), .memory_bytes = (64 << 20), .footer_bytes = (16 << 20)};
    OgLakeSdkFooter *footer = NULL;
    OgLakeSdkError error;
    CHECK(api->footer_open(argv[2], &limits, &footer, &error) == OG_LAKE_SDK_OK);
    const OgLakeSdkFooterInfo *info = api->footer_info(footer);
    CHECK(info && info->num_fields == 2 && info->num_rows == 5000 && info->num_row_groups >= 2);
    CHECK(info->struct_size >= OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo, object_version) && info->object_version);
    CHECK(info->fields[0].field_id == -1 && info->fields[1].field_id == -1);
    CHECK(strcmp(info->fields[0].name, "id") == 0 && info->fields[0].type == OG_LAKE_SDK_INT32);
    CHECK(strcmp(info->fields[1].name, "text_col") == 0 && info->fields[1].type == OG_LAKE_SDK_UTF8);

    const int64_t order[] = {1, 0};
    OgLakeSdkReadOptions opts = {.struct_size = sizeof(opts), .path = argv[2], .limits = limits,
        .batch_rows = 128, .projection_kind = OG_LAKE_SDK_BY_ORDINAL, .num_field_ids = 2,
        .field_ids = order, .num_row_groups = -1};
    CHECK(scan(api, opts, info->object_version, 2) == info->num_rows);
    opts.num_field_ids = 0;
    opts.field_ids = NULL;
    CHECK(scan(api, opts, info->object_version, 0) == info->num_rows);

    opts.num_field_ids = 2;
    opts.field_ids = order;
    opts.projection_kind = OG_LAKE_SDK_BY_FIELD_ID;
    OgLakeSdkReader *bad = NULL;
    CHECK(api->reader_open(&opts, &bad, &error) != OG_LAKE_SDK_OK && !bad);
    opts.projection_kind = OG_LAKE_SDK_BY_ORDINAL;
    const int64_t duplicate[] = {1, 1};
    opts.field_ids = duplicate;
    CHECK(api->reader_open(&opts, &bad, &error) != OG_LAKE_SDK_OK && !bad);
    const int64_t missing[] = {2};
    opts.num_field_ids = 1;
    opts.field_ids = missing;
    CHECK(api->reader_open(&opts, &bad, &error) != OG_LAKE_SDK_OK && !bad);

    printf("{\"status\":\"pass\",\"rows\":5000,\"row_groups\":%d,\"ordinal_projection\":true,"
           "\"empty_projection\":true,\"reject_field_id_duplicate_and_missing\":true}\n", info->num_row_groups);
    api->footer_close(&footer);
    CHECK(!footer);
    CHECK(dlclose(library) == 0);
    return 0;
}