#include "lake_parquet_sdk.h"
#include <cstdio>
#include <cstddef>
int main(){
#define SIZE(t) std::printf("\"" #t "\":%zu,",sizeof(t))
#define OFFSET(t,m) std::printf("\"" #t "." #m "\":%zu,",offsetof(t,m))
    std::printf("{");
    SIZE(OgLakeSdkField);SIZE(OgLakeSdkListField);SIZE(OgLakeSdkColumnChunk);SIZE(OgLakeSdkRowGroup);
    SIZE(OgLakeSdkFooterInfo);SIZE(OgLakeSdkIoStats);SIZE(OgLakeSdkReadOptions);SIZE(OgLakeSdkBatch);
    SIZE(OgLakeSdkWriteOptions);SIZE(OgLakeSdkWrittenFile);SIZE(OgLakeSdkLimits);SIZE(OgLakeSdkError);
    SIZE(OgLakeParquetSdkApi);SIZE(OgLakeParquetLifecycleApi);SIZE(OgLakeParquetMaintenanceApi);SIZE(OgLakeParquetNestedApi);
    OFFSET(OgLakeParquetSdkApi,capabilities);OFFSET(OgLakeParquetSdkApi,footer_open);OFFSET(OgLakeParquetSdkApi,writer_close);
    OFFSET(OgLakeSdkFooterInfo,object_version);OFFSET(OgLakeSdkFooterInfo,list_fields);OFFSET(OgLakeSdkField,name);
    std::printf("\"legacy_enum\":[%u,%u,%u,%u,%u,%u,%u,%u,%u],\"binary_cap\":%u}\n",(unsigned)OG_LAKE_SDK_BOOL,(unsigned)OG_LAKE_SDK_INT32,(unsigned)OG_LAKE_SDK_INT64,(unsigned)OG_LAKE_SDK_UTF8,(unsigned)OG_LAKE_SDK_FLOAT32,(unsigned)OG_LAKE_SDK_FLOAT64,(unsigned)OG_LAKE_SDK_FLOAT32_LIST,(unsigned)OG_LAKE_SDK_BINARY,(unsigned)OG_LAKE_SDK_OTHER,(unsigned)OG_LAKE_SDK_BINARY_VALUES);
}
