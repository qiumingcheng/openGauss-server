/* Native tuple-to-Arrow C Data adapter; no external executor or C++ Arrow ABI. */
#ifndef OG_LAKE_ARROW_BUILDER_H
#define OG_LAKE_ARROW_BUILDER_H
#include "postgres.h"
#include "lake_file_sdk.h"

struct LakeArrowBuilder;
/* Limits cover logical native input buffers, not allocator overhead or RSS. */
extern LakeArrowBuilder* LakeArrowBuilderCreate(MemoryContext parent,
    const OgLakeSdkField* fields,int count,int rows,Size inputBytes,const int32* typmods=NULL);
/* false leaves the batch unchanged: write/reset it, then retry this row.
 * A single row larger than the configured input budget raises an error.
 * Discard the builder after ERROR; only false permits retrying its batch. */
extern bool LakeArrowBuilderAppend(LakeArrowBuilder* builder,const Datum* values,const bool* nulls);
extern int LakeArrowBuilderCount(const LakeArrowBuilder* builder);
/* SDK borrows the batch synchronously; it cannot steal release callbacks. */
extern void LakeArrowBuilderWrite(LakeArrowBuilder* builder,const OgLakeParquetSdkApi* api,
    OgLakeSdkWriter* writer);
extern void LakeArrowBuilderReset(LakeArrowBuilder* builder);
extern void LakeArrowBuilderDestroy(LakeArrowBuilder* builder);
#endif
