/* Test-only transaction-owned candidate calls; no catalog publication. */
#include "postgres.h"
#include "lake_arrow_builder.h"
#include "lake_file_sdk.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(lake_test_candidate_write);

extern "C" Datum lake_test_candidate_write(PG_FUNCTION_ARGS)
{
    if(!superuser())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("candidate test requires administrator")));
    const char* path=text_to_cstring(PG_GETARG_TEXT_PP(0));
    bool finish=PG_GETARG_BOOL(1),publish=PG_GETARG_BOOL(2);
    int fail=PG_GETARG_INT32(3);
    OgLakeSdkField field={1,OG_LAKE_SDK_INT64,0,"id"};
    OgLakeSdkWriteOptions options={};options.struct_size=sizeof(options);options.path=path;
    options.limits=LakeFileLimits();options.num_fields=1;options.fields=&field;
    options.compression=OG_LAKE_SDK_ZSTD;options.row_group_rows=2;
    LakeFileCandidate* candidate=LakeFileCandidateOpen(&options);
    LakeArrowBuilder* batch=LakeArrowBuilderCreate(CurrentMemoryContext,&field,1,2,1024);
    bool nulls[1]={false};Datum values[1]={Int64GetDatum(42)};
    if(!LakeArrowBuilderAppend(batch,values,nulls))elog(ERROR,"empty candidate batch rejected row");
    values[0]=Int64GetDatum(-43);
    if(!LakeArrowBuilderAppend(batch,values,nulls))elog(ERROR,"candidate batch rejected second row");
    LakeArrowBuilderWrite(batch,candidate->api,candidate->writer);
    LakeArrowBuilderDestroy(batch);
    if(finish)LakeFileCandidateFinish(candidate);
    if(publish)LakeFileCandidatePublish(candidate); /* Simulated mark, no catalog rows. */
    if(fail)ereport(ERROR,(errcode(ERRCODE_RAISE_EXCEPTION),errmsg("intentional candidate test error")));
    PG_RETURN_INT64(finish?candidate->written.num_rows:0);
}
