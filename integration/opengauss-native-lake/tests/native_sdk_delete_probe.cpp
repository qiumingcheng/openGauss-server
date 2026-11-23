/* Test-only imports of the actual native provider capability and error path.
 * No catalog cleanup or deletion eligibility is supplied by this wrapper. */
#include "postgres.h"
#include "lake_file_sdk.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(lake_test_sdk_delete_probe);
extern "C" Datum lake_test_sdk_delete_probe(PG_FUNCTION_ARGS)
{
    if(!superuser())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native SDK maintenance probe requires administrator")));
    const OgLakeParquetMaintenanceApi* api=LakeFileMaintenanceProvider();
    char* root=text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* relative=text_to_cstring(PG_GETARG_TEXT_PP(1));
    OgLakeSdkLimits limits=LakeFileLimits();
    uint32_t removed=0;OgLakeSdkError error;
    HOLD_INTERRUPTS();
    int status=api->delete_file(root,relative,&limits,&removed,&error);
    RESUME_INTERRUPTS();
    LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
    PG_RETURN_INT64((int64)removed);
}
