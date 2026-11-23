/* Test-only entrypoints for the real candidate header and IEEE transport.
 * This module does not register a product index or replace a native operator. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "candidate_scalar_key.h"
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(lake_test_float4_key);
PG_FUNCTION_INFO_V1(lake_test_float8_key);
PG_FUNCTION_INFO_V1(lake_test_float4_bits);
PG_FUNCTION_INFO_V1(lake_test_float8_bits);
static void CheckAdmin()
{
    /* Retain the complete TLS type for the mandatory offline DWARF gate. */
    knl_thrd_context* volatile thread_context=&t_thrd;(void)thread_context;
    if(u_sess==NULL||!superuser())ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("scalar key component requires administrator")));}
extern "C" PGDLLEXPORT Datum lake_test_float4_key(PG_FUNCTION_ARGS)
{CheckAdmin();PG_RETURN_INT64(LakeScalarOrderKey(PG_GETARG_DATUM(0),FLOAT4OID));}
extern "C" PGDLLEXPORT Datum lake_test_float8_key(PG_FUNCTION_ARGS)
{CheckAdmin();PG_RETURN_INT64(LakeScalarOrderKey(PG_GETARG_DATUM(0),FLOAT8OID));}
extern "C" PGDLLEXPORT Datum lake_test_float4_bits(PG_FUNCTION_ARGS)
{CheckAdmin();int32 bits=PG_GETARG_INT32(0);float4 value;std::memcpy(&value,&bits,sizeof(value));PG_RETURN_FLOAT4(value);}
extern "C" PGDLLEXPORT Datum lake_test_float8_bits(PG_FUNCTION_ARGS)
{CheckAdmin();int64 bits=PG_GETARG_INT64(0);float8 value;std::memcpy(&value,&bits,sizeof(value));PG_RETURN_FLOAT8(value);}
