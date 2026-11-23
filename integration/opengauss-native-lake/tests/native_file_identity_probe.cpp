/* Test-only loadable diagnostic; never installed by the product extension. */
#include "postgres.h"
#include "lake_file_reader.h"
#include "lake_inline_reader.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(lake_test_file_identities);
extern "C" Datum lake_test_file_identities(PG_FUNCTION_ARGS)
{
    if(!superuser())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("file identity diagnostic requires administrator")));
    Oid relation=PG_GETARG_OID(0);
    LakeInlineBinding* binding=LakeLoadInlineBinding(relation,GetActiveSnapshot());
    if(binding->column_count!=2||binding->columns[0].type!=INT4OID||binding->columns[1].type!=INT4OID)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("file diagnostic requires the two-int test schema")));
    Datum values[2];bool nulls[2];LakeInlineRowIdentity identity;
    LakeFileReader* volatile reader=NULL;StringInfoData result;initStringInfo(&result);appendStringInfoChar(&result,'[');
    int count=0;
    PG_TRY();
    {
        reader=LakeFileReaderOpen(binding);
        while(LakeFileReaderNext(reader,values,nulls,&identity)){
            if(count>=32768||result.len>8*1024*1024)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("file identity diagnostic limit exceeded")));
            if(count++)appendStringInfoChar(&result,',');
            if(nulls[0])ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("test identity key is NULL")));
            appendStringInfo(&result,"[%d,%u,%lld,%lld,%llu,%llu]",DatumGetInt32(values[0]),(unsigned)identity.source,
                (long long)identity.row_id,(long long)identity.begin_snapshot,
                (unsigned long long)identity.file_id,(unsigned long long)identity.file_position);
        }
        LakeFileReaderClose(reader);reader=NULL;
    }
    PG_CATCH();
    {
        LakeFileReaderClose(reader);PG_RE_THROW();
    }
    PG_END_TRY();
    appendStringInfoChar(&result,']');PG_RETURN_TEXT_P(cstring_to_text_with_len(result.data,result.len));
}
