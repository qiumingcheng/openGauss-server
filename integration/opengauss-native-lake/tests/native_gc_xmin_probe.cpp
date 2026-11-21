/* Test-only imports of the actual host watermark and full-width XID rules.
 * This is not a file-GC implementation and performs no deletion. */
#include "postgres.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/procarray.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(lake_test_gc_oldest_xmin);
PG_FUNCTION_INFO_V1(lake_test_gc_xid_precedes);

extern "C" Datum lake_test_gc_oldest_xmin(PG_FUNCTION_ARGS)
{
    if(!superuser())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("host watermark probe requires administrator")));
    Relation volatile relation=NULL;
    TransactionId oldest=InvalidTransactionId;
    PG_TRY();
    {
        relation=heap_open(PG_GETARG_OID(0),AccessShareLock);
        /* Same conservative entry as native VACUUM. No flags that bypass the
         * host's global cached minima, replication slots or cleanup delay. */
        oldest=GetOldestXmin((Relation)relation,false,false);
        heap_close((Relation)relation,NoLock);relation=NULL;
    }
    PG_CATCH();
    {
        if(relation!=NULL)heap_close((Relation)relation,NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_INT64((int64)oldest);
}

extern "C" Datum lake_test_gc_xid_precedes(PG_FUNCTION_ARGS)
{
    if(!superuser())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("host XID probe requires administrator")));
    TransactionId retired=PG_GETARG_TRANSACTIONID(0),oldest=PG_GETARG_TRANSACTIONID(1);
    PG_RETURN_BOOL(TransactionIdPrecedes(retired,oldest));
}
