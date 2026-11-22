/* Native ANALYZE: exact inline stream, bounded file physical-domain sample. */
#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_inline_sources.h"
#include "lake_autoanalyze.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_inline_reader.h"
#include "lake_sample.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "commands/sec_rls_cmds.h"
#include "commands/vacuum.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "storage/buf/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"

/* Test hook has no installed SQL entry; separately reviewed native probe. */
extern void LakeTestAnalyzeAfterRow(Oid relationOid, uint64 visibleRows);

bool LakeIsNativeForeignRelation(Relation relation)
{
    if (relation == NULL || RelationGetRelkind(relation) != RELKIND_FOREIGN_TABLE)
        return false;
    return LakeTrustedForeignServer(GetForeignTable(RelationGetRelid(relation))->serverid);
}

static void LakeAnalyzeAdmission(Relation relation, void* additionalData, bool estimate)
{
    if (!LakeIsNativeForeignRelation(relation))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("native lake ANALYZE requires a trusted lake foreign table")));
    if (!IS_SINGLE_NODE || StreamThreadAmI() || additionalData != NULL || estimate ||
        RelationIsPartitioned(relation) || relation->rd_rel->relhassubclass)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ANALYZE currently supports explicit single-node nonpartitioned tables")));
    if (default_statistics_target <= 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ANALYZE requires a positive default_statistics_target")));
    TupleDesc desc = RelationGetDescr(relation);
    for (int i = 0; i < desc->natts; ++i)
        if (!desc->attrs[i].attisdropped && desc->attrs[i].attstattarget < -1)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake ANALYZE does not support percentage column statistics targets")));
    /* analyze_rel has already applied native owner/ACL_VACUUM admission.
     * This utility has no executor RTE and must not require unrelated SELECT ACL. */
    LakeCheckMaskingAdmission();
    if (RelationHasRlspolicy(RelationGetRelid(relation)) || RelationEnableRowSecurity(relation))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ANALYZE with row security is not supported")));
    LakeRequireAccessPolicy();
    LakeTxnRejectBoundSnapshotWrite(GetForeignTable(RelationGetRelid(relation))->serverid);
}

/* Whole carrier data storage only. Indexes, VM/FSM and metadata are excluded.
 * The physical measurement is approximate during concurrent extension; it
 * does not control logical visibility or row counts. */
BlockNumber LakeInlinePhysicalPages(const LakeInlineBinding* binding)
{
    if (LakeInlineReadSourceCount(binding)==0) return 0;
    Relation volatile carrier = NULL;
    Relation volatile toast = NULL;
    uint64 pages = 0;
    PG_TRY();
    {
        for(int source=0;source<LakeInlineReadSourceCount(binding);++source){
            carrier = heap_open(LakeInlineReadSourceOid(binding,source), AccessShareLock);
            pages += RelationGetNumberOfBlocks((Relation)carrier);
            Oid toastOid = ((Relation)carrier)->rd_rel->reltoastrelid;
            if (OidIsValid(toastOid)) {
                toast = heap_open(toastOid, AccessShareLock);
                if (RelationGetRelkind((Relation)toast) != RELKIND_TOASTVALUE ||
                    ((Relation)toast)->rd_rel->relowner != ((Relation)carrier)->rd_rel->relowner)
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("native lake inline TOAST dependency is not trusted")));
                pages += RelationGetNumberOfBlocks((Relation)toast);
                heap_close((Relation)toast, NoLock);
                toast = NULL;
            }
            heap_close((Relation)carrier, NoLock);
            carrier = NULL;
            if (pages > (uint64)MaxBlockNumber)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("native lake ANALYZE data size exceeds its page-count interface")));
        }
    }
    PG_CATCH();
    {
        if (toast != NULL) heap_close((Relation)toast, NoLock);
        if (carrier != NULL) heap_close((Relation)carrier, NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return (BlockNumber)pages;
}

static int LakeAcquireSampleRows(Relation relation, int elevel, HeapTuple* rows,
    int targrows, double* totalrows, double* totaldeadrows,
    void* additionalData, bool estimate)
{
    LakeAnalyzeAdmission(relation, additionalData, estimate);
    if (targrows <= 0 || (Size)targrows > MaxAllocSize / sizeof(LakeAnalyzeSample))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("invalid native lake ANALYZE sample size")));
    MemoryContext caller = CurrentMemoryContext;
    MemoryContext volatile scratch = AllocSetContextCreate(caller, "Native lake ANALYZE row",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    LakeAnalyzeSample* sample = (LakeAnalyzeSample*)palloc0(sizeof(LakeAnalyzeSample) * targrows);
    TupleDesc desc = RelationGetDescr(relation);
    Datum* values = (Datum*)palloc0(sizeof(Datum) * desc->natts);
    bool* nulls = (bool*)palloc0(sizeof(bool) * desc->natts);
    LakeInlineReader* volatile reader = NULL;
    LakeTxnTable* volatile table = NULL;
    int count = 0;
    uint64 seen = 0;
    double skip = -1;
    double rstate = 0;
    *totalrows = 0;
    *totaldeadrows = 0;
    PG_TRY();
    {
        /* Capture before metadata SPI. No row modifications or user callbacks
         * run between capture and the end of this visible stream. */
        Snapshot host = ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot();
        CommandId commandLimit = GetCurrentCommandId(false);
        table = LakeTxnReadStatementTable(RelationGetRelid(relation), host);
        LakeAutoAnalyzeSampleStart(relation,((LakeTxnTable*)table)->binding,host);
        bool files=((LakeTxnTable*)table)->binding->file_count!=0;
        if(files)count=LakeAcquireFileSampleRows(relation,(LakeTxnTable*)table,commandLimit,
            rows,targrows,totalrows,totaldeadrows,elevel);
        else {
            rstate=anl_init_selection_state(targrows);
            reader = LakeInlineReaderOpen((LakeTxnTable*)table, commandLimit, true);
            for (;;) {
                MemoryContextReset(scratch);
                MemoryContextSwitchTo(scratch);
                LakeInlineRowIdentity identity;
                bool found = LakeInlineReaderNext((LakeInlineReader*)reader, values, nulls, &identity);
                MemoryContextSwitchTo(caller);
                if (!found) break;
                /* totalrows uses double in the host ABI. Do not silently lose unit
                 * precision and label the result an exact full-stream count. */
                if (seen >= UINT64CONST(9007199254740991))
                    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("native lake ANALYZE visible count exceeds exact host representation")));
                int slot = -1;
                if (count < targrows) slot = count++;
                else {
                    if (skip < 0) skip = anl_get_next_S((double)seen, targrows, &rstate);
                    if (skip <= 0) slot = (int)(targrows * anl_random_fract());
                    skip -= 1;
                }
                if (slot >= 0) {
                    Assert(slot < targrows);
                    /* Form first: an ERROR leaves the old entry owned and intact. */
                    HeapTuple next = LakeFormHostTuple(((LakeTxnTable*)table)->binding,desc,values,nulls);
                    if (sample[slot].tuple != NULL) heap_freetuple(sample[slot].tuple);
                    sample[slot].tuple = next;
                    sample[slot].ordinal = seen;
                }
                ++seen;
                LakeTestAnalyzeAfterRow(RelationGetRelid(relation), seen);
            }
        }
        LakeInlineReaderClose((LakeInlineReader*)reader);
        reader = NULL;
        LakeTxnReleaseReadView((LakeTxnTable*)table);table=NULL;
        MemoryContextDelete(scratch);
        scratch = NULL;
        /* Restore physical stream order for native correlation calculations. */
        if(!files){
            qsort(sample, count, sizeof(LakeAnalyzeSample), LakeCompareSampleOrdinal);
            for (int i = 0; i < count; ++i) {
                rows[i] = sample[i].tuple;
                sample[i].tuple = NULL;
            }
            *totalrows = (double)seen;
        }
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if (reader != NULL) LakeInlineReaderClose((LakeInlineReader*)reader);
        if (table != NULL) LakeTxnReleaseReadView((LakeTxnTable*)table);
        /* The heap sample array is stable across longjmp; local count is not. */
        for (int i = 0; i < targrows; ++i)
            if (sample[i].tuple != NULL) heap_freetuple(sample[i].tuple);
        if (scratch != NULL) MemoryContextDelete(scratch);
        PG_RE_THROW();
    }
    PG_END_TRY();
    pfree(values);
    pfree(nulls);
    pfree(sample);
    LakeAutoAnalyzeSampleReturned(RelationGetRelid(relation));
    ereport(elevel, (errmsg("\"%s\": native lake analyzed %.0f visible rows, %d rows in sample",
        RelationGetRelationName(relation), *totalrows, count)));
    return count;
}

static bool LakeAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func,
    BlockNumber* totalpages, void* additionalData, bool estimate)
{
    LakeAnalyzeAdmission(relation, additionalData, estimate);
    Snapshot host = ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot();
    LakeTxnTable* table = LakeTxnReadStatementTable(RelationGetRelid(relation), host);
    if(table->pending!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ANALYZE requires a committed table definition")));
    *func = LakeAcquireSampleRows;
    /* Keep native analyzed-empty bookkeeping out of physical I/O costing. */
    *totalpages = Max((BlockNumber)1, LakeAnalyzePhysicalPages(table->binding));
    LakeTxnReleaseReadView(table);
    return true;
}

void LakeInstallAnalyzeCallbacks(FdwRoutine* routine)
{
    routine->AnalyzeForeignTable = LakeAnalyzeForeignTable;
    routine->AcquireSampleRows = LakeAcquireSampleRows;
}
