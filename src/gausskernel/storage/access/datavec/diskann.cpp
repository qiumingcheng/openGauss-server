/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * diskann.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskann.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/reloptions.h"
#include "storage/freespace.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"
#include "access/datavec/diskann.h"
#include "access/datavec/diskanncostmodel.h"

/*
 * Estimate the cost of an index scan
 */
static void diskanncostestimate_internal(PlannerInfo* root, IndexPath* path, double loopCount, Cost* indexStartupCost,
                                         Cost* indexTotalCost, Selectivity* indexSelectivity, double* indexCorrelation)
{
    GenericCosts costs;
    Relation index;

    Assert(indexTotalCost != nullptr);
    Assert(indexStartupCost != nullptr);
    Assert(indexSelectivity != nullptr);
    Assert(indexCorrelation != nullptr);

    /* Never use index without order */
    if (path->indexorderbys == NULL) {
        *indexStartupCost = DBL_MAX;
        *indexTotalCost = DBL_MAX;
        *indexSelectivity = 0;
        *indexCorrelation = 0;
        return;
    }

    errno_t rc = memset_s(&costs, sizeof(costs), 0, sizeof(costs));
    securec_check_c(rc, "\0", "\0");

    index = index_open(path->indexinfo->indexoid, NoLock);

    index_close(index, NoLock);

    costs.numIndexTuples = path->indexinfo->tuples;

    genericcostestimate(root, path, loopCount, costs.numIndexTuples, &costs.indexStartupCost, &costs.indexTotalCost,
                        &costs.indexSelectivity, &costs.indexCorrelation);

    /*
     * [P2-5] DiskANN-aware adjustments (design doc 13.5), layered on the
     * generic estimate WITHOUT touching other AMs:
     *  - with classified in-filter predicates (annQuals) and a bound K, the
     *    first-round width becomes L = ceil(K/max(s,s_min))*oversample and
     *    the heap-side candidate count drops to C = clamp(K/s, K, N). The
     *    generic estimate assumes whole-index traversal, so discount the
     *    tuple-processing portion by the visited/total ratio and publish C/N
     *    as indexSelectivity - the heap-fetch billing in cost_index()'s
     *    common part then shrinks automatically (13.5 attribution table).
     *  - unfiltered scans keep the generic numbers (zero-regression by
     *    construction).
     */
    if (path->annQuals != NIL) {
        int64_t limitK = (int64_t)path->annCount;
        int64_t nTuples = (int64_t)path->indexinfo->tuples;
        int64_t probes = (int64_t)u_sess->datavec_ctx.diskann_probes;
        int64_t widthL;
        int64_t heapC;
        double visitRatio;
        Selectivity filterSel = path->annQualSelectivity > 0.0 ? path->annQualSelectivity : 1.0;

        if (limitK <= 0) {
            limitK = 1; /* unknown K at plan time: minimal bound */
        }
        if (probes < 1) {
            probes = 1;
        }
        widthL = diskann_cost_search_width(limitK, filterSel, probes, DISKANN_COST_MAX_SEARCH_BUDGET);
        heapC = diskann_cost_heap_candidates(limitK, filterSel, nTuples);

        if (nTuples > 0 && widthL < nTuples) {
            visitRatio = (double)widthL / (double)nTuples;
            costs.indexTotalCost *= (0.2 + 0.8 * visitRatio); /* floor 20%: entry/rerank fixed work */
        }
        if (nTuples > 0) {
            double selRatio = (double)heapC / (double)nTuples;
            if (selRatio < 0.0) {
                selRatio = 0.0;
            }
            if (selRatio > 1.0) {
                selRatio = 1.0;
            }
            costs.indexSelectivity = selRatio;
        }
    }

    /* Use total cost since most work happens before first tuple is returned */
    *indexStartupCost = costs.indexTotalCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
}

/*
 * Parse and validate the reloptions
 */
static bytea* diskannoptions_internal(Datum reloptions, bool validate)
{
    static const relopt_parse_elt tab[] = {
        {"index_size", RELOPT_TYPE_INT, offsetof(DiskAnnOptions, indexSize)},
        {"enable_pq", RELOPT_TYPE_BOOL, offsetof(DiskAnnOptions, enablePQ)},
        {"pq_m", RELOPT_TYPE_INT, offsetof(DiskAnnOptions, pqM)},
        /* [P1-2] must stay paired with the reloptions.cpp registration table */
        {"in_filter", RELOPT_TYPE_BOOL, offsetof(DiskAnnOptions, inFilter)},
        {"max_inline_payload", RELOPT_TYPE_INT, offsetof(DiskAnnOptions, maxInlinePayload)}};

    relopt_value* options;
    int numoptions;
    DiskAnnOptions* rdopts;

    options = parseRelOptions(reloptions, validate, RELOPT_KIND_DISKANN, &numoptions);
    rdopts = (DiskAnnOptions*)allocateReloptStruct(sizeof(DiskAnnOptions), options, numoptions);
    fillRelOptions((void*)rdopts, sizeof(DiskAnnOptions), options, numoptions, validate, tab, lengthof(tab));

    return (bytea*)rdopts;
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool diskannvalidate_internal(Oid opclassoid)
{
    return true;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannhandler);
Datum diskannhandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine* amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = DISKANN_FUNC_NUM;
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false; /* can change direction mid-scan */
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = false;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    /* [P1-1] INCLUDE columns accepted at DDL level (design doc section 8.2);
     * restrictions enforced by CheckDiskAnnIncludeRestrictions in indexcmds.cpp */
    amroutine->amcaninclude = true;
    amroutine->amkeytype = InvalidOid;

    /* Interface functions */
    errno_t rc = 0;
    rc = strcpy_s(amroutine->ambuildfuncname, NAMEDATALEN, "diskannbuild");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->ambuildemptyfuncname, NAMEDATALEN, "diskannbuildempty");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->aminsertfuncname, NAMEDATALEN, "diskanninsert");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->ambulkdeletefuncname, NAMEDATALEN, "diskannbulkdelete");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amvacuumcleanupfuncname, NAMEDATALEN, "diskannvacuumcleanup");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amcostestimatefuncname, NAMEDATALEN, "diskanncostestimate");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amoptionsfuncname, NAMEDATALEN, "diskannoptions");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amvalidatefuncname, NAMEDATALEN, "diskannvalidate");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->ambeginscanfuncname, NAMEDATALEN, "diskannbeginscan");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amrescanfuncname, NAMEDATALEN, "diskannrescan");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amgettuplefuncname, NAMEDATALEN, "diskanngettuple");
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(amroutine->amendscanfuncname, NAMEDATALEN, "diskannendscan");
    securec_check(rc, "\0", "\0");

    PG_RETURN_POINTER(amroutine);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannbuild);
Datum diskannbuild(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo* indexinfo = (IndexInfo*)PG_GETARG_POINTER(2);
    IndexBuildResult* result = diskannbuild_internal(heap, index, indexinfo);

    PG_RETURN_POINTER(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannbuildempty);
Datum diskannbuildempty(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    Relation index = (Relation)PG_GETARG_POINTER(0);
    diskannbuildempty_internal(index);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskanninsert);
Datum diskanninsert(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    Datum* values = (Datum*)PG_GETARG_POINTER(1);
    bool* isnull = reinterpret_cast<bool*>(PG_GETARG_POINTER(2));
    ItemPointer ht_ctid = (ItemPointer)PG_GETARG_POINTER(3);
    Relation heaprel = (Relation)PG_GETARG_POINTER(4);
    IndexUniqueCheck checkunique = (IndexUniqueCheck)PG_GETARG_INT32(5);
    bool result = diskanninsert_internal(rel, values, isnull, ht_ctid, heaprel, checkunique);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannbulkdelete);
Datum diskannbulkdelete(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    IndexVacuumInfo* info = (IndexVacuumInfo*)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult* volatile stats = (IndexBulkDeleteResult*)PG_GETARG_POINTER(1);
    IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback)PG_GETARG_POINTER(2);
    void* callbackState = static_cast<void*>(PG_GETARG_POINTER(3));
    stats = diskannbulkdelete_internal(info, stats, callback, callbackState);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannvacuumcleanup);
Datum diskannvacuumcleanup(PG_FUNCTION_ARGS)
{
    if (IsExtremeRedo()) {
        elog(ERROR, "diskann index do not support extreme rto.");
    }
    IndexVacuumInfo* info = (IndexVacuumInfo*)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult* stats = (IndexBulkDeleteResult*)PG_GETARG_POINTER(1);
    stats = diskannvacuumcleanup_internal(info, stats);

    PG_RETURN_POINTER(stats);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskanncostestimate);
Datum diskanncostestimate(PG_FUNCTION_ARGS)
{
    PlannerInfo* root = (PlannerInfo*)PG_GETARG_POINTER(0);
    IndexPath* path = (IndexPath*)PG_GETARG_POINTER(1);
    double loopcount = static_cast<double>(PG_GETARG_FLOAT8(2));
    Cost* startupcost = (Cost*)PG_GETARG_POINTER(3);
    Cost* totalcost = (Cost*)PG_GETARG_POINTER(4);
    Selectivity* selectivity = (Selectivity*)PG_GETARG_POINTER(5);
    double* correlation = reinterpret_cast<double*>(PG_GETARG_POINTER(6));
    diskanncostestimate_internal(root, path, loopcount, startupcost, totalcost, selectivity, correlation);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannoptions);
Datum diskannoptions(PG_FUNCTION_ARGS)
{
    Datum reloptions = PG_GETARG_DATUM(0);
    bool validate = PG_GETARG_BOOL(1);
    bytea* result = diskannoptions_internal(reloptions, validate);

    if (NULL != result) {
        PG_RETURN_BYTEA_P(result);
    }

    PG_RETURN_NULL();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannvalidate);
Datum diskannvalidate(PG_FUNCTION_ARGS)
{
    Oid opclassoid = PG_GETARG_OID(0);
    bool result = diskannvalidate_internal(opclassoid);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannbeginscan);
Datum diskannbeginscan(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    int nkeys = PG_GETARG_INT32(1);
    int norderbys = PG_GETARG_INT32(2);
    IndexScanDesc scan = diskannbeginscan_internal(rel, nkeys, norderbys);

    PG_RETURN_POINTER(scan);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannrescan);
Datum diskannrescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanKey scankey = (ScanKey)PG_GETARG_POINTER(1);
    int nkeys = PG_GETARG_INT32(2);
    ScanKey orderbys = (ScanKey)PG_GETARG_POINTER(3);
    int norderbys = PG_GETARG_INT32(4);
    diskannrescan_internal(scan, scankey, nkeys, orderbys, norderbys);

    PG_RETURN_VOID();
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskanngettuple);
Datum diskanngettuple(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanDirection direction = (ScanDirection)PG_GETARG_INT32(1);

    if (NULL == scan) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid arguments for function diskanngettuple")));
    }

    bool result = diskanngettuple_internal(scan, direction);

    PG_RETURN_BOOL(result);
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(diskannendscan);
Datum diskannendscan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    diskannendscan_internal(scan);

    PG_RETURN_VOID();
}

bool diskanninsert_internal(Relation index, Datum* values, const bool* isnull, ItemPointer heap_tid, Relation heap,
                            IndexUniqueCheck checkUnique)
{
    Datum dst = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
    Vector* value = (Vector*)DatumGetPointer(dst);

    DiskAnnMetaPageData metapage;
    DiskANNGetMetaPageInfo(index, &metapage);
    FmgrInfo* procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    metapage.params = InitDiskPQParamsOnDisk(index, procinfo, metapage.dimensions, metapage.enablePQ, true);

    BlockNumber blkno = InsertTuple(index, values, isnull, heap_tid, &metapage, false);

    if (0 == metapage.nfrozen) {
        /*
         * [P1-3a] First online insert creates the frozen navigation node.
         * For INCLUDE indexes the frozen node must be navigation-only:
         * invalid TID and all-NULL payload (design doc 10.2) — reusing the
         * real row's values would store a payload no heap row owns.
         */
        ItemPointerData hctid;
        ItemPointerSetInvalid(&hctid);

        int natts = Min(RelationGetNumberOfAttributes(index), INDEX_MAX_KEYS);
        Datum frozenValues[INDEX_MAX_KEYS];
        bool frozenNulls[INDEX_MAX_KEYS];
        frozenValues[0] = values[0];
        frozenNulls[0] = false;
        for (int i = 1; i < natts; i++) {
            frozenValues[i] = (Datum)0;
            frozenNulls[i] = true;
        }

        BlockNumber frozen = InsertTuple(index, frozenValues, frozenNulls, &hctid, &metapage, false);
        InsertFrozenPoint(index, frozen, false);
        DiskANNGetMetaPageInfo(index, &metapage);
    }

    DiskAnnGraphStore* graphStore = New(CurrentMemoryContext) DiskAnnGraphStore(index);
    DiskAnnGraph graph(index, metapage.dimensions, metapage.frozenBlkno[0], graphStore);
    /*
     * [P0-3] Destroy the graph store and the PQ wrapper on both the normal
     * and the error path. They were previously left to context recycling,
     * which accumulates per-INSERT inside one long statement (e.g. bulk
     * INSERT ... SELECT). The PQ arrays inside the wrapper are Relation-owned
     * and survive pfree_ext of the wrapper itself.
     */
    PG_TRY();
    {
        graph.Link(blkno, metapage.indexSize, false);
    }
    PG_CATCH();
    {
        delete graphStore;
        pfree_ext(metapage.params);
        PG_RE_THROW();
    }
    PG_END_TRY();
    delete graphStore;
    pfree_ext(metapage.params);

    return false;
}
IndexBulkDeleteResult* diskannbulkdelete_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats,
                                                  IndexBulkDeleteCallback callback, void* callbackState)
{
    return NULL;
}

/*
 * [P1-3b] Format-v2 (INCLUDE) DiskANN indexes defer deletion to VACUUM:
 * immediate edge-breaking on UPDATE/DELETE destroys old snapshots and
 * graph connectivity (design doc 4.12/12.2). Dispatched from
 * ExecOpenIndices and re-checked at the tableam call sites.
 */
bool DiskAnnIndexUsesDeferredDelete(Relation index)
{
    DiskAnnMetaPageData metapage;
    DiskANNGetMetaPageInfo(index, &metapage);
    return metapage.version >= DISKANN_VERSION_V2;
}

/*
 * [P1-4] pg_am.amcanreturn entry, invoked by index_can_return() with the
 * index relation (diskann is in builtin_indexes_without_amhandler, so the
 * catalog function is used rather than an IndexAmRoutine callback). Only
 * indexes that actually carry INCLUDE columns can return IndexTuples:
 * this keeps legacy indexes (and the possibly-normalized vector key,
 * design doc 4.9) out of index-only paths until P1-5/P1-7 land the
 * per-column returnable logic and the index-only executor branch.
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(diskanncanreturn);
Datum diskanncanreturn(PG_FUNCTION_ARGS)
{
    Relation index = (Relation)PG_GETARG_POINTER(0);
    bool hasInclude = index->rd_index != NULL &&
        RelationGetNumberOfAttributes(index) > IndexRelationGetNumberOfKeyAttributes(index);
    PG_RETURN_BOOL(hasInclude);
}
IndexBulkDeleteResult* diskannvacuumcleanup_internal(IndexVacuumInfo* info, IndexBulkDeleteResult* stats)
{
    if (!stats) {
        /*
         * [P0-6] Called without a prior ambulkdelete (VACUUM that found no
         * dead tuples / VACUUM ANALYZE). The baseline returned all-zero
         * stats, wiping pg_class relpages/reltuples for the index
         * (reproduced: 103 pages / 100 tuples -> 0/0), which poisons the
         * planner's cost model. Report the real page count and an estimated
         * tuple count instead (design doc section 12.3.1). A real
         * pageType-scanning bulkdelete lands with on-disk format v2 (P1).
         */
        stats = (IndexBulkDeleteResult*)palloc0(sizeof(IndexBulkDeleteResult));
        stats->num_pages = RelationGetNumberOfBlocks(info->index);
        stats->num_index_tuples = info->num_heap_tuples;
        stats->estimated_count = true;
    }

    FreeSpaceMapVacuum(info->index);

    return stats;
}
