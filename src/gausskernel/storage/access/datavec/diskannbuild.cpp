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
 * diskannbuild.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannbuild.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <algorithm>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/freespace.h"
#include "access/datavec/diskann.h"
#include "access/datavec/diskannpagecheck.h"
#include "access/generic_xlog.h"
#include "access/tableam.h"
#include "postmaster/bgworker.h"
#include "commands/vacuum.h"

#define CALLBACK_ITEM_POINTER HeapTuple hup

static void GetBaseData(DiskAnnBuildState* buildstate);
static void SampleCallback(Relation index, CALLBACK_ITEM_POINTER, Datum* values, const bool* isnull, bool tupleIsAlive,
                           void* state);
static void AddSample(Datum* values, DiskAnnBuildState* buildstate);

/*
 * Initialize the build state
 */
static void InitBuildState(DiskAnnBuildState* buildstate, Relation heap, Relation index, IndexInfo* indexInfo,
                           bool parallel)
{
    buildstate->heap = heap;
    buildstate->index = index;
    buildstate->indexInfo = indexInfo;
    buildstate->typeInfo = DiskAnnGetTypeInfo(index);

    DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;

    buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

    /* Disallow varbit since require fixed dimensions */
    if (TupleDescAttr(index->rd_att, 0)->atttypid == VARBITOID) {
        elog(ERROR, "type not supported for diskann index");
    }

    /* Require column to have dimensions to be indexed */
    if (buildstate->dimensions < 0) {
        elog(ERROR, "column does not have dimensions");
    }

    if (buildstate->dimensions > buildstate->typeInfo->maxDimensions) {
        elog(ERROR, "column cannot have more than %d dimensions for diskann index",
             buildstate->typeInfo->maxDimensions);
    }

    buildstate->indexSize = opts ? opts->indexSize : DISKANN_DEFAULT_INDEX_SIZE;

    buildstate->reltuples = 0;
    buildstate->indtuples = 0;

    buildstate->diskannleader = NULL;
    buildstate->diskannshared = NULL;

    /* Get support functions */
    buildstate->procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    buildstate->normprocinfo = DiskAnnOptionalProcInfo(index, DISKANN_NORM_PROC);
    buildstate->kmeansnormprocinfo = DiskAnnOptionalProcInfo(index, DISKANN_KMEANS_NORMAL_PROC);
    buildstate->collation = index->rd_indcollation[0];

    /* Require more than one dimension for spherical k-means */
    if (buildstate->kmeansnormprocinfo != NULL && buildstate->dimensions == 1) {
        elog(ERROR, "dimensions must be greater than one for this opclass");
    }

    buildstate->enablePQ = DiskAnnEnablePQ(index);
    if (buildstate->enablePQ && !buildstate->typeInfo->supportPQ) {
        ereport(ERROR, (errmsg("this data type cannot support diskann pq.")));
    }
    if (buildstate->enablePQ && !g_instance.diskann_pq_inited) {
        ereport(ERROR, (errmsg("this instance has not currently loaded the pq dynamic library.")));
    }

    buildstate->pqM = DiskAnnGetPqM(index);
    if (buildstate->enablePQ) {
        if (buildstate->kmeansnormprocinfo != NULL && buildstate->dimensions == 1) {
            ereport(ERROR, (errmsg("dimensions must be greater than one for this opclass.")));
        }

        if (buildstate->pqM > DISKANN_MAX_PQM) {
            ereport(ERROR, (errmsg("pq_M={%d} exceeds max value {%d} for diskann index, please reset pq_M.",
                                   buildstate->pqM, DISKANN_MAX_PQM)));
        }

        if (buildstate->dimensions % buildstate->pqM != 0) {
            ereport(ERROR, (errmsg("dimensions={%d} must be divisible by pq_M={%d}, please reset pq_M.}",
                                   buildstate->dimensions, buildstate->pqM)));
        }
        // Here we use sizeof(float) since we only support Vector type for now
        buildstate->pqTableSize = sizeof(float) * buildstate->dimensions * GENERIC_DEFAULT_PQ_KSUB;
        buildstate->pqcodeSize = buildstate->pqM * sizeof(uint8);
        buildstate->params = parallel ? NULL : InitDiskPQParams(buildstate);
    } else {
        buildstate->pqTableSize = 0;
        buildstate->pqcodeSize = 0;
        buildstate->params = NULL;
    }

    buildstate->nodeSize = offsetof(DiskAnnNodePageData, pqcode) + buildstate->pqcodeSize;
    buildstate->edgeSize = sizeof(DiskAnnEdgePageData);
    buildstate->itemSize = buildstate->nodeSize + buildstate->edgeSize;
    buildstate->graphStore = nullptr;
    buildstate->tmpCtx =
        AllocSetContextCreate(CurrentMemoryContext, "diskann build temporary context", ALLOCSET_DEFAULT_SIZES);
}

static void FreeBuildState(DiskAnnBuildState* buildstate, bool parallel)
{
    // Only free DiskPQParam's content if not parallel since no allocation when parallel
    if (buildstate->enablePQ && !parallel) {
        // First deallocate memory required with new keyword within PQ API
        FreeDiskPQParams(buildstate->params);
        pfree(buildstate->params);
    }
    if (buildstate->enablePQ && parallel) {
        /* [P0-4] parallel workers now allocate their own PQ wrapper (see
         * DiskAnnParallelScanAndInsert); free the shell, not the
         * Relation-owned arrays it points to. */
        pfree_ext(buildstate->metaPage.params);
    }

    if (buildstate->graphStore) {
        delete buildstate->graphStore;
        buildstate->graphStore = nullptr;
    }
    MemoryContextDelete(buildstate->tmpCtx);
}

static BlockNumber CreateRelationLockPage(Relation index, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    BlockNumber blk;
    buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = DiskAnnInitRegisterPage(index, buf);
    ((PageHeader)page)->pd_lower = MAXALIGN(GetPageHeaderSize(page)) + MaxHeapTupleSize;
    blk = BufferGetBlockNumber(buf);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
    return blk;
}

static bool DiskAnnIndexHasInclude(Relation index);

static void CreateMetaPage(Relation index, DiskAnnBuildState* buildstate, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    DiskAnnMetaPage metap;

    buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = DiskAnnInitRegisterPage(index, buf);

    /* Set metapage data */
    metap = DiskAnnPageGetMeta(page);
    metap->magicNumber = DISKANN_MAGIC_NUMBER;
    /*
     * [P1-3b] INCLUDE indexes are stamped on-disk format v2 (v1 image +
     * appended tail, design doc 9.2). Legacy vector-only indexes stay v1.
     */
    bool hasInclude = DiskAnnIndexHasInclude(index);
    metap->version = hasInclude ? DISKANN_VERSION_V2 : DISKANN_VERSION;
    metap->indexSize = buildstate->indexSize;
    metap->dimensions = buildstate->dimensions;
    metap->nodeSize = buildstate->nodeSize;
    metap->edgeSize = buildstate->edgeSize;
    metap->itemSize = metap->nodeSize + metap->edgeSize;
    metap->insertPage = InvalidBlockNumber;
    metap->extendPageLocker = CreateRelationLockPage(index, forkNum);

    /* set PQ info */
    metap->enablePQ = buildstate->enablePQ;
    metap->pqM = buildstate->pqM;
    metap->pqcodeSize = buildstate->pqcodeSize;
    if (buildstate->enablePQ) {
        metap->pqTableSize = (uint32)buildstate->pqTableSize;
        metap->pqTableNblk = (uint16)((metap->pqTableSize + DISKANN_PQDATA_STORAGE_SIZE - 1) /
            DISKANN_PQDATA_STORAGE_SIZE);
        metap->pqCentroidsSize = (uint32)metap->dimensions * sizeof(float);
        metap->pqCentroidsblk = (uint16)((metap->pqCentroidsSize + DISKANN_PQDATA_STORAGE_SIZE - 1) /
            DISKANN_PQDATA_STORAGE_SIZE);
        metap->pqOffsetSize = (uint32)((metap->pqM + 1) * sizeof(uint32));
        metap->pqOffsetblk = (uint16)((metap->pqOffsetSize + DISKANN_PQDATA_STORAGE_SIZE - 1) /
            DISKANN_PQDATA_STORAGE_SIZE);
    } else {
        metap->pqTableSize = 0;
        metap->pqTableNblk = 0;
        metap->pqCentroidsSize = 0;
        metap->pqCentroidsblk = 0;
        metap->pqOffsetSize = 0;
        metap->pqOffsetblk = 0;
    }
    metap->params = buildstate->params;
    Size metaLen = sizeof(DiskAnnMetaPageData);
    if (hasInclude) {
        /* [P1-3b] append the v2 tail after the pinned v1 image */
        DiskAnnMetaV2Tail* tail = (DiskAnnMetaV2Tail*)(((char*)metap) + DISKANN_META_V1_DISK_LENGTH);
        DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;
        tail->metaBytes = (uint32)(DISKANN_META_V1_DISK_LENGTH + sizeof(DiskAnnMetaV2Tail));
        tail->formatFlags = DISKANN_META_HAS_INCLUDE;
        tail->nIndexAttrs = (uint16)RelationGetNumberOfAttributes(index);
        tail->nKeyAttrs = (uint16)IndexRelationGetNumberOfKeyAttributes(index);
        tail->maxInlinePayload =
            (opts != NULL) ? (uint32)opts->maxInlinePayload : (uint32)DISKANN_DEFAULT_MAX_INLINE_PAYLOAD;
        metaLen = tail->metaBytes;
    }
    ((PageHeader)page)->pd_lower = ((char*)metap + metaLen) - (char*)page;

    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

static BlockNumber GetInsertPage(Relation index)
{
    DiskAnnMetaPageData metap;
    DiskANNGetMetaPageInfo(index, &metap);
    BlockNumber insertPage = metap.insertPage;
    return insertPage;
}

static char* ReserveSpace(Page page, Size size)
{
    PageHeader ph = (PageHeader)page;
    Assert((ph->pd_upper > ph->pd_lower));
    Assert((Size)(uint32)(ph->pd_upper - ph->pd_lower) >= size);
    ph->pd_upper -= (uint16)size;
    char* dst = ((char*)ph) + ph->pd_upper;
    return dst;
}

BlockNumber DiskAnnPageExtension(Relation index, BlockNumber pageLocker, Buffer &buf)
{
    Buffer extensionBufLock = ReadBufferExtended(index, MAIN_FORKNUM, pageLocker, RBM_NORMAL, NULL);
    LockBuffer(extensionBufLock, BUFFER_LOCK_EXCLUSIVE);

    BlockNumber blkid = GetPageWithFreeSpace(index, MaxHeapTupleSize);
    buf = ReadBufferExtended(index, MAIN_FORKNUM, (blkid != InvalidBlockNumber) ? blkid : P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    UnlockReleaseBuffer(extensionBufLock);
    return BufferGetBlockNumber(buf);
}

/*
 * Current implementation compute pq code one vector at a time, we need to wrap the vector into a VectorArray
 * to make it compatible for PQ API, which computes PQ Code in batches
 */
VectorArray CreateVectorForExistingData(float *data, int dim, int len = 1)
{
    VectorArray res = (VectorArray)palloc(sizeof(VectorArrayData));
    res->length = len;
    res->dim = dim;
    res->maxlen = len;
    res->itemsize = 0; // Do not need itemsize within PQ API so set it to 0 for now
    res->items = (char *)(data);
    return res;
}

/*
 * [P1-3a] True when the index carries INCLUDE columns (payload attributes
 * beyond the single vector key). Read straight from pg_index — no on-disk
 * flag needed for this runtime decision.
 */
static bool DiskAnnIndexHasInclude(Relation index)
{
    return index->rd_index != NULL &&
        RelationGetNumberOfAttributes(index) > IndexRelationGetNumberOfKeyAttributes(index);
}

/*
 * [P1-3a] Enforce the max_inline_payload product limit (design doc 9.3):
 * payload bytes = aligned full-tuple size - aligned key-only-tuple size,
 * where the key-only tuple is formed from the same normalized vector.
 * The real page capacity check in InsertVectorIntoPage applies on top of
 * this. The option value comes from the index's parsed reloptions.
 */
static void DiskAnnCheckInlinePayload(Relation index, Datum keyValue, IndexTuple fullItup)
{
    if (!DiskAnnIndexHasInclude(index)) {
        return;
    }
    DiskAnnOptions* opts = (DiskAnnOptions*)index->rd_options;
    if (opts == NULL) {
        return; /* defaults only bound the DDL surface; page check still guards */
    }

    TupleDesc keyDesc = CreateTupleDescCopy(RelationGetDescr(index));
    keyDesc->natts = 1;
    bool keyNull[1] = {false};
    IndexTuple keyItup = index_form_tuple(keyDesc, &keyValue, keyNull);
    Size payloadBytes = MAXALIGN(IndexTupleSize(fullItup)) - MAXALIGN(IndexTupleSize(keyItup));
    pfree(keyItup);
    FreeTupleDesc(keyDesc);

    if (payloadBytes > (Size)opts->maxInlinePayload) {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("DiskANN included columns exceed max_inline_payload"),
                 errdetail("included columns require %lu bytes, max_inline_payload is %d bytes; "
                           "reduce the INCLUDE list or REINDEX with a larger limit",
                           (unsigned long)payloadBytes, opts->maxInlinePayload)));
    }
}

/*
 * Insert vector into page, reserve and initialize node & edge page.
 * [P1-3a] indexValues/indexNulls carry ALL index attributes (key first,
 * then INCLUDE columns); the vector key has already been normalized by
 * the caller and stored at indexValues[0].
 */
static BlockNumber InsertVectorIntoPage(Relation index, Datum* indexValues, const bool* indexNulls,
                                        double sqrSum, ItemPointer heaptid, DiskAnnMetaPage metaPage, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    IndexTuple itup;

    /* form index tuple with the full attribute set */
    itup = index_form_tuple(RelationGetDescr(index), indexValues, indexNulls);
    itup->t_tid = *heaptid;

    Vector* vec = DatumGetVector(indexValues[0]);
    DiskAnnCheckInlinePayload(index, indexValues[0], itup);

    Size itemsz = MAXALIGN(IndexTupleSize(itup));

    /*
     * Full page capacity accounting (design doc §9.3): the aligned index tuple,
     * the reserved node+edge area (metaPage->nodeSize/edgeSize), the line
     * pointer and the page opaque must all fit below/above the page header.
     * Use ereport instead of Assert so release builds reject oversized tuples
     * as well, and check before DiskAnnPageExtension() so a rejected tuple does
     * not leave behind an extended but unused page.
     */
    Size requiredBytes = diskann_page_required_bytes(itemsz, MAXALIGN(metaPage->nodeSize),
                                                     MAXALIGN(metaPage->edgeSize), sizeof(ItemIdData),
                                                     MAXALIGN(sizeof(DiskAnnPageOpaqueData)));
    if (!diskann_page_fits(requiredBytes, BLCKSZ - MAXALIGN(SizeOfPageHeaderData))) {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("DiskANN index tuple is too large"),
                 errdetail("index tuple with node and edge metadata requires %lu bytes, page capacity is %d bytes",
                           (unsigned long)requiredBytes,
                           (int)(BLCKSZ - MAXALIGN(SizeOfPageHeaderData)))));
    }

    /* initialize page */
    DiskAnnPageExtension(index, metaPage->extendPageLocker, buf);
    if (building) {
        page = DiskAnnInitRegisterPage(index, buf);
    } else {
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
        DiskAnnInitPage(page, BufferGetPageSize(buf));
    }

    /* reserve space for node & edge page and add item into page */
    ReserveSpace(page, metaPage->itemSize);
    if (PageAddItem(page, (Item)itup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber) {
        UnlockReleaseBuffer(buf);
        pfree(itup);
        elog(ERROR, "Failed to add index item to \"%s\"", RelationGetRelationName(index));
    }

    /* initialize node page */
    IndexTuple ctup = (IndexTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
    DiskAnnNodePage tup = DiskAnnPageGetNode(ctup);
    tup->sqrSum = (sqrSum >= 0) ? sqrSum : VectorSquareNorm(vec->x, vec->dim);
    tup->master = InvalidBlockNumber;

    for (int pos = 0; pos < DISKANN_HEAPTIDS; pos++) {
        ItemPointerSetInvalid(&tup->heaptids[pos]);
    }
    tup->heaptidsLength = (ItemPointerIsValid(heaptid) ? 1 : 0);
    if (ItemPointerIsValid(heaptid)) {
        tup->heaptids[0] = *heaptid;
    }

    if (metaPage->enablePQ) {
        Size codesize = metaPage->params->pqChunks * sizeof(uint8);
        uint8 *pqcode = (uint8*)palloc(codesize);
        VectorArray arr = CreateVectorForExistingData(vec->x, metaPage->params->dim);
        int ret = ComputeVectorPQCode(arr, metaPage->params, pqcode);
        if (ret != 0) {
            ereport(WARNING, (ret, errmsg("ComputeVectorPQCode failed.")));
        }
        errno_t err = memcpy_s(tup->pqcode, codesize, pqcode, codesize);
        securec_check(err, "\0", "\0");
        arr->items = NULL;
        VectorArrayFree(arr);
    }

    /*  initialize edge page */
    DiskAnnEdgePage etup = (DiskAnnEdgePage)((uint8_t*)tup + metaPage->nodeSize);
    for (uint16_t pos = 0; pos < DISKANN_MAX_DEGREE; pos++) {
        etup->nexts[pos] = InvalidBlockNumber;
        etup->distance[pos] = FLT_MAX;
    }
    etup->count = 0;

    BlockNumber blkno = BufferGetBlockNumber(buf);
    if (building) {
        MarkBufferDirty(buf);
    } else {
        GenericXLogFinish(state);
    }
    UnlockReleaseBuffer(buf);

    pfree(itup);
    return blkno;
}

/*
 * [P1-3a] InsertTuple now takes the full values/isnull arrays for ALL index
 * attributes. The normalized navigation vector replaces attribute 0 in a
 * private copy; INCLUDE payload Datums (attributes 1..natts) are copied by
 * value/pointer only — the caller's memory (callback tmpCtx or the
 * executor's per-tuple context) owns them until index_form_tuple copies
 * the bytes (done synchronously inside InsertVectorIntoPage).
 */
BlockNumber InsertTuple(Relation index, Datum* values, const bool* isnull, ItemPointer heaptid,
                        DiskAnnMetaPage metaPage, bool building)
{
    int natts = RelationGetNumberOfAttributes(index);
    if (natts > INDEX_MAX_KEYS) {
        natts = INDEX_MAX_KEYS;
    }

    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
    /* Normalize if needed */
    FmgrInfo* normprocinfo = DiskAnnOptionalProcInfo(index, DISKANN_NORM_PROC);
    if (normprocinfo != NULL) {
        const DiskAnnTypeInfo* typeInfo = DiskAnnGetTypeInfo(index);
        Oid collation = index->rd_indcollation[0];
        if (!DiskAnnCheckNorm(normprocinfo, collation, value)) {
            return InvalidBlockNumber;
        }

        value = DiskAnnNormValue(typeInfo, collation, value);
    }

    /* form vector */
    Vector* vec = InitVector(metaPage->dimensions);
    errno_t rc = memcpy_s(&vec->x[0], metaPage->dimensions * sizeof(float), ((Vector*)DatumGetPointer(value))->x,
                          metaPage->dimensions * sizeof(float));
    securec_check_c(rc, "\0", "\0");

    /* private full-attribute arrays: replace key with the normalized vector */
    Datum indexValues[INDEX_MAX_KEYS];
    bool indexNulls[INDEX_MAX_KEYS];
    rc = memcpy_s(indexValues, natts * sizeof(Datum), values, natts * sizeof(Datum));
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(indexNulls, natts * sizeof(bool), isnull, natts * sizeof(bool));
    securec_check_c(rc, "\0", "\0");
    indexValues[0] = PointerGetDatum(vec);
    indexNulls[0] = false;

    /* insert into page */
    BlockNumber blkno = InsertVectorIntoPage(index, indexValues, indexNulls,
                                             -1,  // calculate sqrSum by VectorSquareNorm
                                             heaptid, metaPage, building);

    BlockNumber currentPage = GetInsertPage(index);
    if (BlockNumberIsValid(blkno) && blkno != currentPage) {
        DiskAnnUpdateMetaPage(index, blkno, MAIN_FORKNUM, building);
    }

    pfree(vec);
    return blkno;
}

static void AddTupleToSort(Relation index, ItemPointer tid, Datum* values, const bool* isnull,
                           DiskAnnBuildState* buildstate)
{
    BlockNumber blkno = InsertTuple(index, values, isnull, tid, &buildstate->metaPage, true);
    /*
     * [P1-3a] Only valid blocks enter the list and the tuple count (design
     * doc 11.2): NULL keys are skipped earlier by BuildCallback, but a
     * failed norm check returns InvalidBlockNumber here.
     */
    if (!BlockNumberIsValid(blkno)) {
        return;
    }
    // parallel build
    if (buildstate->diskannleader) {
        DiskAnnShared *shared = buildstate->diskannleader->diskannshared;
        SpinLockAcquire(&shared->mutex);
        shared->blocksList.push_back(blkno);
        SpinLockRelease(&shared->mutex);
    } else {
        // serial build
        buildstate->blocksList.push_back(blkno);
    }
    buildstate->indtuples++;
}

static void BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum* values, const bool* isnull, bool tupleIsAlive,
                          void* state)
{
    DiskAnnBuildState* buildstate = (DiskAnnBuildState*)state;
    MemoryContext oldCtx;

    ItemPointer tid = &hup->t_self;

    /* Skip nulls */
    if (isnull[0]) {
        return;
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

    /* Add tuple to sort */
    AddTupleToSort(index, tid, values, isnull, buildstate);

    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Perform a worker's portion of a parallel insert
 */
static void DiskAnnParallelScanAndInsert(Relation heapRel, Relation indexRel, DiskAnnShared* diskannshared)
{
    DiskAnnBuildState buildstate;
    TableScanDesc scan;
    double reltuples;
    IndexInfo* indexInfo;

    /* Join parallel scan */
    indexInfo = BuildIndexInfo(indexRel);
    InitBuildState(&buildstate, heapRel, indexRel, indexInfo, true);
    DiskANNGetMetaPageInfo(indexRel, &buildstate.metaPage);
    /*
     * [P0-4] The metapage read no longer resurrects the persisted params
     * pointer (which was the leader process' address — undefined in this
     * worker). Rebuild PQ params from the relation's cached PQ pages.
     */
    if (buildstate.metaPage.enablePQ) {
        FmgrInfo *workerProcinfo = index_getprocinfo(indexRel, 1, DISKANN_DISTANCE_PROC);
        buildstate.metaPage.params =
            InitDiskPQParamsOnDisk(indexRel, workerProcinfo, buildstate.metaPage.dimensions, true, false);
    }
    DiskAnnLeader tmpLeader;
    tmpLeader.diskannshared = diskannshared;
    buildstate.diskannleader = &tmpLeader;

    scan = tableam_scan_begin_parallel(heapRel, &diskannshared->heapdesc);
    reltuples = tableam_index_build_scan(heapRel, indexRel, indexInfo, true, BuildCallback, (void*)&buildstate, scan);

    /* Record statistics */
    SpinLockAcquire(&diskannshared->mutex);
    diskannshared->nparticipantsdone++;
    diskannshared->reltuples += reltuples;
    SpinLockRelease(&diskannshared->mutex);
    FreeBuildState(&buildstate, true);
}

void DiskAnnParallelBuildMain(const BgWorkerContext* bwc)
{
    DiskAnnShared* diskannshared;
    Relation heapRel;
    Relation indexRel;

    /* Look up shared state */
    diskannshared = (DiskAnnShared*)bwc->bgshared;

    /* Open relations within worker */
    heapRel = heap_open(diskannshared->heaprelid, NoLock);
    indexRel = index_open(diskannshared->indexrelid, NoLock);

    if (diskannshared->flag == ParallelBuildFlag::CREATE_ENTRY_PAGE) {
        DiskAnnParallelScanAndInsert(heapRel, indexRel, diskannshared);
    } else if (diskannshared->flag == ParallelBuildFlag::LINK) {
        uint32 workerId = pg_atomic_fetch_add_u32(&diskannshared->workers, 1);
        size_t totalBlocks = diskannshared->blocksList.size();
        size_t workerBlockCount = totalBlocks / diskannshared->parallelWorker;
        size_t remainder = totalBlocks % diskannshared->parallelWorker;

        if (workerId < remainder) {
            workerBlockCount++;
        }

        size_t start = workerId * (totalBlocks / diskannshared->parallelWorker) + Min(workerId, remainder);
        const BlockNumber* workerBlk = diskannshared->blocksList.begin() + start;
        DiskAnnGraphStore graphStore(indexRel);
        for (size_t i = 0; i < workerBlockCount; i++) {
            DiskAnnGraph graph(indexRel, diskannshared->dimensions, diskannshared->frozen, &graphStore);
            BlockNumber blk = workerBlk[i];
            graph.Link(blk, diskannshared->indexSize, true);
        }
    }

    index_close(indexRel, NoLock);
    heap_close(heapRel, NoLock);
}


static void DiskAnnEndParallel(DiskAnnLeader* diskannleader)
{
    pfree_ext(diskannleader);
    BgworkerListSyncQuit();
}

static double ParallelBuild(DiskAnnBuildState* buildstate, int* nparticipanttuplesorts)
{
    DiskAnnShared* diskannshared = buildstate->diskannleader->diskannshared;
    double reltuples;

    BgworkerListWaitFinish(&buildstate->diskannleader->nparticipanttuplesorts);
    pg_memory_barrier();

    *nparticipanttuplesorts = buildstate->diskannleader->nparticipanttuplesorts;
    reltuples = diskannshared->reltuples;

    return reltuples;
}

static DiskAnnShared* DiskAnnParallelInitshared(DiskAnnBuildState* buildstate)
{
    DiskAnnShared* diskannshared;

    /* Store shared build state, for which we reserved space */
    diskannshared = (DiskAnnShared*)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                                                           sizeof(DiskAnnShared));

    /* Initialize immutable state */
    diskannshared->heaprelid = RelationGetRelid(buildstate->heap);
    diskannshared->indexrelid = RelationGetRelid(buildstate->index);

    SpinLockInit(&diskannshared->mutex);
    diskannshared->blocksList = VectorList<BlockNumber>();

    diskannshared->parallelStrategy = GetAccessStrategy(BAS_BULKWRITE);

    diskannshared->dimensions = buildstate->dimensions;
    diskannshared->workers = 0;
    /* Initialize mutable state */
    diskannshared->nparticipantsdone = 0;
    diskannshared->reltuples = 0;
    HeapParallelscanInitialize(&diskannshared->heapdesc, buildstate->heap);

    return diskannshared;
}

/*
 * Begin parallel build
 */
static void DiskAnnBeginParallel(DiskAnnBuildState* buildstate, int nworkers, ParallelBuildFlag flag)
{
    Buffer buf;
    Page page;
    DiskAnnShared* diskannshared;
    DiskAnnLeader* diskannleader = (DiskAnnLeader*)palloc0(sizeof(DiskAnnLeader));

    Assert(nworkers > 0);

    diskannshared = DiskAnnParallelInitshared(buildstate);
    diskannshared->flag = flag;
    diskannshared->parallelWorker = nworkers;

    buf = ReadBuffer(buildstate->index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    DiskAnnMetaPage metapage = DiskAnnPageGetMeta(page);
    diskannshared->indexSize = metapage->indexSize;
    diskannshared->frozen = metapage->frozenBlkno[0];
    UnlockReleaseBuffer(buf);

    diskannshared->blocksList = buildstate->blocksList;

    /* Launch workers, saving status for leader/caller */
    diskannleader->nparticipanttuplesorts =
        LaunchBackgroundWorkers(nworkers, diskannshared, DiskAnnParallelBuildMain, NULL);
    diskannleader->diskannshared = diskannshared;

    /* If no workers were successfully launched, back out (do serial build) */
    if (diskannleader->nparticipanttuplesorts == 0) {
        DiskAnnEndParallel(diskannleader);
        return;
    }

    /* Log participants */
    ereport(DEBUG1, (errmsg("using %d parallel workers", diskannleader->nparticipanttuplesorts)));

    /* Save leader state now that it's clear build will be parallel */
    buildstate->diskannleader = diskannleader;
}

static double AssignTuples(DiskAnnBuildState* buildstate)
{
    buildstate->reltuples = 0;
    if (buildstate->heap == NULL) {
        return buildstate->reltuples;
    }

    int parallelWorkers = PlanCreateIndexWorkers(buildstate->heap, buildstate->indexInfo);
    if (parallelWorkers > 0) {
        DiskAnnBeginParallel(buildstate, parallelWorkers, ParallelBuildFlag::CREATE_ENTRY_PAGE);
    }

    if (!buildstate->diskannleader) {
    serial_build:
        buildstate->reltuples = tableam_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
                                                         true, BuildCallback, (void*)buildstate, NULL);
    } else {
        int nruns;
        buildstate->reltuples = ParallelBuild(buildstate, &nruns);
        if (nruns == 0) {
            /* failed to startup any bgworker, retry to do serial build */
            goto serial_build;
        }
        DiskAnnShared* shared = buildstate->diskannleader->diskannshared;
        SpinLockAcquire(&shared->mutex);
        buildstate->blocksList = shared->blocksList;
        SpinLockRelease(&shared->mutex);
    }

    /* End parallel build */
    if (buildstate->diskannleader) {
        DiskAnnEndParallel(buildstate->diskannleader);
        buildstate->diskannleader = NULL;
    }

    return buildstate->reltuples;
}

static void CreateEntryPages(DiskAnnBuildState* buildstate)
{
    /* Assign */
    DiskAnnBench("DiskAnn assign tuples", AssignTuples(buildstate));
}

/*
 * [P1-3a] The frozen point is a navigation-only node. For INCLUDE indexes
 * (one row per node, no duplicate merging) it must NOT carry a real heap
 * TID or a real payload: with merging disabled there is no master/slave
 * collapse hiding the duplicate, so a real TID here would return that heap
 * row twice (reproduced by the T2 phase-1 evidence). Legacy vector-only
 * indexes keep the historical behavior (the duplicate-merge path relies on
 * the sampled TID; design doc 10.2).
 */
static BlockNumber GenerateFrozenPoint(DiskAnnBuildState* buildstate)
{
    /* select frozen point randomly */
    int pos = gs_random() % buildstate->blocksList.size();
    BlockNumber frozen = buildstate->blocksList[pos];

    float* vec = (float*)palloc(buildstate->dimensions * sizeof(float));
    double sqrSum = 0;
    ItemPointerData hctid;
    buildstate->graphStore->GetVector(frozen, vec, &(sqrSum), &hctid);

    /* form vector */
    Vector* vector = InitVector(buildstate->dimensions);
    errno_t rc =
        memcpy_s(&vector->x[0], buildstate->dimensions * sizeof(float), vec, buildstate->dimensions * sizeof(float));
    securec_check_c(rc, "\0", "\0");

    Relation index = buildstate->index;
    int natts = Min(RelationGetNumberOfAttributes(index), INDEX_MAX_KEYS);

    Datum frozenValues[INDEX_MAX_KEYS];
    bool frozenNulls[INDEX_MAX_KEYS];
    frozenValues[0] = PointerGetDatum(vector);
    frozenNulls[0] = false;
    if (DiskAnnIndexHasInclude(index)) {
        /* navigation-only: invalid TID, all-NULL payload */
        ItemPointerSetInvalid(&hctid);
        for (int i = 1; i < natts; i++) {
            frozenValues[i] = (Datum)0;
            frozenNulls[i] = true;
        }
    } else {
        for (int i = 1; i < natts; i++) {
            frozenValues[i] = (Datum)0;
            frozenNulls[i] = true;
        }
    }

    /* insert into page */
    BlockNumber blkno = InsertVectorIntoPage(index, frozenValues, frozenNulls,
                                             sqrSum,  // use vec sqrsum
                                             &hctid, &buildstate->metaPage, true);

    pfree(vector);
    pfree(vec);

    return blkno;
}

static void BuildVamanaIndex(DiskAnnBuildState* buildstate)
{
    if (buildstate->blocksList.size() == 0) {
        return;
    }

    BlockNumber frozen = GenerateFrozenPoint(buildstate);
    InsertFrozenPoint(buildstate->index, frozen, true);

    int parallelWorkers = 0;
    if (buildstate->heap != NULL) {
        parallelWorkers = PlanCreateIndexWorkers(buildstate->heap, buildstate->indexInfo);
    }

    if (parallelWorkers > 1 && buildstate->blocksList.size() > parallelWorkers * buildstate->indexSize) {
        DiskAnnBeginParallel(buildstate, parallelWorkers, ParallelBuildFlag::LINK);
    }

    if (buildstate->heap != NULL) {
        if (!buildstate->diskannleader) {
        serial_build:
            for (size_t i = 0; i < buildstate->blocksList.size(); i++) {
                DiskAnnGraph graph(buildstate->index, buildstate->dimensions, frozen, buildstate->graphStore);
                BlockNumber blk = buildstate->blocksList[i];
                graph.Link(blk, buildstate->indexSize, true);
            }
        } else {
            int nruns;
            buildstate->reltuples = ParallelBuild(buildstate, &nruns);
            if (nruns == 0) {
                /* failed to startup any bgworker, retry to do serial build */
                goto serial_build;
            }
        }
    }

    /* End parallel build */
    if (buildstate->diskannleader) {
        DiskAnnEndParallel(buildstate->diskannleader);
    }
}

void InsertFrozenPoint(Relation index, BlockNumber frozen, bool building)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    if (building) {
        page = BufferGetPage(buf);
    } else {
        state = GenericXLogStart(index);
        page = GenericXLogRegisterBuffer(state, buf, 0);
    }
    DiskAnnMetaPage metaPage = DiskAnnPageGetMeta(page);
    if (metaPage->nfrozen < FROZEN_POINT_SIZE) {
        metaPage->frozenBlkno[metaPage->nfrozen] = frozen;
        metaPage->nfrozen++;
        if (building) {
            MarkBufferDirty(buf);
        } else {
            GenericXLogFinish(state);
        }
    }
    UnlockReleaseBuffer(buf);
}

static void GeneratePQData(DiskAnnBuildState* buildstate)
{
    int numSamples;

    /* Skip samples for unlogged table */
    if (buildstate->heap == NULL) {
        numSamples = 1;
    } else {
        double num;
        EstimateRows(buildstate->heap, &num);
        numSamples = (int)num;
        numSamples = std::min(numSamples, DISKANN_MAX_PQ_TRAINING_SIZE);
    }
    PG_TRY();
    {
        /* Sample rows */
        buildstate->samples =
            VectorArrayInit(numSamples, buildstate->dimensions, buildstate->typeInfo->itemSize(buildstate->dimensions));
    }
    PG_CATCH();
    {
        ereport(WARNING, (errmsg("memory alloc failed during PQtable sampling, suggest using diskann without PQ.")));
        PG_RE_THROW();
    }
    PG_END_TRY();
    if (buildstate->heap != NULL) {
        GetBaseData(buildstate);
        if (buildstate->samples->length < GENERIC_DEFAULT_PQ_KSUB) {
            ereport(NOTICE,
                    (errmsg("DiskAnn PQ table created with little data"), errdetail("This will cause low recall."),
                     errhint("Drop the index until the table has more data.")));
        }
    }

    MemoryContext pqCtx = AllocSetContextCreate(CurrentMemoryContext,
                                                "DiskAnn PQ temporary context",
                                                ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldCtx = MemoryContextSwitchTo(pqCtx);
    int ret = ComputePQTable(buildstate->samples, buildstate->params);
    if (ret != 0) {
        ereport(WARNING, (ret, errmsg("ComputePQTable failed.")));
    }
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(pqCtx);
    VectorArrayFree(buildstate->samples);
}

/*
 * Build DiskANN Index
 * 1. Initialize the build state
 * 2. Create Meta page,  MetaPageBlkno = 0
 * 3. Create Entry page, insert vector into index page
 * and reserve node page & edge page for further Vamana build process
 * 4. Build Vamana Index:
 *  Build a frozen point as the starting point for search.
 *  Iterate through vectors stored in the index page,
 *  compute and compare distances between nodes,
 *  and store neighboring nodes' block numbers and distance values in each node's edge page.
 */
static void BuildIndex(Relation heap, Relation index, IndexInfo* indexInfo, DiskAnnBuildState* buildstate,
                       ForkNumber forkNum)
{
    InitBuildState(buildstate, heap, index, indexInfo, false);
    /* [P0-5] record the target fork once; all build-time page creation
     * (metapage, lock page, PQ pages) must write here so the unlogged
     * init-fork template matches the main-fork format. */
    buildstate->forkNum = forkNum;

    if (buildstate->enablePQ) {
        GeneratePQData(buildstate);
    }

    /* Create pages */
    CreateMetaPage(index, buildstate, forkNum);

    if (buildstate->enablePQ) {
        DiskAnnFlushPQInfo(buildstate);
    }

    buildstate->graphStore = New(CurrentMemoryContext) DiskAnnGraphStore(index);
    DiskANNGetMetaPageInfo(index, &buildstate->metaPage);
    /*
     * [P0-4] The metapage reader now forces params=NULL (the persisted slot
     * is not a valid address to hand out). The serial build reattaches the
     * in-process PQ object that CreateMetaPage serialized.
     */
    buildstate->metaPage.params = buildstate->params;
    CreateEntryPages(buildstate);

    BuildVamanaIndex(buildstate);
    
    if (RelationNeedsWAL(index) || forkNum == INIT_FORKNUM)
        LogNewpageRange(index, forkNum, 0, RelationGetNumberOfBlocksInFork(index, forkNum), true);

    FreeBuildState(buildstate, false);
}

static void GetBaseData(DiskAnnBuildState* buildstate)
{
    int targsamples = buildstate->samples->maxlen;
    BlockNumber totalblocks = RelationGetNumberOfBlocks(buildstate->heap);

    buildstate->rowstoskip = -1;
    BlockSampler_Init(&buildstate->bs, totalblocks, targsamples);

    buildstate->rstate = anl_init_selection_state(targsamples);
    while (BlockSampler_HasMore(&buildstate->bs)) {
        BlockNumber targblock = BlockSampler_Next(&buildstate->bs);
        tableam_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo, false, SampleCallback,
                                 (void*)buildstate, NULL, targblock, 1);
    }
}

/*
 * Callback for sampling
 */
static void SampleCallback(Relation index, CALLBACK_ITEM_POINTER, Datum* values, const bool* isnull, bool tupleIsAlive,
                           void* state)
{
    DiskAnnBuildState* buildstate = (DiskAnnBuildState*)state;
    MemoryContext oldCtx;

    if (isnull[0]) {
        return;
    }

    /* Use memory context since detoast can allocate */
    oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);
    /* Add sample */
    AddSample(values, buildstate);
    /* Reset memory context */
    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Add sample
 */
static void AddSample(Datum* values, DiskAnnBuildState* buildstate)
{
    VectorArray samples = buildstate->samples;
    int targsamples = samples->maxlen;

    /* Detoast once for all calls */
    Datum value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

    if (buildstate->kmeansnormprocinfo != NULL) {
        if (!DiskAnnCheckNorm(buildstate->kmeansnormprocinfo, buildstate->collation, value)) {
            return;
        }

        value = DiskAnnNormValue(buildstate->typeInfo, buildstate->collation, value);
    }

    if (samples->length < targsamples) {
        VectorArraySet(samples, samples->length, DatumGetPointer(value));
        samples->length++;
    } else {
        if (buildstate->rowstoskip < 0) {
            buildstate->rowstoskip = anl_get_next_S(samples->length, targsamples, &buildstate->rstate);
        }

        if (buildstate->rowstoskip <= 0) {
            int k = (int)(targsamples * anl_random_fract());
            Assert(k >= 0 && k < targsamples);
            VectorArraySet(samples, k, DatumGetPointer(value));
        }

        buildstate->rowstoskip -= 1;
    }
}

DiskPQParams *InitDiskPQParams(DiskAnnBuildState *buildstate)
{
    DiskPQParams *params = (DiskPQParams*)palloc(sizeof(DiskPQParams));
    params->dim = buildstate->dimensions;
    params->funcType = GetFunctionType(buildstate->procinfo, buildstate->normprocinfo);
    params->pqChunks = buildstate->pqM;
    params->pqTable = NULL;
    params->tablesTransposed = NULL;
    params->centroids = NULL;
    params->offsets = NULL;
    return params;
}

void FreeDiskPQParams(DiskPQParams *params)
{
    if (params == NULL) {
        return;
    }
    if (params->pqTable != NULL) {
        delete[] params->pqTable;
    }
    if (params->tablesTransposed != NULL) {
        delete[] params->tablesTransposed;
    }
    if (params->offsets != NULL) {
        delete[] params->offsets;
    }
    if (params->centroids != NULL) {
        delete[] params->centroids;
    }
}

IndexBuildResult* diskannbuild_internal(Relation heap, Relation index, IndexInfo* indexInfo)
{
    IndexBuildResult* result;
    DiskAnnBuildState buildstate;

    BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

    result = (IndexBuildResult*)palloc(sizeof(IndexBuildResult));
    result->heap_tuples = buildstate.reltuples;
    result->index_tuples = buildstate.indtuples;

    return result;
}

void diskannbuildempty_internal(Relation index)
{
    IndexInfo* indexInfo = BuildIndexInfo(index);
    DiskAnnBuildState buildstate;

    BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}

