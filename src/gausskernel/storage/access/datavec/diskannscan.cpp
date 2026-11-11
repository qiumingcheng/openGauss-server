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
* diskannscan.cpp
*
* IDENTIFICATION
*        src/gausskernel/storage/access/datavec/diskannscan.cpp
*
* -------------------------------------------------------------------------
*/

#include <algorithm>
#include "access/datavec/diskann.h"
#include "access/datavec/diskannfilter.h"
#include "utils/memutils.h"

static Datum GetScanValue(IndexScanDesc scan)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    Datum value;

    if (scan->orderByData->sk_flags & SK_ISNULL) {
        value = PointerGetDatum(NULL);
    } else {
        value = scan->orderByData->sk_argument;

        /* Value should not be compressed or toasted */
        Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
        Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

        /* Normalize if needed */
        if (so->normprocinfo != NULL) {
            value = DirectFunctionCall1Coll(l2_normalize, so->collation, value);
        }
    }

    return value;
}

void diskannrescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    errno_t rc = 0;
    if (keys && scan->numberOfKeys > 0) {
        size_t keySize = (uint32_t)scan->numberOfKeys * sizeof(ScanKeyData);
        rc = memmove_s(scan->keyData, keySize, keys, keySize);
        securec_check(rc, "\0", "\0");
    }
    if (orderbys && scan->numberOfOrderBys > 0) {
        size_t orderBySize = (uint32_t)scan->numberOfOrderBys * sizeof(ScanKeyData);
        rc = memmove_s(scan->orderByData, orderBySize, orderbys, orderBySize);
        securec_check(rc, "\0", "\0");
    }
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    /*
     * [P0-2] Full per-query reset. Baseline only reset curpos, so candidates,
     * the visited set and iteration state leaked across rescans (prepared
     * statements, nested-loop inner scans): a second execution with different
     * parameters returned stale candidates from the previous query first.
     */
    so->curpos = 0;
    /*
     * VectorList::clear() is destructive (frees the backing array and nulls
     * the pointer), so reuse resize(0) which only pops elements and keeps
     * the buffer valid for later push_back in this scan.
     */
    so->candidates.resize(0);
    so->ncandidates = 0;
    so->curIterNum = 0;
    so->nexpextedCandidates = 0;
    blockhash_reset(so->blocks);
    /* [P1-7] the previous xs_itup copy is per-query state */
    if (scan->xs_itup != NULL) {
        scan->xs_itup = NULL;
    }
    MemoryContextReset(so->returnTupleCtx);
    /*
     * [P2-4] KNOWN_ISSUES #7: filter verdicts depend on the parameter values
     * the executor just refreshed; stale verdicts from the previous query
     * would mis-admit/mis-reject (recheck kept results correct but the
     * filter degraded). Reset the cache and counters per rescan.
     */
    if (so->filter != NULL) {
        if (so->filterCache != NULL) {
            filterhash_reset(so->filterCache);
        }
        so->filterEvaluated = 0;
        so->filterRejected = 0;
        so->qualifiedResults = 0;
    }
}

IndexScanDesc diskannbeginscan_internal(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    DiskAnnScanOpaque so;
    DiskAnnMetaPageData metapage;

    DiskANNGetMetaPageInfo(index, &metapage);

    scan = RelationGetIndexScan(index, nkeys, norderbys);
    so = (DiskAnnScanOpaque)palloc(sizeof(DiskAnnScanOpaqueData));
    so->rel = index;
    so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext, "DiskANN scan temporary context", ALLOCSET_DEFAULT_SIZES);
    so->nodeSize = metapage.nodeSize;
    so->nexpextedCandidates = 0;
    so->ncandidates = 0;
    so->curpos = 0;
    so->delSearch = false;
    so->curIterNum = 0;
    so->frozenBlks = VectorList<BlockNumber>();
    so->candidates = VectorList<DiskAnnCandidatesData>();
    so->enablePQ = metapage.enablePQ;
    so->procinfo = index_getprocinfo(index, 1, DISKANN_DISTANCE_PROC);
    DiskPQParams *tmpDiskPQParams = InitDiskPQParamsOnDisk(index, so->procinfo, metapage.dimensions, so->enablePQ);
    if (tmpDiskPQParams != NULL) {
        so->params = *tmpDiskPQParams;
        /*
         * [P0-3] The wrapper struct is all we own: pqTable/tablesTransposed/
         * centroids/offsets point into Relation-owned rd_indexcxt memory and
         * must not (and need not) be freed here. Freeing the wrapper per scan
         * bounds growth for scans driven in one long-lived context.
         */
        pfree_ext(tmpDiskPQParams);
    }
    so->normprocinfo = NULL;
    if (OidIsValid(index_getprocid(index, 1, DISKANN_NORM_PROC)))
        so->normprocinfo = index_getprocinfo(index, 1, DISKANN_NORM_PROC);
    so->collation = index->rd_indcollation[0];

    so->nfrozen = metapage.nfrozen;
    for (uint32_t i = 0; i < metapage.nfrozen; i++) {
        so->frozenBlks.push_back(metapage.frozenBlkno[i]);
    }

    MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);
    so->blocks = blockhash_create(CurrentMemoryContext, VAMANA_CAP, NULL);
    so->queue = (NeighborPriorityQueue *)palloc(sizeof(NeighborPriorityQueue));
    so->queue->_data = VectorList<Neighbor>();
    MemoryContextSwitchTo(oldCtx);

    scan->opaque = so;

    /*
     * [P1-7] Index-only support (design doc 16.2): publish the full index
     * TupleDesc; the executor decides via xs_want_itup whether complete
     * IndexTuples are requested. returnTupleCtx owns the per-candidate
     * xs_itup copy. The AM (not the core) owns xs_orderbyvals/nulls
     * storage - gist allocates them in beginscan, diskann must too, or
     * gettuple's exact-distance fill would write through a NULL pointer.
     */
    /* [P2-3] in-filter runtime initialized empty; executor installs via
     * diskann_set_filter() after beginscan (design doc 17.1 ordering) */
    so->filter = NULL;
    so->filterCache = NULL;
    so->filterEvaluated = 0;
    so->filterRejected = 0;
    so->qualifiedResults = 0;

    scan->xs_itupdesc = RelationGetDescr(index);
    if (norderbys > 0) {
        scan->xs_orderbyvals = (Datum*)palloc0(sizeof(Datum) * norderbys);
        scan->xs_orderbynulls = (bool*)palloc0(sizeof(bool) * norderbys);
    }
    so->returnTupleCtx = AllocSetContextCreate(CurrentMemoryContext,
        "DiskANN return tuple context", ALLOCSET_DEFAULT_SIZES);

    return scan;
}

void diskannendscan_internal(IndexScanDesc scan)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    so->candidates.clear();
    so->frozenBlks.clear();
    if (scan->xs_itup != NULL) {
        scan->xs_itup = NULL;
    }
    so->filter = NULL; /* [P2-3] cache dies with tmpCtx below */
    MemoryContextDelete(so->returnTupleCtx);
    MemoryContextDelete(so->tmpCtx);
    pfree(so);
    scan->opaque = NULL;
}

/*
 * [P2-4] Global exact-distance comparator for strict-batch output (design
 * doc 16.3): ties broken by Heap TID for stability.
 */
static int DiskAnnCandidateDistanceCmp(const void* a, const void* b)
{
    const DiskAnnCandidatesData* ca = (const DiskAnnCandidatesData*)a;
    const DiskAnnCandidatesData* cb = (const DiskAnnCandidatesData*)b;

    if (ca->distance < cb->distance) {
        return -1;
    }
    if (ca->distance > cb->distance) {
        return 1;
    }
    /* stable tie-break on TID */
    if (ca->heapTid.ip_blkid.bi_hi != cb->heapTid.ip_blkid.bi_hi) {
        return ca->heapTid.ip_blkid.bi_hi < cb->heapTid.ip_blkid.bi_hi ? -1 : 1;
    }
    if (ca->heapTid.ip_blkid.bi_lo != cb->heapTid.ip_blkid.bi_lo) {
        return ca->heapTid.ip_blkid.bi_lo < cb->heapTid.ip_blkid.bi_lo ? -1 : 1;
    }
    return ca->heapTid.ip_posid < cb->heapTid.ip_posid ? -1 :
           (ca->heapTid.ip_posid > cb->heapTid.ip_posid ? 1 : 0);
}

/*
* Fetch the next tuple in the given scan
*/
bool diskanngettuple_internal(IndexScanDesc scan, ScanDirection dir)
{
    DiskAnnScanOpaque so = (DiskAnnScanOpaque)scan->opaque;
    MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

    Datum value;

    /* Safety check */
    if (scan->orderByData == NULL)
        elog(ERROR, "cannot scan diskann index without order");

    value = GetScanValue(scan);
    so->value = value;

    /*
     * [P2-4] Strict batch (design doc 16.3): with an installed in-filter,
     * complete ALL needed expansion rounds BEFORE the first output and sort
     * the full accumulated candidate list by exact distance once - a later,
     * wider round must never append a closer candidate behind rows already
     * returned (ORDER BY monotonicity). Rounds continue while fewer than
     * limitk qualified results exist, the iteration cap holds, and the last
     * round still produced new candidates (frontier not exhausted).
     */
    if (so->filter != NULL && scan->limitk > 0) {
        while (so->qualifiedResults < (uint64)scan->limitk && so->curIterNum < MAX_SEARCH_ITERATION) {
            size_t before = so->candidates.size();
            uint64 qualifiedBefore = so->qualifiedResults;

            if (so->curIterNum == 0) {
                so->nexpextedCandidates = (uint32_t)DEFAULT_CADIDATES_NUMBER;
            } else {
                so->nexpextedCandidates *= SEARCH_DOUBLE;
            }
            diskannsearch(so);
            so->ncandidates = (uint32_t)so->candidates.size();
            so->curIterNum++;

            /* frontier exhausted: no new candidates and no new qualifiers */
            if (so->candidates.size() == before && so->qualifiedResults == qualifiedBefore) {
                break;
            }
        }
        /* one global exact-distance sort over the strict batch */
        if (so->ncandidates > 1) {
            qsort(&(so->candidates[0]), so->ncandidates, sizeof(DiskAnnCandidatesData),
                DiskAnnCandidateDistanceCmp);
        }
    } else if ((so->curpos == 0 || so->curpos == so->ncandidates) && so->curIterNum < MAX_SEARCH_ITERATION) {
        if (so->curIterNum == 0) {
            so->nexpextedCandidates = (uint32_t)DEFAULT_CADIDATES_NUMBER;
        } else {
            so->nexpextedCandidates *= SEARCH_DOUBLE;
        }

        diskannsearch(so);
        so->ncandidates = (uint32_t)so->candidates.size();

        so->curIterNum++;
    }

    bool found = false;
    if (so->curpos < so->ncandidates) {
        DiskAnnCandidatesData* cand = &so->candidates[so->curpos];
        scan->xs_ctup.t_self = cand->heapTid;
        scan->xs_recheck = false;
        /* [P1-7] exact distance for ORDER BY consumers (non-lossy rerank) */
        if (scan->numberOfOrderBys > 0) {
            scan->xs_orderbyvals[0] = Float8GetDatum(cand->distance);
            scan->xs_orderbynulls[0] = false;
            scan->xs_recheckorderby = false;
        }

        if (scan->xs_want_itup) {
            /*
             * [P1-7] Index-only: copy the complete IndexTuple into
             * returnTupleCtx after re-checking the node page (state/TID
             * may have changed since the candidate was formed - design
             * doc 16.2). The copy stays valid until the next call.
             */
            scan->xs_itup = NULL;
            MemoryContextReset(so->returnTupleCtx);
            Buffer nodeBuf = ReadBuffer(so->rel, cand->id);
            LockBuffer(nodeBuf, BUFFER_LOCK_SHARE);
            Page nodePage = BufferGetPage(nodeBuf);
            IndexTuple pageItup = DiskAnnPageGetIndexTuple(nodePage);
            bool valid = (DiskAnnPageGetOpaque(nodePage)->pageId == DISKANN_PAGE_ID) &&
                         ItemPointerEquals(&pageItup->t_tid, &cand->heapTid) &&
                         !ItemIdIsDead(PageGetItemId(nodePage, FirstOffsetNumber));
            if (valid) {
                MemoryContext retCtx = MemoryContextSwitchTo(so->returnTupleCtx);
                scan->xs_itup = CopyIndexTuple(pageItup);
                (void)MemoryContextSwitchTo(retCtx);
            }
            UnlockReleaseBuffer(nodeBuf);
            if (!valid) {
                /* stale candidate (concurrent VACUUM/DML): skip it */
                so->curpos++;
                MemoryContextSwitchTo(oldCtx);
                return diskanngettuple_internal(scan, dir);
            }
        }

        so->curpos++;
        found = true;
    }

    MemoryContextSwitchTo(oldCtx);
    PG_RETURN_BOOL(found);
}

void diskannsearch(DiskAnnScanOpaque so)
{
    NeighborPriorityQueue *queue = so->queue;
    VectorList<DiskAnnCandidatesData> *searchedResults = &(so->candidates);
    uint32_t expectedSize = so->nexpextedCandidates;
    float *query = DatumGetVector(so->value)->x;
    int16 dim = DatumGetVector(so->value)->dim;

    queue->clear();
    /*
     * [P0-2] The visited set is per-round state for the full-research MVP:
     * each expansion round restarts from the frozen point with a wider beam.
     * Retaining the previous round's visited marks left the frozen point
     * already-visited and prevented wider rounds from actually expanding
     * (design doc section 4.13). Cross-round result dedup stays with the
     * candidates VectorList below.
     */
    blockhash_reset(so->blocks);
    so->querySqrSum = VectorSquareNorm(query, dim);

    SearchFixedPoint(so);

    for (size_t i = 0; i < queue->size(); i++) {
        queue->_data[i].distance = GetDistance(so, queue->_data[i].id, NULL, NULL);
    }

    qsort(&(queue->_data[0]), queue->size(), sizeof(Neighbor), CmpNeighborInfo);

    uint32_t pos = 0;
    bool hasDuplicates = false;

    for (size_t i = 0; i < queue->size(); ++i) {
        int count = 0;
        if (so->delSearch && queue->_data[i].distance > DISKANN_DISTANCE_THRESHOLD) {
            continue;
        }

        /*
         * [P2-3] In-filter (design doc 16.1): evaluate the INCLUDE payload
         * of each frontier node before it becomes a result candidate.
         * Non-matching nodes still navigated the graph during the search -
         * they simply never enter the candidate list. The verdict is cached
         * per node page across rounds (DiskAnnFilterNode).
         */
        if (so->filter != NULL) {
            bool admitted = false;
            if (!DiskAnnFilterNode(so->rel, so, queue->_data[i].id, &admitted) || !admitted) {
                continue;
            }
        }

        for (size_t j = 0; j < queue->_data[i].heaptidsLength; j++) {
            /* [P1-7] carry the exact distance for ORDER BY consumers */
            DiskAnnCandidatesData candidate = {.id = queue->_data[i].id,
                                               .heapTid = queue->_data[i].heaptids[j],
                                               .distance = (double)queue->_data[i].distance};
            if (!searchedResults->contains(candidate)) {
                searchedResults->push_back(candidate);
                if (so->filter != NULL) {
                    so->qualifiedResults++;
                }
            }
        }
    }

    if (hasDuplicates) {
        size_t start = searchedResults->size() - pos;
        qsort(&((*searchedResults)[start]), (size_t)pos, sizeof(DiskAnnIdxScanData), CmpIdxScanData);
    }
}

void SearchFixedPoint(DiskAnnScanOpaque so)
{
    NeighborPriorityQueue *queue = so->queue;
    blockhash_hash *blocks = so->blocks;
    DiskPQParams* params = &so->params;
    bool enablePQ = so->enablePQ;
    float* queryPQDistance = NULL;
    bool found = false;
    float *query = DatumGetVector(so->value)->x;

    queue->reserve(so->nexpextedCandidates);

    VectorList<BlockNumber> *frozenBlknos = &(so->frozenBlks);

    if (enablePQ) {
        size_t tmpSize = sizeof(float) * params->pqChunks * GENERIC_DEFAULT_PQ_KSUB;
        queryPQDistance = (float*)palloc(tmpSize);
        int ret = GetPQDistanceTable((char *)query, params, queryPQDistance);
        if (ret != 0) {
            ereport(WARNING, (ret, errmsg("GetPQDistanceTable failed.")));
        }
    }

    // Add frozen nodes to the priority queue
    for (size_t i = 0; i < frozenBlknos->size(); i++) {
        BlockNumber master = (*frozenBlknos)[i];
        blockhash_insert(blocks, master, &found);
        if (found) {
            continue;
        }
        Neighbor nn;
        float distance = GetDistance(so, (*frozenBlknos)[i], nn.heaptids, &nn.heaptidsLength);
        nn.id = master;
        nn.distance = distance;
        nn.expanded = false;

        queue->insert(nn);
    }

    // Search the graph
    while (queue->has_unexpanded_node()) {
        size_t idx;
        Neighbor *n = GetNextNeighbor(queue, &idx);
        BlockNumber master = n->id;

        bool deleted = IsMarkDeleted(so->rel, n->id);
        if (deleted) {
            queue->remove(idx);
        }

        uint32_t infoFlag = DISKANN_NEIGHBOR_VECTOR | DISKANN_NEIGHBOR_BLKNO;
        VamanaVertexNbIterator *nbIter = CreateIterator(n->id, so, infoFlag);

        for (nbIter->begin(); !nbIter->isFinished(); nbIter->next()) {
            DiskAnnNeighborInfoT nbinfo = {0};
            BlockNumber nbVertex = nbIter->getCurNeighborInfo(&nbinfo);
            Vector *nVec = (Vector *)DatumGetPointer(nbinfo.vector);
            Assert(DatumGetVector(so->value)->dim == nVec->dim);
            float distance = 0;
            if (!enablePQ) {
                distance = DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation, so->value, PointerGetDatum(nVec)));
            } else {
                uint8_t *curNeighborPQCode = nbIter->curNbtup->pqcode;
                int ret = GetPQDistance(curNeighborPQCode, params, queryPQDistance, distance);
                if (ret != 0) {
                    ereport(WARNING, (ret, errmsg("GetPQDistanceTable failed.")));
                }
            }
            if (nbinfo.freeVector) {
                pfree(DatumGetPointer(nbinfo.vector));
            }
            Neighbor nn = Neighbor(nbVertex, distance);
            nn.heaptidsLength = nbIter->curNbtup->heaptidsLength;
            for (int i = 0; i < nn.heaptidsLength; i++) {
                nn.heaptids[i] = nbIter->curNbtup->heaptids[i];
            }

            queue->insert(nn);
        }
        ReleaseIterator(nbIter);
    }
}

float GetDistance(DiskAnnScanOpaque so, BlockNumber blk, ItemPointer heaptids, uint8* heaptidsLength)
{
    Buffer buf = ReadBuffer(so->rel, blk);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    IndexTuple itup = DiskAnnPageGetIndexTuple(page);
    TupleDesc tupdesc = RelationGetDescr(so->rel);
    bool isnull;
    Datum src = index_getattr(itup, 1, tupdesc, &isnull);
    Datum dst = PointerGetDatum(PG_DETOAST_DATUM(src));
    Vector* vec = (Vector*)DatumGetPointer(dst);
    int16 dim = DatumGetVector(so->value)->dim;
    Assert(dim == vec->dim);
    DiskAnnNodePage tup = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(page));
    float *query = DatumGetVector(so->value)->x;
    if (heaptids != NULL) {
        *heaptidsLength = tup->heaptidsLength;
        for (int i = 0; i < DISKANN_HEAPTIDS; i++) {
            if (!ItemPointerIsValid(&tup->heaptids[i])) {
                break;
            }
            heaptids[i] = tup->heaptids[i];
        }
    }

    float distance = DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation, so->value, PointerGetDatum(vec)));
    if (DatumGetPointer(dst) != DatumGetPointer(src)) {
        pfree(DatumGetPointer(dst));
    }
    UnlockReleaseBuffer(buf);
    return distance;
}

Neighbor *GetNextNeighbor(NeighborPriorityQueue* queue, size_t *idx)
{
    queue->_data[queue->_cur].expanded = true;
    size_t pre = queue->_cur;
    while (queue->_cur < queue->_size && queue->_data[queue->_cur].expanded) {
        queue->_cur++;
    }
    *idx = pre;
    return &queue->_data[pre];
}

VamanaVertexNbIterator *CreateIterator(BlockNumber blk, DiskAnnScanOpaque so, uint32_t infoFlag)
{
    VamanaVertexNbIterator *nbIter = (VamanaVertexNbIterator *)palloc(sizeof(VamanaVertexNbIterator));
    nbIter->rel = so->rel;
    nbIter->nodeSize = so->nodeSize;
    nbIter->blocks = so->blocks;
    nbIter->infoFlag = infoFlag;
    nbIter->curNeighborId = DISKANN_MAX_DEGREE;
    nbIter->skipVisited = true;
    nbIter->curNeighborBuf = InvalidBuffer;
    nbIter->curItup = NULL;
    nbIter->curNbtup = NULL;
    nbIter->nodeBuf = ReadBuffer(so->rel, blk);
    LockBuffer(nbIter->nodeBuf, BUFFER_LOCK_SHARE);
    Page cpage = BufferGetPage(nbIter->nodeBuf);
    DiskAnnNodePage tup_src = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(cpage));
    if (DiskAnnNodeIsSlave(tup_src->tag)) {
        Assert(tup_src->master != InvalidBlockNumber);
        Buffer masterBuf = ReadBuffer(so->rel, tup_src->master);
        LockBuffer(masterBuf, BUFFER_LOCK_SHARE);
        Page masterPage = BufferGetPage(masterBuf);
        tup_src = DiskAnnPageGetNode(DiskAnnPageGetIndexTuple(masterPage));
        UnlockReleaseBuffer(masterBuf);
        Assert(DiskAnnNodeIsMaster(tup_src->tag));
    }
    nbIter->edges = (DiskAnnEdgePage)((uint8_t *)tup_src + nbIter->nodeSize);

    return nbIter;
}

void ReleaseIterator(VamanaVertexNbIterator *iter)
{
    if (iter->nodeBuf != InvalidBuffer) {
        UnlockReleaseBuffer(iter->nodeBuf);
    }
    pfree(iter);
}

DiskAnnAliveSlaveIterator *CreateSlaveIterator(Relation index, BlockNumber vertex, float *query, uint16_t dim, double sqrsum)
{
    DiskAnnAliveSlaveIterator *iter = (DiskAnnAliveSlaveIterator *)palloc(sizeof(DiskAnnAliveSlaveIterator));
    iter->index = index;
    iter->master = vertex;
    iter->dim = dim;
    iter->curVertex = InvalidBlockNumber;
    iter->nextVertex = InvalidBlockNumber;
    return iter;
}

