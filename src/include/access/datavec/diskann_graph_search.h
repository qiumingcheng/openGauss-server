/*
 * Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 * openGauss is licensed under Mulan PSL v2.
 * See http://license.coscl.org.cn/MulanPSL2 for the license terms.
 *
 * Storage-neutral frontier from diskannscan.cpp SearchFixedPoint. Storage
 * supplies full/PQ distance and neighbor access; it cannot replace this queue
 * traversal with a different ANN algorithm. A removed result vertex remains
 * navigable. Filtering result rows must never prune its outgoing edges.
 */
#ifndef DISKANN_GRAPH_SEARCH_H
#define DISKANN_GRAPH_SEARCH_H

#include "access/datavec/diskann.h"
#include "miscadmin.h"

template<class Storage>
static void DiskAnnSearchGraph(NeighborPriorityQueue* queue,blockhash_hash* visited,
    VectorList<BlockNumber>* entries,uint32_t beam,Storage& storage)
{
    /* Widening restarts at entries: preceding visits cannot suppress edges. */
    blockhash_reset(visited);
    queue->reserve(beam);
    for(size_t i=0;i<entries->size();++i){
        CHECK_FOR_INTERRUPTS();
        bool found=false;BlockNumber id=(*entries)[i];blockhash_insert(visited,id,&found);
        if(found)continue;
        Neighbor candidate={};candidate.id=id;candidate.distance=storage.EntryDistance(id,candidate);
        candidate.expanded=false;queue->insert(candidate);
    }
    while(queue->has_unexpanded_node()){
        /* Storage owns no page lock across this interruption point. */
        CHECK_FOR_INTERRUPTS();
        size_t position=0;Neighbor* candidate=GetNextNeighbor(queue,&position);
        BlockNumber id=candidate->id;
        if(storage.Deleted(id))queue->remove(position);
        /* Removal shifts the queue: use the captured ID, never the old pointer. */
        storage.Expand(id,queue);
    }
}

#endif
