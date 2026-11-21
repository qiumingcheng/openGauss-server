/* Existing DiskANN frontier and vector kernels over verified Parquet pages.
 * No native Relation/heap TID is synthesized for a lake row address. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_file_ann.h"
#include "lake_file_sdk.h"
#include "lake_file_pq.h"
#include "lake_ann_rescan.h"
#include "access/datavec/diskann_graph_search.h"
#include "utils/memutils.h"
#include <cmath>
#include <climits>

struct LakeAnnFileStorage {
    OgLakeAnnReader* reader;
    const OgLakeAnnSpec* spec;
    Vector* query;
    blockhash_hash* visited;
    const LakeAnnFileMask* mask;
    uint32 visited_nodes;
    uint32 visit_limit;
    bool budget_exhausted;
    LakePqLease* pq;
    LakeAnnRescanEntry* cached;
    float* pq_precomputed;
    bool cached_pq;
    bool pq_failed;

    bool Admitted(const OgLakeAnnAddress& address)
    {return !mask||(address.row_position>=0&&address.row_position<=INT_MAX&&bms_is_member((int)address.row_position,mask->positions));}

    const OgLakeAnnNode* Read(BlockNumber id)
    {
        const OgLakeAnnNode* result=cached?LakeAnnRescanNode(cached,id):NULL;OgLakeSdkError error;
        if(!cached){HOLD_INTERRUPTS();int status=og_lake_ann_reader_node(reader,id,&result,&error);RESUME_INTERRUPTS();
            LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();}
        if(spec->metric==3&&std::fabs(VectorSquareNorm(const_cast<float*>(result->vector),spec->dimension)-1.0)>0.0001)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake cosine graph contains an unnormalized vector")));
        return result;
    }
    float EntryDistance(BlockNumber id,Neighbor& candidate)
    {
        candidate.heaptidsLength=0;
        if(pq_precomputed&&!pq_failed&&id<spec->node_count)return pq_precomputed[id];
        if(cached_pq&&!pq_failed){float distance=0;if(LakeAnnRescanDistance(cached,id,&distance))return distance;pq_failed=true;}
        if(pq&&!pq_failed){float distance=0;if(LakePqDistance(pq,id,&distance))return distance;pq_failed=true;}
        const OgLakeAnnNode* node=Read(id);
        /* Exact full-vector SQL kernel distance, matching native opclasses. */
        return ExactDistance(id);
    }
    float ExactDistance(BlockNumber id)
    {
        const OgLakeAnnNode* node=Read(id);
        return spec->metric==1?VectorL2SquaredDistance(spec->dimension,query->x,const_cast<float*>(node->vector)):
            -VectorInnerProduct(spec->dimension,query->x,const_cast<float*>(node->vector));
    }
    bool Deleted(BlockNumber id)
    {
        const auto* node=Read(id);if(node->deleted)return true;
        if(!mask)return false;
        for(uint32 i=0;i<node->posting_count;++i)if(Admitted(node->postings[i]))return false;
        /* DiskAnnSearchGraph still expands this vertex after removal. */
        return true;
    }
    void Expand(BlockNumber id,NeighborPriorityQueue* queue)
    {
        const OgLakeAnnNode* node=Read(id);uint32 neighbors[OG_LAKE_ANN_MAX_DEGREE];uint32 count=node->neighbor_count;
        /* Reading another node can replace the one-page cache. Copy topology
         * first; no borrowed page/vector pointer survives that cache miss. */
        for(uint32 i=0;i<count;++i)neighbors[i]=node->neighbors[i];
        for(uint32 i=0;i<count;++i){
            if(visited_nodes>=visit_limit){budget_exhausted=true;break;}
            bool found=false;blockhash_insert(visited,neighbors[i],&found);if(found)continue;
            ++visited_nodes;
            Neighbor candidate={};candidate.id=neighbors[i];candidate.distance=EntryDistance(neighbors[i],candidate);
            candidate.expanded=false;queue->insert(candidate);}
    }
};

LakeFileAnnCandidates* LakeSearchAnnFile(OgLakeAnnReader* reader,Datum argument,uint32 beam,const LakeAnnFileMask* mask,const LakePqInput* pq,LakeAnnRescanEntry* cached)
{
    const OgLakeAnnSpec* spec=cached?LakeAnnRescanSpec(cached):og_lake_ann_reader_spec(reader);
    if(!spec||!DatumGetPointer(argument)||beam<1||beam>1000)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake file DiskANN query or beam")));
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext work=AllocSetContextCreate(owner,"Lake file DiskANN search",ALLOCSET_DEFAULT_SIZES);
    LakeFileAnnCandidates* result=NULL;
    LakePqLease* volatile lease=NULL;
    PG_TRY();{
        MemoryContextSwitchTo(work);
        Vector* query=DatumGetVector(PG_DETOAST_DATUM(argument));
        if(query->dim!=(int)spec->dimension)
            ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("lake file DiskANN query dimension differs from the graph")));
        for(int i=0;i<query->dim;++i)if(!std::isfinite(query->x[i]))
            ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("lake file DiskANN query must be finite")));
        if(spec->metric==3){
            if(DatumGetFloat8(DirectFunctionCall1(vector_norm,PointerGetDatum(query)))<=0)
                ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("zero cosine query requires complete SQL evaluation")));
            query=DatumGetVector(DirectFunctionCall1(l2_normalize,PointerGetDatum(query)));
        }
        bool cachedPq=cached?LakeAnnRescanQuery(cached,query->x):false;
        lease=pq&&!cached?LakePqOpen(LakeFileProvider(),spec,&pq->descriptor,pq->source_file,
            pq->source_hash,pq->schema_hash,pq->directory,query->x):NULL;
        bool pqUsed=lease!=NULL||cachedPq;
        float* pqPrecomputed=NULL;
        if(pqUsed&&spec->node_count>0&&spec->node_count<=262144){
            float* buf=(float*)palloc(sizeof(float)*spec->node_count);
            bool ok=cachedPq?LakeAnnRescanPrecomputeDistances(cached,spec->node_count,buf):
                LakePqPrecomputeDistances(lease,spec->node_count,buf);
            if(ok)pqPrecomputed=buf;else pfree(buf);
        }
        const uint32* sourceEntries=NULL;uint32 entryCount=0;OgLakeSdkError error;
        int status=OG_LAKE_SDK_OK;
        if(cached)sourceEntries=LakeAnnRescanEntries(cached,&entryCount);
        else {HOLD_INTERRUPTS();status=og_lake_ann_reader_entries(reader,&sourceEntries,&entryCount,&error);RESUME_INTERRUPTS();}
        LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
        /* Sparse masks can navigate many rejected bridge vertices. Bound
         * that extra work and force complete source fallback if exhausted. */
        uint32 visitLimit=mask?Min((uint32)VAMANA_CAP,(uint32)Max((int64)beam,Min((int64)VAMANA_CAP,LakeFileLimits().memory_bytes/256))):UINT_MAX;
        VectorList<BlockNumber> entries;
        if(entryCount<=visitLimit)for(uint32 i=0;i<entryCount;++i)entries.push_back(sourceEntries[i]);
        blockhash_hash* visited=blockhash_create(work,mask?visitLimit:VAMANA_CAP,NULL);
        NeighborPriorityQueue queue={};queue.clear();
        LakeAnnFileStorage storage={reader,spec,query,visited,mask,entryCount,visitLimit,entryCount>visitLimit,lease,cached,pqPrecomputed,cachedPq,false};
        storage.cached=cached;storage.cached_pq=cachedPq;
        if(!storage.budget_exhausted)DiskAnnSearchGraph(&queue,visited,&entries,beam,storage);
        if(storage.pq_failed){
            /* No source row escaped. A partially PQ-scored queue cannot be
             * mixed with raw scores: restart the same graph from its entries. */
            LakePqClose((LakePqLease**)&lease);LakeAnnRescanEndQuery(cached);storage.pq=NULL;storage.cached_pq=false;storage.pq_precomputed=NULL;storage.pq_failed=false;pqUsed=false;
            storage.visited_nodes=entryCount;storage.budget_exhausted=entryCount>visitLimit;
            queue.clear();blockhash_reset(visited);
            if(!storage.budget_exhausted)DiskAnnSearchGraph(&queue,visited,&entries,beam,storage);
        }
        LakePqClose((LakePqLease**)&lease);LakeAnnRescanEndQuery(cached);storage.pq=NULL;storage.cached_pq=false;
        auto* temporary=(LakeFileAnnCandidate*)palloc0(sizeof(LakeFileAnnCandidate)*beam*OG_LAKE_ANN_MAX_POSTINGS);
        uint32 count=0;
        for(size_t i=0;i<queue.size();++i){const OgLakeAnnNode* node=storage.Read(queue._data[i].id);
            if(node->deleted)continue;
            /* Exact rerank against full cached/verified vector so downstream Top-K truncation is exact. */
            float exactDist=spec->metric==1?VectorL2SquaredDistance(spec->dimension,query->x,const_cast<float*>(node->vector)):
                -VectorInnerProduct(spec->dimension,query->x,const_cast<float*>(node->vector));
            for(uint32 j=0;j<node->posting_count;++j){if(!storage.Admitted(node->postings[j]))continue;
                temporary[count].address=node->postings[j];temporary[count].node_id=node->node_id;
                temporary[count].internal_distance=exactDist;++count;}}
        if(count>1){
            qsort(temporary,count,sizeof(LakeFileAnnCandidate),[](const void* pa,const void* pb)->int{
                const auto* a=(const LakeFileAnnCandidate*)pa;
                const auto* b=(const LakeFileAnnCandidate*)pb;
                if(a->internal_distance<b->internal_distance)return -1;
                if(a->internal_distance>b->internal_distance)return 1;
                if(a->address.row_position<b->address.row_position)return -1;
                if(a->address.row_position>b->address.row_position)return 1;
                return 0;
            });
        }
        result=(LakeFileAnnCandidates*)MemoryContextAllocZero(owner,sizeof(LakeFileAnnCandidates));
        result->count=count;result->visited_candidates=queue.size();
        result->budget_exhausted=storage.budget_exhausted;result->pq_used=pqUsed;
        if(count){result->rows=(LakeFileAnnCandidate*)MemoryContextAlloc(owner,sizeof(LakeFileAnnCandidate)*count);
            errno_t copied=memcpy_s(result->rows,sizeof(LakeFileAnnCandidate)*count,temporary,sizeof(LakeFileAnnCandidate)*count);
            securec_check(copied,"\0","\0");}
        MemoryContextSwitchTo(owner);MemoryContextDelete(work);
    }PG_CATCH();{LakePqClose((LakePqLease**)&lease);LakeAnnRescanEndQuery(cached);MemoryContextSwitchTo(owner);MemoryContextDelete(work);PG_RE_THROW();}PG_END_TRY();
    return result;
}
