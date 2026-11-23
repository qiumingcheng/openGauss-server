/* Decode existing native DiskANN pages into explicit portable graph records.
 * The caller builds the native graph; no Vamana construction is reimplemented. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_ann_export.h"
#include "lake_file_sdk.h"
#include "access/datavec/diskann.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/relcache.h"

struct LakeAnnExport {
    Relation index;
    DiskAnnMetaPageData metadata;
    BlockNumber blocks;
    uint32* to_node;
    OgLakeAnnSpec spec;
};
struct LakeAnnNativeNode {
    float vector[OG_LAKE_ANN_MAX_DIM];
    ItemPointerData tids[OG_LAKE_ANN_MAX_POSTINGS];
    BlockNumber neighbors[OG_LAKE_ANN_MAX_DEGREE];
    uint32 postings,degree,deleted;
    BlockNumber master;
    uint32 tag;
};

static void LakeAnnReadNative(LakeAnnExport* plan,BlockNumber block,LakeAnnNativeNode* result,bool edgesOnly=false)
{
    if(block<DISKANN_HEAD_BLKNO||block>=plan->blocks)
        ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export refers to an invalid block")));
    Buffer buffer=ReadBuffer(plan->index,block);LockBuffer(buffer,BUFFER_LOCK_SHARE);
    PG_TRY();{
        Page page=BufferGetPage(buffer);PageHeader header=(PageHeader)page;
        if(PageIsNew(page)||PageGetSpecialSize(page)!=MAXALIGN(sizeof(DiskAnnPageOpaqueData))||
            DiskAnnPageGetOpaque(page)->pageId!=DISKANN_PAGE_ID||PageGetMaxOffsetNumber(page)<FirstOffsetNumber)
            ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("invalid native ANN export page")));
        ItemId item=PageGetItemId(page,FirstOffsetNumber);
        uint32 offset=ItemIdGetOffset(item),length=ItemIdGetLength(item);
        if(!ItemIdHasStorage(item)||length<sizeof(IndexTupleData)||offset<header->pd_upper||
            offset+length>header->pd_special)
            ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("invalid native ANN export tuple bounds")));
        IndexTuple tuple=(IndexTuple)PageGetItem(page,item);uint32 size=IndexTupleSize(tuple);
        if(size<sizeof(IndexTupleData)||size>length||offset+size+plan->metadata.itemSize>header->pd_special)
            ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export node exceeds page bounds")));
        DiskAnnNodePage node=DiskAnnPageGetNode(tuple);
        result->master=node->master;result->tag=node->tag;result->deleted=ItemIdIsDead(item)?1:0;
        if(node->heaptidsLength>OG_LAKE_ANN_MAX_POSTINGS)
            ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export posting count is invalid")));
        result->postings=node->heaptidsLength;
        for(uint32 i=0;i<result->postings;++i){
            if(!ItemPointerIsValid(&node->heaptids[i]))
                ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export contains an invalid staging TID")));
            result->tids[i]=node->heaptids[i];}
        auto edges=(DiskAnnEdgePage)((char*)node+plan->metadata.nodeSize);
        if(edges->count>OG_LAKE_ANN_MAX_DEGREE)
            ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export degree is invalid")));
        result->degree=edges->count;for(uint32 i=0;i<result->degree;++i)result->neighbors[i]=edges->nexts[i];
        if(!edgesOnly){bool absent=false;Datum original=index_getattr(tuple,1,RelationGetDescr(plan->index),&absent);
            if(absent)ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export vector is NULL")));
            Vector* vector=DatumGetVector(PG_DETOAST_DATUM(original));
            if(vector->dim!=(int)plan->spec.dimension||VARSIZE(vector)!=VECTOR_SIZE(plan->spec.dimension))
                ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export vector dimension changed")));
            errno_t copied=memcpy_s(result->vector,sizeof(result->vector),vector->x,plan->spec.dimension*sizeof(float));securec_check(copied,"\0","\0");
            if((Pointer)vector!=DatumGetPointer(original))pfree(vector);}
        UnlockReleaseBuffer(buffer);buffer=InvalidBuffer;
    }PG_CATCH();{if(BufferIsValid(buffer))UnlockReleaseBuffer(buffer);PG_RE_THROW();}PG_END_TRY();
}

LakeAnnExport* LakeAnnExportOpen(Relation index,int metric,const char* context)
{
    if(!context||strlen(context)!=64||!index||index->rd_rel->relam!=DISKANN_AM_OID||index->rd_index->indnatts!=1||
        index->rd_index->indkey.values[0]<=0||metric<1||metric>3||index->rd_opfamily[0]!=(Oid)(8399+metric)||
        RelationGetDescr(index)->natts!=1||RelationGetDescr(index)->attrs[0].atttypid!=VECTOROID||
        RelationGetIndexExpressions(index)!=NIL||RelationGetIndexPredicate(index)!=NIL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("ANN export requires an owned native single-vector staging index")));
    /* Blocks inserts, VACUUM and DDL on the private staging index only. The
     * caller separately locks its staging heap; business lake rows stay live. */
    LockRelationOid(RelationGetRelid(index),ShareLock);
    auto* plan=(LakeAnnExport*)palloc0(sizeof(LakeAnnExport));plan->index=index;
    DiskANNGetMetaPageInfo(index,&plan->metadata);const auto& meta=plan->metadata;
    if(meta.magicNumber!=DISKANN_MAGIC_NUMBER||meta.version!=DISKANN_VERSION||meta.enablePQ||
        meta.dimensions<1||meta.dimensions>OG_LAKE_ANN_MAX_DIM||meta.nfrozen>FROZEN_POINT_SIZE||
        meta.nodeSize!=offsetof(DiskAnnNodePageData,pqcode)||meta.edgeSize!=sizeof(DiskAnnEdgePageData)||
        meta.itemSize!=meta.nodeSize+meta.edgeSize)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native ANN export format or PQ is not qualified")));
    plan->blocks=RelationGetNumberOfBlocks(index);
    if(plan->blocks<DISKANN_HEAD_BLKNO||plan->blocks-DISKANN_HEAD_BLKNO>OG_LAKE_ANN_MAX_PAGES*OG_LAKE_ANN_PAGE_ROWS)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native ANN staging index exceeds one portable segment")));
    plan->to_node=(uint32*)palloc(sizeof(uint32)*plan->blocks);
    for(BlockNumber i=0;i<plan->blocks;++i)plan->to_node[i]=UINT32_MAX;
    plan->spec={sizeof(plan->spec),OG_LAKE_ANN_FORMAT_VERSION,meta.dimensions,(uint32)metric,0,0,0,pstrdup(context)};
    for(BlockNumber block=DISKANN_HEAD_BLKNO;block<plan->blocks;++block){CHECK_FOR_INTERRUPTS();LakeAnnNativeNode raw;
        LakeAnnReadNative(plan,block,&raw);plan->to_node[block]=plan->spec.node_count++;
        plan->spec.posting_count+=raw.postings;
        for(uint32 i=0;i<meta.nfrozen;++i)if(meta.frozenBlkno[i]==block)++plan->spec.entry_count;}
    if(plan->spec.entry_count!=meta.nfrozen||((plan->spec.node_count==0)!=(meta.nfrozen==0)))
        ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export entry coverage is incomplete")));
    return plan;
}
const OgLakeAnnSpec* LakeAnnExportSpec(const LakeAnnExport* plan){return plan?&plan->spec:NULL;}
void LakeAnnExportWrite(LakeAnnExport* plan,LakeAnnResolveAddress resolve,void* argument,OgLakeAnnWriter* writer)
{
    if(!plan||!resolve||!writer)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native ANN export output")));
    for(BlockNumber block=DISKANN_HEAD_BLKNO;block<plan->blocks;++block){CHECK_FOR_INTERRUPTS();LakeAnnNativeNode raw;
        LakeAnnReadNative(plan,block,&raw);
        /* Native slave traversal uses its master's edges, but its own vector
         * and posting list. Copy the master while that page is actually locked. */
        if(DiskAnnNodeIsSlave(raw.tag)){LakeAnnNativeNode master;LakeAnnReadNative(plan,raw.master,&master,true);
            if(!DiskAnnNodeIsMaster(master.tag))
                ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export slave has no master")));
            raw.degree=master.degree;for(uint32 i=0;i<raw.degree;++i)raw.neighbors[i]=master.neighbors[i];}
        uint32 neighbors[OG_LAKE_ANN_MAX_DEGREE];OgLakeAnnAddress addresses[OG_LAKE_ANN_MAX_POSTINGS];
        for(uint32 i=0;i<raw.degree;++i){BlockNumber target=raw.neighbors[i];
            if(target>=plan->blocks||plan->to_node[target]==UINT32_MAX)
                ereport(ERROR,(errcode(ERRCODE_INDEX_CORRUPTED),errmsg("native ANN export edge leaves the segment")));
            neighbors[i]=plan->to_node[target];}
        for(uint32 i=0;i<raw.postings;++i)resolve(&raw.tids[i],&addresses[i],argument);
        uint32 entry=0;for(uint32 i=0;i<plan->metadata.nfrozen;++i)if(plan->metadata.frozenBlkno[i]==block)entry=1;
        OgLakeAnnNode row={plan->to_node[block],entry,raw.deleted,raw.degree,raw.postings,raw.vector,neighbors,addresses};OgLakeSdkError error;
        HOLD_INTERRUPTS();int status=og_lake_ann_writer_append(writer,&row,1,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(status,&error);
    }
}
void LakeAnnExportClose(LakeAnnExport* plan)
{if(plan){pfree((void*)plan->spec.context_hash);pfree(plan->to_node);pfree(plan);}}
