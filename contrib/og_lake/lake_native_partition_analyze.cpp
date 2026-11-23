/* Manual whole-parent ANALYZE source lease. Internal, capability remains closed.
 * Native analyze_rel retains owner/ACL_VACUUM, SUE/NOWAIT and pg_statistic writes.
 * No local partition heap is sampled and no relation/leaf ID is synthesized. */
#include "postgres.h"
#include "lake_native_partition.h"
#include "lake_txn_private.h"
#include "lake_sample.h"
#include "access/og_lake.h"
#include "access/xact.h"
#include "access/genam.h"
#include "access/reloptions.h"
#include "catalog/namespace.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "commands/sec_rls_cmds.h"
#include "commands/vacuum.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_statistic.h"
#include "storage/lock/lock.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include <cmath>
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "optimizer/stream_check.h"
#include "pgxc/pgxc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"

struct LakeNativeAnalyzeState {
    MemoryContext context;
    ResourceOwner snapshot_owner;
    Snapshot snapshot;
    CommandId command;
    Oid parent_oid;
    int count;
    bool sampled;
    LakeTxnTable* views[LAKE_NATIVE_RANGE_LEAF_LIMIT];
};

PG_FUNCTION_INFO_V1(og_lake_native_parent_statistics_upgrade);

struct LakeNativeStatsUpgradeState {
    MemoryContext context;
    ResourceOwner snapshot_owner;
    Snapshot snapshot;
    uint32 count;
    Oid* ids;
    Relation* parents;
};

static void LakeNativeStatsUpgradeClose(LakeNativeStatsUpgradeState* state)
{
    if(!state)return;
    if(state->parents)for(uint32 i=0;i<state->count;++i)if(state->parents[i]){
        heap_close(state->parents[i],NoLock);state->parents[i]=NULL;
    }
    if(state->snapshot){UnregisterSnapshotFromOwner(state->snapshot,state->snapshot_owner);state->snapshot=NULL;}
    if(state->context){MemoryContext context=state->context;state->context=NULL;MemoryContextDelete(context);}
}

static void LakeNativeStatsUpgradeCapture(LakeNativeStatsUpgradeState* state)
{
    if(state->snapshot){UnregisterSnapshotFromOwner(state->snapshot,state->snapshot_owner);state->snapshot=NULL;}
    ResourceOwnerEnlargeSnapshots(state->snapshot_owner);
    state->snapshot=RegisterSnapshotOnOwner(PgFdwCopySnapshot(GetLatestSnapshot()),state->snapshot_owner);
    state->snapshot->curcid=GetCurrentCommandId(false);
}

static int LakeNativeStatsUpgradeOidCompare(const void* left,const void* right)
{
    Oid a=*(const Oid*)left,b=*(const Oid*)right;return a<b?-1:a>b?1:0;
}

/* The registry is trusted natively by LakeLockCatalogRegistration before any
 * SQL resolves its names. Its SRE lock serializes every controlled new bind,
 * promotion and owner handover, but does not pretend to lock arbitrary SQL. */
static LakeMetaRows* LakeNativeStatsUpgradeRegistry(Snapshot snapshot)
{
    return LakeMetadataQuery("SELECT DISTINCT parent_relid FROM og_lake_internal.partition_binding ORDER BY parent_relid",
        0,NULL,NULL,NULL,snapshot,true,SPI_OK_SELECT);
}

/* Compare the durable core marker inventory too. An absent owner/registry
 * row must never turn a marked parent into a skipped local-heap relation. */
static void LakeNativeStatsUpgradeCheckMarkers(const LakeNativeStatsUpgradeState* state)
{
    Relation classes=heap_open(RelationRelationId,AccessShareLock);
    SysScanDesc scan=systable_beginscan(classes,InvalidOid,false,SnapshotNow,0,NULL);
    HeapTuple tuple=NULL;uint32 marked=0;
    while(HeapTupleIsValid(tuple=systable_getnext(scan))){
        CHECK_FOR_INTERRUPTS();Form_pg_class form=(Form_pg_class)GETSTRUCT(tuple);
        if(form->relkind!=RELKIND_RELATION)continue;
        bool absent=false;Datum raw=heap_getattr(tuple,Anum_pg_class_reloptions,RelationGetDescr(classes),&absent);
        if(absent)continue;
        bytea* options=heap_reloptions(RELKIND_RELATION,raw,false);
        bool marker=options&&((StdRdOptions*)options)->lake_partition_root;
        if(options)pfree(options);
        if(!marker)continue;
        Oid id=HeapTupleGetOid(tuple);
        if(!state->count||!bsearch(&id,state->ids,state->count,sizeof(Oid),LakeNativeStatsUpgradeOidCompare))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("native statistics upgrade found an unbound durable parent marker")));
        if(marked==PG_UINT32_MAX)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native statistics upgrade parent inventory is too large")));
        ++marked;
    }
    systable_endscan(scan);heap_close(classes,NoLock);
    if(marked!=state->count)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native statistics upgrade marker and binding inventories differ")));
}

Datum og_lake_native_parent_statistics_upgrade(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();
    Oid extension=get_extension_oid("og_lake",true);
    if(!creating_extension||!OidIsValid(extension)||u_sess->cmd_cxt.CurrentExtensionObject!=extension||
       !LakeNativePartitionAnalyzeVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native parent statistics reset is restricted to its coordinated extension upgrade")));
    if(u_sess->attr.attr_common.XactReadOnly||u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED||
       !IS_SINGLE_NODE||StreamThreadAmI()||IS_SPQ_RUNNING)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native statistics upgrade requires a writable serial READ COMMITTED transaction")));
    LakeTxnRejectBindingDdl();LakeLockCatalogRegistration();
    MemoryContext caller=CurrentMemoryContext;
    auto* state=(LakeNativeStatsUpgradeState*)palloc0(sizeof(LakeNativeStatsUpgradeState));
    state->snapshot_owner=t_thrd.utils_cxt.CurrentResourceOwner;
    state->context=AllocSetContextCreate(caller,"Native bound-parent statistics upgrade",ALLOCSET_DEFAULT_SIZES);
    PG_TRY();{
        MemoryContextSwitchTo(state->context);LakeNativeStatsUpgradeCapture(state);
        LakeMetaRows* inventory=LakeNativeStatsUpgradeRegistry(state->snapshot);state->count=inventory->count;
        if(state->count>MaxAllocSize/sizeof(Oid)||state->count>MaxAllocSize/sizeof(Relation))
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native statistics upgrade parent inventory exceeds memory limits")));
        if(state->count){
            state->ids=(Oid*)palloc(sizeof(Oid)*state->count);
            state->parents=(Relation*)palloc0(sizeof(Relation)*state->count);
        }
        for(uint32 i=0;i<state->count;++i){
            CHECK_FOR_INTERRUPTS();state->ids[i]=DatumGetObjectId(LakeMetadataValue(inventory,i,1,OIDOID));
            if(!OidIsValid(state->ids[i])||(i&&state->ids[i]<=state->ids[i-1]))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native statistics upgrade parent OIDs are not unique and ordered")));
        }
        LakeNativeStatsUpgradeCheckMarkers(state);
        if(state->count&&!OidIsValid(LakeMetadataOwner()))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("bound native parents require their configured private owner during upgrade")));
        /* Explicit upgrade policy: any busy parent aborts the entire ALTER
         * EXTENSION with55P03. Never skip a parent, wait under a registration
         * lock for a reverse-order writer, or commit a partial batch. */
        for(uint32 i=0;i<state->count;++i){
            CHECK_FOR_INTERRUPTS();
            if(!ConditionalLockRelationOid(state->ids[i],AccessExclusiveLock))
                ereport(ERROR,(errcode(ERRCODE_LOCK_NOT_AVAILABLE),errmsg("native statistics upgrade conflicts with an active parent")));
            state->parents[i]=heap_open(state->ids[i],NoLock);
        }
        /* A new latest copy, after every parent AX, is the one visibility
         * boundary used by all validation below. No user data is sampled. */
        LakeNativeStatsUpgradeCapture(state);
        LakeMetaRows* current=LakeNativeStatsUpgradeRegistry(state->snapshot);
        if(current->count!=state->count)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native parent set changed during statistics upgrade")));
        for(uint32 i=0;i<state->count;++i)
            if(DatumGetObjectId(LakeMetadataValue(current,i,1,OIDOID))!=state->ids[i])
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native parent identity changed during statistics upgrade")));
        LakeNativeStatsUpgradeCheckMarkers(state);
        MemoryContext scratch=AllocSetContextCreate(state->context,"Native statistics upgrade parent validation",ALLOCSET_DEFAULT_SIZES);
        for(uint32 i=0;i<state->count;++i){
            CHECK_FOR_INTERRUPTS();MemoryContextSwitchTo(scratch);
            Relation parent=state->parents[i];
            if(!LakeHasNativePartitionMarker(parent)||!OidIsValid(parent->rd_rel->relowner))
                ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native statistics upgrade requires a real marked parent and live owner")));
            HeapTuple owner=SearchSysCache1(AUTHOID,ObjectIdGetDatum(parent->rd_rel->relowner));
            if(!HeapTupleIsValid(owner))
                ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("native statistics upgrade parent owner disappeared")));
            ReleaseSysCache(owner);
            if(!pg_class_ownercheck(state->ids[i],GetUserId()))
                aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(parent));
            LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);
            Oid type=OIDOID;Datum argument=ObjectIdGetDatum(state->ids[i]);
            LakeMetaRows* count=LakeMetadataQuery("SELECT count(*)::bigint FROM og_lake_internal.partition_binding WHERE parent_relid=$1",
                1,&type,&argument,NULL,state->snapshot,true,SPI_OK_SELECT);
            LakeMetadataRequireCount(count,1,"native statistics upgrade complete leaf map");
            if(DatumGetInt64(LakeMetadataValue(count,0,1,INT8OID))!=layout.leaf_count)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native statistics upgrade parent has missing or extra leaf bindings")));
            LakeNativeLeafReadBinding leaves[LAKE_NATIVE_RANGE_LEAF_LIMIT]={};
            for(int j=0;j<layout.leaf_count;++j){
                leaves[j]=LakeLoadNativePartitionLeaf(parent,&layout.leaves[j],state->snapshot);
                if(j&&(leaves[j].binding->server_oid!=leaves[0].binding->server_oid||
                    leaves[j].certified_snapshot!=leaves[0].certified_snapshot||
                    leaves[j].binding->read_only!=leaves[0].binding->read_only))
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native statistics upgrade leaves do not share one certified catalog snapshot")));
                for(int p=0;p<j;++p)
                    if(LakePermanentTableId(leaves[j].binding)==LakePermanentTableId(leaves[p].binding)||
                       !strcmp(leaves[j].binding->table_uuid,leaves[p].binding->table_uuid)||
                       !strcmp(leaves[j].binding->binding_id,leaves[p].binding->binding_id))
                        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native statistics upgrade leaves do not have distinct identities in one certified catalog")));
            }
            LakeNativeRangeRequirePhysicallyEmpty(parent,&layout);
            MemoryContextSwitchTo(state->context);MemoryContextReset(scratch);
        }
        MemoryContextDelete(scratch);
        /* Validate ALL parents first. The only writes are standard host
         * statistics removals and transactional size resets; a later ERROR or
         * outer ROLLBACK restores these along with pg_extension.extversion. */
        for(uint32 i=0;i<state->count;++i){
            CHECK_FOR_INTERRUPTS();LakeNativeParentInvalidateStatistics(state->parents[i]);
        }
        MemoryContextSwitchTo(caller);LakeNativeStatsUpgradeClose(state);pfree(state);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);LakeNativeStatsUpgradeClose(state);pfree(state);PG_RE_THROW();
    }PG_END_TRY();PG_RETURN_VOID();
}

void LakeNativeParentAnalyzeEnd(LakeNativeAnalyzeState* state)
{
    if(!state)return;
    for(int i=0;i<state->count;++i)if(state->views[i]){
        LakeTxnReleaseReadView(state->views[i]);state->views[i]=NULL;
    }
    if(state->snapshot){UnregisterSnapshotFromOwner(state->snapshot,state->snapshot_owner);state->snapshot=NULL;}
    MemoryContext context=state->context;state->context=NULL;
    if(context)MemoryContextDelete(context);
}

LakeNativeAnalyzeState* LakeNativeParentAnalyzeBegin(Relation parent,BlockNumber* pages)
{
    if(!parent||!pages||!LakeHasNativePartitionMarker(parent)||!LakeNativePartitionAnalyzeVersionEnabled()||
       !IS_SINGLE_NODE||StreamThreadAmI()||IS_SPQ_RUNNING||u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent manual ANALYZE is not activated in this context")));
    if(default_statistics_target<=0)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent ANALYZE requires a positive statistics target")));
    TupleDesc desc=RelationGetDescr(parent);
    for(int i=0;i<desc->natts;++i)if(desc->attrs[i].attisdropped||desc->attrs[i].attstattarget< -1)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent ANALYZE does not admit dropped columns or percentage targets")));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(caller,"Native parent ANALYZE source",ALLOCSET_DEFAULT_SIZES);
    auto* state=(LakeNativeAnalyzeState*)MemoryContextAllocZero(context,sizeof(LakeNativeAnalyzeState));
    state->context=context;state->snapshot_owner=t_thrd.utils_cxt.CurrentResourceOwner;
    state->parent_oid=RelationGetRelid(parent);state->command=GetCurrentCommandId(false);
    PG_TRY();{
        /* Reserve first: no owner-array palloc ERROR can strand a completed
         * copied snapshot between copy and RememberSnapshot. This dedicated
         * copy does not borrow a mutable active/secondary snapshot buffer. */
        ResourceOwnerEnlargeSnapshots(state->snapshot_owner);
        Snapshot origin=ActiveSnapshotSet()?GetActiveSnapshot():GetTransactionSnapshot();
        state->snapshot=RegisterSnapshotOnOwner(PgFdwCopySnapshot(origin),state->snapshot_owner);
        state->snapshot->curcid=state->command;
        MemoryContextSwitchTo(context);
        LakeCheckMaskingAdmission();
        if(RelationHasRlspolicy(RelationGetRelid(parent))||RelationEnableRowSecurity(parent))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent ANALYZE with row security is not supported")));
        LakeRequireAccessPolicy();
        List* indexes=RelationGetIndexList(parent);bool hasIndexes=indexes!=NIL;list_free(indexes);
        if(hasIndexes||parent->rd_rel->relhassubclass)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent ANALYZE does not support local indexes or inheritance children")));
        LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);
        (void)LakeNativePartitionServer(parent,state->snapshot);
        uint64 physicalPages=0;
        for(int i=0;i<layout.leaf_count;++i){
            const LakeNativePartitionIdentity* key=&layout.leaves[i];
            LakeNativeLeafReadBinding source=LakeLoadNativePartitionLeaf(parent,key,state->snapshot);
            int at=state->count;
            state->views[at]=source.binding->read_only?
                LakeTxnCreateQueryReadView(source.binding,state->snapshot,desc):
                LakeTxnReadStatementPartitionTable(parent,key,state->snapshot);
            ++state->count;
            if(i&&(state->views[i]->binding->server_oid!=state->views[0]->binding->server_oid||
                state->views[i]->binding->snapshot_id!=state->views[0]->binding->snapshot_id))
                ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native parent ANALYZE leaves changed their common catalog snapshot")));
            physicalPages+=LakeAnalyzePhysicalPages(state->views[at]->binding);
            if(physicalPages>(uint64)MaxBlockNumber)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native parent ANALYZE physical size exceeds the host page interface")));
        }
        /* Analyzed-empty sentinel only. Actual planner I/O uses source bytes. */
        *pages=(BlockNumber)Max(UINT64CONST(1),physicalPages);
        MemoryContextSwitchTo(caller);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);LakeNativeParentAnalyzeEnd(state);PG_RE_THROW();
    }PG_END_TRY();return state;
}

int LakeNativeParentAnalyzeSample(LakeNativeAnalyzeState* state,Relation parent,int elevel,
    HeapTuple* rows,int target,double* totalrows,double* deadrows)
{
    if(!state||!parent||state->parent_oid!=RelationGetRelid(parent)||!state->snapshot||state->sampled||
       !LakeNativePartitionAnalyzeVersionEnabled()||!LakeHasNativePartitionMarker(parent)||
       state->count<1||state->count>LakeNativePartitionLeafLimit())
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native parent ANALYZE has no captured whole-parent source")));
    /* One probability and one budget over both complete typed domains. It is
     * intentionally not two independently sampled arrays concatenated later. */
    int count=LakeAcquireTableSampleRows(parent,state->views,state->count,state->command,
        rows,target,totalrows,deadrows,elevel);
    state->sampled=true;return count;
}

/* Future controlled activation must invoke this for every already-bound
 * parent before enabling ANALYZE/CBO use. No SQL/version assignment is made
 * here. Native transactional catalog updates deliberately avoid VACUUM's
 * in-place relstats writes: rollback must restore the old scaffold statistics. */
void LakeNativeParentInvalidateStatistics(Relation parent)
{
    if(!parent)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native statistics reset has no parent")));
    LOCKTAG lock;SET_LOCKTAG_RELATION(lock,u_sess->proc_cxt.MyDatabaseId,RelationGetRelid(parent));
    if(!CheckLock(&lock,AccessExclusiveLock,false))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native statistics activation requires parent AccessExclusiveLock")));
    if(!pg_class_ownercheck(RelationGetRelid(parent),GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(parent));
    LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);
    LakeNativeRangeRequirePhysicallyEmpty(parent,&layout);
    RemoveStatistics<STARELKIND_CLASS>(RelationGetRelid(parent),0);
    Relation classes=heap_open(RelationRelationId,RowExclusiveLock);
    HeapTuple tuple=SearchSysCacheCopy1(RELOID,ObjectIdGetDatum(RelationGetRelid(parent)));
    if(!HeapTupleIsValid(tuple))ereport(ERROR,(errcode(ERRCODE_UNDEFINED_TABLE),errmsg("native statistics reset parent disappeared")));
    Form_pg_class cls=(Form_pg_class)GETSTRUCT(tuple);
    cls->relpages=0;cls->reltuples=0;cls->relallvisible=0;
    simple_heap_update(classes,&tuple->t_self,tuple);CatalogUpdateIndexes(classes,tuple);
    heap_freetuple(tuple);heap_close(classes,NoLock);
    Relation partitions=heap_open(PartitionRelationId,RowExclusiveLock);
    for(int i=0;i<layout.leaf_count;++i){
        Oid oid=layout.leaves[i].leaf_partition_oid;
        RemoveStatistics<STARELKIND_PARTITION>(oid,0);
        tuple=SearchSysCacheCopy1(PARTRELID,ObjectIdGetDatum(oid));
        if(!HeapTupleIsValid(tuple))ereport(ERROR,(errcode(ERRCODE_UNDEFINED_TABLE),errmsg("native statistics reset leaf disappeared")));
        Form_pg_partition part=(Form_pg_partition)GETSTRUCT(tuple);
        if(part->parentid!=RelationGetRelid(parent)||part->parttype!=PART_OBJ_TYPE_TABLE_PARTITION)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native statistics reset leaf identity changed")));
        part->relpages=0;part->reltuples=0;part->relallvisible=0;
        simple_heap_update(partitions,&tuple->t_self,tuple);CatalogUpdateIndexes(partitions,tuple);
        heap_freetuple(tuple);CacheInvalidatePartcacheByPartid(oid);
    }
    heap_close(partitions,NoLock);
    CacheInvalidateRelcacheByRelid(RelationGetRelid(parent));CommandCounterIncrement();
}

/* Keep standard column statistics and size estimates in the same host xact.
 * Ordinary VACUUM's in-place size writes intentionally have different rollback
 * semantics, so they must not be used by this prepared manual-only source. */
void LakeNativeParentAnalyzePublishSize(LakeNativeAnalyzeState* state,Relation parent,
    BlockNumber pages,double rows)
{
    if(!state||!parent||!state->sampled||!state->snapshot||state->parent_oid!=RelationGetRelid(parent)||
       !LakeNativePartitionAnalyzeVersionEnabled()||!LakeHasNativePartitionMarker(parent)||!std::isfinite(rows)||rows<0)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native parent ANALYZE size has no completed captured sample")));
    Relation classes=heap_open(RelationRelationId,RowExclusiveLock);
    HeapTuple tuple=SearchSysCacheCopy1(RELOID,ObjectIdGetDatum(state->parent_oid));
    if(!HeapTupleIsValid(tuple))ereport(ERROR,(errcode(ERRCODE_UNDEFINED_TABLE),errmsg("native parent ANALYZE target disappeared")));
    Form_pg_class cls=(Form_pg_class)GETSTRUCT(tuple);
    cls->relpages=Max((BlockNumber)1,pages);cls->reltuples=rows;cls->relallvisible=0;
    simple_heap_update(classes,&tuple->t_self,tuple);CatalogUpdateIndexes(classes,tuple);
    heap_freetuple(tuple);heap_close(classes,NoLock);
    CacheInvalidateRelcacheByRelid(state->parent_oid);CommandCounterIncrement();
}
