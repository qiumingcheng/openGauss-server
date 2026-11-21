/* Typed native row routing -> existing same-catalog transaction journal.
 * Closed private slice: no public SQL or core callback invokes these yet.
 * No business identity, row lock or directory commit is synthesized here. */
#include "postgres.h"
#include "lake_native_partition.h"
#include "lake_txn_private.h"
#include "access/og_lake.h"
#include "access/heapam.h"
#include "knl/knl_variable.h"
#include "lake_row_lock.h"
#include "catalog/pg_class.h"
#include "storage/lock/lock.h"
#include "miscadmin.h"
#include "optimizer/stream_check.h"
#include "pgxc/pgxc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

static void LakeNativeRequireWriteRoute(Relation parent,TupleTableSlot* slot,
    Snapshot snapshot,CommandId command,uint64 statement)
{
    LakeRequireNativePartitionVersion();
    if(!LakeNativePartitionWriteVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake partition write routing is not activated")));
    if(!parent||!LakeHasNativePartitionMarker(parent)||!slot||!snapshot||command==InvalidCommandId||statement==0||
       StreamThreadAmI()||IS_SPQ_RUNNING||u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid serial native partition write route")));
    LOCKTAG lock;SET_LOCKTAG_RELATION(lock,u_sess->proc_cxt.MyDatabaseId,RelationGetRelid(parent));
    if(!CheckLock(&lock,RowExclusiveLock,false))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native write routing requires the parent RowExclusiveLock")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot write a native lake parent in a read-only transaction")));
}

void LakeNativePartitionRouteAppend(Relation parent,TupleTableSlot* slot,
    Snapshot snapshot,CommandId command,uint64 statement)
{
    LakeNativeRequireWriteRoute(parent,slot,snapshot,command,statement);
    LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);
    /* Native OG boundary/NULL semantics; the returned leaf is a real
     * pg_partition OID+partitionno, never heap_open(leafOID). */
    LakeNativePartitionIdentity target=LakeNativeRangeRoute(parent,&layout,slot);
    LakeTxnTable* view=LakeTxnReadStatementPartitionTable(parent,&target,snapshot);
    PG_TRY(); {
        LakeTxnCheckModify(view,parent,CMD_INSERT);
        LakeTxnAppend(view,slot,command,statement);
        LakeTxnReleaseReadView(view);
    } PG_CATCH(); {LakeTxnReleaseReadView(view);PG_RE_THROW();} PG_END_TRY();
}

void LakeNativePartitionRouteUpdated(Relation parent,LakeTxnTable* source,
    const LakeRowLocator* old,TupleTableSlot* newValue,
    CommandId command,uint64 statement)
{
    if(!source)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native update source view missing")));
    /* Destination sees the exact fresh/own-overlay view used by source EPQ,
     * never the unrelated original scan snapshot of another executor row. */
    Snapshot snapshot=LakeTxnBusinessSnapshot(source);
    LakeNativeRequireWriteRoute(parent,newValue,snapshot,command,statement);
    if(!source||!old||source->binding->relation_oid!=RelationGetRelid(parent)||
       !OidIsValid(source->native_leaf_partition_oid)||old->leaf_oid!=source->native_leaf_partition_oid)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native update source leaf identity changed")));
    /* This validation retains original/fresh read-view identity. It never
     * relaxes ordinary non-partition locators or treats equal numeric R in
     * another table_id as the same business row. */
    LakeValidateInlineLocator(old,source,LakeTxnNonce(),command);
    LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);
    LakeNativePartitionIdentity target=LakeNativeRangeRoute(parent,&layout,newValue);
    if(target.leaf_partition_oid==source->native_leaf_partition_oid) {
        if(target.partition_no!=source->native_partition_no)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native update leaf was replaced")));
        LakeTxnCheckModify(source,parent,CMD_UPDATE);
        LakeTxnModifyRow(source,old,newValue,command,statement);return;
    }
    /* Match the native OG permission to move a row; enabling movement must be
     * explicit host DDL before binding until its DDL adapter is qualified. */
    if(!parent->rd_rel->relrowmovement)
        ereport(ERROR,(errcode(ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED),
            errmsg("fail to update partitioned table \"%s\"",RelationGetRelationName(parent)),errdetail("disable row movement")));
    LakeTxnTable* destination=LakeTxnReadStatementPartitionTable(parent,&target,snapshot);
    PG_TRY(); {
        if(source->binding->server_oid!=destination->binding->server_oid||
           LakePermanentTableId(source->binding)==LakePermanentTableId(destination->binding)||
           strcmp(source->binding->binding_id,destination->binding->binding_id)==0)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("cross-leaf update requires distinct table identities in one catalog")));
        LakeTxnCheckModify(source,parent,CMD_UPDATE);LakeTxnCheckModify(destination,parent,CMD_INSERT);
        /* Caller already owns old row-head lock and has recomputed SQL via
         * native EPQ. End old table_id,R and create an independent identity in
         * destination table. Both remain in the same outer transaction and
         * catalog publication, with no commit between these journal entries.
         * ERROR propagates to ordinary statement/subtransaction rollback. */
        LakeTxnModifyRow(source,old,NULL,command,statement);
        LakeTxnAppend(destination,newValue,command,statement);
        LakeTxnReleaseReadView(destination);
    } PG_CATCH(); {LakeTxnReleaseReadView(destination);PG_RE_THROW();} PG_END_TRY();
}

void LakeNativePartitionCheckInsertTarget(Relation parent,Snapshot snapshot,bool requireWritable)
{
    LakeRequireNativePartitionVersion();
    if(!LakeNativePartitionWriteVersionEnabled()||!parent||!LakeHasNativePartitionMarker(parent)||!snapshot||
       !LakeRowLocksEnabled()||!LakeFileModificationsEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent INSERT requires the complete coordinated RC/inline write capability")));
    (void)LakeNativePartitionServer(parent,snapshot);LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);
    if(parent->trigdesc||parent->rd_rules||parent->rd_mlogoid!=InvalidOid||parent->rd_isblockchain)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent actions and auxiliary writers are not enabled")));
    List* indexes=RelationGetIndexList(parent);bool any=indexes!=NIL;list_free(indexes);
    if(any)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent index constraints require their write adapter")));
    for(int i=0;i<layout.leaf_count;++i) {
        LakeNativeLeafReadBinding leaf=LakeLoadNativePartitionLeaf(parent,&layout.leaves[i],snapshot);
        if(requireWritable&&leaf.binding->read_only)
            ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("native parent is still certified read-only")));
        /* Reuse all existing host/default/carrier/options/type checks without
         * creating a journal or granting a private metadata write capability. */
        LakeTxnTable candidate={};candidate.binding=leaf.binding;candidate.descriptor=RelationGetDescr(parent);
        candidate.native_leaf_partition_oid=layout.leaves[i].leaf_partition_oid;
        candidate.native_partition_no=layout.leaves[i].partition_no;
        LakeValidateInlineWriteTarget(&candidate,parent,snapshot);
    }
}

struct LakeNativeInsertState {
    uint64 tag;
    CommandId command;
    uint64 statement;
    Snapshot snapshot; /* Executor-owned; views below have registered copies. */
    LakeNativeRangeLayout layout;
    LakeTxnTable* leaves[LAKE_NATIVE_RANGE_LEAF_LIMIT];
    MemoryContext rows;
};
static const uint64 LakeNativeInsertStateTag=UINT64CONST(0x4f474c50494e5331);

static LakeNativeInsertState* LakeNativeInsertInitialize(EState* estate,ResultRelInfo* info,TupleTableSlot* slot)
{
    if(!estate||!info||!slot||!info->ri_RelationDesc)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native parent INSERT context missing")));
    Relation parent=info->ri_RelationDesc;
    /* Capture original command ID before SPI. No PARAM_EXEC expression is
     * evaluated by initialization; the host supplied the completed row slot. */
    CommandId command=estate->es_output_cid;
    if(command==InvalidCommandId)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native INSERT command identity missing")));
    LakeNativeInsertState* state=(LakeNativeInsertState*)info->ri_FdwState;
    if(!state) {
        MemoryContext caller=MemoryContextSwitchTo(estate->es_query_cxt);
        state=(LakeNativeInsertState*)palloc0(sizeof(LakeNativeInsertState));
        state->command=command;state->snapshot=estate->es_snapshot;
        state->rows=AllocSetContextCreate(estate->es_query_cxt,"Native parent INSERT row",ALLOCSET_SMALL_SIZES);
        MemoryContext init=AllocSetContextCreate(estate->es_query_cxt,"Native INSERT admission",ALLOCSET_SMALL_SIZES);
        PG_TRY(); {
            MemoryContextSwitchTo(init);LakeCheckExecutorPermissions(estate);state->statement=LakeTxnNextStatementToken();
            LakeNativeRequireWriteRoute(parent,slot,estate->es_snapshot,command,state->statement);
            LakeNativePartitionCheckInsertTarget(parent,estate->es_snapshot);state->layout=LakeNativeRangeDescribe(parent);
            MemoryContextSwitchTo(caller);MemoryContextDelete(init);
        } PG_CATCH(); {MemoryContextSwitchTo(caller);MemoryContextDelete(init);MemoryContextDelete(state->rows);pfree(state);PG_RE_THROW();} PG_END_TRY();
        state->tag=LakeNativeInsertStateTag;info->ri_FdwState=state;
    }
    if(state->tag!=LakeNativeInsertStateTag||state->command!=command||state->snapshot!=estate->es_snapshot)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native INSERT cached state belongs to another command")));
    return state;
}

/* Capability-only preflight used before BeginCopy can consume input or create
 * error logging state. Policy/SPI admission is performed once by Begin below. */
void LakeNativePartitionRequireCopyFrom(Relation parent)
{
    if(!parent||!LakeHasNativePartitionMarker(parent)||!LakeNativePartitionCopyFromVersionEnabled()||
       !IS_SINGLE_NODE||StreamThreadAmI()||IS_SPQ_RUNNING||u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent COPY FROM routing is not activated or supported in this context")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot COPY to a native lake parent in a read-only transaction")));
    /* Preserve the existing bounded leaf writer contract, before COPY builds
     * or evaluates defaults and before it can consume input. This is a native
     * descriptor check only; policy/SPI admission remains in Begin. */
    TupleDesc descriptor=RelationGetDescr(parent);
    if(descriptor->constr&&(descriptor->constr->num_check||descriptor->constr->num_defval))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent COPY requires nullable columns with NULL defaults and no CHECK constraints")));
    for(int i=0;i<descriptor->natts;++i)
        if(descriptor->attrs[i].attnotnull)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent COPY does not support NOT NULL columns in this slice")));
}

void LakeNativePartitionBeginCopyFrom(EState* estate,ResultRelInfo* info,TupleTableSlot* slot,CommandId command)
{
    LakeNativePartitionRequireCopyFrom(info?info->ri_RelationDesc:NULL);
    if(!estate||!info||!slot||info->ri_FdwState||command==InvalidCommandId||estate->es_output_cid!=command||!estate->es_snapshot)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native parent COPY command identity or pinned statement snapshot is missing")));
    /* Empty COPY must perform the same ACL/RC/positive-limit checks, before
     * NextCopyFrom evaluates a default or consumes the first input row. */
    (void)LakeNativeInsertInitialize(estate,info,slot);
}

void LakeNativePartitionExecInsert(EState* estate,ResultRelInfo* info,TupleTableSlot* slot)
{
    LakeNativeInsertState* state=LakeNativeInsertInitialize(estate,info,slot);
    Relation parent=info->ri_RelationDesc;
    MemoryContext caller=CurrentMemoryContext;MemoryContextReset(state->rows);
    PG_TRY(); {
        MemoryContextSwitchTo(state->rows);
        /* Fixed same-statement metadata, at most two views regardless of row
         * count. Only native routing + existing bounded tuple copy per row. */
        LakeNativePartitionIdentity target=LakeNativeRangeRoute(parent,&state->layout,slot);int index=-1;
        for(int i=0;i<state->layout.leaf_count;++i)if(LakeNativePartitionIdentityEqual(&target,&state->layout.leaves[i]))index=i;
        if(index<0)ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native INSERT target left its statement layout")));
        if(!state->leaves[index]) {
            state->leaves[index]=LakeTxnReadStatementPartitionTable(parent,&target,state->snapshot);
            LakeTxnCheckModify(state->leaves[index],parent,CMD_INSERT);
        }
        LakeTxnAppend(state->leaves[index],slot,state->command,state->statement);
        MemoryContextSwitchTo(caller);MemoryContextReset(state->rows);
    } PG_CATCH(); {MemoryContextSwitchTo(caller);MemoryContextReset(state->rows);PG_RE_THROW();} PG_END_TRY();
}

void LakeNativePartitionEndModify(ResultRelInfo* info)
{
    if(LakeNativePartitionEndMutation(info))return;
    if(!info||!info->ri_FdwState)return;
    LakeNativeInsertState* state=(LakeNativeInsertState*)info->ri_FdwState;
    if(state->tag!=LakeNativeInsertStateTag)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("unknown native parent modification state")));
    info->ri_FdwState=NULL;state->tag=0;
    for(int i=0;i<state->layout.leaf_count;++i)if(state->leaves[i]) {
        LakeTxnReleaseReadView(state->leaves[i]);state->leaves[i]=NULL;
    }
    if(state->rows)MemoryContextDelete(state->rows);pfree(state);
}
