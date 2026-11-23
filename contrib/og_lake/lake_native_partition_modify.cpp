/* Closed native-parent mutation target state. Native executor glue is a
 * separate layer: this file never impersonates a heap ctid or a pg_class leaf.
 * It reuses the ordinary stable head / latest resolver / EvalPlanQual APIs. */
#include "postgres.h"
#include "lake_native_partition.h"
#include "lake_txn_private.h"
#include "lake_row_lock.h"
#include "access/og_lake.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "optimizer/stream_check.h"
#include "utils/rel.h"

bool LakeNativePartitionAddUpdateTargets(Query* query,RangeTblEntry* rte,Relation parent)
{
    if(!LakeHasNativePartitionMarker(parent))return false;
    if(!LakeNativePartitionMutationVersionEnabled()||!query||!rte||
       (query->commandType!=CMD_UPDATE&&query->commandType!=CMD_DELETE)||query->hasModifyingCTE||
       list_length(query->resultRelations)!=1||rte->relid!=RelationGetRelid(parent))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent mutation target rewrite is not activated or supported")));
    Index target=linitial_int(query->resultRelations);
    ListCell* cell=NULL;foreach(cell,query->targetList) {
        TargetEntry* entry=(TargetEntry*)lfirst(cell);
        if(entry->resjunk&&entry->resname&&(!strcmp(entry->resname,"og_lake_row_locator")||!strcmp(entry->resname,"og_lake_old_row")))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("duplicate native parent mutation target")));
    }
    Var* whole=makeWholeRowVar(rte,target,0,false);
    FuncExpr* marker=makeFuncExpr(LakeLocatorMarkerOid(false),BYTEAOID,list_make1(copyObject(whole)),
        InvalidOid,InvalidOid,COERCE_EXPLICIT_CALL);
    TargetEntry* locator=makeTargetEntry((Expr*)marker,list_length(query->targetList)+1,pstrdup("og_lake_row_locator"),true);
    locator->rtindex=target;query->targetList=lappend(query->targetList,locator);
    TargetEntry* oldrow=makeTargetEntry((Expr*)whole,list_length(query->targetList)+1,pstrdup("og_lake_old_row"),true);
    oldrow->rtindex=target;query->targetList=lappend(query->targetList,oldrow);return true;
}

static const uint64 LakeNativeMutationTag=UINT64CONST(0x4f474c504d555431);
struct LakeNativeSeenVersion {
    Oid leaf;
    uint32 source;
    uint64 row_id,birth,token,file_id,position;
};
struct LakeNativeSeenLogical { uint64 leaf,row_id; };
struct LakeNativeMutationState {
    uint64 tag;
    MemoryContext context,rows;
    CmdType operation;
    CommandId command;
    uint64 statement;
    LakeNativeRangeLayout layout;
    LakeTxnTable* views[LAKE_NATIVE_RANGE_LEAF_LIMIT];
    TupleTableSlot* latest;
    TupleDesc epq_descriptor;
    AttrNumber locator_attnum,oldrow_attnum;
    HTAB *seen_versions,*seen_logical;
    LakeTxnTable* prepared_view;
    LakeRowLocator prepared_locator;
    bool prepared;
};
struct LakeNativeTargetScan { Index rti; TupleDesc descriptor; int found; };
static bool LakeNativeFindTargetScan(PlanState* node,void* pointer)
{
    if(!node)return false;
    auto* target=(LakeNativeTargetScan*)pointer;
    if(IsA(node->plan,ForeignScan)&&((ForeignScan*)node->plan)->scan.scanrelid==target->rti) {
        auto* scan=(ForeignScanState*)node;
        if(!LakeIsNativePartitionRoutine(scan->fdwroutine))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native mutation target lost its typed lake scan")));
        ++target->found;target->descriptor=scan->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    }
    return planstate_tree_walker(node,LakeNativeFindTargetScan,pointer);
}
static AttrNumber LakeNativeJunk(List* target,const char* name,Oid type)
{
    AttrNumber number=ExecFindJunkAttributeInTlist(target,name);int found=0;
    ListCell* cell=NULL;
    foreach(cell,target) {
        TargetEntry* entry=(TargetEntry*)lfirst(cell);
        if(entry->resjunk&&entry->resname&&strcmp(entry->resname,name)==0) {
            if(entry->resno!=number||exprType((Node*)entry->expr)!=type)
                ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native mutation junk target %s changed type or slot",name)));
            ++found;
        }
    }
    if(!AttributeNumberIsValid(number)||found!=1)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native mutation target requires one %s",name)));
    return number;
}
static bool LakeNativeOwnStatementView(const LakeNativeMutationState* state,const LakeTxnTable* view)
{
    for(int i=0;i<state->layout.leaf_count;++i)if(view==state->views[i])return true;
    return false;
}
static void LakeNativeReleasePrepared(LakeNativeMutationState* state)
{
    if(state->prepared_view&&!LakeNativeOwnStatementView(state,state->prepared_view))
        LakeTxnReleaseReadView(state->prepared_view);
    state->prepared_view=NULL;state->prepared=false;
}
static void LakeNativeCloseMutation(LakeNativeMutationState* state)
{
    if(!state)return;
    LakeNativeReleasePrepared(state);
    if(state->latest) {ExecDropSingleTupleTableSlot(state->latest);state->latest=NULL;}
    for(int i=0;i<state->layout.leaf_count;++i) {
        LakeTxnReleaseReadView(state->views[i]);state->views[i]=NULL;
    }
}
static LakeNativeMutationState* LakeNativeMutationFor(ResultRelInfo* info)
{
    if(!LakeNativePartitionMutationVersionEnabled()||!info||!info->ri_RelationDesc||
       !LakeHasNativePartitionMarker(info->ri_RelationDesc)||!info->ri_FdwState)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent UPDATE/DELETE is not activated or initialized")));
    auto* state=(LakeNativeMutationState*)info->ri_FdwState;
    if(state->tag!=LakeNativeMutationTag)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native parent target state belongs to another operation")));
    return state;
}
void LakeNativePartitionBeginMutation(ModifyTableState* node,ResultRelInfo* info,int subplan,int flags)
{
    if(flags&EXEC_FLAG_EXPLAIN_ONLY)return;
    if(!LakeNativePartitionMutationVersionEnabled()||!node||!info||!info->ri_RelationDesc||
       !LakeHasNativePartitionMarker(info->ri_RelationDesc)||info->ri_FdwState||
       (node->operation!=CMD_UPDATE&&node->operation!=CMD_DELETE)||subplan!=0||node->mt_nplans!=1||
       StreamThreadAmI()||IS_SPQ_RUNNING||node->ps.plan->dop>1||
       u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent mutation requires one serial row READ COMMITTED target")));
    EState* estate=node->ps.state;
    CommandId command=estate->es_output_cid;
    if(command==InvalidCommandId||!estate->es_snapshot||!info->ri_junkFilter)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native parent mutation command/junk snapshot is missing")));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(estate->es_query_cxt,"Native parent mutation target",ALLOCSET_DEFAULT_SIZES);
    auto* state=(LakeNativeMutationState*)MemoryContextAllocZero(context,sizeof(LakeNativeMutationState));
    PG_TRY(); {
        MemoryContextSwitchTo(context);
        state->context=context;state->operation=node->operation;state->command=command;
        state->rows=AllocSetContextCreate(context,"Native parent mutation row",ALLOCSET_DEFAULT_SIZES);
        LakeCheckExecutorPermissions(estate);
        LakeNativePartitionCheckInsertTarget(info->ri_RelationDesc,estate->es_snapshot);
        state->layout=LakeNativeRangeDescribe(info->ri_RelationDesc);
        state->statement=LakeTxnNextStatementToken();
        for(int i=0;i<state->layout.leaf_count;++i) {
            state->views[i]=LakeTxnReadStatementPartitionTable(info->ri_RelationDesc,&state->layout.leaves[i],estate->es_snapshot);
            LakeTxnCheckModify(state->views[i],info->ri_RelationDesc,node->operation);
        }
        List* targets=node->mt_plans[subplan]->plan->targetlist;
        state->locator_attnum=LakeNativeJunk(targets,"og_lake_row_locator",BYTEAOID);
        state->oldrow_attnum=LakeNativeJunk(targets,"og_lake_old_row",RelationGetDescr(info->ri_RelationDesc)->tdtypeid);
        LakeNativeTargetScan scan={info->ri_RangeTableIndex,NULL,0};
        (void)LakeNativeFindTargetScan(node->mt_plans[subplan],&scan);
        if(scan.found==0&&is_dummy_plan(node->mt_plans[subplan]->plan)) {
            /* The proven-empty native plan has no target rows to recheck;
             * markers/own snapshot admission above still participate. */
            state->epq_descriptor=NULL;
        } else {
            if(scan.found!=1||!scan.descriptor||scan.descriptor->natts!=RelationGetDescr(info->ri_RelationDesc)->natts+2)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent mutation requires one complete N+2 target scan")));
            state->epq_descriptor=CreateTupleDescCopy(scan.descriptor);
        }
        state->latest=MakeSingleTupleTableSlot(state->views[0]->descriptor);
        HASHCTL control={};control.hcxt=context;
        control.keysize=sizeof(LakeNativeSeenVersion);control.entrysize=sizeof(LakeNativeSeenVersion);
        state->seen_versions=hash_create("Native parent old leaf versions",128,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        control.keysize=sizeof(LakeNativeSeenLogical);control.entrysize=sizeof(LakeNativeSeenLogical);
        state->seen_logical=hash_create("Native parent logical leaf identities",128,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        state->tag=LakeNativeMutationTag;info->ri_FdwState=state;MemoryContextSwitchTo(caller);
    } PG_CATCH(); {
        MemoryContextSwitchTo(caller);LakeNativeCloseMutation(state);MemoryContextDelete(context);PG_RE_THROW();
    } PG_END_TRY();
}
static LakeTxnTable* LakeNativeSourceView(LakeNativeMutationState* state,const LakeRowLocator* locator)
{
    for(int i=0;i<state->layout.leaf_count;++i)
        if(state->layout.leaves[i].leaf_partition_oid==locator->leaf_oid) {
            LakeTxnTable* view=state->views[i];
            LakeValidateInlineLocator(locator,view,LakeTxnNonce(),state->command);return view;
        }
    ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native mutation locator names a different leaf")));
    return NULL;
}
static bool LakeNativeReadScanned(LakeNativeMutationState* state,TupleTableSlot* slot,
    LakeRowLocator* locator,LakeTxnTable** source)
{
    bool isnull=false;Datum value=ExecGetJunkAttribute(slot,state->locator_attnum,&isnull);
    if(isnull)ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("native parent old locator is NULL")));
    *locator=LakeDecodeRowLocator(value);*source=LakeNativeSourceView(state,locator);
    LakeNativeSeenVersion key={};key.leaf=locator->leaf_oid;key.source=locator->source;
    key.row_id=locator->row_id;key.birth=locator->begin_snapshot;key.token=locator->local_version_token;
    key.file_id=locator->file_id;key.position=locator->file_row_position;
    bool seen=false;(void)hash_search(state->seen_versions,&key,HASH_ENTER,&seen);return !seen;
}
bool LakeNativePartitionRecheckMutation(EState* estate,ResultRelInfo* info,
    TupleTableSlot** valueSlot,TupleTableSlot** planSlot,EPQState* epq,ModifyTableState* node)
{
    auto* state=LakeNativeMutationFor(info);LakeNativeReleasePrepared(state);
    if(!estate||!node||state->operation!=node->operation||!planSlot||TupIsNull(*planSlot)||!epq||
       state->command!=estate->es_output_cid||!state->epq_descriptor)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native mutation target row is missing")));
    /* A persistent slot must release its tuple before its owning row context
     * resets. EPQ/RETURNING may borrow the previous row until this next call. */
    ExecClearTuple(state->latest);MemoryContextReset(state->rows);
    MemoryContext caller=MemoryContextSwitchTo(state->rows);
    struct Attempt {LakeTxnTable* fresh;bool injected;};
    Attempt* attempt=(Attempt*)palloc0(sizeof(Attempt));
    bool accept=false;
    PG_TRY(); {
        LakeRowLocator scanned;LakeTxnTable* source=NULL;
        if(LakeNativeReadScanned(state,*planSlot,&scanned,&source)) {
            if(scanned.source==LAKE_LOCATOR_TXN) {
                state->prepared_locator=scanned;state->prepared_view=source;state->prepared=true;accept=true;
            } else {
                LakeNativeSeenLogical key={};key.leaf=scanned.leaf_oid;key.row_id=scanned.row_id;
                bool seen=false;(void)hash_search(state->seen_logical,&key,HASH_ENTER,&seen);
                if(!seen) {
                    LakeLockLogicalRow(source,scanned.row_id);
                    LakeRowLocator latest;
                    bool exists=LakeTxnResolveLatestRow(source,scanned.row_id,state->command,state->latest,&latest,&attempt->fresh);
                    if(!exists&&info->ri_RelationDesc->rd_rel->relrowmovement)
                        ereport(ERROR,(errcode(ERRCODE_TRANSACTION_ROLLBACK),errmsg("native parent row was concurrently deleted or moved"),
                            errhint("Retry the statement using a fresh snapshot, as with native partition row movement.")));
                    if(exists) {
                        if(!attempt->fresh)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native latest row has no typed view")));
                        LakeValidateInlineLocator(&latest,attempt->fresh,LakeTxnNonce(),state->command);
                        accept=true;
                        if(latest.source==LAKE_LOCATOR_TXN||latest.begin_snapshot!=scanned.begin_snapshot) {
                            int columns=state->views[0]->descriptor->natts;
                            Datum* values=(Datum*)palloc0(sizeof(Datum)*(columns+2));
                            bool* nulls=(bool*)palloc0(sizeof(bool)*(columns+2));
                            tableam_tslot_getallattrs(state->latest);
                            for(int i=0;i<columns;++i) {values[i]=state->latest->tts_values[i];nulls[i]=state->latest->tts_isnull[i];}
                            HeapTuple business=heap_form_tuple(source->descriptor,values,nulls);
                            values[columns]=HeapTupleGetDatum(business);values[columns+1]=PointerGetDatum(LakeEncodeRowLocator(&latest));
                            HeapTuple tuple=heap_form_tuple(state->epq_descriptor,values,nulls);
                            /* EPQ owns an executor tree beyond this row and the
                             * lake target's End callback. Create its context
                             * under the outer query, never the resettable rows. */
                            MemoryContextSwitchTo(estate->es_query_cxt);
                            EvalPlanQualBegin(epq,estate);EvalPlanQualSetTuple(epq,info->ri_RangeTableIndex,tuple);attempt->injected=true;
                            EvalPlanQualFetchRowMarks(epq);TupleTableSlot* checked=EvalPlanQualNext(epq);
                            if(!TupIsNull(checked))(void)ExecMaterializeSlot(checked);
                            EvalPlanQualSetTuple(epq,info->ri_RangeTableIndex,NULL);attempt->injected=false;
                            MemoryContextSwitchTo(state->rows);
                            accept=!TupIsNull(checked);
                            if(accept) {
                                *planSlot=checked;
                                if(state->operation==CMD_UPDATE) {
                                    if(!valueSlot)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native UPDATE has no paired value slot")));
                                    *valueSlot=ExecFilterJunk(info->ri_junkFilter,checked);
                                }
                            }
                        }
                        if(accept) {
                            state->prepared_locator=latest;state->prepared_view=attempt->fresh;state->prepared=true;attempt->fresh=NULL;
                        }
                    }
                }
            }
        }
        if(attempt->fresh&&!LakeNativeOwnStatementView(state,attempt->fresh))LakeTxnReleaseReadView(attempt->fresh);
        MemoryContextSwitchTo(caller);
    } PG_CATCH(); {
        MemoryContextSwitchTo(caller);
        if(attempt->injected)EvalPlanQualSetTuple(epq,info->ri_RangeTableIndex,NULL);
        if(attempt->fresh&&!LakeNativeOwnStatementView(state,attempt->fresh))LakeTxnReleaseReadView(attempt->fresh);
        PG_RE_THROW();
    } PG_END_TRY();return accept;
}
TupleTableSlot* LakeNativePartitionExecMutation(EState* estate,ResultRelInfo* info,
    TupleTableSlot* value,TupleTableSlot* plan,CmdType operation)
{
    auto* state=LakeNativeMutationFor(info);
    if(!estate||state->command!=estate->es_output_cid||!state->prepared||!state->prepared_view||
       state->operation!=operation||TupIsNull(plan))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native parent mutation has no locked EPQ target")));
    LakeValidateInlineLocator(&state->prepared_locator,state->prepared_view,LakeTxnNonce(),state->command);
    if(operation==CMD_UPDATE) {
        if(TupIsNull(value))ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("native parent UPDATE value is empty")));
        LakeNativePartitionRouteUpdated(info->ri_RelationDesc,state->prepared_view,&state->prepared_locator,value,state->command,state->statement);
    } else if(operation==CMD_DELETE) {
        bool isnull=false;Datum datum=ExecGetJunkAttribute(plan,state->oldrow_attnum,&isnull);
        if(isnull)ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("native parent DELETE old row is NULL")));
        HeapTupleHeader header=DatumGetHeapTupleHeader(datum);
        if(HeapTupleHeaderGetTypeId(header)!=state->views[0]->descriptor->tdtypeid)
            ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native parent DELETE old row type changed")));
        HeapTupleData row={};row.t_len=HeapTupleHeaderGetDatumLength(header);row.t_data=header;ItemPointerSetInvalid(&row.t_self);
        MemoryContext caller=MemoryContextSwitchTo(state->rows);HeapTuple copy=heap_copytuple(&row);MemoryContextSwitchTo(caller);
        LakeTxnModifyRow(state->prepared_view,&state->prepared_locator,NULL,state->command,state->statement);
        if(!value||value->tts_tupleDescriptor!=RelationGetDescr(info->ri_RelationDesc))
            ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native DELETE RETURNING slot has a different descriptor")));
        ExecStoreTuple(copy,value,InvalidBuffer,false);
    } else ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported native parent mutation operation")));
    LakeNativeReleasePrepared(state);return value;
}
bool LakeNativePartitionEndMutation(ResultRelInfo* info)
{
    if(!info||!info->ri_FdwState)return false;
    auto* state=(LakeNativeMutationState*)info->ri_FdwState;
    if(state->tag!=LakeNativeMutationTag)return false;
    info->ri_FdwState=NULL;state->tag=0;MemoryContext context=state->context;
    LakeNativeCloseMutation(state);MemoryContextDelete(context);return true;
}
