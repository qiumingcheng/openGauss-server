/* Ordinary OG ForeignScan callbacks under native PartIterator. This is a
 * readonly adapter to existing lake readers, not a separate executor. */
#include "postgres.h"
#include "lake_native_partition.h"
#include "lake_txn_private.h"
#include "lake_inline_reader.h"
#include "lake_cost.h"
#include "utils/spccache.h"
#include "access/og_lake.h"
#include "access/heapam.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/pruning.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/stream_check.h"
#include "parser/parsetree.h"
#include <math.h>
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/rel_gs.h"

struct LakeNativeReadPlan { Cost startup,total; double physical; };
struct LakeNativeReadState {
    MemoryContext context;
    MemoryContext leaf_context;
    ForeignScanState* node;
    LakeNativeRangeLayout layout;
    LakeNativeLeafReadBinding selected[LAKE_NATIVE_RANGE_LEAF_LIMIT];
    LakeTxnTable* selected_views[LAKE_NATIVE_RANGE_LEAF_LIMIT];
    int count;
    int requested;
    bool prepared;
    bool selected_ready;
    LakeInlineReader* reader;
    CommandId command_limit;
    AttrNumber whole_row_slot,locator_slot;
    int64 returned;
};

static Snapshot LakeNativePlannerSnapshot(void)
{ return ActiveSnapshotSet()?GetActiveSnapshot():GetTransactionSnapshot(); }

/* Future activated ANALYZE uses standard parent statistics for cardinality.
 * Current bounded parent has no certified leaf-level statistics: ignore any
 * old scaffold leaf estimates and apply native partition pseudo predicates.
 * Physical local-SDK Parquet work remains a separate, uncalibrated host-unit
 * ledger. pg_class equivalent pages are never used as heap I/O addresses or
 * as a claim about remote/object-store latency. */
static LakeNativeReadPlan* LakeNativeAnalyzedCost(PlannerInfo* root,RelOptInfo* rel,Relation parent,
    LakeInlineBinding** bindings,int count,double physical,double dataPages)
{
    bool analyzed=parent->rd_rel->relpages>0&&parent->rd_rel->reltuples>=0;
    double visible=analyzed?(double)parent->rd_rel->reltuples:physical;
    rel->statisticFlag=TABLE_LEVEL_STATISTIC;
    rel->tuples=visible;rel->pages=dataPages;
    RangeTblEntry* rte=planner_rt_fetch(rel->relid,root);List* pseudo=NIL;
    if((rte->isContainPartition&&OidIsValid(rte->partitionOid))||
       (rte->isContainSubPartition&&OidIsValid(rte->subpartitionOid)))
        pseudo=PartitionPseudoPredicate(root,rel,rte);
    set_baserel_size_estimates(root,rel,pseudo);list_free_deep(pseudo);
    double fileIo=0,inlineIo=0;int files=0;
    for(int i=0;i<count;++i){
        LakeInlineBinding* binding=bindings[i];
        if(binding->file_count<0||(Size)binding->file_count>MaxAllocSize/sizeof(LakeDataFile)-(Size)files)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native parent planner file inventory is too large")));
        files+=binding->file_count;
        double localPage=u_sess->attr.attr_sql.seq_page_cost;
        if(OidIsValid(binding->inline_oid)){
            Relation carrier=heap_open(binding->inline_oid,AccessShareLock);
            get_tablespace_page_costs(carrier->rd_rel->reltablespace,NULL,&localPage);heap_close(carrier,NoLock);
        }
        inlineIo+=LakeInlinePhysicalPages(binding)*localPage;
        for(int f=0;f<binding->file_count;++f){
            const LakeDataFile* file=&binding->files[f];
            /* A restrictive predicate or lower visible count does not erase
             * old physical versions, footer/data or deletion-object reads. */
            fileIo+=ceil((double)file->file_bytes/BLCKSZ)*u_sess->attr.attr_sql.seq_page_cost;
            if(file->deletes)fileIo+=ceil((double)file->deletes->file_bytes/BLCKSZ)*u_sess->attr.attr_sql.seq_page_cost;
        }
    }
    if(files<0||(Size)files>MaxAllocSize/sizeof(LakeDataFile))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native parent planner file inventory is too large")));
    LakeInlineBinding merged={};merged.inline_oid=InvalidOid;merged.file_count=files;
    merged.files=(LakeDataFile*)palloc0(sizeof(LakeDataFile)*Max(1,files));
    int at=0;for(int i=0;i<count;++i)for(int f=0;f<bindings[i]->file_count;++f)merged.files[at++]=bindings[i]->files[f];
    double startup=rel->baserestrictcost.startup;
    double visibility=physical*(u_sess->attr.attr_sql.cpu_tuple_cost+3*u_sess->attr.attr_sql.cpu_operator_cost);
    /* No leaf distribution is invented. Work is bounded by the selected
     * physical estimate; output cardinality above uses the host statistics. */
    double visibleWork=Min(visible,physical)*(u_sess->attr.attr_sql.cpu_tuple_cost+rel->baserestrictcost.per_tuple);
    double target=0;
    if(root->parse->is_flt_frame){startup+=rel->reltarget->cost.startup;target=rel->reltarget->cost.per_tuple*rel->rows;}
    double total=startup+fileIo+inlineIo+visibility+visibleWork+target;
    LakeCostLedger* ledger=LakeCostCreate(&merged,startup,total,visible,physical,visibility,visibleWork,target,0,
        u_sess->attr.attr_sql.seq_page_cost,u_sess->attr.attr_sql.random_page_cost,
        u_sess->attr.attr_sql.cpu_tuple_cost,u_sess->attr.attr_sql.cpu_operator_cost,false);
    LakeCostResult full=LakeCostFinish(ledger);
    auto* result=(LakeNativeReadPlan*)palloc0(sizeof(LakeNativeReadPlan));
    result->physical=physical;result->startup=full.startup;result->total=full.total;return result;
}

static void LakeNativeReadSize(PlannerInfo* root,RelOptInfo* rel,Oid relationOid)
{
    LakeRequireNativePartitionVersion();
    bool mutation=(root->parse->commandType==CMD_UPDATE||root->parse->commandType==CMD_DELETE)&&LakeNativePartitionMutationVersionEnabled();
    if((root->parse->commandType!=CMD_SELECT && !(root->parse->commandType==CMD_INSERT&&LakeNativePartitionWriteVersionEnabled())&&!mutation) ||
       root->parse->rowMarks!=NIL || root->parse->hasModifyingCTE)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake partition bridge is readonly without row locking")));
    Relation parent=heap_open(relationOid,AccessShareLock);(void)LakeNativeRangeDescribe(parent);
    Snapshot snapshot=LakeNativePlannerSnapshot();(void)LakeNativePartitionServer(parent,snapshot);
    RangeTblEntry* rte=planner_rt_fetch(rel->relid,root);
    rel->pruning_result=rte->partitionOidList==NIL?partitionPruningForRestrictInfo(root,rte,parent,rel->baserestrictinfo):
        PartitionPruningForPartitionList(rte,parent);
    if(!rel->pruning_result || rel->pruning_result->expr || rel->pruning_result->paramArg)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent dynamic partition pruning is not enabled")));
    List* seqs=rel->pruning_result->ls_rangeSelectedPartitions;List* numbers=rel->pruning_result->ls_selectedPartitionnos;
    if(list_length(seqs)!=list_length(numbers)||list_length(seqs)>LakeNativePartitionLeafLimit())
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake pruning result")));
    rel->partItrs=list_length(seqs);double physical=0,pages=0;
    LakeInlineBinding* selected[LAKE_NATIVE_RANGE_LEAF_LIMIT]={};int selectedCount=0;
    ListCell* a=NULL;ListCell* b=NULL;
    forboth(a,seqs,b,numbers){int no=lfirst_int(b);Oid oid=getPartitionOidFromSequence(parent,lfirst_int(a),no);
        LakeNativePartitionIdentity key={relationOid,oid,no};LakeNativeLeafReadBinding leaf=LakeLoadNativePartitionLeaf(parent,&key,snapshot);
        selected[selectedCount++]=leaf.binding;
        for(int f=0;f<leaf.binding->file_count;++f){physical+=leaf.binding->files[f].record_count;
            pages+=ceil((double)leaf.binding->files[f].file_bytes/BLCKSZ);}
        physical+=LakeInlinePhysicalPages(leaf.binding)*Max(1.0,BLCKSZ/Max(1,rel->reltarget->width));
        pages+=LakeInlinePhysicalPages(leaf.binding);
    }
    if(LakeNativePartitionAnalyzeVersionEnabled()){
        rel->fdw_private=LakeNativeAnalyzedCost(root,rel,parent,selected,selectedCount,physical,pages);
        heap_close(parent,NoLock);return;
    }
    QualCost qual;cost_qual_eval(&qual,rel->baserestrictinfo,root);
    LakeNativeReadPlan* cost=(LakeNativeReadPlan*)palloc0(sizeof(LakeNativeReadPlan));
    cost->physical=physical;cost->startup=qual.startup;
    cost->total=cost->startup+pages*u_sess->attr.attr_sql.seq_page_cost+
        physical*(u_sess->attr.attr_sql.cpu_tuple_cost+qual.per_tuple);
    rel->rows=clamp_row_est(physical);rel->fdw_private=cost;heap_close(parent,NoLock);
}

static void LakeNativeReadPaths(PlannerInfo* root,RelOptInfo* rel,Oid)
{
    LakeNativeReadPlan* cost=(LakeNativeReadPlan*)rel->fdw_private;
    if(!cost)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake parent cost image missing")));
    add_path(root,rel,(Path*)create_foreignscan_path(root,rel,cost->startup,cost->total,NIL,NULL,NULL,NIL,1));
}

struct LakeNativeSystemVar { Index relation; };
static bool LakeNativeRejectSystemVar(Node* node,LakeNativeSystemVar* context)
{
    if(!node)return false;
    if(IsA(node,Var)){Var* value=(Var*)node;
        if(value->varlevelsup==0&&value->varno==context->relation&&value->varattno<0)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent system columns are not enabled")));
        return false;
    }
    return expression_tree_walker(node,(bool(*)())LakeNativeRejectSystemVar,context);
}

static ForeignScan* LakeNativeReadPlanNode(PlannerInfo* root,RelOptInfo* rel,Oid relationOid,ForeignPath*,List* target,List* clauses,Plan*)
{
    LakeNativeSystemVar context={rel->relid};List* quals=extract_actual_clauses(clauses,false);
    /* RestrictInfo is planner metadata, not an expression_tree_walker node. */
    (void)LakeNativeRejectSystemVar((Node*)target,&context);(void)LakeNativeRejectSystemVar((Node*)quals,&context);
    Expr* locator=NULL;ListCell* cell=NULL;
    foreach(cell,target) {
        TargetEntry* entry=(TargetEntry*)lfirst(cell);
        if(LakeIsLocatorPlaceholder(entry->expr,rel->relid)) {
            if(locator&&!equal(locator,entry->expr))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("multiple native parent locator placeholders")));
            locator=entry->expr;
        }
    }
    List* scanList=NIL;List* privateData=list_make1(makeInteger(1));
    if(locator) {
        if(!LakeNativePartitionMutationVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent locator scan is not activated")));
        Relation parent=heap_open(relationOid,NoLock);TupleDesc descriptor=RelationGetDescr(parent);
        for(int i=0;i<descriptor->natts;++i) {
            Form_pg_attribute attribute=&descriptor->attrs[i];
            if(attribute->attisdropped)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent locator cannot project dropped columns")));
            Var* column=makeVar(rel->relid,i+1,attribute->atttypid,attribute->atttypmod,attribute->attcollation,0);
            scanList=lappend(scanList,makeTargetEntry((Expr*)column,i+1,pstrdup(NameStr(attribute->attname)),false));
        }
        Var* whole=makeWholeRowVar(planner_rt_fetch(rel->relid,root),rel->relid,0,false);
        scanList=lappend(scanList,makeTargetEntry((Expr*)whole,descriptor->natts+1,pstrdup("og_lake_old_row"),false));
        scanList=lappend(scanList,makeTargetEntry((Expr*)copyObject(locator),descriptor->natts+2,pstrdup("og_lake_row_locator"),false));
        privateData=list_make3(makeInteger(2),makeInteger(descriptor->natts+1),makeInteger(descriptor->natts+2));
        heap_close(parent,NoLock);
    }
    return make_foreignscan(target,quals,rel->relid,NIL,privateData,scanList,NIL,NULL);
}

static void LakeNativeReadCloseLeaf(LakeNativeReadState* state)
{
    if(state->reader){LakeInlineReaderClose(state->reader);state->reader=NULL;}
    if(state->leaf_context){MemoryContextDelete(state->leaf_context);state->leaf_context=NULL;}
}

static void LakeNativeReadPrepare(LakeNativeReadState* state)
{
    if(state->prepared)return;
    ForeignScan* plan=(ForeignScan*)state->node->ss.ps.plan;
    if(!plan->scan.isPartTbl||!plan->scan.pruningInfo||plan->scan.pruningInfo->expr||plan->scan.pruningInfo->paramArg)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent requires its static native PartIterator plan")));
    Relation parent=state->node->ss.ss_currentRelation;
    state->layout=LakeNativeRangeDescribe(parent);Snapshot snapshot=state->node->ss.ps.state->es_snapshot;
    (void)LakeNativePartitionServer(parent,snapshot);
    List* seqs=plan->scan.pruningInfo->ls_rangeSelectedPartitions;
    List* numbers=plan->scan.pruningInfo->ls_selectedPartitionnos;
    if(list_length(seqs)!=list_length(numbers)||list_length(seqs)>LakeNativePartitionLeafLimit())
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake parent scan pruning list changed")));
    ListCell* a=NULL;ListCell* b=NULL;
    forboth(a,seqs,b,numbers){int no=lfirst_int(b);Oid oid=getPartitionOidFromSequence(parent,lfirst_int(a),no);
        Partition part=PartitionOpenWithPartitionno(parent,oid,no,AccessShareLock);
        if(!part||part->pd_id!=oid)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native certified leaf was replaced")));
        partitionClose(parent,part,NoLock); /* Keep the transaction AS lock, not a cache handle. */
        for(int prior=0;prior<state->count;++prior)if(state->selected[prior].host.leaf_partition_oid==oid)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native pruning selected the same certified leaf twice")));
        int at=state->count;LakeNativePartitionIdentity key={RelationGetRelid(parent),oid,no};
        state->selected[at]=LakeLoadNativePartitionLeaf(parent,&key,snapshot);
        LakeInlineBinding* binding=state->selected[at].binding;
        /* Build every selected leaf view during Begin/DECLARE, before a later
         * FETCH can enter a child subtransaction. A parent portal can survive
         * that child's rollback, so its view AND canonical journal/catalog
         * must already belong to the portal's creation transaction level.
         * Only static pruning/metadata are read here: no ParamExecData, quals
         * or index expressions are evaluated, and no active leaf is guessed. */
        state->selected_views[at]=binding->read_only?
            LakeTxnCreateQueryReadView(binding,snapshot,RelationGetDescr(parent)):
            LakeTxnReadStatementPartitionTable(parent,&key,snapshot);
        ++state->count;
    }
    state->node->ss.part_id=state->count;state->prepared=true;
}

int LakeNativePartitionScanCount(ForeignScanState* node)
{
    if(!node||!LakeIsNativePartitionRoutine(node->fdwroutine))
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native partition count called for another FDW")));
    LakeNativeReadState* state=(LakeNativeReadState*)node->fdw_state;if(!state)return 0;
    MemoryContext caller=MemoryContextSwitchTo(state->context);LakeNativeReadPrepare(state);MemoryContextSwitchTo(caller);
    return state->count;
}

void LakeNativePartitionIteratorSelect(ForeignScanState* node,int selected)
{
    int count=LakeNativePartitionScanCount(node);
    if(selected<0||selected>=count)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native partition iterator selected an invalid certified leaf")));
    LakeNativeReadState* state=(LakeNativeReadState*)node->fdw_state;state->requested=selected;state->selected_ready=true;
}

static void LakeNativeReadBegin(ForeignScanState* node,int flags)
{
    LakeRequireNativePartitionVersion();LakeCheckExecutorPermissions(node->ss.ps.state);
    if(flags&EXEC_FLAG_EXPLAIN_ONLY)return;
    if(StreamThreadAmI()||IS_SPQ_RUNNING||node->ss.ps.plan->dop>1||IsA(node->ss.ps.plan,VecForeignScan)||
       u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent scan requires serial row READ COMMITTED execution")));
    ForeignScan* plan=(ForeignScan*)node->ss.ps.plan;
    bool business=list_length(plan->fdw_private)==1&&IsA(linitial(plan->fdw_private),Integer)&&intVal(linitial(plan->fdw_private))==1;
    bool locator=list_length(plan->fdw_private)==3&&IsA(linitial(plan->fdw_private),Integer)&&intVal(linitial(plan->fdw_private))==2&&
        IsA(lsecond(plan->fdw_private),Integer)&&IsA(lthird(plan->fdw_private),Integer)&&LakeNativePartitionMutationVersionEnabled();
    int columns=RelationGetDescr(node->ss.ss_currentRelation)->natts;
    if((!business&&!locator)||(business&&node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts!=columns)||
       (locator&&(intVal(lsecond(plan->fdw_private))!=columns+1||intVal(lthird(plan->fdw_private))!=columns+2||
                   node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts!=columns+2)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported native lake parent read/locator plan layout")));
    MemoryContext context=AllocSetContextCreate(node->ss.ps.state->es_query_cxt,"Native lake parent read",ALLOCSET_DEFAULT_SIZES);
    LakeNativeReadState* state=(LakeNativeReadState*)MemoryContextAllocZero(context,sizeof(LakeNativeReadState));
    state->context=context;state->node=node;state->requested=-1;node->fdw_state=state;
    state->command_limit=node->ss.ps.state->es_snapshot->curcid;
    if(locator){state->whole_row_slot=columns+1;state->locator_slot=columns+2;}
    /* PortalStart calls Begin during DECLARE using the portal's owner. Pin
     * every statically selected view at that same transaction level. Opening
     * the physical reader remains lazy and PortalRunFetch restores that same
     * portal ResourceOwner before it calls ReScan. */
    MemoryContext caller=MemoryContextSwitchTo(context);
    PG_TRY(); {LakeNativeReadPrepare(state);MemoryContextSwitchTo(caller);}
    PG_CATCH(); {
        MemoryContextSwitchTo(caller);
        /* Begin can fail on a later leaf before Executor owns a complete
         * scan state. Release each already linked statement snapshot now. */
        for(int i=0;i<state->count;++i)LakeTxnReleaseReadView(state->selected_views[i]);
        node->fdw_state=NULL;MemoryContextDelete(context);PG_RE_THROW();
    } PG_END_TRY();
}

static void LakeNativeReadReScan(ForeignScanState* node)
{
    LakeNativeReadState* state=(LakeNativeReadState*)node->fdw_state;if(!state)return;
    LakeNativeReadCloseLeaf(state);
    if(!state->selected_ready)return;
    MemoryContext caller=MemoryContextSwitchTo(state->context);
    LakeNativeReadPrepare(state);
    if(state->requested<0||state->requested>=state->count)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native parent requested leaf is outside pruning")));
    state->leaf_context=AllocSetContextCreate(state->context,"Native lake active leaf",ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(state->leaf_context);
    LakeTxnTable* view=state->selected_views[state->requested];
    if(!view)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native parent statement leaf view is missing")));
    state->reader=LakeInlineReaderOpen(view,state->command_limit);MemoryContextSwitchTo(caller);
}

static TupleTableSlot* LakeNativeReadNext(ForeignScanState* node)
{
    LakeNativeReadState* state=(LakeNativeReadState*)node->fdw_state;TupleTableSlot* slot=node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);
    if(!state||!state->selected_ready||!state->reader)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent read requires explicit PartIterator leaf selection")));
    LakeInlineRowIdentity identity={};
    if(!LakeInlineReaderNext(state->reader,slot->tts_values,slot->tts_isnull,&identity))return slot;
    if(state->locator_slot) {
        LakeTxnTable* view=state->selected_views[state->requested];
        HeapTuple business=heap_form_tuple(view->descriptor,slot->tts_values,slot->tts_isnull);
        slot->tts_values[state->whole_row_slot-1]=HeapTupleGetDatum(business);slot->tts_isnull[state->whole_row_slot-1]=false;
        LakeRowLocator locator={};locator.source=identity.source;locator.database_oid=u_sess->proc_cxt.MyDatabaseId;
        locator.relation_oid=view->binding->relation_oid;locator.leaf_oid=view->native_leaf_partition_oid;
        locator.server_oid=view->binding->server_oid;locator.schema_version=view->binding->schema_version;
        locator.identity=view->binding->identity;locator.read_snapshot=view->binding->snapshot_id;
        locator.transaction_nonce=LakeTxnNonce();locator.producer_command_id=state->command_limit;
        if(identity.source==LAKE_LOCATOR_TXN)locator.local_version_token=identity.local_token;
        else {
            locator.row_id=identity.row_id;locator.begin_snapshot=identity.begin_snapshot;
            if(identity.source==LAKE_LOCATOR_INLINE)locator.inline_oid=identity.inline_oid;
            else {locator.file_id=identity.file_id;locator.file_row_position=identity.file_position;}
        }
        slot->tts_values[state->locator_slot-1]=PointerGetDatum(LakeEncodeRowLocator(&locator));slot->tts_isnull[state->locator_slot-1]=false;
    }
    ++state->returned;ExecStoreVirtualTuple(slot);return slot;
}

static void LakeNativeReadEnd(ForeignScanState* node)
{
    LakeNativeReadState* state=(LakeNativeReadState*)node->fdw_state;if(!state)return;
    LakeNativeReadCloseLeaf(state);
    /* One statement view per selected leaf, shared across iterator rescans.
     * Close physical scans first, then their snapshot/journal dependencies. */
    for(int i=0;i<state->count;++i) {
        LakeTxnReleaseReadView(state->selected_views[i]);state->selected_views[i]=NULL;
    }
    node->fdw_state=NULL;MemoryContextDelete(state->context);
}

static void LakeNativeReadExplain(ForeignScanState* node,ExplainState* explain)
{
    Relation parent=node&&node->ss.ss_currentRelation?node->ss.ss_currentRelation:NULL;
    const char* label="certified readonly native RANGE leaves";
    if(parent&&parent->partMap){
        if(PartitionMapIsList(parent->partMap))label="certified native LIST leaves";
        else if(PartitionMapIsHash(parent->partMap))label="certified native HASH leaves";
    }
    ExplainPropertyText("Native Lake Parent",label,explain);
}

bool LakeIsNativePartitionRoutine(const FdwRoutine* routine)
{ return routine&&routine->BeginForeignScan==LakeNativeReadBegin; }

FdwRoutine* LakeNativePartitionRoutine(Relation)
{
    LakeRequireNativePartitionVersion();FdwRoutine* routine=makeNode(FdwRoutine);
    routine->GetForeignRelSize=LakeNativeReadSize;routine->GetForeignPaths=LakeNativeReadPaths;
    routine->GetForeignPlan=LakeNativeReadPlanNode;routine->BeginForeignScan=LakeNativeReadBegin;
    routine->IterateForeignScan=LakeNativeReadNext;routine->ReScanForeignScan=LakeNativeReadReScan;
    routine->EndForeignScan=LakeNativeReadEnd;routine->ExplainForeignScan=LakeNativeReadExplain;return routine;
}
