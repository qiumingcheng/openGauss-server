/* First native lake SMP candidate: one read-only base relation; whole-file
 * static tasks and one inline owner. Shared bindings contain no reader/SPI or
 * transaction journal. Every task uses the same pre-startup business snapshot. */
#include "postgres.h"
#include "lake_column_default.h"
#include "lake_inline_sources.h"
#include "lake_types.h"
#include "knl/knl_variable.h"
#include "lake_stream.h"
#include "lake_version.h"
#include "lake_nullability.h"
#include "lake_file_reader.h"
#include "lake_file_scalar.h"
#include "lake_file_ann_catalog.h"
#include "lake_file_ann.h"
#include "lake_inline_ann.h"
#include "lake_file_sdk.h"
#include "lake_row_lock.h"
#include "access/og_lake.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "distributelayer/streamCore.h"
#include "distributelayer/streamProducer.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmem_walker.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "storage/lock/lock.h"

static const int LAKE_SMP_MAX_FILES=64;
static const Size LAKE_SMP_SHARED_LIMIT=16*1024*1024;
static const Size LAKE_SMP_SHARED_CONTEXT_LIMIT=20*1024*1024;
struct LakeStreamEntry {
    int node_id,cursor_level,cursor_owner,boundary,participants;
    Oid database,user,outer;bool guc_smp,guc_ann;int guc_reuse;
    LakeInlineBinding* binding;
    /* Query-lifetime leader facts, bound by the existing entry identity
     * and snapshot. No SQL lookup, epoch or persistent token in worker. */
    bool int2_sql47_qualified,int2_plain_fullscan,partition_qualified,ix_smp_extended,vec_scan,vector_smp_qualified;
    int int2_domain_columns,int2_columns;
    Snapshot snapshot;ParamListInfo parameters;
    char** versions;
    LakeScalarFileSources* scalar_sources;
    LakeAnnFileSources* ann_sources;
    LakeTxnTable* main_view; /* dereferenced/released only by the top consumer */
    int test_mode,test_participant;int64 test_gate;
    volatile uint32 started[4],claimed[4],done[4],reader_ready[4];
    int scalar_files[4],bitmap_files[4],fallback_files[4],ann_files[4];
    int64 ann_candidates[4];
    uint64 rows[4]; /* one writer per claimed participant; read after join */
    LakeStreamEntry* next;
};
struct LakeStreamRegistry {
    MemoryContext context;uint64 nonce;bool ready;LakeStreamEntry* entries;
    LakeConstraintProof* constraints;Snapshot constraint_snapshot;
    Oid constraint_database,constraint_user,constraint_outer;
};
struct LakeStreamWorker {LakeStreamEntry* entry;int participant;bool claimed;bool first_row;bool ended;LakeScalarFileSources* scalar_sources;LakeAnnFileSources* ann_sources;};

/* This diagnostic is not extension public SQL. The acceptance database must
 * explicitly register/revoke this superuser symbol. TLS is used only with
 * thread_pool=off and cannot affect ordinary queries until explicitly armed. */
struct LakeSmpTestState {
    int armed,participant;int64 gate;Oid database,user;
    uint64 query;int scan,participants;int64 snapshot;
    uint32 started[4],claimed[4],done[4],ready[4];uint64 rows[4];int scalar_files[4],bitmap_files[4],fallback_files[4],ann_files[4];
    int64 ann_candidates[4];
    int files;int64 file_ids[64];bool inline_task;
};
static THR_LOCAL LakeSmpTestState lakeSmpTest;
extern "C" Datum og_lake_test_smp_probe(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_test_smp_probe);
Datum og_lake_test_smp_probe(PG_FUNCTION_ARGS)
{
    if(!superuser()||GetUserId()!=GetOuterUserId())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native lake SMP diagnostics require the outer superuser")));
    if(!IS_SINGLE_NODE||StreamThreadAmI()||g_instance.attr.attr_common.enable_thread_pool)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP diagnostics require an ordinary backend with thread_pool=off")));
    if(PG_ARGISNULL(0)||PG_ARGISNULL(1)||PG_ARGISNULL(2))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("SMP diagnostic arguments cannot be NULL")));
    int mode=PG_GETARG_INT32(0),participant=PG_GETARG_INT32(1);int64 gate=PG_GETARG_INT64(2);
    if(mode<-1||mode>4||participant<0||participant>3||(mode==1&&gate<=0))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid SMP diagnostic mode/participant/gate")));
    if(mode==-1){lakeSmpTest={};}
    if(mode>0){if(lakeSmpTest.armed)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("SMP diagnostic is already armed")));
        lakeSmpTest.armed=mode;lakeSmpTest.participant=participant;lakeSmpTest.gate=gate;
        lakeSmpTest.database=u_sess->proc_cxt.MyDatabaseId;lakeSmpTest.user=GetUserId();}
    StringInfoData output;initStringInfo(&output);
    appendStringInfo(&output,"{\"armed\":%d,\"query\":%llu,\"scan\":%d,\"snapshot\":%lld,\"participants\":%d,\"inline_task\":%s,\"workers\":[",
        lakeSmpTest.armed,(unsigned long long)lakeSmpTest.query,lakeSmpTest.scan,(long long)lakeSmpTest.snapshot,lakeSmpTest.participants,lakeSmpTest.inline_task?"true":"false");
    for(int p=0;p<lakeSmpTest.participants;++p)appendStringInfo(&output,"%s{\"participant\":%d,\"started\":%u,\"claimed\":%u,\"done\":%u,\"reader_ready\":%u,\"rows\":%llu,\"scalar_files\":%d,\"bitmap_files\":%d,\"fallback_files\":%d,\"ann_files\":%d,\"ann_candidates\":%lld}",
        p?",":"",p,lakeSmpTest.started[p],lakeSmpTest.claimed[p],lakeSmpTest.done[p],lakeSmpTest.ready[p],(unsigned long long)lakeSmpTest.rows[p],lakeSmpTest.scalar_files[p],lakeSmpTest.bitmap_files[p],lakeSmpTest.fallback_files[p],lakeSmpTest.ann_files[p],(long long)lakeSmpTest.ann_candidates[p]);
    appendStringInfoString(&output,"],\"files\":[");for(int f=0;f<lakeSmpTest.files;++f)appendStringInfo(&output,"%s%lld",f?",":"",(long long)lakeSmpTest.file_ids[f]);
    appendStringInfoString(&output,"]}");PG_RETURN_TEXT_P(cstring_to_text(output.data));
}

static bool LakeStreamStaticParameters(ParamListInfo parameters)
{
    if(!parameters)return true;
    if(parameters->numParams<0||parameters->numParams>64||parameters->paramFetch||parameters->paramFetchArg||
       parameters->parserSetup||parameters->parserSetupArg||parameters->params_lazy_bind||parameters->params_need_process)return false;
    for(int p=0;p<parameters->numParams;++p)if(parameters->params[p].tabInfo)return false;
    return true;
}
static ParamListInfo LakeStreamCopyParameters(ParamListInfo parameters)
{
    if(!LakeStreamStaticParameters(parameters))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP requires bounded static external parameters")));
    Size bytes=0;
    if(parameters)for(int p=0;p<parameters->numParams;++p){const ParamExternData* value=&parameters->params[p];
        if(value->isnull||!OidIsValid(value->ptype))continue;
        int16 length;bool byval;get_typlenbyval(value->ptype,&length,&byval);
        Size item=datumGetSize(value->value,byval,length);
        if(item>1024*1024||bytes>1024*1024-item)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP external parameter images exceed1MiB")));
        bytes+=item;
    }
    return copyParamList(parameters);
}
static bool LakeStreamSameParameters(ParamListInfo expected,ParamListInfo actual)
{
    if(!LakeStreamStaticParameters(actual))return false;
    int count=expected?expected->numParams:0;if((actual?actual->numParams:0)!=count)return false;
    for(int p=0;p<count;++p){const ParamExternData* a=&expected->params[p];const ParamExternData* b=&actual->params[p];
        if(a->ptype!=b->ptype||a->isnull!=b->isnull||a->pflags!=b->pflags)return false;
        if(a->isnull||!OidIsValid(a->ptype))continue;
        int16 length;bool byval;get_typlenbyval(a->ptype,&length,&byval);
        if(!datumIsEqual(a->value,b->value,byval,length))return false;
    }
    return true;
}

static bool LakeSmpExpression(Node* node,void*)
{
    if(!node)return false;
    if(IsA(node,Param)&&((Param*)node)->paramkind!=PARAM_EXTERN)return true;
    if(IsA(node,SubPlan)||IsA(node,AlternativeSubPlan)||IsA(node,Query))return true;
    if(IsA(node,FuncExpr)&&((FuncExpr*)node)->funcid>=FirstNormalObjectId)return true;
    if(IsA(node,Aggref)&&((Aggref*)node)->aggfnoid>=FirstNormalObjectId)return true;
    if(IsA(node,WindowFunc)&&((WindowFunc*)node)->winfnoid>=FirstNormalObjectId)return true;
    if(IsA(node,OpExpr)||IsA(node,DistinctExpr)||IsA(node,NullIfExpr)){
        auto* op=(OpExpr*)node;
        if(op->opno>=FirstNormalObjectId||get_opcode(op->opno)>=FirstNormalObjectId)return true;
    }
    if(IsA(node,ScalarArrayOpExpr)){
        auto* op=(ScalarArrayOpExpr*)node;
        if(op->opno>=FirstNormalObjectId||get_opcode(op->opno)>=FirstNormalObjectId)return true;
    }
    if(IsA(node,RowCompareExpr)){ListCell* cell=NULL;
        foreach(cell,((RowCompareExpr*)node)->opnos){Oid op=lfirst_oid(cell);if(op>=FirstNormalObjectId||get_opcode(op)>=FirstNormalObjectId)return true;}}
    if(IsA(node,SortGroupClause)){auto* sort=(SortGroupClause*)node;
        if(sort->sortop>=FirstNormalObjectId||sort->eqop>=FirstNormalObjectId||
           (OidIsValid(sort->sortop)&&get_opcode(sort->sortop)>=FirstNormalObjectId)||
           (OidIsValid(sort->eqop)&&get_opcode(sort->eqop)>=FirstNormalObjectId))return true;}
    if(IsA(node,CoerceViaIO)||IsA(node,ArrayCoerceExpr)||IsA(node,CoerceToDomain))return true;
    return expression_tree_walker(node,(bool(*)())LakeSmpExpression,NULL);
}
/* Pure schema/plan recognition. Consumer admission owns installed-SQL reads;
 * the worker only validates these facts in its existing immutable entry. */
static int LakeStreamInt2Columns(const LakeInlineBinding* binding)
{
    int count=0;
    if(binding)for(int c=0;c<binding->column_count;++c)
        if(binding->columns[c].type==INT2OID)++count;
    return count;
}

static bool LakeStreamInt2RowDomain(const LakeInlineBinding* binding,bool extended,bool vectorSmp)
{
    if(!binding||!binding->column_evolution||binding->column_count<1||binding->column_count>MaxHeapAttributeNumber)return false;
    for(int c=0;c<binding->column_count;++c){Oid type=binding->columns[c].type;
        if(type!=BOOLOID&&type!=INT2OID&&type!=INT4OID&&type!=INT8OID&&
           type!=FLOAT4OID&&type!=FLOAT8OID&&type!=TEXTOID&&
           (!vectorSmp||!LakeNativeTypeIsTemporal(type))&&
           (!extended||(type!=BYTEAOID&&type!=UUIDOID&&type!=JSONOID&&type!=NUMERICOID)))return false;
    }
    return LakeStreamInt2Columns(binding)>0;
}

static bool LakeStreamPlainFullScan(const ForeignScan* scan,bool allowVec=false)
{
    bool rowScan=scan&&IsA(scan,ForeignScan)&&!scan->scan.plan.vec_output;
    bool vecScan=allowVec&&scan&&IsA(scan,VecForeignScan)&&scan->scan.plan.vec_output;
    return (rowScan||vecScan)&&
        scan->fdw_exprs==NIL&&scan->fdw_scan_tlist==NIL&&
        list_length(scan->fdw_private)==5&&list_nth(scan->fdw_private,4)==NIL;
}

static void LakeStreamCheckInt2Entry(const LakeStreamEntry* entry)
{
    int count=LakeStreamInt2Columns(entry->binding);
    if(count==0){
        if(entry->int2_sql47_qualified||entry->int2_plain_fullscan||entry->int2_domain_columns||entry->int2_columns)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake INT2 Stream qualification does not match its binding")));
        return;
    }
    if(!entry->int2_sql47_qualified||!entry->int2_plain_fullscan||
       entry->int2_domain_columns!=entry->binding->column_count||entry->int2_columns!=count||
       !LakeStreamInt2RowDomain(entry->binding,entry->ix_smp_extended,entry->vector_smp_qualified))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake INT2 Stream binding has no complete leader SQL47 full-scan qualification")));
}

static bool LakeStreamUnsupportedHistorical(const LakeInlineBinding* binding)
{
    if(!binding)return false;
    if(!LakePartitionComboVersionEnabled())return LakeInlineHasHistoricalSources(binding);
    return binding->has_added_columns||binding->has_dropped_columns||
        binding->retired_count>0||binding->future_count>0;
}

int LakeStreamPlanDop(PlannerInfo* root,RelOptInfo* rel,const LakeInlineBinding* binding,bool plainInt2Row)
{
    const bool extended=LakeIxSmpExtendedVersionEnabled();
    const bool vectorSmp=LakeVectorSmpVersionEnabled();
    if((!extended&&(LakeBindingHasBinary(binding)||LakeBindingHasUuid(binding)||LakeBindingHasJson(binding)||LakeBindingHasNumeric(binding)))||LakeStreamUnsupportedHistorical(binding)||(LakeIdentityPartitioned(binding)&&!LakePartitionComboVersionEnabled())||!u_sess->attr.attr_sql.enable_lake_smp||!LakeRowLocksEnabled()||!IS_SINGLE_NODE||!IS_STREAM_PLAN||
       root->parent_root||root->parse->utilityStmt||!LakeStreamStaticParameters(root->glob->boundParams)||root->parse->commandType!=CMD_SELECT||root->parse->hasModifyingCTE||root->parse->hasRecursive||root->parse->hasTargetSRFs||
       root->parse->hasSubLinks||root->parse->cteList!=NIL||root->parse->setOperations||root->parse->rowMarks!=NIL||
       root->hasLateralRTEs||root->hasJoinRTEs||list_length(root->parse->rtable)!=1||
       !root->parse->jointree||list_length(root->parse->jointree->fromlist)!=1||
       !IsA(linitial(root->parse->jointree->fromlist),RangeTblRef)||
       !LakeTxnStreamReadEligible()||!binding||binding->identity.kind!=LAKE_TABLE_PERMANENT||
       binding->file_count>LAKE_SMP_MAX_FILES||GetUserId()!=GetSessionUserId()||GetUserId()!=GetOuterUserId()||
       contain_volatile_functions((Node*)root->parse,true)||contain_volatile_functions((Node*)root->parse->windowClause)||
       query_tree_walker(root->parse,(bool(*)())LakeSmpExpression,NULL,0)||
       LakeSmpExpression((Node*)root->parse->sortClause,NULL)||LakeSmpExpression((Node*)root->parse->groupClause,NULL)||
       LakeSmpExpression((Node*)root->parse->windowClause,NULL))return 1;
    RangeTblEntry* rte=rt_fetch(rel->relid,root->parse->rtable);
    if(rte->rtekind!=RTE_RELATION||rte->lateral||rte->securityQuals!=NIL)return 1;
    /* Temporal SMP unlocks in SQL0.72; historical SQL keeps temporal serial. */
    if(!vectorSmp)
        for(int c=0;c<binding->column_count;++c)
            if(LakeNativeTypeIsTemporal(binding->columns[c].type))return 1;
    if(LakeStreamInt2Columns(binding)>0&&(!plainInt2Row||
       !LakeInt2RowSmpVersionEnabled()||!LakeStreamInt2RowDomain(binding,extended,vectorSmp)))return 1;
    int tasks=binding->file_count+(OidIsValid(binding->inline_oid)?1:0);
    return u_sess->opt_cxt.query_dop>=4&&tasks>=4?4:u_sess->opt_cxt.query_dop>=2&&tasks>=2?2:1;
}
/* This gate is deliberately narrower than serial ANN and ordinary lake SMP.
 * FDW candidates are not truncated by SQL K; the host may apply exact local
 * Sort/Limit before global merge. Default SDK limits and existing beam are
 * unchanged. Per participant: one SDK pool (64MiB) + at most32MiB bounded native
 * graph/entries/visited/frontier/masks/candidates/delete copies and row context.
 * 2*96MiB +20MiB shared owner (including 1MiB parameters and ANN descriptors)
 * +at most8MiB consumer binding/view copies fits a224MiB lake-module envelope.
 * The first slice also bounds paths to4KiB, total delete input to4096 positions
 * and source groups to1024/file, so consumer/worker copies cannot scale with N. SQL Sort/Stream/work_mem, buffer cache and
 * process RSS are outside this module bound. Sources/graph handles are used
 * sequentially; a completed graph is closed before the next graph is opened. */
static const int64 LAKE_ANN_SMP_WORKING_KB=224*1024;
static bool LakeStreamAnnBinding(const LakeInlineBinding* binding,bool extended)
{
    int maxFiles=extended?LAKE_SMP_MAX_FILES:2;
    int maxCols=extended?MaxHeapAttributeNumber:16;
    if(!binding||binding->file_count<1||binding->file_count>maxFiles||binding->column_count<2||binding->column_count>maxCols||
       u_sess->attr.attr_sql.ann_orderby_distance_reuse!=0)return false;
    if(u_sess->attr.attr_sql.statement_max_mem>0&&
       u_sess->attr.attr_sql.statement_max_mem<LAKE_ANN_SMP_WORKING_KB)return false;
    int vectors=0;
    for(int c=0;c<binding->column_count;++c){const auto* col=&binding->columns[c];
        if(col->type==VECTOROID){if(++vectors>1||col->typmod<1||col->typmod>1536)return false;}
        else if(col->type!=BOOLOID&&col->type!=INT4OID&&col->type!=INT8OID&&
                col->type!=FLOAT4OID&&col->type!=FLOAT8OID&&
                (!extended||(col->type!=INT2OID&&col->type!=TEXTOID&&col->type!=BYTEAOID&&
                    col->type!=UUIDOID&&col->type!=JSONOID&&col->type!=NUMERICOID)))return false;
    }
    if(vectors!=1||strlen(binding->data_root)>4096)return false;
    int64 deletes=0;
    for(int f=0;f<binding->file_count;++f){const auto* file=&binding->files[f];
        if(file->record_count<0||file->record_count>1048576||file->row_group_count<0||file->row_group_count>1024||
           strlen(file->path)>4096||file->inline_delete_count<0)return false;
        deletes+=file->inline_delete_count;
        if(file->deletes){if(file->deletes->record_count<0||strlen(file->deletes->path)>4096)return false;
            deletes+=file->deletes->record_count;}
        if(deletes>4096)return false;
    }
    return true;
}
static bool LakeStreamAnnExpr(List* filter,bool serialized,bool extended,List* fdwExprs,int depth,int* nodes,int* leaves)
{
    if(!extended){
        if(filter==NIL||!IsA(filter,List)||list_length(filter)!=7)return false;
        for(int i=0;i<6;++i)if(!IsA(list_nth(filter,i),Integer))return false;
        if(intVal(linitial(filter))!=LAKE_ANN_SCALAR_LEAF||intVal(list_nth(filter,5))!=0)return false;
        Oid type=(Oid)intVal(list_nth(filter,3));
        if(type!=INT4OID&&type!=INT8OID&&type!=FLOAT4OID&&type!=FLOAT8OID)return false;
        Node* rhs=(Node*)list_nth(filter,6);
        if(serialized)return IsA(rhs,Integer)&&intVal(rhs)==1;
        return IsA(rhs,Const)||(IsA(rhs,Param)&&((Param*)rhs)->paramkind==PARAM_EXTERN);
    }
    if(filter==NIL||!IsA(filter,List)||depth>LAKE_ANN_SCALAR_MAX_DEPTH||++*nodes>LAKE_ANN_SCALAR_MAX_NODES||list_length(filter)<1||!IsA(linitial(filter),Integer))return false;
    int kind=intVal(linitial(filter));
    if(kind==LAKE_ANN_SCALAR_LEAF){
        if(list_length(filter)!=7||++*leaves>LAKE_ANN_SCALAR_MAX_LEAVES)return false;
        for(int i=1;i<6;++i)if(!IsA(list_nth(filter,i),Integer))return false;
        Oid type=(Oid)intVal(list_nth(filter,3));
        int nullMode=intVal(list_nth(filter,4)),array=intVal(list_nth(filter,5));
        if((type!=INT4OID&&type!=INT8OID&&type!=FLOAT4OID&&type!=FLOAT8OID)||nullMode<0||nullMode>2||array<0||array>1)return false;
        Node* rhs=(Node*)list_nth(filter,6);
        if(!rhs)return false;
        if(serialized){
            if(!IsA(rhs,Integer))return false;
            int idx=intVal(rhs);
            if(idx<1||idx>=list_length(fdwExprs))return false;
            Node* expr=(Node*)list_nth(fdwExprs,idx);
            return expr&&(IsA(expr,Const)||(IsA(expr,Param)&&((Param*)expr)->paramkind==PARAM_EXTERN));
        }
        return IsA(rhs,Const)||(IsA(rhs,Param)&&((Param*)rhs)->paramkind==PARAM_EXTERN);
    }
    if((kind!=LAKE_ANN_SCALAR_AND&&kind!=LAKE_ANN_SCALAR_OR&&kind!=LAKE_ANN_SCALAR_NOT)||
       list_length(filter)!=2||!lsecond(filter)||!IsA(lsecond(filter),List))return false;
    List* children=(List*)lsecond(filter);int count=list_length(children);
    if((kind==LAKE_ANN_SCALAR_NOT?count!=1:count<2)||count>LAKE_ANN_SCALAR_MAX_LEAVES)return false;
    ListCell* cell=NULL;
    foreach(cell,children)if(!LakeStreamAnnExpr((List*)lfirst(cell),serialized,true,fdwExprs,depth+1,nodes,leaves))return false;
    return true;
}
int LakeStreamAnnPlanDop(PlannerInfo* root,RelOptInfo* rel,const LakeInlineBinding* binding,
    int metric,List* filter,bool pq)
{
    const bool extended=LakeIxSmpExtendedVersionEnabled();
    if(!extended){
        if(!LakeDirectInsertVersionEnabled()||metric<1||metric>3||pq||!LakeStreamAnnExpr(filter,false,false,NIL,1,NULL,NULL)||!LakeStreamAnnBinding(binding,false)||
           LakeStreamPlanDop(root,rel,binding)<2)return 1;
        return 2;
    }
    int nodes=0,leaves=0;
    if(metric<1||metric>3||(filter!=NIL&&!LakeStreamAnnExpr(filter,false,true,NIL,1,&nodes,&leaves))||!LakeStreamAnnBinding(binding,true))return 1;
    return LakeStreamPlanDop(root,rel,binding);
}
static Size LakeStreamPartitionBytes(const LakePartitionSpec* spec,const LakePartitionTuple* tuple)
{
    Size n=0;
    if(spec&&spec->enabled){
        if(spec->key_count<1||spec->key_count>MaxHeapAttributeNumber)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake SMP partition descriptor")));
        n+=sizeof(LakePartitionField)*(Size)spec->key_count;
    }
    if(tuple&&tuple->count>0&&tuple->values){
        n+=sizeof(LakePartitionValue)*(Size)tuple->count;
        for(int k=0;k<tuple->count;++k){
            if(tuple->values[k].is_null)continue;
            Size item=(tuple->values[k].canonical_text?strlen(tuple->values[k].canonical_text)+1:0)+64;
            if(spec&&spec->enabled&&k<spec->key_count&&(spec->keys[k].result_type==TEXTOID||spec->keys[k].result_type==BYTEAOID||spec->keys[k].result_type==NUMERICOID))
                item+=toast_raw_datum_size(tuple->values[k].value);
            if(item>LAKE_SMP_SHARED_LIMIT||n>LAKE_SMP_SHARED_LIMIT-item)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP partition descriptor exceeds budget")));
            n+=item;
        }
    }
    return n;
}
static Size LakeStreamColumnBytes(const LakeInlineColumn* column)
{
    Size n=strlen(column->name)+1;
    if(column->inline_name)n+=strlen(column->inline_name)+1;
    /* SET DEFAULT can attach byref current data to a non-added column.
     * Include the exact deep copy in the existing shared owner budget. */
    Size defaults=LakeColumnDefaultBytes(column);
    if(defaults>LAKE_SMP_SHARED_LIMIT||n>LAKE_SMP_SHARED_LIMIT-defaults)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP default map exceeds shared budget")));
    return n+defaults;
}
static Size LakeStreamBytes(const LakeInlineBinding* b)
{
    if(b->column_count<1||b->column_count>MaxHeapAttributeNumber||b->file_count<0||b->file_count>LAKE_SMP_MAX_FILES)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP binding exceeds bounded schema/source shape")));
    Size n=sizeof(*b)+sizeof(LakeInlineColumn)*b->column_count+sizeof(LakeDataFile)*b->file_count;
    n+=strlen(b->metadata_schema)+strlen(b->binding_id)+strlen(b->table_uuid)+strlen(b->data_root)+4;
    Size partBytes=LakeStreamPartitionBytes(&b->partition,NULL)+LakeStreamPartitionBytes(&b->inline_partition,NULL);
    if(partBytes>LAKE_SMP_SHARED_LIMIT||n>LAKE_SMP_SHARED_LIMIT-partBytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP descriptor exceeds budget")));
    n+=partBytes;
    for(int c=0;c<b->column_count;++c){
        Size colBytes=LakeStreamColumnBytes(&b->columns[c]);
        if(colBytes>LAKE_SMP_SHARED_LIMIT||n>LAKE_SMP_SHARED_LIMIT-colBytes)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP default map exceeds shared budget")));
        n+=colBytes;
    }
    if(b->inline_sources_loaded&&b->inline_source_count>0){
        if(b->inline_source_count>LAKE_INLINE_SOURCE_LIMIT||b->inline_sources==NULL)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake Stream inline source list")));
        Size srcArr=sizeof(LakeInlineSource)*(Size)b->inline_source_count;
        if(srcArr>LAKE_SMP_SHARED_LIMIT||n>LAKE_SMP_SHARED_LIMIT-srcArr)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP inline sources exceed shared budget")));
        n+=srcArr;
        for(int i=0;i<b->inline_source_count;++i){
            const auto* s=&b->inline_sources[i];
            if(!s->table_name||s->column_count<1||s->column_count>MaxHeapAttributeNumber||!s->column_names||!s->columns)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake Stream inline source descriptor")));
            Size item=strlen(s->table_name)+1+(sizeof(char*)+sizeof(LakeInlineColumn))*(Size)s->column_count+
                sizeof(int)*(Size)b->column_count+LakeStreamPartitionBytes(&s->partition,NULL);
            for(int c=0;c<s->column_count;++c){
                if(!s->column_names[c])ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("missing lake Stream source column name")));
                Size sc=strlen(s->column_names[c])+1+LakeStreamColumnBytes(&s->columns[c]);
                if(sc>LAKE_SMP_SHARED_LIMIT||item>LAKE_SMP_SHARED_LIMIT-sc)
                    ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP inline source columns exceed budget")));
                item+=sc;
            }
            if(item>LAKE_SMP_SHARED_LIMIT||n>LAKE_SMP_SHARED_LIMIT-item)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP inline sources exceed budget")));
            n+=item;
        }
    }
    uint64 deletes=0;
    for(int f=0;f<b->file_count;++f){const auto* d=&b->files[f];
        if(d->inline_delete_count<0||d->inline_delete_count>LAKE_FILE_DELETE_POSITION_LIMIT)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake Stream delete descriptor")));
        Size fpBytes=LakeStreamPartitionBytes(&d->partition.spec,&d->partition.tuple);
        if(fpBytes>LAKE_SMP_SHARED_LIMIT||n>LAKE_SMP_SHARED_LIMIT-fpBytes)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP file partition exceeds budget")));
        n+=fpBytes+strlen(d->path)+1+sizeof(int64)*d->inline_delete_count;
        deletes+=d->inline_delete_count;
        if(d->deletes){if(d->deletes->record_count<0||d->deletes->record_count>(int64)(LAKE_SMP_SHARED_LIMIT/sizeof(int64)))ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake Stream delete population")));
            n+=sizeof(LakeDeleteFile)+strlen(d->deletes->path)+1;deletes+=d->deletes->record_count;}
    }
    if(n>LAKE_SMP_SHARED_LIMIT||deletes>LAKE_SMP_SHARED_LIMIT/sizeof(int64))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake SMP descriptor/delete working set exceeds first-slice budget")));
    return n;
}
static LakeInlineBinding* LakeStreamCopyBinding(const LakeInlineBinding* source)
{
    (void)LakeStreamBytes(source);auto* b=(LakeInlineBinding*)palloc(sizeof(LakeInlineBinding));*b=*source;
    if(LakeStreamUnsupportedHistorical(source))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("historical lake carriers require serial execution; replan serially")));
    b->inline_sources_loaded=false;b->inline_sources=NULL;b->inline_source_count=0;b->inline_write_index=-1;
    b->retired_count=0;b->retired_columns=NULL;b->future_count=0;b->future_columns=NULL;
    b->partition={};b->partition.end_snapshot=-1;
    b->inline_partition={};b->inline_partition.end_snapshot=-1;
    if(source->partition.present)b->partition=LakePartitionCopySpec(&source->partition,CurrentMemoryContext);
    if(source->inline_partition.present)b->inline_partition=LakePartitionCopySpec(&source->inline_partition,CurrentMemoryContext);
    b->metadata_schema=pstrdup(source->metadata_schema);b->binding_id=pstrdup(source->binding_id);
    b->table_uuid=pstrdup(source->table_uuid);b->data_root=pstrdup(source->data_root);
    b->columns=(LakeInlineColumn*)palloc(sizeof(LakeInlineColumn)*b->column_count);
    for(int c=0;c<b->column_count;++c){b->columns[c]=source->columns[c];
        LakeCopyColumnDefault(&b->columns[c],&source->columns[c],NULL);
        b->columns[c].name=pstrdup(source->columns[c].name);
        b->columns[c].inline_name=source->columns[c].inline_name?pstrdup(source->columns[c].inline_name):NULL;}
    if(source->inline_sources_loaded)LakeCopyInlineSources(b,source,CurrentMemoryContext);
    b->files=(LakeDataFile*)palloc0(sizeof(LakeDataFile)*Max(1,b->file_count));
    for(int f=0;f<b->file_count;++f){b->files[f]=source->files[f];b->files[f].path=pstrdup(source->files[f].path);
        b->files[f].partition={};b->files[f].partition.spec.end_snapshot=-1;
        if(source->files[f].partition.spec.present){
            b->files[f].partition.spec=LakePartitionCopySpec(&source->files[f].partition.spec,CurrentMemoryContext);
            if(b->files[f].partition.spec.enabled)
                b->files[f].partition.tuple=LakePartitionCopyTuple(&b->files[f].partition.spec,&source->files[f].partition.tuple,CurrentMemoryContext);
        }
        /* Filled from the fully validated visible union below; participants
         * never reopen delete files or borrow main-thread delete buffers. */
        b->files[f].deletes=NULL;b->files[f].inline_delete_count=0;b->files[f].inline_delete_positions=NULL;}
    return b;
}
static Snapshot LakeStreamCopySnapshot(Snapshot original)
{
    if(!original||original->satisfies!=SNAPSHOT_MVCC||original->user_data||original->xcnt>4096||
       original->subxcnt<0||original->subxcnt>4096||original->prepared_count<0||original->prepared_array_capacity<0||original->prepared_array_capacity>4096||original->prepared_count>original->prepared_array_capacity)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP requires a bounded ordinary MVCC snapshot")));
    Snapshot copy=(Snapshot)palloc(sizeof(SnapshotData));*copy=*original;copy->active_count=copy->regd_count=0;copy->copied=true;
    if(copy->xcnt){if(!original->xip)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("MVCC xip missing")));copy->xip=(TransactionId*)palloc(copy->xcnt*sizeof(TransactionId));memcpy(copy->xip,original->xip,copy->xcnt*sizeof(TransactionId));}
    if(copy->subxcnt){if(!original->subxip)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("MVCC subxip missing")));copy->subxip=(TransactionId*)palloc(copy->subxcnt*sizeof(TransactionId));memcpy(copy->subxip,original->subxip,copy->subxcnt*sizeof(TransactionId));}
    if(copy->prepared_array_capacity>0){if(!original->prepared_array)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("MVCC prepared array missing")));Size bytes=copy->prepared_array_capacity*sizeof(TransactionId);copy->prepared_array=(TransactionId*)palloc(bytes);memcpy(copy->prepared_array,original->prepared_array,bytes);}
    return copy;
}
struct LakeStreamWalk {MethodPlanWalkerContext walker;PlannedStmt* plan;EState* estate;Stream* boundary;int participants;bool valid_boundary;LakeStreamRegistry* registry;};
static bool LakeStreamBindNode(Node* node,void* pointer)
{
    if(!node)return false;auto* walk=(LakeStreamWalk*)pointer;
    if(IsA(node,Stream)||IsA(node,VecStream))return false; /* nearest producer boundary owns this subtree */
    if(IsA(node,ForeignScan)||IsA(node,VecForeignScan)){
        auto* scan=(ForeignScan*)node;if(scan->scan.plan.dop<=1)return false;
        const bool vecScan=IsA(node,VecForeignScan);
        RangeTblEntry* rte=rt_fetch(scan->scan.scanrelid,walk->plan->rtable);
        Relation host=heap_open(rte->relid,AccessShareLock);bool lake=LakeIsNativeForeignRelation(host);heap_close(host,NoLock);if(!lake)return false;
        /* Admission is rechecked for cached dop>1 plans as well. This gate
         * runs only after recognizing a native lake scan, before binding I/O
         * and before ANY StartupStreamThread; unrelated Stream queries pass. */
        if(!u_sess->attr.attr_sql.enable_lake_smp)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cached native lake parallel plan is disabled by enable_lake_smp"),
                errhint("Replan this statement with enable_lake_smp disabled to select a serial plan.")));
        if(!walk->valid_boundary)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP requires a matching local2/4 producer boundary")));
        LakeCheckExecutorPermissions(walk->estate);
        if(walk->registry->entries)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("first native lake SMP slice supports one base scan")));
        if(walk->plan->commandType!=CMD_SELECT||walk->plan->hasModifyingCTE||walk->plan->subplans!=NIL||
           (walk->estate->es_top_eflags&(EXEC_FLAG_REWIND|EXEC_FLAG_BACKWARD|EXEC_FLAG_MARK))||
           !LakeTxnStreamReadEligible()||list_length(scan->fdw_private)!=5||
           intVal(lsecond(scan->fdw_private))||intVal(lthird(scan->fdw_private)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cached native lake SMP plan requires unsupported writes/re-execution state; replan serially")));
        const bool extended=LakeIxSmpExtendedVersionEnabled();
        const bool vectorSmp=LakeVectorSmpVersionEnabled();
        List* indexData=(List*)list_nth(scan->fdw_private,4);List* scalarData=NIL;List* annData=NIL;
        if(vecScan){
            if(!vectorSmp)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP vectorized batch scan requires SQL0.72; replan serially")));
            if(indexData!=NIL||!LakeStreamPlainFullScan(scan,true))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP vectorized batch scan requires a plain full scan; replan serially")));
        }
        if(indexData!=NIL){
            if(!IsA(indexData,List)||!IsA(linitial(indexData),String))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake SMP index plan metadata")));
            if(list_length(indexData)==6){
                scalarData=indexData;
                if(list_length(scan->fdw_exprs)!=1)
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP scalar input requires one static RHS")));
                for(int i=1;i<6;++i)if(!IsA(list_nth(scalarData,i),Integer))
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake SMP scalar plan metadata")));
                Node* rhs=(Node*)linitial(scan->fdw_exprs);
                if((!IsA(rhs,Const)&&!IsA(rhs,Param))||(IsA(rhs,Param)&&((Param*)rhs)->paramkind!=PARAM_EXTERN)||
                   intVal(list_nth(scalarData,5))<0||intVal(list_nth(scalarData,5))>2||intVal(lsecond(scalarData))!=InvalidOid)
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP scalar input requires a static RHS and complete inline scan")));
            }else if(list_length(indexData)==8||(extended&&list_length(indexData)==7)){
                annData=indexData;
                if(!LakeDirectInsertVersionEnabled())
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP ANN requires installed SQL0.19; replan serially")));
                for(int i=1;i<7;++i)if(!IsA(list_nth(annData,i),Integer))
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake SMP ANN plan metadata")));
                int metric=intVal(list_nth(annData,5))-10;
                bool hasFilter=list_length(annData)==8;
                int nodes=0,leaves=0;
                if((!extended&&walk->participants!=2)||walk->boundary->smpDesc.consumerDop!=1||intVal(lsecond(annData))!=InvalidOid||
                   metric<1||metric>3||(Oid)intVal(list_nth(annData,3))!=LakeAnnOperator(metric)||intVal(list_nth(annData,4))!=VECTOROID||
                   intVal(list_nth(annData,6))<1||intVal(list_nth(annData,6))>1000||
                   (hasFilter?(!LakeStreamAnnExpr((List*)list_nth(annData,7),true,extended,scan->fdw_exprs,1,&nodes,&leaves)||
                               list_length(scan->fdw_exprs)!=(extended?1+leaves:2)):list_length(scan->fdw_exprs)!=1))
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP ANN requires bounded metric-matched candidates; replan serially")));
                ListCell* arg=NULL;foreach(arg,scan->fdw_exprs){Node* rhs=(Node*)lfirst(arg);
                    if((!IsA(rhs,Const)&&!IsA(rhs,Param))||(IsA(rhs,Param)&&((Param*)rhs)->paramkind!=PARAM_EXTERN))
                        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP ANN requires static external expressions")));}
            }else ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP index metadata requires a qualified scalar or ANN path")));
        }
        MemoryContext caller=CurrentMemoryContext;
        /* Bind via the normal main-thread RC view. No SPI/metadata access is
         * repeated by a participant. Keep its registered snapshot until join. */
        MemoryContextSwitchTo(walk->estate->es_query_cxt);
        LakeTxnTable* view=LakeTxnReadStatementTable(rte->relid,walk->estate->es_snapshot);
        MemoryContextSwitchTo(walk->registry->context);
        auto* e=(LakeStreamEntry*)palloc0(sizeof(LakeStreamEntry));e->main_view=view;e->next=walk->registry->entries;walk->registry->entries=e;
        e->node_id=scan->scan.plan.plan_node_id;e->cursor_level=scan->scan.plan.cursor_expr_level;e->cursor_owner=scan->scan.plan.cursor_owner_node_id;
        e->boundary=walk->boundary->scan.plan.plan_node_id;e->participants=walk->participants;
        e->database=u_sess->proc_cxt.MyDatabaseId;e->user=GetUserId();e->outer=GetOuterUserId();
        e->guc_smp=u_sess->attr.attr_sql.enable_lake_smp;e->guc_ann=u_sess->attr.attr_sql.enable_lake_ann;
        e->guc_reuse=u_sess->attr.attr_sql.ann_orderby_distance_reuse;
        e->ix_smp_extended=extended;
        e->vec_scan=vecScan;
        e->vector_smp_qualified=vectorSmp&&(vecScan||LakeBindingHasTemporal(view->binding));
        if(!e->ix_smp_extended){
            LakeRejectBinaryAcceleration(view->binding,"cached SMP execution");
            LakeRejectUuidAcceleration(view->binding,"cached SMP execution");
            LakeRejectJsonAcceleration(view->binding,"cached SMP execution");
            LakeRejectNumericAcceleration(view->binding,"cached SMP execution");
        }
        if(vecScan&&!LakeVectorBindingSupported(view->binding,true))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cached native lake SMP vectorized binding is no longer qualified; replan serially")));
        if(LakeBindingHasTemporal(view->binding)&&!vectorSmp)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake temporal SMP scan requires SQL0.72; replan serially")));
        if(LakeStreamUnsupportedHistorical(view->binding)||(LakeIdentityPartitioned(view->binding)&&!LakePartitionComboVersionEnabled()))
            LakeRejectIdentityPartitionIntersection(view->binding,"cached SMP execution; replan serially");
        e->partition_qualified=LakeIdentityPartitioned(view->binding);
        if(!LakeRowLocksEnabled()||view->pending||view->binding->file_count>LAKE_SMP_MAX_FILES||
            GetUserId()!=GetSessionUserId()||GetUserId()!=GetOuterUserId())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP requires committed SQL0.17 RC identity")));
        int int2Columns=LakeStreamInt2Columns(view->binding);
        if(int2Columns>0){
            /* Fresh executor admission for cached plans, before file
             * preflight and before ANY worker can be started. */
            bool qualified=LakeInt2RowSmpVersionEnabled();
            if(!qualified||indexData!=NIL||!LakeStreamPlainFullScan(scan,e->vector_smp_qualified)||
               !LakeStreamInt2RowDomain(view->binding,e->ix_smp_extended,e->vector_smp_qualified))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake INT2 parallel execution requires SQL47 and a plain row full scan; replan serially")));
            e->int2_sql47_qualified=qualified;e->int2_plain_fullscan=true;
            e->int2_domain_columns=view->binding->column_count;e->int2_columns=int2Columns;
        }
        if(lakeSmpTest.armed){
            if(lakeSmpTest.database!=e->database||lakeSmpTest.user!=e->user||!superuser()||g_instance.attr.attr_common.enable_thread_pool)
                ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("SMP diagnostic identity changed")));
            e->test_mode=lakeSmpTest.armed;e->test_participant=lakeSmpTest.participant;e->test_gate=lakeSmpTest.gate;lakeSmpTest.armed=0;
        }
        if(annData!=NIL&&(!e->guc_ann||!LakeStreamAnnBinding(view->binding,e->ix_smp_extended)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cached lake SMP ANN binding/budget is no longer qualified; replan serially")));
        e->binding=LakeStreamCopyBinding(view->binding);e->snapshot=LakeStreamCopySnapshot(LakeTxnBusinessSnapshot(view));
        e->parameters=LakeStreamCopyParameters(walk->estate->es_param_list_info);
        LakeStreamCheckInt2Entry(e);
        if(LakeIdentityPartitioned(e->binding)&&!e->partition_qualified)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake partition Stream binding has no leader SQL63 qualification")));
        int tasks=e->binding->file_count+(OidIsValid(e->binding->inline_oid)?1:0);
        if(tasks<e->participants)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP input tasks shrank before execution; replan serially")));
        e->versions=(char**)palloc0(sizeof(char*)*Max(1,e->binding->file_count));
        MemoryContext validation=AllocSetContextCreate(walk->estate->es_query_cxt,"Lake Stream source preflight",ALLOCSET_DEFAULT_SIZES);
        PG_TRY();{MemoryContextSwitchTo(validation);
            LakeFileReader* reader=LakeFileReaderOpen(view->binding,NULL,view->statement_snapshot->curcid);
            for(int f=0;f<e->binding->file_count;++f){const char* version=LakeFileReaderObjectVersion(reader,f);
                if(!version||strlen(version)>512)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP source requires immutable-object identity")));
                e->versions[f]=MemoryContextStrdup(walk->registry->context,version);
                int count=0;const int64* positions=LakeFileReaderVisibleDeletes(reader,f,&count);
                e->binding->files[f].inline_delete_count=count;
                if(count){Size bytes=sizeof(int64)*(Size)count;
                    e->binding->files[f].inline_delete_positions=(int64*)MemoryContextAlloc(walk->registry->context,bytes);
                    memcpy(e->binding->files[f].inline_delete_positions,positions,bytes);}}
            LakeFileReaderClose(reader);
            if(scalarData!=NIL){
                AttrNumber column=intVal(lthird(scalarData));
                if(column<1||column>view->binding->column_count||
                   (Oid)intVal(list_nth(scalarData,4))!=view->binding->columns[column-1].type)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake SMP scalar key mapping changed")));
                e->scalar_sources=LakePrepareScalarFileSources(view->binding,strVal(linitial(scalarData)),column,
                    e->versions,walk->registry->context,1024*1024);
            }
            if(annData!=NIL){
                AttrNumber column=intVal(lthird(annData));
                if(column<1||column>view->binding->column_count||view->binding->columns[column-1].type!=VECTOROID)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake SMP ANN vector key mapping changed")));
                e->ann_sources=LakePrepareAnnFileSources(view->binding,strVal(linitial(annData)),column,intVal(list_nth(annData,5))-10,
                    e->versions,walk->registry->context,1024*1024,!e->ix_smp_extended);
            }
            MemoryContextSwitchTo(caller);MemoryContextDelete(validation);
        }PG_CATCH();{MemoryContextSwitchTo(caller);MemoryContextDelete(validation);PG_RE_THROW();}PG_END_TRY();
        if(e->test_mode==3)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake SMP diagnostic failed before first worker startup")));
        return false;
    }
    return plan_tree_walker(node,(MethodWalker)LakeStreamBindNode,pointer);
}
void LakePrepareStreamBindings(PlannedStmt* plan,EState* estate,StreamNodeGroup* group)
{
    if(!group||!plan||!estate)return;
    if(group->m_lakeScanBindings)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake SMP bindings already prepared")));
    /* An empty registry is discarded before any worker starts. No access
     * policy/capability check is imposed on unrelated ordinary Stream queries. */
    MemoryContext caller=CurrentMemoryContext;LakeStreamRegistry* registry=NULL;
    PG_TRY();{
        MemoryContext context=AllocSetContextCreate(group->m_streamRuntimeContext,"Lake immutable Stream bindings",ALLOCSET_DEFAULT_SIZES,SHARED_CONTEXT,LAKE_SMP_SHARED_CONTEXT_LIMIT);
        MemoryContextSwitchTo(context);registry=(LakeStreamRegistry*)palloc0(sizeof(LakeStreamRegistry));registry->context=context;registry->nonce=plan->queryId;group->m_lakeScanBindings=registry;
        /* Build in private leader scratch, retain only bounded plain facts in
         * this existing shared context. Covers RTEs removed from planTree too.
         * The extra1MiB proof plus bounded snapshot fits the existing20MiB
         * shared context cap; no default payload or second cache is created. */
        MemoryContextSwitchTo(estate->es_query_cxt);
        registry->constraints=LakeCaptureNotNullPlanProof(plan,estate->es_snapshot,context);
        MemoryContextSwitchTo(context);
        if(registry->constraints){
            registry->constraint_snapshot=LakeStreamCopySnapshot(estate->es_snapshot);
            registry->constraint_database=u_sess->proc_cxt.MyDatabaseId;
            registry->constraint_user=GetUserId();registry->constraint_outer=GetOuterUserId();
        }
        const List* pairs=group->getStreamPairList();ListCell* cell=NULL;
        foreach(cell,pairs){auto* pair=(StreamPair*)lfirst(cell);auto* producer=(StreamProducer*)linitial(pair->producerList);Stream* boundary=(Stream*)producer->getStream();
            int n=list_length(pair->producerList);
            bool valid=producer->isLocalStream()&&(n==2||n==4)&&(boundary->smpDesc.consumerDop==1||boundary->smpDesc.consumerDop==2||boundary->smpDesc.consumerDop==4)&&boundary->smpDesc.producerDop==n;
            LakeStreamWalk walk={};exec_init_plan_tree_base(&walk.walker.base,plan);walk.plan=plan;walk.estate=estate;walk.boundary=boundary;walk.participants=n;walk.valid_boundary=valid;walk.registry=registry;
            (void)LakeStreamBindNode((Node*)boundary->scan.plan.lefttree,&walk);
            list_free(walk.walker.base.init_plans);if(walk.walker.base.traverse_flag)pfree(walk.walker.base.traverse_flag);
        }
        /* Finish all ordinary binding/source work, then recheck the
         * query-lifetime SQL47 qualification before making ANY entry
         * visible to StartupStreamThread. Unrelated/old-type scans
         * never read this separate installed capability. */
        bool needsInt2Qualification=false,needsPartitionQualification=false,needsVectorSmpQualification=false;
        for(LakeStreamEntry* e=registry->entries;e;e=e->next){
            LakeStreamCheckInt2Entry(e);
            if(LakeIdentityPartitioned(e->binding)&&!e->partition_qualified)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake partition Stream binding has no leader SQL63 qualification")));
            if((e->vec_scan||LakeBindingHasTemporal(e->binding))&&!e->vector_smp_qualified)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake vectorized/temporal Stream binding has no leader SQL72 qualification")));
            if(e->vec_scan&&!LakeVectorBindingSupported(e->binding,true))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake vectorized Stream binding is no longer qualified")));
            if(e->int2_sql47_qualified)needsInt2Qualification=true;
            if(e->partition_qualified)needsPartitionQualification=true;
            if(e->vector_smp_qualified)needsVectorSmpQualification=true;
        }
        if(needsInt2Qualification&&!LakeInt2RowSmpVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake INT2 Stream SQL47 qualification changed before worker startup")));
        if(needsPartitionQualification&&!LakePartitionComboVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake partition Stream SQL63 qualification changed before worker startup")));
        if(needsVectorSmpQualification&&!LakeVectorSmpVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake vectorized/temporal Stream SQL72 qualification changed before worker startup")));
        registry->ready=true;__sync_synchronize();MemoryContextSwitchTo(caller);
        if(!registry->entries&&!registry->constraints)LakeReleaseStreamBindings(group);
    }PG_CATCH();{MemoryContextSwitchTo(caller);LakeReleaseStreamBindings(group);PG_RE_THROW();}PG_END_TRY();
}
void LakeCheckStreamConstraintSnapshot(PlannedStmt* plan,Snapshot host)
{
    auto* group=u_sess->stream_cxt.global_obj;auto* producer=u_sess->stream_cxt.producer_obj;
    auto* registry=group?(LakeStreamRegistry*)group->m_lakeScanBindings:NULL;
    if(!StreamThreadAmI()||!group||!producer||!registry||!registry->ready||!registry->constraints||
       registry->nonce!=plan->queryId||producer->getKey().queryId!=registry->nonce||
       registry->constraint_database!=u_sess->proc_cxt.MyDatabaseId||registry->constraint_user!=GetUserId()||
       registry->constraint_outer!=GetOuterUserId())
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake Stream constraint proof has no matching execution identity")));
    __sync_synchronize();
    bool owned=false;const List* pairs=group->getStreamPairList();ListCell* cell=NULL;
    foreach(cell,pairs){auto* pair=(StreamPair*)lfirst(cell);ListCell* participant=NULL;
        foreach(participant,pair->producerList){auto* actual=(StreamProducer*)lfirst(participant);
            if(actual==producer){
                const Plan* boundary=(const Plan*)producer->getStream();
                owned=producer->getKey().planNodeId==(uint32)boundary->plan_node_id&&
                    producer->getKey().smpIdentifier==(uint32)u_sess->stream_cxt.smp_id&&
                    u_sess->stream_cxt.producer_dop==(uint32)producer->getParallelDesc().producerDop;
            }
        }
    }
    if(!owned)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake Stream constraint producer does not belong to this group")));
    Snapshot expected=registry->constraint_snapshot;
    if(!host||!expected||host->satisfies!=expected->satisfies||host->xmin!=expected->xmin||host->xmax!=expected->xmax||
       host->snapshotcsn!=expected->snapshotcsn||host->timeline!=expected->timeline||host->curcid!=expected->curcid||
       host->read_lsn!=expected->read_lsn||host->subxid!=expected->subxid||host->xcnt!=expected->xcnt||
       host->subxcnt!=expected->subxcnt||host->suboverflowed!=expected->suboverflowed||
       host->takenDuringRecovery!=expected->takenDuringRecovery||host->gtm_snapshot_type!=expected->gtm_snapshot_type||
       host->prepared_count!=expected->prepared_count||host->user_data||
       (host->xcnt&&(!host->xip||memcmp(host->xip,expected->xip,sizeof(TransactionId)*host->xcnt)))||
       (host->subxcnt&&(!host->subxip||memcmp(host->subxip,expected->subxip,sizeof(TransactionId)*host->subxcnt)))||
       (host->prepared_count&&(!host->prepared_array||memcmp(host->prepared_array,expected->prepared_array,sizeof(TransactionId)*host->prepared_count))))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake Stream constraint snapshot differs from leader proof")));
    /* Syscache/host descriptors only. Never perform metadata SPI in worker. */
    LakeVerifyNotNullPlanProof(registry->constraints,plan);
}
void LakeReleaseStreamBindings(StreamNodeGroup* group)
{
    if(!group||!group->m_lakeScanBindings)return;auto* registry=(LakeStreamRegistry*)group->m_lakeScanBindings;group->m_lakeScanBindings=NULL;
    __sync_synchronize();
    for(LakeStreamEntry* e=registry->entries;e;e=e->next){
        if(e->binding){
            lakeSmpTest.query=registry->nonce;lakeSmpTest.scan=e->node_id;lakeSmpTest.snapshot=e->binding->snapshot_id;
            lakeSmpTest.participants=e->participants;lakeSmpTest.files=e->binding->file_count;lakeSmpTest.inline_task=OidIsValid(e->binding->inline_oid);
            for(int f=0;f<lakeSmpTest.files;++f)lakeSmpTest.file_ids[f]=e->binding->files[f].file_id;
            for(int p=0;p<e->participants;++p){lakeSmpTest.started[p]=e->started[p];lakeSmpTest.claimed[p]=e->claimed[p];lakeSmpTest.done[p]=e->done[p];lakeSmpTest.ready[p]=e->reader_ready[p];lakeSmpTest.rows[p]=e->rows[p];lakeSmpTest.scalar_files[p]=e->scalar_files[p];lakeSmpTest.bitmap_files[p]=e->bitmap_files[p];lakeSmpTest.fallback_files[p]=e->fallback_files[p];lakeSmpTest.ann_files[p]=e->ann_files[p];lakeSmpTest.ann_candidates[p]=e->ann_candidates[p];}
        }
        if(e->binding)for(int p=0;p<e->participants;++p)write_stderr("LOG: lake SMP completion query=%llu scan=%d snapshot=%lld participant=%d/%d claimed=%u done=%u rows=%llu\n",
            (unsigned long long)registry->nonce,e->node_id,(long long)e->binding->snapshot_id,p,e->participants,e->claimed[p],e->done[p],(unsigned long long)e->rows[p]);
        LakeTxnReleaseReadView(e->main_view);
    }
    MemoryContextDelete(registry->context);
}
void LakeStreamThreadStarted(StreamNodeGroup* group,const Plan* boundary,unsigned int participant)
{
    if(!group||!group->m_lakeScanBindings)return;
    auto* registry=(LakeStreamRegistry*)group->m_lakeScanBindings;
    for(auto* e=registry->entries;e;e=e->next)if(e->boundary==boundary->plan_node_id){
        if(participant>=(unsigned int)e->participants)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("lake SMP started an out-of-range task owner")));
        e->started[participant]=1;
        if(e->test_mode==4&&participant==0)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake SMP diagnostic failed after first real worker startup")));
    }
}
static LakeStreamEntry* LakeStreamFind(const Plan* plan)
{
    auto* group=u_sess->stream_cxt.global_obj;if(!group||!group->m_lakeScanBindings)return NULL;
    auto* registry=(LakeStreamRegistry*)group->m_lakeScanBindings;if(!registry->ready)return NULL;
    for(auto* e=registry->entries;e;e=e->next)if(e->node_id==plan->plan_node_id&&e->cursor_level==plan->cursor_expr_level&&e->cursor_owner==plan->cursor_owner_node_id)return e;
    return NULL;
}
static void LakeStreamWorkerReset(std::shared_ptr<void> pointer)
{
    auto* worker=(LakeStreamWorker*)pointer.get();
    /* Installed before SDK handles: reset callbacks are LIFO, so later SDK
     * handles close first. This records abnormal context retirement, not a
     * claim that an errored task completed its rows. Group join still guards
     * every shared descriptor until host transaction resource cleanup ends. */
    if(worker->claimed&&!worker->ended){worker->ended=true;__sync_synchronize();worker->entry->done[worker->participant]=2;}
}
LakeStreamWorker* LakeStreamOpenWorker(ForeignScanState* node,int flags,LakeTxnTable** table,char*** versions)
{
    const Plan* plan=node->ss.ps.plan;auto* e=LakeStreamFind(plan);auto* producer=u_sess->stream_cxt.producer_obj;
    auto* group=u_sess->stream_cxt.global_obj;auto* registry=group?(LakeStreamRegistry*)group->m_lakeScanBindings:NULL;
    int participant=(int)u_sess->stream_cxt.smp_id;
    if(!StreamThreadAmI()||!e||!producer||!registry||producer->getKey().queryId!=registry->nonce||
       (int)producer->getKey().planNodeId!=e->boundary||u_sess->stream_cxt.producer_dop!=(uint32)e->participants||
       participant<0||participant>=e->participants||(flags&(EXEC_FLAG_REWIND|EXEC_FLAG_BACKWARD|EXEC_FLAG_MARK))||
       e->database!=u_sess->proc_cxt.MyDatabaseId||e->user!=GetUserId()||e->outer!=GetOuterUserId()||
       e->guc_smp!=u_sess->attr.attr_sql.enable_lake_smp||e->guc_ann!=u_sess->attr.attr_sql.enable_lake_ann||
       e->guc_reuse!=u_sess->attr.attr_sql.ann_orderby_distance_reuse)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP worker has no matching prebound execution identity")));
    /* No installed-version/catalog/SPI lookup here. These complete
     * facts were captured/rechecked by the leader in the same entry
     * whose query/scan identity and snapshot this worker authenticates. */
    LakeStreamCheckInt2Entry(e);
    if(LakeIdentityPartitioned(e->binding)&&!e->partition_qualified)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake partition Stream binding has no leader SQL63 qualification")));
    if(IsA(plan,VecForeignScan)!=e->vec_scan||(e->vec_scan&&!e->vector_smp_qualified))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP vectorized worker plan lacks leader SQL72 qualification")));
    if(LakeBindingHasTemporal(e->binding)&&!e->vector_smp_qualified)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP worker temporal binding has no SQL72 qualification")));
    if(e->vec_scan&&!LakeVectorBindingSupported(e->binding,true))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP vectorized worker binding is not qualified")));
    if(e->int2_columns>0&&!LakeStreamPlainFullScan((ForeignScan*)plan,e->vector_smp_qualified))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake INT2 worker plan is not its qualified full scan")));
    if(e->ann_sources&&!LakeStreamAnnBinding(e->binding,e->ix_smp_extended))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP ANN worker budget or schema differs from prebinding")));
    if(!LakeStreamSameParameters(e->parameters,node->ss.ps.state->es_param_list_info))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake SMP worker external parameters differ from prebinding")));
    Snapshot host=node->ss.ps.state->es_snapshot;
    if(!host||host->satisfies!=e->snapshot->satisfies||host->xmin!=e->snapshot->xmin||host->xmax!=e->snapshot->xmax||
       host->snapshotcsn!=e->snapshot->snapshotcsn||host->timeline!=e->snapshot->timeline||
       host->read_lsn!=e->snapshot->read_lsn||host->subxid!=e->snapshot->subxid||host->xcnt!=e->snapshot->xcnt||
       host->subxcnt!=e->snapshot->subxcnt||host->suboverflowed!=e->snapshot->suboverflowed||
       host->takenDuringRecovery!=e->snapshot->takenDuringRecovery||host->gtm_snapshot_type!=e->snapshot->gtm_snapshot_type||
       host->prepared_count!=e->snapshot->prepared_count||host->user_data||
       (host->xcnt&&(!host->xip||memcmp(host->xip,e->snapshot->xip,sizeof(TransactionId)*host->xcnt)))||
       (host->subxcnt&&(!host->subxip||memcmp(host->subxip,e->snapshot->subxip,sizeof(TransactionId)*host->subxcnt)))||
       (host->prepared_count&&(!host->prepared_array||memcmp(host->prepared_array,e->snapshot->prepared_array,sizeof(TransactionId)*host->prepared_count))))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake SMP worker host snapshot differs from prebinding")));
    auto* worker=(LakeStreamWorker*)palloc0(sizeof(LakeStreamWorker));worker->entry=e;worker->participant=participant;
    MemoryContextCallback* reset=(MemoryContextCallback*)palloc(sizeof(MemoryContextCallback));
    new(reset) MemoryContextCallback();reset->func=LakeStreamWorkerReset;
    reset->arg=std::shared_ptr<void>(std::shared_ptr<void>(),worker);
    MemoryContextRegisterResetCallback(CurrentMemoryContext,reset);
    if(!__sync_bool_compare_and_swap(&e->claimed[participant],0,1))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake SMP task owner claimed twice")));
    worker->claimed=true;
    /* Compute capacity from this participant's immutable task partition
     * before resetting its local filled count. Inline remains participant0. */
    int offset=LakeInlineReadSourceCount(e->binding)>0?1:0;
    int laneFiles=0;
    for(int f=0;f<e->binding->file_count;++f)
        if((f+offset)%e->participants==participant)++laneFiles;
    if(e->scalar_sources&&(e->scalar_sources->files!=e->binding->file_count||e->scalar_sources->entries==NULL))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake SMP scalar source partition is inconsistent")));
    auto* local=(LakeInlineBinding*)palloc(sizeof(LakeInlineBinding));*local=*e->binding;
    local->files=(LakeDataFile*)palloc(sizeof(LakeDataFile)*Max(1,laneFiles));
    *versions=(char**)palloc(sizeof(char*)*Max(1,laneFiles));local->file_count=0;
    if(e->scalar_sources){
        worker->scalar_sources=(LakeScalarFileSources*)palloc(sizeof(LakeScalarFileSources));
        *worker->scalar_sources=*e->scalar_sources;
        worker->scalar_sources->entries=(LakeScalarFileSource*)palloc0(sizeof(LakeScalarFileSource)*Max(1,laneFiles));
        worker->scalar_sources->files=0;
    }
    if(e->ann_sources){
        worker->ann_sources=(LakeAnnFileSources*)palloc(sizeof(LakeAnnFileSources));*worker->ann_sources=*e->ann_sources;
        /* Source descriptors may contain multiple ordered READY alternatives.
         * Partition by owned file_id, never by descriptor ordinal. */
        uint32 count=0;
        for(uint32 r=0;r<e->ann_sources->count;++r)for(int f=0;f<e->binding->file_count;++f)
            if((f+offset)%e->participants==participant&&e->ann_sources->entries[r].file_id==e->binding->files[f].file_id)++count;
        worker->ann_sources->entries=(LakeAnnFileSource*)palloc0(sizeof(LakeAnnFileSource)*Max((uint32)1,count));
        worker->ann_sources->count=0;
        for(uint32 r=0;r<e->ann_sources->count;++r)for(int f=0;f<e->binding->file_count;++f)
            if((f+offset)%e->participants==participant&&e->ann_sources->entries[r].file_id==e->binding->files[f].file_id)
                worker->ann_sources->entries[worker->ann_sources->count++]=e->ann_sources->entries[r];
        Assert(worker->ann_sources->count==count);
    }
    if(participant!=0){
        local->inline_oid=InvalidOid;
        local->inline_sources_loaded=false;
        local->inline_sources=NULL;
        local->inline_source_count=0;
        local->inline_write_index=-1;
    }
    if(offset&&participant==0)write_stderr("LOG: lake SMP task query=%llu scan=%d snapshot=%lld participant=0/%d task=0 inline=1\n",(unsigned long long)registry->nonce,e->node_id,(long long)e->binding->snapshot_id,e->participants);
    for(int f=0;f<e->binding->file_count;++f)if((f+offset)%e->participants==participant){
        Assert(local->file_count<laneFiles);
        if(local->file_count>=laneFiles)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake SMP file partition exceeded its allocated capacity")));
        int at=local->file_count++;local->files[at]=e->binding->files[f];(*versions)[at]=e->versions[f];
        if(worker->scalar_sources){
            Assert(worker->scalar_sources->files==at);
            worker->scalar_sources->entries[at]=e->scalar_sources->entries[f];++worker->scalar_sources->files;
        }
        write_stderr("LOG: lake SMP task query=%llu scan=%d snapshot=%lld participant=%d/%d task=%d file=%lld inline=0\n",(unsigned long long)registry->nonce,e->node_id,(long long)e->binding->snapshot_id,participant,e->participants,f+offset,(long long)e->binding->files[f].file_id);}
    Assert(local->file_count==laneFiles);
    Assert(!worker->scalar_sources||worker->scalar_sources->files==laneFiles);
    if(local->file_count!=laneFiles||(worker->scalar_sources&&worker->scalar_sources->files!=laneFiles))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake SMP file partition did not fill its declared capacity")));
    *table=LakeTxnCreateStreamReadView(local,e->snapshot,RelationGetDescr(node->ss.ss_currentRelation));
    return worker;
}
const LakeAnnFileSources* LakeStreamWorkerAnnSources(const LakeStreamWorker* worker)
{return worker?worker->ann_sources:NULL;}
const LakeScalarFileSources* LakeStreamWorkerScalarSources(const LakeStreamWorker* worker)
{return worker?worker->scalar_sources:NULL;}
bool LakeStreamWorkerVectorSmpQualified(const LakeStreamWorker* worker)
{return worker&&worker->entry&&worker->entry->vector_smp_qualified;}
void LakeStreamWorkerReady(LakeStreamWorker* worker)
{
    if(worker)worker->entry->reader_ready[worker->participant]=1;
}
void LakeStreamWorkerIndexReady(LakeStreamWorker* worker,int verified,int bitmap,int fallback)
{
    if(!worker)return;
    if(verified<0||bitmap<0||bitmap>verified||fallback<0)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("invalid lake SMP scalar coverage counters")));
    auto* e=worker->entry;int p=worker->participant;
    e->scalar_files[p]=verified;e->bitmap_files[p]=bitmap;e->fallback_files[p]=fallback;
    write_stderr("LOG: lake SMP index query=%llu scan=%d snapshot=%lld participant=%d/%d verified=%d bitmap=%d fallback=%d\n",
        (unsigned long long)u_sess->stream_cxt.producer_obj->getKey().queryId,e->node_id,
        (long long)e->binding->snapshot_id,p,e->participants,verified,bitmap,fallback);
}
void LakeStreamWorkerAnnReady(LakeStreamWorker* worker,int verified,int pq,int fallback,int64 candidates)
{
    if(!worker)return;
    if(!worker->ann_sources||verified<0||pq<0||(!worker->entry->ix_smp_extended&&pq!=0)||pq>verified||fallback<0||candidates<0||candidates>(int64)verified*10000)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("invalid lake SMP ANN coverage counters")));
    auto* e=worker->entry;int p=worker->participant;e->ann_files[p]=verified;e->fallback_files[p]=fallback;e->ann_candidates[p]=candidates;
    write_stderr("LOG: lake SMP ANN query=%llu scan=%d snapshot=%lld participant=%d/%d verified=%d pq=%d fallback=%d candidates=%lld local_k=none\n",
        (unsigned long long)u_sess->stream_cxt.producer_obj->getKey().queryId,e->node_id,
        (long long)e->binding->snapshot_id,p,e->participants,verified,pq,fallback,(long long)candidates);
}
void LakeStreamWorkerFirstRow(LakeStreamWorker* worker)
{
    if(!worker||worker->first_row)return;worker->first_row=true;
    auto* e=worker->entry;int p=worker->participant;
    /* For a file-only participant this point follows a real SDK reader_next,
     * with its batch pinned, before this worker emits its first business row. */
    if(e->test_participant!=p)return;
    if(e->test_mode==2)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake SMP diagnostic failed inside live worker reader")));
    if(e->test_mode==1){uint64 key=(uint64)e->test_gate;LOCKTAG tag;
        SET_LOCKTAG_ADVISORY(tag,u_sess->proc_cxt.MyDatabaseId,(uint32)(key>>32),(uint32)key,1);
        (void)LockAcquire(&tag,ExclusiveLock,false,false);
        if(!LockRelease(&tag,ExclusiveLock,false))ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake SMP diagnostic gate was not acquired")));
        CHECK_FOR_INTERRUPTS();
    }
}
void LakeStreamWorkerRow(LakeStreamWorker* worker){if(worker)(void)__sync_fetch_and_add(&worker->entry->rows[worker->participant],1);}
void LakeStreamWorkerAddRows(LakeStreamWorker* worker,uint64 count){if(worker&&count>0)(void)__sync_fetch_and_add(&worker->entry->rows[worker->participant],count);}
void LakeStreamWorkerEnd(LakeStreamWorker* worker)
{if(worker&&!worker->ended){worker->ended=true;__sync_synchronize();worker->entry->done[worker->participant]=1;}}
void LakeStreamExplain(const Plan* plan,ExplainState* es)
{
    if(plan->dop>1){ExplainPropertyText("Lake SMP","native Stream; immutable whole-file tasks; inline owner0",es);ExplainPropertyInteger("Lake SMP Planned DOP",plan->dop,es);
        if(IsA(plan,ForeignScan)||IsA(plan,VecForeignScan)){const auto* scan=(const ForeignScan*)plan;
            if(list_length(scan->fdw_private)==5){List* data=(List*)list_nth(scan->fdw_private,4);
                if(data!=NIL&&IsA(data,List)&&(list_length(data)==7||(list_length(data)==8&&intVal(list_nth(data,5))>=11))){
                    int metric=intVal(list_nth(data,5))-10;
                    const char* names[]={"L2","IP","cosine"};
                    if(metric<1||metric>3)
                        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake SMP ANN metric in plan")));
                    const char* filterMode="unfiltered";
                    if(list_length(data)==8){
                        List* filter=(List*)list_nth(data,7);
                        filterMode=(filter!=NIL&&IsA(filter,List)&&intVal(linitial(filter))==LAKE_ANN_SCALAR_LEAF)?"single-filter":"boolean-filter";
                    }
                    ExplainPropertyText("Lake SMP ANN",psprintf("raw %s %s file candidate union; complete inline; global SQL rerank",names[metric-1],filterMode),es);
                    if(LakeIxSmpExtendedVersionEnabled())
                        ExplainPropertyText("Lake SMP ANN PQ","admitted",es);
                    ExplainPropertyInteger("Lake SMP ANN Module Budget KB",(long)LAKE_ANN_SMP_WORKING_KB,es);
                }
            }
        }
    }
    else ExplainPropertyText("Lake SMP","serial path; parallel candidate requires admitted read-only non-rescannable inputs",es);
}
