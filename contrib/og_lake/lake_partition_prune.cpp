/* Follow pg_lake@873f5103 data_file_pruning.c: file constraints are refuted
 * by native query predicates. This adapter consumes DuckLake descriptors;
 * it does not copy Iceberg metadata or replace the original executor quals. */
#include "postgres.h"
#include "lake_version.h"
#include "lake_numeric.h"
#include "lake_partition_prune.h"
#include "lake_inline_private.h"
#include "lake_temporal.h"
#include "utils/datetime.h"
#include <cmath>
#include "access/attnum.h"
#include "access/tuptoaster.h"
#include "access/transam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/typcache.h"

struct LakePartitionPrune {
    const LakeInlineBinding* binding;
    ForeignScanState* node;
    List* clauses;
    Var** variables;
    MemoryContext refresh_context;
    bool* skipped;
    int skipped_count;
    int nodes;
};

bool LakePartitionPruneVersionEnabled(void)
{return LakeIdentityPartitionKeysVersionEnabled();}

/* Query proof requires its exact installed SQL33 capability in lake_catalog.cpp.
 * Retained SQL32 time file rules do not grant temporal predicate pruning. */

/* Only installed SQL35 adds monotone epoch ranges. SQL33/34 stay eq/NULL. */
static bool LakeTimePartitionRangePruneVersionEnabled(void)
{return LakeCoreFollowupsVersionEnabled();}

static bool LakeTimePartitionPruneType(Oid type)
{
    return LakeTimePartitionPruneVersionEnabled()&&LakeTimePartitionVersionEnabled()&&
        (type==DATEOID||type==TIMESTAMPOID||type==TIMESTAMPTZOID);
}

static bool LakePartitionPruneType(Oid type)
{
    if(type==BOOLOID||type==INT2OID||type==INT4OID||
       type==INT8OID||type==TEXTOID||LakeTimePartitionPruneType(type))return true;
    return LakePartitionComboVersionEnabled()&&
        (type==UUIDOID||type==NUMERICOID||type==BYTEAOID||type==FLOAT4OID||type==FLOAT8OID);
}

static int LakePartitionPruneColumn(const LakeInlineBinding* binding,AttrNumber attribute)
{
    int found=-1;
    for(int c=0;c<binding->column_count;++c){
        if(binding->columns[c].output_attnum!=attribute)continue;
        if(found>=0)return -1;
        found=c;
    }
    return found;
}

static Var* LakePartitionPruneVar(LakePartitionPrune* state,Node* node)
{
    if(!node||!IsA(node,Var))return NULL;
    Var* var=(Var*)node;
    ForeignScan* scan=(ForeignScan*)state->node->ss.ps.plan;
    if(var->varlevelsup!=0||var->varno!=scan->scan.scanrelid||
       var->varattno<1||
       !LakePartitionPruneType(var->vartype))return NULL;
    int index=LakePartitionPruneColumn(state->binding,var->varattno);
    if(index<0)return NULL;
    const LakeInlineColumn* column=&state->binding->columns[index];
    if(var->vartype!=column->type||var->vartypmod!=column->typmod)return NULL;
    return var;
}

static bool LakePartitionPruneValue(Node* node,Oid type)
{
    if(!node||(!IsA(node,Const)&&!IsA(node,Param))||exprType(node)!=type)return false;
    if(IsA(node,Param)){
        Param* param=(Param*)node;
        return param->paramkind==PARAM_EXTERN||param->paramkind==PARAM_EXEC;
    }
    return true;
}

static bool LakePartitionPruneIntegerType(Oid type)
{return type==INT2OID||type==INT4OID||type==INT8OID;}

static bool LakePartitionPruneBuiltinOperator(Oid op,Oid leftType,Oid rightType)
{
    if(!OidIsValid(op)||op>=FirstNormalObjectId)return false;
    Oid left=InvalidOid,right=InvalidOid;
    op_input_types(op,&left,&right);
    if(left!=leftType||right!=rightType)return false;
    Oid function=get_opcode(op);
    if(!OidIsValid(function)||function>=FirstNormalObjectId||!op_strict(op)||
       func_volatile(function)!=PROVOLATILE_IMMUTABLE||get_func_retset(function))return false;
    Oid* args=NULL;int count=0;
    Oid result=get_func_signature(function,&args,&count);
    bool matches=result==BOOLOID&&count==2&&args!=NULL&&args[0]==leftType&&args[1]==rightType;
    if(args)pfree(args);
    return matches;
}

/* predtest enumerates ALL btree/ubtree interpretations, not just the default
 * type-cache family. It can commute a clause and execute a family-selected
 * comparison between the file constant and the query constant. Validate that
 * complete bounded type-pair closure without executing any of those functions.
 * This also hardens the previously admitted same-type predicates. */
static bool LakePartitionPruneProofFamily(Oid family,Oid leftType,Oid rightType)
{
    if(!OidIsValid(family)||family>=FirstNormalObjectId)return false;
    Oid types[2]={leftType,rightType};int count=leftType==rightType?1:2;
    for(int l=0;l<count;++l)for(int r=0;r<count;++r){
        for(int strategy=1;strategy<=5;++strategy){
            Oid op=get_opfamily_member(family,types[l],types[r],strategy);
            if(!LakePartitionPruneBuiltinOperator(op,types[l],types[r]))return false;
            Oid commuted=get_opfamily_member(family,types[r],types[l],6-strategy);
            if(get_commutator(op)!=commuted||
               !LakePartitionPruneBuiltinOperator(commuted,types[r],types[l]))return false;
            if(strategy==3){
                /* predtest uses the equality negator as its sixth, <> test. */
                Oid different=get_negator(op);
                if(!LakePartitionPruneBuiltinOperator(different,types[l],types[r])||
                   get_negator(different)!=op)return false;
            }
        }
    }
    return true;
}

static bool LakePartitionPruneInterpretations(Oid op,Oid leftType,Oid rightType)
{
    List* interpretations=get_op_btree_interpretation(op);
    bool trusted=interpretations!=NIL&&list_length(interpretations)<=32;
    ListCell* cell=NULL;
    foreach(cell,interpretations){
        if(!trusted)break;
        const OpBtreeInterpretation* entry=(const OpBtreeInterpretation*)lfirst(cell);
        trusted=entry->oplefttype==leftType&&entry->oprighttype==rightType&&
            entry->strategy>=1&&entry->strategy<=5&&
            LakePartitionPruneProofFamily(entry->opfamily_id,leftType,rightType);
    }
    list_free_deep(interpretations);
    return trusted;
}

static bool LakePartitionPruneOperator(Oid op,Oid leftType,Oid rightType,bool equalityOnly)
{
    /* Keep the original operand types and operator direction. In particular,
     * never narrow an int4/int8 constant or Param into an int2 file key. */
    if(!OidIsValid(op)||op>=FirstNormalObjectId||!LakePartitionPruneType(leftType)||
       !LakePartitionPruneType(rightType)||(leftType!=rightType&&
       !(LakePartitionPruneIntegerType(leftType)&&LakePartitionPruneIntegerType(rightType))))return false;
    TypeCacheEntry* left=lookup_type_cache(leftType,TYPECACHE_BTREE_OPFAMILY);
    TypeCacheEntry* right=lookup_type_cache(rightType,TYPECACHE_BTREE_OPFAMILY);
    if(!OidIsValid(left->btree_opf)||left->btree_opf>=FirstNormalObjectId||
       left->btree_opf!=right->btree_opf)return false;
    int strategy=get_op_opfamily_strategy(op,left->btree_opf);
    if(strategy<1||strategy>5||(equalityOnly&&strategy!=3)||
       get_opfamily_member(left->btree_opf,leftType,rightType,strategy)!=op||
       !LakePartitionPruneBuiltinOperator(op,leftType,rightType))return false;
    Oid commuted=get_commutator(op);
    if(!LakePartitionPruneBuiltinOperator(commuted,rightType,leftType)||
       !LakePartitionPruneInterpretations(op,leftType,rightType))return false;
    return commuted==op||LakePartitionPruneInterpretations(commuted,rightType,leftType);
}

static Node* LakePartitionPruneBoolean(Var* var,BoolTestType test)
{
    if(!var||var->vartype!=BOOLOID||var->varcollid!=InvalidOid)return NULL;
    if(test==IS_UNKNOWN||test==IS_NOT_UNKNOWN){
        NullTest* result=makeNode(NullTest);result->arg=(Expr*)copyObject(var);
        result->nulltesttype=test==IS_UNKNOWN?IS_NULL:IS_NOT_NULL;result->argisrow=false;
        return (Node*)result;
    }
    if(test!=IS_TRUE&&test!=IS_FALSE&&test!=IS_NOT_TRUE&&test!=IS_NOT_FALSE)return NULL;
    TypeCacheEntry* cache=lookup_type_cache(BOOLOID,TYPECACHE_EQ_OPR);
    if(!LakePartitionPruneOperator(cache->eq_opr,BOOLOID,BOOLOID,true))return NULL;
    bool truth=test==IS_TRUE||test==IS_NOT_FALSE;
    Const* value=makeConst(BOOLOID,-1,InvalidOid,sizeof(bool),BoolGetDatum(truth),false,true);
    Node* equality=(Node*)make_opclause(cache->eq_opr,BOOLOID,false,
        (Expr*)copyObject(var),(Expr*)value,InvalidOid,InvalidOid);
    if(test==IS_NOT_TRUE||test==IS_NOT_FALSE){
        Node* absent=LakePartitionPruneBoolean(var,IS_UNKNOWN);
        return (Node*)makeBoolExpr(OR_EXPR,list_make2(equality,absent),-1);
    }
    return equality;
}

/* A subset of an AND is necessary for the original qual. All OR children
 * must be understood. Boolean normalization preserves the WHERE true set;
 * it is used only in these positive contexts. NOT accepts a single original
 * bool Var, never a recursively weakened AND/OR or any arbitrary expression. */
static Node* LakePartitionPruneQual(LakePartitionPrune* state,Node* node,int depth)
{
    if(!node||depth>32||++state->nodes>256)return NULL;
    Var* var=NULL;Node* normalized=NULL;
    if(IsA(node,BoolExpr)){
        BoolExpr* expression=(BoolExpr*)node;List* children=NIL;ListCell* cell=NULL;
        if(expression->boolop==NOT_EXPR){
            if(list_length(expression->args)!=1)return NULL;
            var=LakePartitionPruneVar(state,(Node*)linitial(expression->args));
            normalized=LakePartitionPruneBoolean(var,IS_FALSE);
            if(!normalized)return NULL;
        }else{
            if(expression->boolop!=AND_EXPR&&expression->boolop!=OR_EXPR)return NULL;
            foreach(cell,expression->args){
                Node* child=LakePartitionPruneQual(state,(Node*)lfirst(cell),depth+1);
                if(child)children=lappend(children,child);
                else if(expression->boolop!=AND_EXPR)return NULL;
            }
            if(children==NIL)return NULL;
            return (Node*)makeBoolExpr(expression->boolop,children,-1);
        }
    }else if(IsA(node,Var)){
        var=LakePartitionPruneVar(state,node);
        normalized=LakePartitionPruneBoolean(var,IS_TRUE);
        if(!normalized)return NULL;
    }else if(IsA(node,BooleanTest)){
        BooleanTest* test=(BooleanTest*)node;
        var=LakePartitionPruneVar(state,(Node*)test->arg);
        normalized=LakePartitionPruneBoolean(var,test->booltesttype);
        if(!normalized)return NULL;
    }else if(IsA(node,NullTest)){
        NullTest* test=(NullTest*)node;
        if(test->argisrow||(test->nulltesttype!=IS_NULL&&test->nulltesttype!=IS_NOT_NULL))return NULL;
        var=LakePartitionPruneVar(state,(Node*)test->arg);
    }else if(IsA(node,OpExpr)){
        OpExpr* op=(OpExpr*)node;
        if(op->opresulttype!=BOOLOID||op->opretset||list_length(op->args)!=2)return NULL;
        Node* left=(Node*)linitial(op->args);Node* right=(Node*)lsecond(op->args);
        var=LakePartitionPruneVar(state,left);Node* value=right;
        if(!var){var=LakePartitionPruneVar(state,right);value=left;}
        if(!var||!LakePartitionPruneValue(value,exprType(value))||
           op->inputcollid!=var->varcollid||
           !LakePartitionPruneOperator(op->opno,exprType(left),exprType(right),
               LakeTimePartitionPruneType(var->vartype)&&
               !LakeTimePartitionRangePruneVersionEnabled()))return NULL;
    }else if(IsA(node,ScalarArrayOpExpr)){
        ScalarArrayOpExpr* op=(ScalarArrayOpExpr*)node;
        if(!op->useOr||list_length(op->args)!=2)return NULL;
        var=LakePartitionPruneVar(state,(Node*)linitial(op->args));
        if(var&&LakeTimePartitionPruneType(var->vartype)&&!LakePartitionComboVersionEnabled())return NULL;
        Node* value=(Node*)lsecond(op->args);
        if(!var||get_element_type(exprType(value))!=var->vartype||
           !LakePartitionPruneValue(value,exprType(value))||op->inputcollid!=var->varcollid||
           !LakePartitionPruneOperator(op->opno,var->vartype,var->vartype,true))return NULL;
    }else return NULL;
    if(!var)return NULL;
    int column=LakePartitionPruneColumn(state->binding,var->varattno);
    if(column<0)return NULL;
    Var* previous=state->variables[column];
    if(previous&&!equal(previous,var))return NULL;
    state->variables[column]=(Var*)copyObject(var);
    return normalized?normalized:(Node*)copyObject(node);
}

LakePartitionPrune* LakePartitionPruneCreate(ForeignScanState* node,const LakeInlineBinding* binding)
{
    if(!LakePartitionPruneVersionEnabled()||!binding||binding->file_count==0||
       !LakeIdentityPartitioned(binding)||!(IsA(node->ss.ps.plan,ForeignScan)||IsA(node->ss.ps.plan,VecForeignScan))||
       node->ss.ps.state->es_plannedstmt->commandType!=CMD_SELECT||node->ss.ps.plan->dop>1)return NULL;
    ForeignScan* scan=(ForeignScan*)node->ss.ps.plan;
    /* Extended scan tlists contain setrefs INDEX_VAR mappings. Keep all for
     * that independent DML/row-locator path until its mapping is qualified. */
    if(scan->fdw_scan_tlist!=NIL||scan->scan.scanrelid==0)return NULL;
    LakePartitionPrune* state=(LakePartitionPrune*)palloc0(sizeof(LakePartitionPrune));
    state->binding=binding;state->node=node;
    state->variables=(Var**)palloc0(sizeof(Var*)*binding->column_count);
    ListCell* cell=NULL;
    foreach(cell,node->ss.ps.plan->qual){
        Node* clause=LakePartitionPruneQual(state,(Node*)lfirst(cell),0);
        if(clause)state->clauses=lappend(state->clauses,clause);
    }
    if(state->clauses==NIL)return NULL;
    state->skipped=(bool*)palloc0(sizeof(bool)*binding->file_count);
    state->refresh_context=AllocSetContextCreate(CurrentMemoryContext,"Lake file partition proof",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    return state;
}

/* Do not execute InitPlans or parameter-fetch callbacks merely to prune.
 * PARAM_EXEC valueType is not a SQL type in all OG executor paths; the Param
 * expression supplies the trusted type, exactly as in native evaluation. */
static Node* LakePartitionPruneParameter(Node* node,void* argument)
{
    if(!node)return NULL;
    LakePartitionPrune* state=(LakePartitionPrune*)argument;
    if(IsA(node,Param)){
        Param* param=(Param*)node;ExprContext* context=state->node->ss.ps.ps_ExprContext;
        Datum value=(Datum)0;bool absent=false;bool ready=false;
        if(param->paramkind==PARAM_EXEC&&param->paramid>=0&&
           param->paramid<state->node->ss.ps.state->es_plannedstmt->nParamExec&&context->ecxt_param_exec_vals){
            ParamExecData* data=&context->ecxt_param_exec_vals[param->paramid];
            if(data->execPlan==NULL&&data->paramVector==NULL){value=data->value;absent=data->isnull;ready=true;}
        }else if(param->paramkind==PARAM_EXTERN&&context->ecxt_param_list_info){
            ParamListInfo params=context->ecxt_param_list_info;
            if(param->paramid>0&&param->paramid<=params->numParams&&!params->params_lazy_bind&&!params->params_need_process){
                ParamExternData* data=&params->params[param->paramid-1];
                if(data->ptype==param->paramtype){value=data->value;absent=data->isnull;ready=true;}
            }
        }
        if(!ready)return (Node*)copyObject(node);
        int16 length=0;bool byValue=false;get_typlenbyval(param->paramtype,&length,&byValue);
        return (Node*)makeConst(param->paramtype,param->paramtypmod,param->paramcollid,length,
            absent?(Datum)0:datumCopy(value,byValue,length),absent,byValue);
    }
    return expression_tree_mutator(node,LakePartitionPruneParameter,argument);
}

/* Recheck catalog-dependent proof admission for each refreshed parameter set.
 * A later family change must never turn a cached safe clause into permission
 * to execute an unknown proof function. Unknown means keep every file. */
static bool LakePartitionPruneUntrusted(Node* node,void* argument)
{
    if(!node)return false;
    if(IsA(node,OpExpr)){
        OpExpr* op=(OpExpr*)node;
        if(list_length(op->args)!=2||
           !LakePartitionPruneOperator(op->opno,exprType((Node*)linitial(op->args)),
               exprType((Node*)lsecond(op->args)),false))return true;
    }else if(IsA(node,ScalarArrayOpExpr)){
        ScalarArrayOpExpr* op=(ScalarArrayOpExpr*)node;
        if(list_length(op->args)!=2||
           !LakePartitionPruneOperator(op->opno,exprType((Node*)linitial(op->args)),
               get_element_type(exprType((Node*)lsecond(op->args))),true))return true;
    }
    return expression_tree_walker(node,(bool (*)())LakePartitionPruneUntrusted,argument);
}

static List* LakePartitionPruneConstraints(LakePartitionPrune* state,const LakeDataFile* file)
{
    const LakePartitionSpec* spec=&file->partition.spec;
    if(!spec->enabled)return NIL;
    if(spec->key_count<1||spec->keys==NULL||file->partition.tuple.count!=spec->key_count||
       file->partition.tuple.values==NULL)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake partition proof descriptor")));
    List* constraints=NIL;
    for(int k=0;k<spec->key_count;++k){
        const LakePartitionField* field=&spec->keys[k];
        if(!field->host_bound||field->transform!=LAKE_PARTITION_IDENTITY||field->parameter!=0||
           field->column_index<0||field->column_index>=state->binding->column_count)continue;
        Var* var=state->variables[field->column_index];
        if(!var||var->vartype!=field->type||var->vartypmod!=field->typmod||var->varcollid!=field->collation||
           state->binding->columns[field->column_index].column_id!=field->column_id)continue;
        const LakePartitionValue* value=&file->partition.tuple.values[k];
        NullTest* nullTest=makeNode(NullTest);nullTest->arg=(Expr*)copyObject(var);
        nullTest->nulltesttype=value->is_null?IS_NULL:IS_NOT_NULL;nullTest->argisrow=false;
        constraints=lappend(constraints,nullTest);
        if(value->is_null)continue;
        TypeCacheEntry* cache=lookup_type_cache(field->type,TYPECACHE_EQ_OPR);
        if(!OidIsValid(cache->eq_opr)||!LakePartitionPruneOperator(cache->eq_opr,field->type,field->type,true))continue;
        int16 length=0;bool byValue=false;get_typlenbyval(field->type,&length,&byValue);
        Const* constant=makeConst(field->type,field->typmod,field->collation,length,
            datumCopy(value->value,byValue,length),false,byValue);
        constraints=lappend(constraints,make_opclause(cache->eq_opr,BOOLOID,false,
            (Expr*)copyObject(var),(Expr*)constant,InvalidOid,field->collation));
    }
    return constraints;
}

/* pg_lake's synthetic proof: x=C implies T(x)=T(C) for a deterministic
 * NULL-preserving transform. Bucket uses INT4; time rules use BIGINT. This
 * equality implication does not assume calendar month/day/hour monotonicity.
 * Proof Vars never enter executor quals or fdw_private. A historical key
 * ordinal has one distinct attribute even when source columns repeat.
 *
 * Bucket and time share one budget: at most32 transformed keys/1024 visited
 * proof nodes per file and4MiB processed constants across one full Refresh.
 * Exhaustion discards the whole uncommitted transformed proof, never an OR arm.
 */
static const int LAKE_TRANSFORM_PRUNE_KEYS=32;
static const int LAKE_TRANSFORM_PRUNE_NODES=1024;
static const Size LAKE_TRANSFORM_PRUNE_BYTES=4*1024*1024;
struct LakeTransformPruneProof {
    const LakePartitionField* field;
    Var* source;
    Var* synthetic;
    Oid equality;
    int* nodes;
    Size* remaining_bytes; /* Shared across one refresh, including Rescan. */
    bool* exhausted;
};

static Node* LakeTransformPruneNull(Var* var,NullTestType test)
{
    NullTest* result=makeNode(NullTest);result->arg=(Expr*)copyObject(var);
    result->nulltesttype=test;result->argisrow=false;return (Node*)result;
}

static Node* LakeTransformPruneCompare(const LakeTransformPruneProof* proof,Datum value,Oid comparison)
{
    Oid type=proof->synthetic->vartype;
    Oid collation=proof->synthetic->varcollid;
    int16 length=0;bool byValue=false;
    get_typlenbyval(type,&length,&byValue);
    Const* constant=makeConst(type,-1,collation,length,
        datumCopy(value,byValue,length),false,byValue);
    return (Node*)make_opclause(comparison,BOOLOID,false,
        (Expr*)copyObject(proof->synthetic),(Expr*)constant,InvalidOid,collation);
}

static Node* LakeTransformPruneEqual(const LakeTransformPruneProof* proof,Datum value)
{return LakeTransformPruneCompare(proof,value,proof->equality);}

/* Epoch time transforms and truncate(W) (integer floor, UTF-8 char prefix under
 * C collation, BYTEA byte prefix) are nondecreasing in the source order. */
static bool LakeTransformRangePruneField(const LakePartitionField* field)
{
    if(field->transform==LAKE_PARTITION_TRUNCATE)
        return LakeTruncatePartitionVersionEnabled()&&LakePartitionFieldSupported(field);
    if(!LakeTimePartitionRangePruneVersionEnabled()||
       !LakeTimePartitionPruneType(field->type)||field->result_type!=INT8OID)return false;
    switch(field->transform){
        case LAKE_PARTITION_EPOCH_YEAR:
        case LAKE_PARTITION_EPOCH_MONTH:
        case LAKE_PARTITION_EPOCH_DAY:
        case LAKE_PARTITION_EPOCH_HOUR:return true;
        default:return false;
    }
}

static Oid LakeTransformPruneComparison(const LakeTransformPruneProof* proof,
    Oid sourceOperator,bool sourceOnLeft,Oid leftType,Oid rightType)
{
    const LakePartitionField* field=proof->field;
    bool ranges=LakeTransformRangePruneField(field);
    if(!LakePartitionPruneOperator(sourceOperator,leftType,rightType,!ranges))return InvalidOid;
    TypeCacheEntry* source=lookup_type_cache(field->type,TYPECACHE_BTREE_OPFAMILY);
    int strategy=get_op_opfamily_strategy(sourceOperator,source->btree_opf);
    if(!sourceOnLeft)strategy=6-strategy;
    if(strategy==3)return proof->equality;
    if(!ranges||strategy<1||strategy>5)return InvalidOid;
    /* x<c implies f(x)<=f(c), never f(x)<f(c): one partition can contain
     * values on both sides of c. Likewise x>c only implies f(x)>=f(c). */
    int necessary=strategy<3?2:4;
    Oid resultType=field->result_type;
    TypeCacheEntry* target=lookup_type_cache(resultType,TYPECACHE_BTREE_OPFAMILY);
    if(!OidIsValid(target->btree_opf)||target->btree_opf>=FirstNormalObjectId)return InvalidOid;
    Oid op=get_opfamily_member(target->btree_opf,resultType,resultType,necessary);
    /* The generic check validates every btree interpretation and commutator,
     * including custom family membership, before predtest may execute it. */
    return LakePartitionPruneOperator(op,resultType,resultType,false)?op:InvalidOid;
}

/* A query constant has the host type's wider domain. Validate without the
 * writer's ERROR wrappers before reusing the exact existing transform. This
 * includes infinity, transport epoch overflow and unsupported float datetimes.
 * timestamp2tm(NULL tzp) is repeated deliberately: the transform must be known
 * to decompose this same instant without adding an error to the original qual. */
static bool LakeTimePruneConstantInRange(Oid type,Datum value)
{
    if(type==DATEOID){
        DateADT date=DatumGetDateADT(value);int32_t output=0;
        return LakeTemporalDateInRange(date)&&OgLakeTemporalDateToUnix(date,&output);
    }
#ifdef HAVE_INT64_TIMESTAMP
    if(type==TIMESTAMPOID||type==TIMESTAMPTZOID){
        Timestamp stamp=type==TIMESTAMPTZOID?DatumGetTimestampTz(value):DatumGetTimestamp(value);
        int64_t output=0;struct pg_tm tm;fsec_t fsec;
        return !TIMESTAMP_NOT_FINITE(stamp)&&IS_VALID_TIMESTAMP(stamp)&&
            OgLakeTemporalMicrosToUnix(stamp,&output)&&
            timestamp2tm(stamp,NULL,&tm,&fsec,NULL,NULL)==0;
    }
#else
    (void)value;
#endif
    return false;
}

/* Pruning cannot turn a valid SELECT into a new text-budget/encoding error.
 * The writer's stricter admission remains unchanged. Do not run casts, user
 * functions or parameter callbacks to manufacture a hashable constant. */
static bool LakeTransformPruneConstant(LakeTransformPruneProof* proof,const Const* value,Datum* result)
{
    Size bytes=sizeof(int64);
    if(proof->field->type==TEXTOID||proof->field->type==BYTEAOID){
        if(proof->field->type==TEXTOID&&(GetDatabaseEncoding()!=PG_UTF8||!lc_collate_is_c(proof->source->varcollid)))return false;
        Size size=toast_raw_datum_size(value->constvalue);
        if(size<VARHDRSZ||size>VARHDRSZ+LAKE_TRANSFORM_PRUNE_BYTES)return false;
        bytes=size-VARHDRSZ;
    }else if(proof->field->type==NUMERICOID){
        Size size=toast_raw_datum_size(value->constvalue);
        if(size<VARHDRSZ||size>VARHDRSZ+256)return false;
        bytes=size;
    }
    if(bytes>*proof->remaining_bytes){*proof->exhausted=true;return false;}
    *proof->remaining_bytes-=bytes;
    if((LakePartitionIsTimeTransform(proof->field->transform)||
        proof->field->type==DATEOID||proof->field->type==TIMESTAMPOID||proof->field->type==TIMESTAMPTZOID)&&
       !LakeTimePruneConstantInRange(proof->field->type,value->constvalue))return false;
    if(proof->field->type==FLOAT4OID||proof->field->type==FLOAT8OID){
        double v=proof->field->type==FLOAT4OID?(double)DatumGetFloat4(value->constvalue):DatumGetFloat8(value->constvalue);
        if(std::isnan(v))return false;
    }
    if(proof->field->type==NUMERICOID){
        unsigned char coeff16[16];
        if(!LakeNumericTryCoefficient(value->constvalue,proof->field->typmod,coeff16,true))return false;
    }
    if(proof->field->type==TEXTOID){
        text* input=DatumGetTextPP(value->constvalue);
        bool valid=pg_verify_mbstr(PG_UTF8,VARDATA_ANY(input),VARSIZE_ANY_EXHDR(input),true);
        if((Pointer)input!=DatumGetPointer(value->constvalue))pfree(input);
        if(!valid)return false;
    }
    if(proof->field->transform==LAKE_PARTITION_TRUNCATE&&
       LakePartitionPruneIntegerType(proof->field->type)&&
       LakePartitionPruneIntegerType(value->consttype)){
        int64 src=value->consttype==INT2OID?(int64)DatumGetInt16(value->constvalue):
            value->consttype==INT4OID?(int64)DatumGetInt32(value->constvalue):
            DatumGetInt64(value->constvalue);
        int64 res=0;
        if(!LakePartitionTruncateInt64Floor(src,proof->field->parameter,&res))
            return false;
        if(proof->field->result_type==INT4OID){
            if(res<PG_INT32_MIN||res>PG_INT32_MAX)return false;
            *result=Int32GetDatum((int32)res);return true;
        }
        if(proof->field->result_type==INT8OID){
            *result=Int64GetDatum(res);return true;
        }
        return false;
    }
    *result=LakePartitionApplyTransform(proof->field,value->constvalue);return true;
}

/* Return a necessary positive transformed condition. The existing parser already
 * normalized bare BOOL/NOT BOOL/BooleanTest without weakening their WHERE true
 * set. Unknown AND children may be omitted; every OR child must be derivable.
 * Source ranges require the independent epoch-only gate; cross-type
 * comparisons and ScalarArrayOpExpr are not admitted.
 */
static Node* LakeTransformPruneNecessary(LakeTransformPruneProof* proof,Node* node,int depth)
{
    if(!node||*proof->exhausted)return NULL;
    if(depth>32||++*proof->nodes>LAKE_TRANSFORM_PRUNE_NODES){*proof->exhausted=true;return NULL;}
    if(IsA(node,BoolExpr)){
        BoolExpr* expression=(BoolExpr*)node;
        if(expression->boolop!=AND_EXPR&&expression->boolop!=OR_EXPR)return NULL;
        List* children=NIL;ListCell* cell=NULL;
        foreach(cell,expression->args){
            Node* child=LakeTransformPruneNecessary(proof,(Node*)lfirst(cell),depth+1);
            if(*proof->exhausted)return NULL;
            if(child)children=lappend(children,child);
            else if(expression->boolop==OR_EXPR)return NULL;
        }
        return children==NIL?NULL:(Node*)makeBoolExpr(expression->boolop,children,-1);
    }
    if(IsA(node,NullTest)){
        NullTest* test=(NullTest*)node;
        if(test->argisrow||!equal(test->arg,proof->source)||
           (test->nulltesttype!=IS_NULL&&test->nulltesttype!=IS_NOT_NULL))return NULL;
        /* Every admitted bucket/time transform is NULL iff its source is NULL. */
        return LakeTransformPruneNull(proof->synthetic,test->nulltesttype);
    }
    if(!IsA(node,OpExpr))return NULL;
    OpExpr* op=(OpExpr*)node;
    if(op->opresulttype!=BOOLOID||op->opretset||list_length(op->args)!=2||
       op->inputcollid!=proof->source->varcollid)return NULL;
    Node* left=(Node*)linitial(op->args);Node* right=(Node*)lsecond(op->args);
    bool sourceOnLeft=equal(left,proof->source);
    Node* raw=sourceOnLeft?right:equal(right,proof->source)?left:NULL;
    Oid leftType=exprType(left),rightType=exprType(right);
    bool crossInt=proof->field->transform==LAKE_PARTITION_TRUNCATE&&
        LakePartitionPruneIntegerType(proof->field->type)&&
        LakePartitionPruneIntegerType(sourceOnLeft?rightType:leftType);
    if(!raw||!IsA(raw,Const)||
       (sourceOnLeft?leftType!=proof->field->type:rightType!=proof->field->type)||
       (!crossInt&&(sourceOnLeft?rightType!=proof->field->type:leftType!=proof->field->type)))return NULL;
    Oid comparison=LakeTransformPruneComparison(proof,op->opno,sourceOnLeft,leftType,rightType);
    if(!OidIsValid(comparison))return NULL;
    Const* value=(Const*)raw;
    /* Every admitted builtin comparison is strict, hence NULL has no true row. */
    if(value->constisnull)return (Node*)makeBoolConst(false,false);
    Datum transformed=(Datum)0;
    if(!LakeTransformPruneConstant(proof,value,&transformed))return NULL;
    return LakeTransformPruneCompare(proof,transformed,comparison);
}

static void LakePartitionPruneTransformed(LakePartitionPrune* state,const LakeDataFile* file,
    List* original,Size* remainingBytes,List** constraints,List** clauses)
{
    const LakePartitionSpec* spec=&file->partition.spec;
    bool bucketEnabled=LakeBucketPartitionVersionEnabled();
    bool timeEnabled=LakeTimePartitionPruneVersionEnabled()&&LakeTimePartitionVersionEnabled();
    bool truncateEnabled=LakeTruncatePartitionVersionEnabled();
    if(!spec->enabled||(!bucketEnabled&&!timeEnabled&&!truncateEnabled))return;
    /* The descriptor was validated by the ordinary file loader. Still guard
     * all proof-only numbering and do not assume dense business attnums. */
    int maximum=0,transformKeys=0;
    for(int c=0;c<state->binding->column_count;++c){
        int attribute=state->binding->columns[c].output_attnum;
        if(attribute<1||attribute>MaxAttrNumber)return;
        maximum=Max(maximum,attribute);
    }
    if(spec->key_count<1||spec->key_count>MaxAttrNumber-maximum)return;
    for(int k=0;k<spec->key_count;++k){
        LakePartitionTransform transform=spec->keys[k].transform;
        if((bucketEnabled&&transform==LAKE_PARTITION_BUCKET)||
           (timeEnabled&&LakePartitionIsTimeTransform(transform))||
           (truncateEnabled&&transform==LAKE_PARTITION_TRUNCATE))++transformKeys;
    }
    if(transformKeys==0||transformKeys>LAKE_TRANSFORM_PRUNE_KEYS)return;
    TypeCacheEntry* bucketCache=lookup_type_cache(INT4OID,TYPECACHE_EQ_OPR);
    bool bucketTrusted=bucketEnabled&&LakePartitionPruneOperator(bucketCache->eq_opr,INT4OID,INT4OID,true);
    TypeCacheEntry* timeCache=timeEnabled?lookup_type_cache(INT8OID,TYPECACHE_EQ_OPR):NULL;
    bool timeTrusted=timeCache&&LakePartitionPruneOperator(timeCache->eq_opr,INT8OID,INT8OID,true);
    if(!bucketTrusted&&!timeTrusted&&!truncateEnabled)return;
    int nodes=0;bool exhausted=false;List* derived=NIL;List* bounds=NIL;
    for(int k=0;k<spec->key_count;++k){
        CHECK_FOR_INTERRUPTS();const LakePartitionField* field=&spec->keys[k];
        bool bucket=bucketTrusted&&field->transform==LAKE_PARTITION_BUCKET&&
            field->result_type==INT4OID&&field->parameter>=1&&field->parameter<=PG_INT32_MAX;
        bool time=timeTrusted&&LakePartitionIsTimeTransform(field->transform)&&
            field->result_type==INT8OID&&field->parameter==0&&LakeTimePartitionPruneType(field->type);
        bool truncate=truncateEnabled&&field->transform==LAKE_PARTITION_TRUNCATE&&
            field->parameter>=1&&field->parameter<=PG_INT32_MAX&&LakePartitionFieldSupported(field);
        if(!field->host_bound||(!bucket&&!time&&!truncate)||field->column_index<0||
           field->column_index>=state->binding->column_count)continue;
        Var* source=state->variables[field->column_index];
        if(!source||source->vartype!=field->type||source->vartypmod!=field->typmod||
           source->varcollid!=field->collation||
           state->binding->columns[field->column_index].column_id!=field->column_id)continue;
        Oid resultType=truncate?field->result_type:(time?INT8OID:INT4OID);
        Oid equality=InvalidOid;
        if(bucket)equality=bucketCache->eq_opr;
        else if(time)equality=timeCache->eq_opr;
        else{
            TypeCacheEntry* eqCache=lookup_type_cache(resultType,TYPECACHE_EQ_OPR);
            if(OidIsValid(eqCache->eq_opr)&&LakePartitionPruneOperator(eqCache->eq_opr,resultType,resultType,true))
                equality=eqCache->eq_opr;
        }
        if(!OidIsValid(equality))continue;
        Oid resultCollation=truncate?field->collation:InvalidOid;
        Var* synthetic=makeVar(source->varno,(AttrNumber)(maximum+k+1),resultType,-1,resultCollation,0);
        LakeTransformPruneProof proof={field,source,synthetic,equality,&nodes,remainingBytes,&exhausted};
        List* keyClauses=NIL;ListCell* cell=NULL;
        foreach(cell,original){
            Node* necessary=LakeTransformPruneNecessary(&proof,(Node*)lfirst(cell),0);
            if(exhausted)return; /* Discard all uncommitted per-file transformed work. */
            if(necessary)keyClauses=lappend(keyClauses,necessary);
        }
        if(keyClauses==NIL)continue;
        const LakePartitionValue* value=&file->partition.tuple.values[k];
        bounds=lappend(bounds,LakeTransformPruneNull(synthetic,value->is_null?IS_NULL:IS_NOT_NULL));
        if(!value->is_null)bounds=lappend(bounds,LakeTransformPruneEqual(&proof,value->value));
        derived=list_concat(derived,keyClauses);
    }
    if(derived==NIL)return;
    /* Native proof may inspect both original and synthetic operators. Recheck
     * the entire generated closure under this refreshed catalog view. */
    if(LakePartitionPruneUntrusted((Node*)bounds,NULL)||LakePartitionPruneUntrusted((Node*)derived,NULL))return;
    *constraints=list_concat(*constraints,bounds);
    *clauses=list_concat(list_copy(original),derived);
}

const bool* LakePartitionPruneRefresh(LakePartitionPrune* state,ExprContext* context)
{
    if(!state||context!=state->node->ss.ps.ps_ExprContext)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake partition proof has a different execution context")));
    MemoryContextReset(state->refresh_context);
    MemoryContext previous=MemoryContextSwitchTo(state->refresh_context);
    PG_TRY();
    {
        List* clauses=(List*)LakePartitionPruneParameter((Node*)state->clauses,state);
        bool trusted=!LakePartitionPruneUntrusted((Node*)clauses,NULL);
        MemoryContext fileContext=AllocSetContextCreate(state->refresh_context,"Lake single file proof",
            ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        Size transformBytes=LAKE_TRANSFORM_PRUNE_BYTES;
        state->skipped_count=0;
        for(int f=0;f<state->binding->file_count;++f){
            CHECK_FOR_INTERRUPTS();
            MemoryContextSwitchTo(fileContext);
            List* constraints=trusted?LakePartitionPruneConstraints(state,&state->binding->files[f]):NIL;
            List* fileClauses=clauses;
            if(trusted)LakePartitionPruneTransformed(state,&state->binding->files[f],clauses,&transformBytes,&constraints,&fileClauses);
            state->skipped[f]=constraints!=NIL&&predicate_refuted_by(constraints,fileClauses,false);
            if(state->skipped[f])++state->skipped_count;
            MemoryContextSwitchTo(state->refresh_context);
            MemoryContextReset(fileContext);
        }
        MemoryContextDelete(fileContext);
        MemoryContextSwitchTo(previous);
    }
    PG_CATCH();{MemoryContextSwitchTo(previous);PG_RE_THROW();}PG_END_TRY();
    return state->skipped;
}

int LakePartitionPruneSkipped(const LakePartitionPrune* state)
{return state?state->skipped_count:0;}
