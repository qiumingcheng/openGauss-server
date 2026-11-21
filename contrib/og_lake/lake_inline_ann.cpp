/* Existing DiskANN graph/IndexAM, over standard TEXT inline vector encoding.
 * A graph entry is a physical candidate; lake visibility remains authoritative. */
#include "postgres.h"
#include "lake_version.h"
#include "lake_cost.h"
#include "knl/knl_variable.h"
#include "lake_inline_ann.h"
#include "lake_file_ann.h"
#include "lake_inline_reader.h"
#include "lake_file_ann_catalog.h"
#include "lake_txn_private.h"
#include "lake_stream.h"
#include "lake_scalar_key.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_index.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/tlist.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "storage/lmgr.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/spccache.h"
#include "access/datavec/diskann.h"

/* Fixed builtin identities from the pinned native catalogs, not search_path. */
int LakeAnnMetric(const char* name)
{
    if(strcmp(name,"l2")==0)return 1;
    if(strcmp(name,"ip")==0)return 2;
    if(strcmp(name,"cosine")==0)return 3;
    ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("lake DiskANN metric must be l2, ip or cosine")));
    return 0;
}
Oid LakeAnnOperator(int metric)
{
    return metric>=1&&metric<=3?(Oid)(8310+metric):InvalidOid;
}

static Expr* LakeAnnExpression(AttrNumber attribute,int dimension)
{
    Var* column=makeVar(1,attribute,TEXTOID,-1,DEFAULT_COLLATION_OID,0);
    Const* brackets=makeConst(TEXTOID,-1,DEFAULT_COLLATION_OID,-1,CStringGetTextDatum("[]"),false,false);
    FuncExpr* trim=makeFuncExpr(884,TEXTOID,list_make2(column,brackets),DEFAULT_COLLATION_OID,DEFAULT_COLLATION_OID,COERCE_EXPLICIT_CALL);
    Const* comma=makeConst(TEXTOID,-1,DEFAULT_COLLATION_OID,-1,CStringGetTextDatum(","),false,false);
    FuncExpr* split=makeFuncExpr(394,TEXTARRAYOID,list_make2(trim,comma),DEFAULT_COLLATION_OID,DEFAULT_COLLATION_OID,COERCE_EXPLICIT_CALL);
    ArrayCoerceExpr* array=makeNode(ArrayCoerceExpr);
    array->arg=(Expr*)split;array->elemfuncid=4192;array->resulttype=FLOAT4ARRAYOID;
    array->resulttypmod=-1;array->resultcollid=InvalidOid;array->isExplicit=true;
    array->coerceformat=COERCE_EXPLICIT_CAST;array->location=-1;
    return (Expr*)makeFuncExpr(8216,VECTOROID,list_make3(array,
        makeConst(INT4OID,-1,InvalidOid,4,Int32GetDatum(dimension),false,true),
        makeConst(BOOLOID,-1,InvalidOid,1,BoolGetDatum(true),false,true)),InvalidOid,InvalidOid,COERCE_EXPLICIT_CAST);
}

struct LakeAnnVariable {Var* variable;int count;};
static bool LakeAnnFindVariable(Node* node,LakeAnnVariable* found)
{
    if(node==NULL)return false;
    if(IsA(node,Var)){found->variable=(Var*)node;++found->count;return false;}
    return expression_tree_walker(node,(bool(*)())LakeAnnFindVariable,(void*)found);
}
static bool LakeAnnCanonical(Node* expression,AttrNumber* attribute,int* dimension)
{
    if(!IsA(expression,FuncExpr))return false;
    FuncExpr* function=(FuncExpr*)expression;
    if(function->funcid!=8216||list_length(function->args)!=3||!IsA(lsecond(function->args),Const))return false;
    Const* size=(Const*)lsecond(function->args);
    if(size->consttype!=INT4OID||size->constisnull)return false;
    int dims=DatumGetInt32(size->constvalue);
    LakeAnnVariable found={NULL,0};LakeAnnFindVariable(expression,&found);
    if(dims<1||dims>DISKANN_MAX_DIM||found.count!=1||found.variable->varno!=1||
        found.variable->varlevelsup!=0||found.variable->varattno<=0||found.variable->vartype!=TEXTOID)return false;
    if(!equal(expression,LakeAnnExpression(found.variable->varattno,dims)))return false;
    *attribute=found.variable->varattno;*dimension=dims;return true;
}

void LakeAnnCheckMethod(void)
{
    HeapTuple tuple=SearchSysCache1(AMOID,ObjectIdGetDatum(DISKANN_AM_OID));
    if(!HeapTupleIsValid(tuple))ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native DiskANN access method is unavailable")));
    Form_pg_am method=(Form_pg_am)GETSTRUCT(tuple);
    bool valid=strcmp(NameStr(method->amname),"diskann")==0&&method->amhandler==8532&&
        method->ambeginscan==8534&&method->amgettuple==8535&&method->amrescan==8536&&
        method->amendscan==8537&&method->ambuild==8538&&method->amcostestimate==8542&&method->amcanreturn==InvalidOid&&
        method->aminsert==8533&&method->ambuildempty==8539&&method->ambulkdelete==8540&&method->amvacuumcleanup==8541&&
        method->amoptions==8543&&method->amgetbitmap==InvalidOid&&method->ammarkpos==InvalidOid&&method->amrestrpos==InvalidOid&&
        method->ammerge==InvalidOid&&method->amdelete==InvalidOid&&method->amsupport==4&&method->amstrategies==0&&
        method->amcanorderbyop&&!method->amcanorder&&!method->amcanbackward&&!method->amcanunique&&!method->amcanmulticol;
    ReleaseSysCache(tuple);
    if(!valid)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        errmsg("native DiskANN catalog does not match this build"),errhint("Use matching native initialization/catalog upgrade; do not patch callbacks to bypass validation.")));
}

bool LakeIsAnnInlineIndex(Oid indexOid)
{
    HeapTuple tuple=SearchSysCache1(RELOID,ObjectIdGetDatum(indexOid));
    if(!HeapTupleIsValid(tuple))return false;
    bool result=((Form_pg_class)GETSTRUCT(tuple))->relam==DISKANN_AM_OID;ReleaseSysCache(tuple);return result;
}

Relation LakeOpenTrustedAnnInlineIndex(Oid indexOid,Oid owner)
{
    LockRelationOid(indexOid,AccessShareLock);LakeAnnCheckMethod();
    HeapTuple cls=SearchSysCache1(RELOID,ObjectIdGetDatum(indexOid));
    if(!HeapTupleIsValid(cls))ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake DiskANN index disappeared")));
    Form_pg_class form=(Form_pg_class)GETSTRUCT(cls);
    if(form->relkind!=RELKIND_INDEX||form->relam!=DISKANN_AM_OID||form->relowner!=owner)
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("untrusted lake DiskANN ownership or method")));
    ReleaseSysCache(cls);
    HeapTuple tuple=SearchSysCache1(INDEXRELID,ObjectIdGetDatum(indexOid));
    if(!HeapTupleIsValid(tuple))ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake DiskANN definition disappeared")));
    Form_pg_index index=(Form_pg_index)GETSTRUCT(tuple);bool absent=false;
    Datum predicate=SysCacheGetAttr(INDEXRELID,tuple,Anum_pg_index_indpred,&absent);
    (void)predicate;
    if(!absent||index->indnatts!=1||index->indkey.values[0]!=0||index->indisunique||index->indisprimary||
        index->indisexclusion||!index->indimmediate||!index->indisvalid||!index->indisready)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake DiskANN requires a valid single canonical expression")));
    Datum encoded=SysCacheGetAttr(INDEXRELID,tuple,Anum_pg_index_indexprs,&absent);
    if(absent)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake DiskANN has no vector expression")));
    Node* expressions=(Node*)stringToNode(TextDatumGetCString(encoded));AttrNumber attribute=0;int dimension=0;
    if(!IsA(expressions,List)||list_length((List*)expressions)!=1||
        !LakeAnnCanonical((Node*)linitial((List*)expressions),&attribute,&dimension)||
        get_atttype(index->indrelid,attribute)!=TEXTOID)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unreviewed lake DiskANN expression")));
    oidvector* classes=(oidvector*)DatumGetPointer(SysCacheGetAttr(INDEXRELID,tuple,Anum_pg_index_indclass,&absent));
    if(absent||classes->dim1!=1||classes->values[0]<8917||classes->values[0]>8919)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unreviewed lake DiskANN operator class")));
    Oid opclass=classes->values[0];
    oidvector* collations=(oidvector*)DatumGetPointer(SysCacheGetAttr(INDEXRELID,tuple,Anum_pg_index_indcollation,&absent));
    if(absent||collations->dim1!=1||OidIsValid(collations->values[0]))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake DiskANN has an unreviewed collation")));
    ReleaseSysCache(tuple);
    HeapTuple opTuple=SearchSysCache1(CLAOID,ObjectIdGetDatum(opclass));
    if(!HeapTupleIsValid(opTuple))ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native DiskANN operator class disappeared")));
    Form_pg_opclass op=(Form_pg_opclass)GETSTRUCT(opTuple);
    if(op->opcmethod!=DISKANN_AM_OID||op->opcowner!=BOOTSTRAP_SUPERUSERID||op->opcnamespace!=PG_CATALOG_NAMESPACE||
        op->opcintype!=VECTOROID||op->opcfamily!=8400+opclass-8917)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native DiskANN operator class changed")));
    ReleaseSysCache(opTuple);
    /* Only after validating the raw expression and builtin method may relcache
     * evaluate/fold the expression or initialize AM/support callbacks. */
    Relation result=index_open(indexOid,AccessShareLock);
    for(int i=0;i<result->rd_am->amsupport;++i)
        if(result->rd_support[i]>=FirstNormalObjectId)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake DiskANN has an unreviewed support function")));
    if(DiskAnnEnablePQ(result))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake inline DiskANN PQ platform is not qualified")));
    return result;
}

Relation LakeOpenInlineAnnIndex(Oid indexOid,const LakeInlineBinding* binding,AttrNumber column,int metric)
{
    if(!OidIsValid(binding->inline_oid)||column<=0||column>binding->column_count||metric<1||metric>3||
        binding->columns[column-1].type!=VECTOROID)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("invalid lake DiskANN binding")));
    Relation index=LakeOpenTrustedAnnInlineIndex(indexOid,LakeMetadataOwner());
    AttrNumber attribute=get_attnum(binding->inline_oid,LakeInlinePhysicalName(&binding->columns[column-1]));
    List* expressions=RelationGetIndexExpressions(index);
    if(index->rd_index->indrelid!=binding->inline_oid||index->rd_opfamily[0]!=(Oid)(8399+metric)||
        attribute<=0||list_length(expressions)!=1||
        !equal(linitial(expressions),LakeAnnExpression(attribute,binding->columns[column-1].typmod)))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake DiskANN column, dimension or metric changed")));
    return index;
}

Oid LakeBuildInlineAnnIndex(LakeInlineBinding* binding,int column,const char* uuid,int metric)
{
    LakeAnnCheckMethod();
    if(!OidIsValid(binding->inline_oid))ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("inline DiskANN requires a committed carrier")));
    Relation carrier=heap_open(binding->inline_oid,ShareLock);Oid owner=LakeMetadataOwner();
    LakeValidatePrivateRelation(carrier,owner,false);
    /* The carrier lock may have waited for a DML/flush commit after admission.
     * Register the source horizon only after that wait, while ShareLock keeps
     * subsequent physical publication out of this synchronous native build. */
    Snapshot latest=RegisterSnapshot(GetLatestSnapshot());
    PG_TRY();{
        LakeInlineBinding* fresh=LakeLoadCommittedInlineBinding(binding->relation_oid,latest);
        if(fresh->inline_oid!=binding->inline_oid||strcmp(fresh->table_uuid,binding->table_uuid)!=0||
            column>=fresh->column_count||fresh->columns[column].column_id!=binding->columns[column].column_id||
            fresh->columns[column].type!=binding->columns[column].type||fresh->columns[column].typmod!=binding->columns[column].typmod)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("lake DiskANN source changed while waiting for build lock")));
        binding->snapshot_id=fresh->snapshot_id;UnregisterSnapshot(latest);
    }PG_CATCH();{UnregisterSnapshot(latest);PG_RE_THROW();}PG_END_TRY();
    if(RelationIsUstoreFormat(carrier))ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake DiskANN Ustore adapter is not qualified")));
    int dimension=binding->columns[column].typmod;
    AttrNumber attribute=get_attnum(binding->inline_oid,LakeInlinePhysicalName(&binding->columns[column]));
    if(attribute<=0||get_atttype(binding->inline_oid,attribute)!=TEXTOID||dimension<1||dimension>DISKANN_MAX_DIM)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake DiskANN requires TEXT inline vectors of dimension1..1536")));
    char compact[33];int length=0;
    for(const char* p=uuid;*p;++p)if(*p!='-'){if(length>=32)elog(ERROR,"invalid lake index UUID");compact[length++]=*p;}
    if(length!=32)elog(ERROR,"invalid lake index UUID");compact[length]='\0';
    IndexStmt* statement=makeNode(IndexStmt);
    statement->relation=makeRangeVar(binding->metadata_schema,pstrdup(RelationGetRelationName(carrier)),-1);
    statement->schemaname=pstrdup(binding->metadata_schema);statement->idxname=psprintf("_og_lake_ix_%s",compact);
    statement->accessMethod=pstrdup("diskann");statement->isvalidated=true;
    IndexElem* key=makeNode(IndexElem);key->expr=(Node*)LakeAnnExpression(attribute,dimension);
    const char* classes[]={"vector_l2_ops","vector_ip_ops","vector_cosine_ops"};
    key->opclass=list_make2(makeString(pstrdup("pg_catalog")),makeString(pstrdup(classes[metric-1])));
    statement->indexParams=list_make1(key);
    statement->options=list_make1(makeDefElem(pstrdup("enable_pq"),(Node*)makeString(pstrdup("off"))));
    Oid savedUser;int savedSecurity;GetUserIdAndSecContext(&savedUser,&savedSecurity);Oid result=InvalidOid;
    PG_TRY();{
        SetUserIdAndSecContext(owner,savedSecurity|SECURITY_LOCAL_USERID_CHANGE|SECURITY_RESTRICTED_OPERATION);
        ObjectAddress address=DefineIndex(binding->inline_oid,statement,InvalidOid,false,true,false,true);result=address.objectId;
        SetUserIdAndSecContext(savedUser,savedSecurity);
    }PG_CATCH();{SetUserIdAndSecContext(savedUser,savedSecurity);PG_RE_THROW();}PG_END_TRY();
    CommandCounterIncrement();Relation checked=LakeOpenInlineAnnIndex(result,binding,column+1,metric);index_close(checked,NoLock);
    heap_close(carrier,NoLock);return result;
}

extern bool LakeInlineIndexSnapshotSafe(Relation index,Snapshot snapshot);
bool LakeInlineAnnAvailable(const LakeInlineBinding* binding,const char* uuid,Oid indexOid,AttrNumber column,int metric,Snapshot businessSnapshot)
{
    if(column<=0||column>binding->column_count||metric<1||metric>3||
        !u_sess->attr.attr_sql.enable_lake_ann||!OidIsValid(binding->inline_oid)||binding->identity.kind!=LAKE_TABLE_PERMANENT)return false;
    Oid types[2]={UUIDOID,UUIDOID};Datum args[2]={DirectFunctionCall1(uuid_in,CStringGetDatum(uuid)),DirectFunctionCall1(uuid_in,CStringGetDatum(binding->binding_id))};
    Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());bool usable=false;
    PG_TRY();{
        LakeMetaRows* rows=LakeMetadataQuery("SELECT inline_index_oid,column_id,type_oid,table_uuid::text,definition->>'metric',registered_snapshot FROM "
            "og_lake_internal.index_definition WHERE index_uuid=$1 AND binding_id=$2 AND state='ACTIVE' AND definition->>'kind'='vector'",
            2,types,args,NULL,snapshot,true,SPI_OK_SELECT);
        if(rows->count>1)elog(ERROR,"duplicate lake DiskANN definition");
        if(rows->count==1){bool absent=false;Datum stored=LakeMetadataValue(rows,0,1,OIDOID,true,&absent);
            if(!absent&&DatumGetObjectId(stored)==indexOid&&DatumGetInt64(LakeMetadataValue(rows,0,2,INT8OID))==binding->columns[column-1].column_id&&
                DatumGetObjectId(LakeMetadataValue(rows,0,3,OIDOID))==VECTOROID&&strcmp(LakeMetadataText(rows,0,4),binding->table_uuid)==0&&
                LakeAnnMetric(LakeMetadataText(rows,0,5))==metric&&
                DatumGetInt64(LakeMetadataValue(rows,0,6,INT8OID))<=binding->snapshot_id&&get_rel_name(indexOid)!=NULL){
                Relation index=LakeOpenInlineAnnIndex(indexOid,binding,column,metric);
                usable=LakeInlineIndexSnapshotSafe(index,businessSnapshot);index_close(index,NoLock);
            }
        }
        UnregisterSnapshot(snapshot);
    }PG_CATCH();{UnregisterSnapshot(snapshot);PG_RE_THROW();}PG_END_TRY();return usable;
}

static bool LakeAnnColumn(Node* node,Index relation,AttrNumber column)
{
    if(!IsA(node,Var))return false;Var* var=(Var*)node;
    return var->varno==relation&&var->varlevelsup==0&&var->varattno==column&&var->vartype==VECTOROID;
}
static bool LakeAnnNonNullQual(Node* node,Index relation,AttrNumber column)
{
    if(!IsA(node,NullTest))return false;NullTest* test=(NullTest*)node;
    return !test->argisrow&&test->nulltesttype==IS_NOT_NULL&&LakeAnnColumn((Node*)test->arg,relation,column);
}
static bool LakeAnnPositiveNormQual(Node* node,Index relation,AttrNumber column)
{
    if(!IsA(node,OpExpr))return false;OpExpr* op=(OpExpr*)node;
    if(list_length(op->args)!=2||get_opcode(op->opno)!=F_FLOAT8GT||!IsA(linitial(op->args),FuncExpr)||!IsA(lsecond(op->args),Const))return false;
    FuncExpr* fn=(FuncExpr*)linitial(op->args);Const* zero=(Const*)lsecond(op->args);
    const char* name=get_func_name(fn->funcid);
    return fn->funcid<FirstNormalObjectId&&get_func_namespace(fn->funcid)==PG_CATALOG_NAMESPACE&&name&&strcmp(name,"vector_norm")==0&&
        list_length(fn->args)==1&&LakeAnnColumn((Node*)linitial(fn->args),relation,column)&&
        zero->consttype==FLOAT8OID&&!zero->constisnull&&DatumGetFloat8(zero->constvalue)==0;
}

/* Only native builtin numeric predicates enter the derived candidate tree.
 * Original SQL quals remain on ForeignScan, including NULL and array semantics. */
static List* LakeAnnScalarFilter(Node* clause,Index relation,int depth,int* nodes,int* leaves)
{
    if(!clause||depth>LAKE_ANN_SCALAR_MAX_DEPTH||++*nodes>LAKE_ANN_SCALAR_MAX_NODES)return NIL;
    if(IsA(clause,BoolExpr)){
        BoolExpr* expr=(BoolExpr*)clause;int kind=0,count=list_length(expr->args);
        if(expr->boolop==AND_EXPR)kind=LAKE_ANN_SCALAR_AND;
        else if(expr->boolop==OR_EXPR)kind=LAKE_ANN_SCALAR_OR;
        else if(expr->boolop==NOT_EXPR)kind=LAKE_ANN_SCALAR_NOT;
        if(!kind||(kind==LAKE_ANN_SCALAR_NOT?count!=1:count<2)||count>LAKE_ANN_SCALAR_MAX_LEAVES)return NIL;
        List* children=NIL;ListCell* cell=NULL;
        foreach(cell,expr->args){
            List* child=LakeAnnScalarFilter((Node*)lfirst(cell),relation,depth+1,nodes,leaves);
            if(child==NIL)return NIL;children=lappend(children,child);
        }
        return list_make2(makeInteger(kind),children);
    }
    if(++*leaves>LAKE_ANN_SCALAR_MAX_LEAVES)return NIL;
    Node* left=NULL;Node* right=NULL;Oid op=InvalidOid;int nullMode=0;bool array=false,negate=false;
    if(IsA(clause,OpExpr)){
        OpExpr* expr=(OpExpr*)clause;
        if(expr->opretset||expr->opresulttype!=BOOLOID||list_length(expr->args)!=2)return NIL;
        left=(Node*)linitial(expr->args);right=(Node*)lsecond(expr->args);op=expr->opno;
        if(!IsA(left,Var)&&IsA(right,Var)){Node* swap=left;left=right;right=swap;op=get_commutator(op);}
    }else if(IsA(clause,NullTest)){
        NullTest* test=(NullTest*)clause;
        if(test->argisrow||(test->nulltesttype!=IS_NULL&&test->nulltesttype!=IS_NOT_NULL))return NIL;
        left=(Node*)test->arg;nullMode=test->nulltesttype==IS_NULL?1:2;
    }else if(IsA(clause,ScalarArrayOpExpr)){
        ScalarArrayOpExpr* expr=(ScalarArrayOpExpr*)clause;
        if(list_length(expr->args)!=2)return NIL;
        left=(Node*)linitial(expr->args);right=(Node*)lsecond(expr->args);op=expr->opno;array=true;negate=!expr->useOr;
    }else return NIL;
    if(!IsA(left,Var))return NIL;Var* var=(Var*)left;Oid type=var->vartype;
    if(var->varno!=relation||var->varlevelsup!=0||var->varattno<=0||type==INT2OID||!LakeScalarIndexedType(type))return NIL;
    if(nullMode)right=(Node*)makeNullConst(type,-1,InvalidOid);
    else{
        if(!right||(!IsA(right,Const)&&!IsA(right,Param))||!OidIsValid(op)||op>=FirstNormalObjectId)return NIL;
        Oid rightType=array?get_element_type(exprType(right)):exprType(right);
        if(!LakeScalarArgumentType(type,rightType))return NIL;
        int strategy=get_op_opfamily_strategy(op,LakeScalarFamily(type));
        /* simplify_boolean_equality/preprocess_qual_conditions may already
         * have rewritten NOT (=) to <> and NOT IN to <> ALL. Only the exact
         * builtin negator of family equality is canonicalized back to NOT. */
        if(negate||(!array&&strategy==0)){
            Oid original=op,originalFunction=get_opcode(original),equal=get_negator(original);
            if(!OidIsValid(originalFunction)||originalFunction>=FirstNormalObjectId||!func_strict(originalFunction)||
                !OidIsValid(equal)||equal>=FirstNormalObjectId||get_negator(equal)!=original||
                get_op_opfamily_strategy(equal,LakeScalarFamily(type))!=BTEqualStrategyNumber||
                depth==LAKE_ANN_SCALAR_MAX_DEPTH||++*nodes>LAKE_ANN_SCALAR_MAX_NODES)return NIL;
            op=equal;strategy=BTEqualStrategyNumber;negate=true;
        }
        Oid leftType,familyRight;
        Oid function=get_opcode(op);
        if(strategy<1||strategy>5||(array&&strategy!=BTEqualStrategyNumber)||
            !OidIsValid(function)||function>=FirstNormalObjectId||!func_strict(function))return NIL;
        get_op_opfamily_properties(op,LakeScalarFamily(type),false,&strategy,&leftType,&familyRight);
        if(leftType!=type||familyRight!=rightType)return NIL;
    }
    /* Leaf: kind,column,operator,type,NULL-mode,array-flag,native RHS Expr. */
    List* result=list_make3(makeInteger(LAKE_ANN_SCALAR_LEAF),makeInteger(var->varattno),makeInteger((int)op));
    result=lappend(result,makeInteger((int)type));result=lappend(result,makeInteger(nullMode));
    result=lappend(result,makeInteger(array?1:0));result=lappend(result,copyObject(right));
    return negate?list_make2(makeInteger(LAKE_ANN_SCALAR_NOT),list_make1(result)):result;
}

void LakeAddInlineAnnPaths(PlannerInfo* root,RelOptInfo* relation,Oid relationOid,double startup,double total,const LakeCostLedger* ledger)
{
    Query* query=root->parse;
    if(!u_sess->attr.attr_sql.enable_lake_ann||!u_sess->attr.attr_sql.enable_indexscan||!LakeHasIndexRootVersion()||
        query->commandType!=CMD_SELECT||query->hasAggs||query->hasWindowFuncs||query->hasSubLinks||query->hasModifyingCTE||query->cteList!=NIL||query->setOperations||
        query->distinctClause!=NIL||query->groupClause!=NIL||query->limitOffset||query->rowMarks!=NIL||
        list_length(query->rtable)!=1||list_length(query->jointree->fromlist)!=1||
        !IsA(linitial(query->jointree->fromlist),RangeTblRef)||
        ((RangeTblRef*)linitial(query->jointree->fromlist))->rtindex!=relation->relid||
        list_length(query->sortClause)!=1||!query->limitCount||!IsA(query->limitCount,Const))return;
    Const* limit=(Const*)query->limitCount;
    if(limit->constisnull||(limit->consttype!=INT8OID&&limit->consttype!=INT4OID))return;
    int64 k=limit->consttype==INT8OID?DatumGetInt64(limit->constvalue):DatumGetInt32(limit->constvalue);
    if(k<1||k>1000)return;
    SortGroupClause* sort=(SortGroupClause*)linitial(query->sortClause);
    /* Native ascending float8 distance, with no hidden limit or order promise. */
    if(sort->nulls_first||get_opcode(sort->sortop)!=F_FLOAT8LT)return;
    Node* order=get_sortgroupclause_expr(sort,query->targetList);
    if(!IsA(order,OpExpr))return;OpExpr* distance=(OpExpr*)order;
    if(distance->opretset||distance->opresulttype!=FLOAT8OID||list_length(distance->args)!=2)return;
    int metric=distance->opno>=8311&&distance->opno<=8313?distance->opno-8310:0;
    Node* column=(Node*)linitial(distance->args);Node* argument=(Node*)lsecond(distance->args);
    if(!metric||!IsA(column,Var)||(!IsA(argument,Const)&&!IsA(argument,Param))||exprType(argument)!=VECTOROID)return;
    Var* var=(Var*)column;
    if(!LakeAnnColumn(column,relation->relid,var->varattno))return;
    bool nonnull=false,positive=false;ListCell* cell=NULL;List* filter=NIL;List* scalarClauses=NIL;
    foreach(cell,relation->baserestrictinfo){Node* clause=(Node*)((RestrictInfo*)lfirst(cell))->clause;
        if(LakeAnnNonNullQual(clause,relation->relid,var->varattno))nonnull=true;
        else if(metric==3&&LakeAnnPositiveNormQual(clause,relation->relid,var->varattno)){positive=true;nonnull=true;}
        else scalarClauses=lappend(scalarClauses,clause);
    }
    int nodes=0,leaves=0;
    if(list_length(scalarClauses)==1){
        filter=LakeAnnScalarFilter((Node*)linitial(scalarClauses),relation->relid,1,&nodes,&leaves);
        if(filter==NIL)return;
    }else if(scalarClauses!=NIL){
        if(list_length(scalarClauses)>LAKE_ANN_SCALAR_MAX_LEAVES)return;
        List* children=NIL;nodes=1;
        foreach(cell,scalarClauses){
            List* child=LakeAnnScalarFilter((Node*)lfirst(cell),relation->relid,2,&nodes,&leaves);
            if(child==NIL)return;children=lappend(children,child);
        }
        filter=list_make2(makeInteger(LAKE_ANN_SCALAR_AND),children);
    }
    if(!nonnull||(metric==3&&!positive))return;
    Snapshot snapshot=ActiveSnapshotSet()?GetActiveSnapshot():GetTransactionSnapshot();
    LakeInlineBinding* binding=LakeLoadInlineBinding(relationOid,snapshot);
    if((!LakeIxSmpExtendedVersionEnabled()&&(LakeBindingHasBinary(binding)||LakeBindingHasUuid(binding)||LakeBindingHasJson(binding)||LakeBindingHasNumeric(binding)))||binding->has_dropped_columns||(!LakePartitionComboVersionEnabled()&&LakeIdentityPartitioned(binding))||binding->identity.kind!=LAKE_TABLE_PERMANENT||var->varattno<=0||
        var->varattno>binding->column_count||binding->columns[var->varattno-1].type!=VECTOROID)return;
    double seqCost,randomCost;get_tablespace_page_costs(InvalidOid,&randomCost,&seqCost);bool nativeCarrier=false;
    if(OidIsValid(binding->inline_oid)){
        Relation carrier=heap_open(binding->inline_oid,AccessShareLock);nativeCarrier=!RelationIsUstoreFormat(carrier);
        get_tablespace_page_costs(carrier->rd_rel->reltablespace,&randomCost,&seqCost);heap_close(carrier,NoLock);}
    Oid types[3]={UUIDOID,INT8OID,TEXTOID};const char* metrics[]={"l2","ip","cosine"};
    Datum args[3]={DirectFunctionCall1(uuid_in,CStringGetDatum(binding->binding_id)),
        Int64GetDatum(binding->columns[var->varattno-1].column_id),CStringGetTextDatum(metrics[metric-1])};
    LakeMetaRows* definitions=LakeMetadataQuery("SELECT index_uuid::text,inline_index_oid,COALESCE(definition->>'pq','false')::boolean FROM og_lake_internal.index_definition "
        "WHERE binding_id=$1 AND state='ACTIVE' AND type_oid=8305 AND definition->>'kind'='vector' AND column_id=$2 "
        "AND definition->>'metric'=$3 ORDER BY index_uuid LIMIT 65",3,types,args,NULL,snapshot,true,SPI_OK_SELECT);
    if(definitions->count>64)return;
    for(uint32 i=0;i<definitions->count;++i){bool absent=false;
        Oid indexOid=DatumGetObjectId(LakeMetadataValue(definitions,i,2,OIDOID,true,&absent));
        if(absent)indexOid=InvalidOid;const char* uuid=LakeMetadataText(definitions,i,1);
        bool inlineUsable=nativeCarrier&&OidIsValid(indexOid)&&LakeInlineAnnAvailable(binding,uuid,indexOid,var->varattno,metric,snapshot);
        /* Filtered native inline ANN needs its own visible expansion contract.
         * This slice scans inline/private rows completely and filters files. */
        if(filter!=NIL)inlineUsable=false;
        double coveredRows,coveredBytes,coveredGroups;int coveredFiles;
        LakeCostLedger* costLedger=ledger?LakeCostCopy(ledger):NULL;
        LakeAnnFileCoverageEstimate(binding,uuid,var->varattno,snapshot,&coveredRows,&coveredBytes,&coveredGroups,&coveredFiles,costLedger);
        if(!inlineUsable&&coveredFiles==0)continue;
        double indexPages=0;
        if(inlineUsable){Relation index=LakeOpenInlineAnnIndex(indexOid,binding,var->varattno,metric);
            indexPages=Max(1,index->rd_rel->relpages);index_close(index,NoLock);}else indexOid=InvalidOid;
        double probes=Min(Max(1.0,relation->tuples),Max(100.0,(double)k*8));
        /* Conservative, uncalibrated native units. All file and private work
         * stays in the baseline; no invented fixed preference for ANN. */
        double cost=Max(startup,total-(inlineUsable?LakeInlinePhysicalPages(binding)*seqCost:0))+
            (inlineUsable?randomCost*(1+Min(indexPages,probes)):0)+
            (inlineUsable?probes:0)*(u_sess->attr.attr_sql.cpu_tuple_cost+
                binding->columns[var->varattno-1].typmod*u_sess->attr.attr_sql.cpu_operator_cost);
        /* Retain baseline file cost until remote/page-cache calibration. This
         * permits explicit diagnostic selection without claiming a speedup. */
        if(coveredFiles)cost+=coveredFiles*randomCost*(1+Min(1000.0,coveredRows));
        if(filter!=NIL)cost+=coveredRows*leaves*(u_sess->attr.attr_sql.cpu_tuple_cost+u_sess->attr.attr_sql.cpu_operator_cost);
        double pathStartup=startup;
        if(costLedger){
            uint32 effCandidates=(filter!=NIL)?(uint32)Min((int64)1000,Max((int64)256,k*8))
                :(uint32)Min((int64)1000,Max((int64)k+20,k*3));
            LakeCostAnn(costLedger,effCandidates,binding->columns[var->varattno-1].typmod,filter!=NIL?leaves:0);
            double inlineIo=inlineUsable?randomCost*(1+Min(indexPages,probes))+
                probes*(u_sess->attr.attr_sql.cpu_tuple_cost+
                    binding->columns[var->varattno-1].typmod*u_sess->attr.attr_sql.cpu_operator_cost):-1;
            LakeCostResult priced=LakeCostFinish(costLedger,inlineIo);
            cost=priced.total;pathStartup=priced.startup;
        }
        List* data=list_make3(makeString(pstrdup(uuid)),makeInteger((int)indexOid),makeInteger(var->varattno));
        data=lappend(data,makeInteger((int)distance->opno));data=lappend(data,makeInteger((int)VECTOROID));
        data=lappend(data,makeInteger(10+metric));data=lappend(data,copyObject(argument));data=lappend(data,makeInteger((int)k));
        if(filter!=NIL)data=lappend(data,copyObject(filter));
        bool pq=DatumGetBool(LakeMetadataValue(definitions,i,3,BOOLOID));
        int dop=LakeStreamAnnPlanDop(root,relation,binding,metric,filter,pq);
        if(!(u_sess->attr.attr_sql.enable_force_smp&&dop>1))
            add_path(root,relation,(Path*)create_foreignscan_path(root,relation,pathStartup,cost,NIL,NULL,NULL,data,1));
        if(dop>1){
            /* FDW does not truncate per-file candidates by SQL K; the host
             * may apply exact local Sort/Limit before the global merge.
             * Graph distances never become SQL order keys. */
            double parallelStartup=startup,parallelCost=0;
            if(costLedger){
                /* Filtered ANN keeps complete inline scanning on owner0.
                 * Charge each source on its actual lane; preserve the maximum
                 * lane preparation in startup before LIMIT prorates run work. */
                LakeCostResult priced=LakeCostFinish(costLedger,-1,dop,u_sess->opt_cxt.smp_thread_cost);
                parallelStartup=priced.startup;parallelCost=priced.total;
            }else{
                /* Compatibility for a caller without the optional ledger. */
                double fileRows=0;for(int f=0;f<binding->file_count;++f)fileRows+=binding->files[f].record_count;
                double physical=Max(1.0,Max(relation->tuples,fileRows));double lane[2]={0,0};
                int offset=OidIsValid(binding->inline_oid)?1:0;
                for(int f=0;f<binding->file_count;++f)lane[(f+offset)%2]+=binding->files[f].record_count;
                if(offset)lane[0]+=Max(0.0,physical-fileRows);
                double share=Max(1.0/dop,Max(lane[0],lane[1])/physical);
                parallelCost=startup+Max(0.0,cost-startup)*share+u_sess->opt_cxt.smp_thread_cost*(dop-1);
            }
            List* parallelData=list_make3(makeString(pstrdup(uuid)),makeInteger((int)InvalidOid),makeInteger(var->varattno));
            parallelData=lappend(parallelData,makeInteger((int)distance->opno));parallelData=lappend(parallelData,makeInteger((int)VECTOROID));
            parallelData=lappend(parallelData,makeInteger(10+metric));parallelData=lappend(parallelData,copyObject(argument));parallelData=lappend(parallelData,makeInteger((int)k));
            if(filter!=NIL)parallelData=lappend(parallelData,copyObject(filter));
            add_path(root,relation,(Path*)create_foreignscan_path(root,relation,parallelStartup,parallelCost,NIL,NULL,NULL,
                parallelData,dop));
        }
    }
}
