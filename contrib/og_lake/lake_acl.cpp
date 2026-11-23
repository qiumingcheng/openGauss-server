/* Ordinary lake ACL admission and a fixed, least-privilege metadata identity. */
#include "postgres.h"
#include "lake_nullability.h"
#include "lake_native_partition.h"
#include "lake_autoanalyze.h"
#include "lake_inline_ann.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_index_private.h"
#include "lake_row_lock.h"
#include "lake_partition.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/og_lake.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/gs_masking_policy.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_index.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_trigger.h"
#include "nodes/readfuncs.h"
#include "utils/array.h"
#include "catalog/pg_language.h"
#include "nodes/plannodes.h"
#include "storage/lmgr.h"
#include "utils/relcache.h"
#include "catalog/pg_class.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/sec_rls_cmds.h"
#include "commands/schemacmds.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/reltrigger.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

extern "C" Datum og_lake_configure_internal_owner(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_configure_internal_owner);

static Oid LakeExtensionOwner(Oid* extension, bool* supportsPolicy)
{
    Relation rel=heap_open(ExtensionRelationId,AccessShareLock);
    ScanKeyData key;
    ScanKeyInit(&key,Anum_pg_extension_extname,BTEqualStrategyNumber,F_NAMEEQ,CStringGetDatum("og_lake"));
    SysScanDesc scan=systable_beginscan(rel,ExtensionNameIndexId,true,NULL,1,&key);
    HeapTuple tuple=systable_getnext(scan);
    if(!HeapTupleIsValid(tuple))
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("native lake extension is not installed")));
    *extension=HeapTupleGetOid(tuple);
    Oid owner=((Form_pg_extension)GETSTRUCT(tuple))->extowner;
    bool isnull=false;
    Datum value=tableam_tops_tuple_getattr(tuple,Anum_pg_extension_extversion,RelationGetDescr(rel),&isnull);
    char* version=isnull?NULL:TextDatumGetCString(value);
    *supportsPolicy=version!=NULL && (strcmp(version,"0.5")==0 || strcmp(version,"0.6")==0 || strcmp(version,"0.7")==0 || strcmp(version,"0.8")==0 || strcmp(version,"0.9")==0 || (strcmp(version,"0.10")==0||strcmp(version,"0.11")==0||strcmp(version,"0.12")==0||strcmp(version,"0.13")==0||(strcmp(version,"0.14")==0||(strcmp(version,"0.15")==0||(strcmp(version,"0.16")==0||(strcmp(version,"0.17")==0||(strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||(strcmp(version,"0.22")==0||(strcmp(version,"0.23")==0||LakeNativeFutureContractEnabled(version)))))))))))));
    systable_endscan(scan); heap_close(rel,AccessShareLock);
    if(!superuser_arg(owner))
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native lake extension must retain a trusted administrator owner")));
    if(*supportsPolicy) {
        /* The policy table retains this owner. A normal INHERIT member could
         * otherwise keep owner ACL rights in a prepared direct-table plan,
         * even after private-namespace ownership changes. Every transitive
         * membership path has a final incoming edge, so forbid all such edges.
         */
        CatCList* members=SearchSysCacheList1(AUTHMEMROLEMEM,ObjectIdGetDatum(owner));
        bool hasMembers=members->n_members!=0;
        ReleaseSysCacheList(members);
        if(hasMembers)
            ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native lake policy administrator owner must have no role members")));
    }
    LakeFastVersionStore(version);
    if (version != NULL)
        pfree(version);
    return owner;
}

/* Reject even column-level grants. Internal owner is not a business role. */
static void LakeCheckPrivateAcl(Datum value, bool isnull, Oid owner)
{
    if(isnull) return; /* Ordinary table/schema defaults grant nothing to PUBLIC. */
    Acl* acl=DatumGetAclPCopy(value);
    for(int i=0;i<ACL_NUM(acl);++i) {
        const AclItem* item=&ACL_DAT(acl)[i];
        /* A superuser grantee can have ordinary INHERIT members. Administrators
         * already have host bypass and never need explicit private grants. */
        if(ACLITEM_GET_RIGHTS(*item)!=0 && item->ai_grantee!=owner)
            ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("native lake internal objects must not grant privileges beyond their exact owner")));
    }
    pfree(acl);
}

static void LakeCheckOwnerRole(Oid owner)
{
    HeapTuple tuple=SearchSysCache1(AUTHOID,ObjectIdGetDatum(owner));
    if(!HeapTupleIsValid(tuple))
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("native lake internal owner role disappeared")));
    Form_pg_authid role=(Form_pg_authid)GETSTRUCT(tuple);
    bool forbidden=role->rolsuper || role->rolsystemadmin || role->rolinherit || role->rolcreaterole ||
        role->rolcreatedb || role->rolcatupdate || role->rolcanlogin || role->rolreplication || role->rolauditadmin;
    /* These fields follow varlena attributes in this openGauss catalog. */
    const int attributes[]={Anum_pg_authid_rolmonitoradmin,Anum_pg_authid_roloperatoradmin,Anum_pg_authid_rolpolicyadmin};
    for(Size i=0;i<lengthof(attributes);++i) {
        bool isnull=false;
        Datum value=SysCacheGetAttr(AUTHOID,tuple,attributes[i],&isnull);
        forbidden |= !isnull && DatumGetBool(value);
    }
    bool kindNull=false,parentNull=false;
    Datum kind=SysCacheGetAttr(AUTHOID,tuple,Anum_pg_authid_rolkind,&kindNull);
    Datum parent=SysCacheGetAttr(AUTHOID,tuple,Anum_pg_authid_rolparentid,&parentNull);
    forbidden |= (!kindNull && DatumGetChar(kind)!=ROLKIND_NORMAL) ||
        (!parentNull && OidIsValid(DatumGetObjectId(parent)));
    ReleaseSysCache(tuple);
    if(forbidden)
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("native lake internal owner requires NOLOGIN NOINHERIT and no administrative capabilities")));
    /* No login may assume this role; it cannot inherit another role either. */
    const int caches[]={AUTHMEMROLEMEM,AUTHMEMMEMROLE};
    for(Size i=0;i<lengthof(caches);++i) {
        CatCList* members=SearchSysCacheList1(caches[i],ObjectIdGetDatum(owner));
        bool hasMembers=members->n_members!=0;
        ReleaseSysCacheList(members);
        if(hasMembers)
            ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("native lake internal owner must have no role membership in either direction")));
    }
}

void LakeValidatePrivateNamespace(Oid namespaceOid, Oid owner)
{
    LockDatabaseObject(NamespaceRelationId,namespaceOid,0,AccessShareLock);
    HeapTuple tuple=SearchSysCache1(NAMESPACEOID,ObjectIdGetDatum(namespaceOid));
    if(!HeapTupleIsValid(tuple))
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_SCHEMA),errmsg("native lake internal namespace disappeared")));
    Form_pg_namespace ns=(Form_pg_namespace)GETSTRUCT(tuple);
    if(ns->nspowner!=owner)
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native lake internal namespace has an untrusted owner")));
    bool isnull=false;
    Datum acl=SysCacheGetAttr(NAMESPACEOID,tuple,Anum_pg_namespace_nspacl,&isnull);
    LakeCheckPrivateAcl(acl,isnull,owner);
    ReleaseSysCache(tuple);
}

static bool LakeInternalType(Oid type)
{
    const Oid allowed[]={NAMEOID,BOOLOID,INT2OID,INT4OID,INT8OID,OIDOID,UUIDOID,TEXTOID,VARCHAROID,
        BYTEAOID,FLOAT4OID,FLOAT8OID,NUMERICOID,DATEOID,TIMESTAMPOID,TIMESTAMPTZOID,JSONBOID};
    for(Size i=0;i<lengthof(allowed);++i) if(type==allowed[i]) return true;
    return false;
}

/* Reject executable index definitions from raw catalogs BEFORE relcache helpers
 * can fold expressions or initialize an unreviewed access method. */
static Relation LakeOpenTrustedIndex(Oid oid, Oid owner,bool ustore=false)
{
    LockRelationOid(oid,AccessShareLock);
    HeapTuple cls=SearchSysCache1(RELOID,ObjectIdGetDatum(oid));
    if(!HeapTupleIsValid(cls))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake index disappeared")));
    Form_pg_class form=(Form_pg_class)GETSTRUCT(cls);
    if(form->relkind!=RELKIND_INDEX || form->relam!=(ustore?UBTREE_AM_OID:BTREE_AM_OID) || form->relowner!=owner)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata requires trusted builtin B-tree indexes")));
    ReleaseSysCache(cls);
    HeapTuple tuple=SearchSysCache1(INDEXRELID,ObjectIdGetDatum(oid));
    if(!HeapTupleIsValid(tuple))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake index definition disappeared")));
    Form_pg_index definition=(Form_pg_index)GETSTRUCT(tuple);
    if(!definition->indimmediate || definition->indisexclusion || !definition->indisvalid || !definition->indisready)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata index must be immediate and valid")));
    const int expressions[]={Anum_pg_index_indexprs,Anum_pg_index_indpred};
    for(Size i=0;i<lengthof(expressions);++i) {
        bool isnull=false;Datum expression=SysCacheGetAttr(INDEXRELID,tuple,expressions[i],&isnull);
        if(!isnull && !(expressions[i]==Anum_pg_index_indpred &&
            LakeIndexValidPartialPredicate(oid,(Node*)stringToNode(TextDatumGetCString(expression)))))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata cannot use expression or partial indexes")));
    }
    bool isnull=false;Datum classesValue=SysCacheGetAttr(INDEXRELID,tuple,Anum_pg_index_indclass,&isnull);
    if(isnull)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake index has no operator classes")));
    oidvector* classes=(oidvector*)DatumGetPointer(classesValue);
    if(classes->dim1!=definition->indnatts)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata does not use covering indexes")));
    for(int i=0;i<classes->dim1;++i)
        if(classes->values[i]>=FirstNormalObjectId)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake index uses an unreviewed operator class")));
    Datum collationsValue=SysCacheGetAttr(INDEXRELID,tuple,Anum_pg_index_indcollation,&isnull);
    if(isnull)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake index has no collation vector")));
    oidvector* collations=(oidvector*)DatumGetPointer(collationsValue);
    if(collations->dim1!=definition->indnatts)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake index has an invalid collation vector")));
    for(int i=0;i<collations->dim1;++i)
        if(OidIsValid(collations->values[i]) && collations->values[i]!=DEFAULT_COLLATION_OID &&
           collations->values[i]!=C_COLLATION_OID && collations->values[i]!=POSIX_COLLATION_OID)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake index uses an unreviewed collation")));
    ReleaseSysCache(tuple);
    Relation index=index_open(oid,AccessShareLock);
    int keys=IndexRelationGetNumberOfKeyAttributes(index);
    if(keys!=IndexRelationGetNumberOfAttributes(index))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata does not use covering indexes")));
    for(int i=0;i<keys;++i)
        if(index->rd_opfamily[i]>=FirstNormalObjectId)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake index uses an unreviewed operator family")));
    for(int i=0;i<keys*index->rd_am->amsupport;++i)
        if(index->rd_support[i]>=FirstNormalObjectId)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake index uses an unreviewed support function")));
    return index;
}

Relation LakeOpenTrustedInlineIndex(Oid indexOid,Oid owner)
{
    HeapTuple tuple=SearchSysCache1(RELOID,ObjectIdGetDatum(indexOid));
    if(!HeapTupleIsValid(tuple))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake inline index disappeared")));
    bool ustore=((Form_pg_class)GETSTRUCT(tuple))->relam==UBTREE_AM_OID;
    ReleaseSysCache(tuple);return LakeOpenTrustedIndex(indexOid,owner,ustore);
}

/* Fixed host control catalog contract; all reads are native system-catalog reads.
 * No SQL/deparser/user function is executed to establish this trust boundary. */
static int LakeControlNumber(const char* name)
{
    const char* names[]={"catalog_binding","table_binding","relation_binding","column_binding","metadata_relation","access_policy",
        "index_definition","index_segment","index_coverage","row_lock_binding","analyze_state","partition_binding"};
    for(Size i=0;i<lengthof(names);++i) if(strcmp(name,names[i])==0) return (int)i;
    ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("unrecognized native lake control relation")));
    return -1;
}

static void LakeBadConstraint(void)
{
    ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        errmsg("native lake fixed control constraint or trigger was changed")));
}

static bool LakeConstraintArray(HeapTuple tuple, int attribute, Oid element, const int* expected, int length)
{
    bool isnull=false;
    Datum value=SysCacheGetAttr(CONSTROID,tuple,attribute,&isnull);
    if(isnull) return false;
    ArrayType* array=DatumGetArrayTypeP(value);
    if(ARR_ELEMTYPE(array)!=element || ARR_NDIM(array)!=1) return false;
    Datum* values=NULL;bool* nulls=NULL;int count=0;
    deconstruct_array(array,element,element==INT2OID?2:4,true,element==INT2OID?'s':'i',&values,&nulls,&count);
    bool valid=count==length;
    for(int i=0;valid && i<count;++i) {
        int actual=element==INT2OID?(int)DatumGetInt16(values[i]):(int)DatumGetObjectId(values[i]);
        valid=!nulls[i] && actual==expected[i];
    }
    pfree(values);pfree(nulls);
    return valid;
}

static bool LakeCheckExpression(Node* node, AttrNumber column, Oid type, Oid function,
                                int64 number, const char* expectedText)
{
    if(node==NULL || !IsA(node,OpExpr)) return false;
    OpExpr* op=(OpExpr*)node;
    Oid code=get_opcode(op->opno);
    /* SQL's unadorned integer literal may use the builtin int8/int4 overload. */
    bool accepted=code==function || (function==F_INT8GT && code==F_INT84GT) ||
        (function==F_INT8GE && code==F_INT84GE);
    if(!accepted || (OidIsValid(op->opfuncid) && op->opfuncid!=code) ||
       op->opresulttype!=BOOLOID || op->opretset || list_length(op->args)!=2) return false;
    Node* left=(Node*)linitial(op->args);Node* right=(Node*)lsecond(op->args);
    if(!IsA(left,Var) || !IsA(right,Const)) return false;
    Var* var=(Var*)left;Const* value=(Const*)right;
    if(var->varno!=1 || var->varlevelsup!=0 || var->varattno!=column || var->vartype!=type || value->constisnull)
        return false;
    if(expectedText!=NULL)
        return value->consttype==TEXTOID && strcmp(TextDatumGetCString(value->constvalue),expectedText)==0;
    if(value->consttype==INT4OID) return DatumGetInt32(value->constvalue)==number;
    if(value->consttype==INT8OID) return DatumGetInt64(value->constvalue)==number;
    if(value->consttype==OIDOID) return DatumGetObjectId(value->constvalue)==(Oid)number;
    return false;
}

void LakeCheckIndexControlShape(Relation relation,int kind)
{
    static const char* names[3][15]={
        {"index_uuid","binding_id","index_name","server_oid","table_id","table_uuid","column_id","type_oid","collation_oid",
         "definition_generation","definition_hash","definition","registered_snapshot","state","inline_index_oid"},
        {"segment_uuid","index_uuid","definition_generation","build_token","build_snapshot","source_manifest_hash",
         "algorithm_version","format_version","artifact_root_uri","manifest_hash","state"},
        {"segment_uuid","source_file_id","index_uuid","definition_generation","source_checksum","schema_mapping_hash",
         "physical_row_count","active","source_file_bytes","source_path"}};
    static const Oid types[3][15]={
        {UUIDOID,UUIDOID,NAMEOID,OIDOID,INT8OID,UUIDOID,INT8OID,OIDOID,OIDOID,INT8OID,TEXTOID,JSONBOID,INT8OID,TEXTOID,OIDOID},
        {UUIDOID,UUIDOID,INT8OID,UUIDOID,INT8OID,TEXTOID,INT4OID,INT4OID,TEXTOID,TEXTOID,TEXTOID},
        {UUIDOID,INT8OID,UUIDOID,INT8OID,TEXTOID,TEXTOID,INT8OID,BOOLOID,INT8OID,TEXTOID}};
    const int lengths[]={LakeHasInlineIndexVersion()?15:14,11,10};
    const uint32 nullable[]={(1U<<1)|(LakeHasInlineIndexVersion()?(1U<<14):0),1U<<9,0};
    int index=kind-6;if(index<0 || index>=3) LakeBadConstraint();
    TupleDesc desc=RelationGetDescr(relation);if(desc->natts!=lengths[index]) LakeBadConstraint();
    for(int i=0;i<desc->natts;++i)
        if(desc->attrs[i].attisdropped || strcmp(NameStr(desc->attrs[i].attname),names[index][i])!=0 ||
           desc->attrs[i].atttypid!=types[index][i] || desc->attrs[i].attnotnull==((nullable[index]&(1U<<i))!=0))
            LakeBadConstraint();
}

bool LakeIndexValidPartialPredicate(Oid indexOid,Node* predicate)
{
    /* pg_index stores explicit Expr; relcache callers may use implicit AND.
     * Inspect either exact representation without evaluating the expression. */
    if(predicate==NULL) return false;
    Node* expression=predicate;
    if(IsA(predicate,List)) {
        if(list_length((List*)predicate)!=1) return false;
        expression=(Node*)linitial((List*)predicate);
    }
    HeapTuple tuple=SearchSysCache1(INDEXRELID,ObjectIdGetDatum(indexOid));
    if(!HeapTupleIsValid(tuple)) return false;
    Oid parent=((Form_pg_index)GETSTRUCT(tuple))->indrelid;ReleaseSysCache(tuple);
    if(get_rel_namespace(parent)!=get_namespace_oid("og_lake_internal",false) ||
       getExtensionOfObject(RelationRelationId,parent)!=get_extension_oid("og_lake",false)) return false;
    char* name=get_rel_name(parent);
    if(name!=NULL && strcmp(name,"index_definition")==0)
        return LakeCheckExpression(expression,14,TEXTOID,F_TEXTEQ,0,"ACTIVE");
    if(name!=NULL && strcmp(name,"index_coverage")==0 && IsA(expression,Var)) {
        Var* var=(Var*)expression;
        return var->varno==1 && var->varlevelsup==0 && var->varattno==8 && var->vartype==BOOLOID;
    }
    return false;
}

static bool LakeIndexOrParts(Node* expression,List** parts,int depth=0)
{
    if(expression==NULL || depth>4) return false;
    if(IsA(expression,BoolExpr) && ((BoolExpr*)expression)->boolop==OR_EXPR) {
        List* args=((BoolExpr*)expression)->args;if(list_length(args)<2 || list_length(args)>4) return false;
        ListCell* cell=NULL;foreach(cell,args) if(!LakeIndexOrParts((Node*)lfirst(cell),parts,depth+1)) return false;
    } else {*parts=lappend(*parts,expression);if(list_length(*parts)>4)return false;}
    return true;
}

static bool LakeIndexNotNull(Node* expression,AttrNumber column,Oid type)
{
    if(expression==NULL || !IsA(expression,NullTest)) return false;
    NullTest* test=(NullTest*)expression;
    if(test->nulltesttype!=IS_NOT_NULL || test->argisrow || !IsA(test->arg,Var)) return false;
    Var* var=(Var*)test->arg;
    return var->varno==1 && var->varlevelsup==0 && var->varattno==column && var->vartype==type;
}

static uint32 LakeIndexCheckConstraint(Node* expression,int kind)
{
    if(kind==6) {
        if(LakeCheckExpression(expression,5,INT8OID,F_INT8GE,0,NULL))return 1;
        if(LakeCheckExpression(expression,7,INT8OID,F_INT8GE,0,NULL))return 2;
        if(LakeCheckExpression(expression,10,INT8OID,F_INT8GT,0,NULL))return 4;
        if(LakeCheckExpression(expression,13,INT8OID,F_INT8GE,0,NULL))return 8;
    } else if(kind==7) {
        if(LakeCheckExpression(expression,3,INT8OID,F_INT8GT,0,NULL))return 1;
        if(LakeCheckExpression(expression,5,INT8OID,F_INT8GE,0,NULL))return 2;
        if(LakeCheckExpression(expression,7,INT4OID,F_INT4GT,0,NULL))return 4;
        if(LakeCheckExpression(expression,8,INT4OID,F_INT4GT,0,NULL))return 8;
    } else if(kind==8) {
        if(LakeCheckExpression(expression,2,INT8OID,F_INT8GE,0,NULL))return 1;
        if(LakeCheckExpression(expression,4,INT8OID,F_INT8GT,0,NULL))return 2;
        if(LakeCheckExpression(expression,7,INT8OID,F_INT8GE,0,NULL))return 4;
        if(LakeCheckExpression(expression,9,INT8OID,F_INT8GE,0,NULL))return 8;
        return 0;
    } else return 0;
    List* parts=NIL;if(!LakeIndexOrParts(expression,&parts))return 0;
    const char* values[4]={kind==6?"ACTIVE":"BUILDING",kind==6?"DROPPING":"READY","RETIRED","FAILED"};
    int length=kind==6?2:4;AttrNumber state=kind==6?14:11;
    uint32 seen=0;ListCell* cell=NULL;
    foreach(cell,parts)for(int i=0;i<length;++i)
        if(LakeCheckExpression((Node*)lfirst(cell),state,TEXTOID,F_TEXTEQ,0,values[i]))seen|=1U<<i;
    if(list_length(parts)==length && seen==(1U<<length)-1) return 16;
    if(list_length(parts)==2) {
        Node* a=(Node*)linitial(parts);Node* b=(Node*)lsecond(parts);
        for(int order=0;order<2;++order) {
            Node* left=order==0?a:b;Node* right=order==0?b:a;
            if(kind==6 && LakeCheckExpression(left,14,TEXTOID,F_TEXTEQ,0,"DROPPING") && LakeIndexNotNull(right,2,UUIDOID))return 32;
            if(kind==7 && LakeCheckExpression(left,11,TEXTOID,F_TEXTNE,0,"READY") && LakeIndexNotNull(right,10,TEXTOID))return 32;
        }
    }
    return 0;
}

/* Exact builtin domain check: (leafOID=0)=(partitionNo=0), plus no negative
 * partitionNo. It prevents an ordinary0/0 head from aliasing a native leaf. */
static uint32 LakeNativeRowHeadDomainCheck(Node* node)
{
    if(LakeCheckExpression(node,18,INT4OID,F_INT4GE,0,NULL))return 1;
    if(!node||!IsA(node,OpExpr))return 0;
    OpExpr* equality=(OpExpr*)node;
    Oid code=get_opcode(equality->opno);
    if(equality->opresulttype!=BOOLOID||code!=F_BOOLEQ||equality->opretset||
       (OidIsValid(equality->opfuncid)&&equality->opfuncid!=code)||list_length(equality->args)!=2)return 0;
    Node* a=(Node*)linitial(equality->args);Node* b=(Node*)lsecond(equality->args);
    for(int swap=0;swap<2;++swap) {
        Node* leaf=swap?b:a;Node* part=swap?a:b;
        if(LakeCheckExpression(leaf,17,OIDOID,F_OIDEQ,0,NULL)&&LakeCheckExpression(part,18,INT4OID,F_INT4EQ,0,NULL))return 2;
    }
    return 0;
}

static void LakeCheckControlShape(Relation relation, int kind)
{
    if(kind==11) {
        const char* names[]={"parent_relid","leaf_partition_oid","partition_no","binding_id","certified_snapshot","source_hash"};
        const Oid types[]={OIDOID,OIDOID,INT4OID,UUIDOID,INT8OID,TEXTOID};
        TupleDesc desc=RelationGetDescr(relation);if(desc->natts!=6) LakeBadConstraint();
        for(int i=0;i<6;++i)if(desc->attrs[i].attisdropped||!desc->attrs[i].attnotnull||
            strcmp(NameStr(desc->attrs[i].attname),names[i])||desc->attrs[i].atttypid!=types[i]) LakeBadConstraint();
        return;
    }
    if(kind==9) {
        TupleDesc desc=RelationGetDescr(relation);
        if(!LakeRowHeadReclaimEnabled()) {
            const char* names[]={"binding_id","relation_oid","row_head_oid"};
            const Oid types[]={UUIDOID,OIDOID,OIDOID};
            if(desc->natts!=3) LakeBadConstraint();
            for(int i=0;i<3;++i)
                if(strcmp(NameStr(desc->attrs[i].attname),names[i])!=0 || desc->attrs[i].atttypid!=types[i] ||
                   desc->attrs[i].attisdropped || !desc->attrs[i].attnotnull) LakeBadConstraint();
            return;
        }
        const char* names[]={"binding_id","relation_oid","row_head_oid","reclaim_cursor","reclaim_round_end",
            "reclaim_scanned","reclaimed_rows","reclaim_rounds","last_reclaim_at","last_vacuum_at",
            "live_rows_estimate","dead_rows_estimate","head_bytes","soft_budget_exceeded","last_delete_xid","oldest_xmin_observed",
            "leaf_partition_oid","partition_no"};
        const Oid types[]={UUIDOID,OIDOID,OIDOID,INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,TIMESTAMPTZOID,
            TIMESTAMPTZOID,INT8OID,INT8OID,INT8OID,BOOLOID,INT8OID,INT8OID,OIDOID,INT4OID};
        int columns=LakeNativePartitionVersionEnabled()?18:16;
        if(desc->natts!=columns) LakeBadConstraint();
        for(int i=0;i<columns;++i)
            if(strcmp(NameStr(desc->attrs[i].attname),names[i])!=0 || desc->attrs[i].atttypid!=types[i] ||
               desc->attrs[i].attisdropped || desc->attrs[i].attnotnull!=(i<8 || i==13 || i>=16)) LakeBadConstraint();
        return;
    }
    if(kind>=6) {LakeCheckIndexControlShape(relation,kind);return;}
    static const char* names[6][12]={
        {"server_oid","metadata_schema_oid","catalog_id","data_root","credential_ref","format_version","read_only","binding_generation","index_root"},
        {"binding_id","server_oid","table_id","table_uuid","schema_version","object_kind","state","read_only",
            "flush_birth_cutoff","flush_carrier_oid","flush_source_schema","flush_source_origin"},
        {"relation_oid","binding_id"},
        {"binding_id","column_id","attnum","type_oid","typmod","collation_oid","codec_version"},
        {"metadata_schema_oid","relation_oid","relation_name"},
        {"singleton","internal_owner_oid"}};
    static const Oid types[6][12]={
        {OIDOID,OIDOID,UUIDOID,TEXTOID,TEXTOID,TEXTOID,BOOLOID,INT8OID,TEXTOID},
        {UUIDOID,OIDOID,INT8OID,UUIDOID,INT8OID,TEXTOID,TEXTOID,BOOLOID,INT8OID,OIDOID,INT8OID,INT8OID},
        {OIDOID,UUIDOID},
        {UUIDOID,INT8OID,INT4OID,OIDOID,INT4OID,OIDOID,INT4OID},
        {OIDOID,OIDOID,NAMEOID},{BOOLOID,OIDOID}};
    const int lengths[]={LakeHasIndexRootVersion()?9:8,(LakeIdentityPartitionVersionEnabled()||LakeNativePartitionVersionEnabled())?12:8,2,7,3,2};
    TupleDesc desc=RelationGetDescr(relation);
    if(desc->natts!=lengths[kind]) LakeBadConstraint();
    for(int i=0;i<desc->natts;++i)
        if(strcmp(NameStr(desc->attrs[i].attname),names[kind][i])!=0 || desc->attrs[i].atttypid!=types[kind][i] ||
           desc->attrs[i].attisdropped || desc->attrs[i].attnotnull!=((kind!=0 || (i!=4 && i!=8)) && (kind!=1 || i<8))) LakeBadConstraint();
}

/* Return fixed FK index: table->catalog, relation->table, column->table. */
static int LakeValidateControlFK(HeapTuple tuple, Oid namespaceOid, Oid owner)
{
    Form_pg_constraint con=(Form_pg_constraint)GETSTRUCT(tuple);
    if(con->contype!=CONSTRAINT_FOREIGN || con->connamespace!=namespaceOid || con->condeferrable ||
       con->condeferred || !con->convalidated || con->consoft || con->coninhcount!=0 || !con->conislocal ||
       con->confupdtype!=FKCONSTR_ACTION_NOACTION || con->confmatchtype!=FKCONSTR_MATCH_UNSPECIFIED)
        LakeBadConstraint();
    bool disabledNull=false;
    Datum disabled=SysCacheGetAttr(CONSTROID,tuple,Anum_pg_constraint_condisable,&disabledNull);
    if(!disabledNull && DatumGetBool(disabled)) LakeBadConstraint();
    Relation child=heap_open(con->conrelid,AccessShareLock);
    Relation parent=heap_open(con->confrelid,AccessShareLock);
    Oid extension=get_extension_oid("og_lake",false);
    if(RelationGetNamespace(child)!=namespaceOid || RelationGetNamespace(parent)!=namespaceOid ||
       child->rd_rel->relowner!=owner || parent->rd_rel->relowner!=owner ||
       getExtensionOfObject(RelationRelationId,con->conrelid)!=extension ||
       getExtensionOfObject(RelationRelationId,con->confrelid)!=extension) LakeBadConstraint();
    int from=LakeControlNumber(RelationGetRelationName(child));
    int to=LakeControlNumber(RelationGetRelationName(parent));
    LakeCheckControlShape(child,from);LakeCheckControlShape(parent,to);
    int fk=-1,count=1;int childColumns[3]={0,0,0},parentColumns[3]={1,0,0};
    if(from==1 && to==0 && con->confdeltype==FKCONSTR_ACTION_NOACTION) {fk=0;childColumns[0]=2;}
    if(from==2 && to==1 && con->confdeltype==FKCONSTR_ACTION_CASCADE) {fk=1;childColumns[0]=2;}
    if(from==3 && to==1 && con->confdeltype==FKCONSTR_ACTION_CASCADE) {fk=2;childColumns[0]=1;}
    if(from==6 && to==1 && con->confdeltype==FKCONSTR_ACTION_SETNULL) {fk=3;childColumns[0]=2;}
    if(from==11 && to==1 && con->confdeltype==FKCONSTR_ACTION_CASCADE) {fk=6;childColumns[0]=4;}
    if(from==7 && to==6 && con->confdeltype==FKCONSTR_ACTION_NOACTION) {fk=4;childColumns[0]=2;}
    if(from==8 && to==7 && con->confdeltype==FKCONSTR_ACTION_NOACTION) {
        fk=5;count=3;childColumns[0]=1;childColumns[1]=3;childColumns[2]=4;parentColumns[1]=2;parentColumns[2]=3;
    }
    if(fk<0) LakeBadConstraint();
    if(!LakeConstraintArray(tuple,Anum_pg_constraint_conkey,INT2OID,childColumns,count) ||
       !LakeConstraintArray(tuple,Anum_pg_constraint_confkey,INT2OID,parentColumns,count)) LakeBadConstraint();
    const int equalityAttributes[]={Anum_pg_constraint_conpfeqop,Anum_pg_constraint_conppeqop,Anum_pg_constraint_conffeqop};
    for(Size i=0;i<lengthof(equalityAttributes);++i) {
        bool isnull=false;Datum arrayValue=SysCacheGetAttr(CONSTROID,tuple,equalityAttributes[i],&isnull);
        if(isnull) LakeBadConstraint();
        ArrayType* array=DatumGetArrayTypeP(arrayValue);
        if(ARR_ELEMTYPE(array)!=OIDOID || ARR_NDIM(array)!=1 || ARR_DIMS(array)[0]!=count || ARR_HASNULL(array)) LakeBadConstraint();
        for(int key=0;key<count;++key) {
            Oid expected=fk==0?F_OIDEQ:(fk==5 && key==2?F_INT8EQ:F_UUID_EQ);
            if(get_opcode(((Oid*)ARR_DATA_PTR(array))[key])!=expected) LakeBadConstraint();
        }
    }
    Relation index=LakeOpenTrustedIndex(con->conindid,owner);
    if(index->rd_index->indrelid!=con->confrelid || !index->rd_index->indisunique ||
       !index->rd_index->indimmediate || IndexRelationGetNumberOfKeyAttributes(index)!=count) LakeBadConstraint();
    for(int key=0;key<count;++key)if(index->rd_index->indkey.values[key]!=parentColumns[key])LakeBadConstraint();
    index_close(index,NoLock);heap_close(child,NoLock);heap_close(parent,NoLock);
    return fk;
}

static void LakeValidateControlConstraints(Relation relation, Oid owner, bool indexBootstrap=false)
{
    int kind=LakeControlNumber(RelationGetRelationName(relation));
    if(kind==10){LakeCheckAutoAnalyzeControl(relation,owner);return;}
    LakeCheckControlShape(relation,kind);
    if(RelationGetNamespace(relation)!=get_namespace_oid("og_lake_internal",false) ||
       getExtensionOfObject(RelationRelationId,RelationGetRelid(relation))!=get_extension_oid("og_lake",false)) LakeBadConstraint();
    uint32 checks=0;int checkCount=0;
    TupleConstr* descriptor=RelationGetDescr(relation)->constr;
    if(descriptor!=NULL) for(int i=0;i<descriptor->num_check;++i) {
        Node* expr=(Node*)stringToNode(descriptor->check[i].ccbin);uint32 bit=0;
        if(kind==0) {
            if(LakeCheckExpression(expr,6,TEXTOID,F_TEXTEQ,0,"1.1")) bit=1;
            if(LakeCheckExpression(expr,8,INT8OID,F_INT8GT,0,NULL)) bit=2;
        } else if(kind==1) {
            if(LakeCheckExpression(expr,3,INT8OID,F_INT8GE,0,NULL)) bit=1;
            if(LakeCheckExpression(expr,5,INT8OID,F_INT8GE,0,NULL)) bit=2;
            if(!LakeNativePartitionVersionEnabled() && LakeCheckExpression(expr,6,TEXTOID,F_TEXTEQ,0,"relation")) bit=4;
            if(LakeNativePartitionVersionEnabled()) {
                List* parts=NIL;uint32 kinds=0;ListCell* part=NULL;
                if(LakeIndexOrParts(expr,&parts)&&list_length(parts)==2) {
                    foreach(part,parts) {
                        if(LakeCheckExpression((Node*)lfirst(part),6,TEXTOID,F_TEXTEQ,0,"relation"))kinds|=1;
                        if(LakeCheckExpression((Node*)lfirst(part),6,TEXTOID,F_TEXTEQ,0,"partition"))kinds|=2;
                    }
                    if(kinds==3)bit=4;
                }
                list_free(parts);
            }
            if(LakeCheckExpression(expr,7,TEXTOID,F_TEXTEQ,0,"active")) bit=8;
        } else if(kind==11) {
            if(LakeCheckExpression(expr,3,INT4OID,F_INT4GT,0,NULL))bit=1;
            if(LakeCheckExpression(expr,5,INT8OID,F_INT8GE,0,NULL))bit=2;
        } else if(kind==9 && LakeNativePartitionVersionEnabled()) {
            bit=LakeNativeRowHeadDomainCheck(expr);
        } else if(kind>=6) {
            bit=LakeIndexCheckConstraint(expr,kind);
        } else if(kind==3) {
            if(LakeCheckExpression(expr,2,INT8OID,F_INT8GE,0,NULL)) bit=1;
            if(LakeCheckExpression(expr,3,INT4OID,F_INT4GT,0,NULL)) bit=2;
            if(LakeCheckExpression(expr,7,INT4OID,F_INT4EQ,1,NULL)) bit=4;
        }
        if(bit==0 || (checks&bit)!=0) LakeBadConstraint();
        checks|=bit;
        ++checkCount;
    }
    const uint32 expectedChecks[]={3,15,0,7,0,0,63,63,15,0,0,3};
    uint32 requiredChecks=kind==9&&LakeNativePartitionVersionEnabled()?3:expectedChecks[kind];
    if(checks!=requiredChecks) LakeBadConstraint();
    Relation constraints=heap_open(ConstraintRelationId,AccessShareLock);
    ScanKeyData key;ScanKeyInit(&key,Anum_pg_constraint_conrelid,BTEqualStrategyNumber,F_OIDEQ,ObjectIdGetDatum(RelationGetRelid(relation)));
    SysScanDesc scan=systable_beginscan(constraints,ConstraintRelidIndexId,true,SnapshotNow,1,&key);
    HeapTuple row;uint32 keys=0;int fkCount=0;int catalogChecks=0;
    while(HeapTupleIsValid(row=systable_getnext(scan))) {
        HeapTuple tuple=SearchSysCache1(CONSTROID,ObjectIdGetDatum(HeapTupleGetOid(row)));
        if(!HeapTupleIsValid(tuple)) LakeBadConstraint();
        Form_pg_constraint con=(Form_pg_constraint)GETSTRUCT(tuple);
        if(con->condeferrable || con->condeferred || !con->convalidated || con->consoft || con->coninhcount!=0 || !con->conislocal)
            LakeBadConstraint();
        bool isnull=false;Datum disabled=SysCacheGetAttr(CONSTROID,tuple,Anum_pg_constraint_condisable,&isnull);
        if(!isnull && DatumGetBool(disabled)) LakeBadConstraint();
        if(con->contype==CONSTRAINT_CHECK) ++catalogChecks;
        else if(con->contype==CONSTRAINT_FOREIGN) {
            int fk=LakeValidateControlFK(tuple,RelationGetNamespace(relation),owner);
            if(fk!=(kind==11?6:(kind>=6?kind-3:kind-1))) LakeBadConstraint();
            ++fkCount;
        } else if(con->contype==CONSTRAINT_PRIMARY || con->contype==CONSTRAINT_UNIQUE) {
            int expected[3]={1,0,0};int count=1;uint32 bit=0;
            if(con->contype==CONSTRAINT_PRIMARY) {
                bit=1;
                if(kind==3 || kind==8 || kind==11) {expected[1]=2;count=2;} else if(kind==4) expected[0]=2;
            } else {
                if(kind==0) {
                    int second=2,third=3;
                    if(LakeConstraintArray(tuple,Anum_pg_constraint_conkey,INT2OID,&second,1)) {expected[0]=2;bit=2;}
                    else if(LakeConstraintArray(tuple,Anum_pg_constraint_conkey,INT2OID,&third,1)) {expected[0]=3;bit=4;}
                } else if(kind==1) {expected[0]=2;expected[1]=3;count=2;bit=2;}
                else if(kind==2) {expected[0]=2;bit=2;}
                else if(kind==3 || kind==4) {expected[0]=1;expected[1]=3;count=2;bit=2;}
                else if(kind==11) {expected[0]=4;bit=2;}
                else if(kind==9) {
                    int host=2,head=3,typedHost[2]={2,17};
                    if(LakeNativePartitionVersionEnabled()&&LakeConstraintArray(tuple,Anum_pg_constraint_conkey,INT2OID,typedHost,2)) {
                        expected[0]=2;expected[1]=17;count=2;bit=2;
                    } else if(!LakeNativePartitionVersionEnabled()&&LakeConstraintArray(tuple,Anum_pg_constraint_conkey,INT2OID,&host,1)) {expected[0]=2;bit=2;}
                    else if(LakeConstraintArray(tuple,Anum_pg_constraint_conkey,INT2OID,&head,1)) {expected[0]=3;bit=4;}
                }
                else if(kind==7) {
                    int uri=9,composite[3]={1,2,3};
                    if(LakeConstraintArray(tuple,Anum_pg_constraint_conkey,INT2OID,&uri,1)) {expected[0]=9;bit=2;}
                    else if(LakeConstraintArray(tuple,Anum_pg_constraint_conkey,INT2OID,composite,3)) {
                        expected[0]=1;expected[1]=2;expected[2]=3;count=3;bit=4;
                    }
                }
            }
            if(bit==0 || (keys&bit)!=0 || !LakeConstraintArray(tuple,Anum_pg_constraint_conkey,INT2OID,expected,count)) LakeBadConstraint();
            Relation index=LakeOpenTrustedIndex(con->conindid,owner);
            if(index->rd_index->indrelid!=RelationGetRelid(relation) || !index->rd_index->indisunique ||
               index->rd_index->indisprimary!=(con->contype==CONSTRAINT_PRIMARY) || IndexRelationGetNumberOfKeyAttributes(index)!=count) LakeBadConstraint();
            for(int i=0;i<count;++i) if(index->rd_index->indkey.values[i]!=expected[i]) LakeBadConstraint();
            index_close(index,NoLock);keys|=bit;
        } else LakeBadConstraint();
        ReleaseSysCache(tuple);
    }
    systable_endscan(scan);heap_close(constraints,AccessShareLock);
    const uint32 expectedKeys[]={7,3,3,3,3,1,1,7,1,7,0,3};
    int expectedFk=((kind>=1 && kind<=3)||(kind>=6 && kind<=8)||kind==11)?1:0;
    if(kind==6 && indexBootstrap) expectedFk=0;
    if(keys!=expectedKeys[kind] || catalogChecks!=checkCount || fkCount!=expectedFk) LakeBadConstraint();
    if(kind==6 || kind==8) {
        List* indexes=RelationGetIndexList(relation);ListCell* cell=NULL;int partials=0;
        foreach(cell,indexes) {
            HeapTuple tuple=SearchSysCache1(INDEXRELID,ObjectIdGetDatum(lfirst_oid(cell)));
            if(!HeapTupleIsValid(tuple))LakeBadConstraint();
            bool absent=false;Datum value=SysCacheGetAttr(INDEXRELID,tuple,Anum_pg_index_indpred,&absent);
            if(!absent) {
                if(!LakeIndexValidPartialPredicate(lfirst_oid(cell),(Node*)stringToNode(TextDatumGetCString(value))))LakeBadConstraint();
                Relation index=LakeOpenTrustedIndex(lfirst_oid(cell),owner);
                int expected[3]={kind==6?2:3,kind==6?3:4,2};int count=kind==6?2:3;
                if(!index->rd_index->indisunique || index->rd_index->indisprimary ||
                   IndexRelationGetNumberOfKeyAttributes(index)!=count)LakeBadConstraint();
                for(int i=0;i<count;++i)if(index->rd_index->indkey.values[i]!=expected[i])LakeBadConstraint();
                index_close(index,NoLock);++partials;
            }
            ReleaseSysCache(tuple);
        }
        list_free(indexes);if(partials!=1)LakeBadConstraint();
    }
    uint32 triggers=0;
    if(relation->trigdesc!=NULL) for(int i=0;i<relation->trigdesc->numtriggers;++i) {
        Trigger* trigger=&relation->trigdesc->triggers[i];
        if(!trigger->tgisinternal || trigger->tgdeferrable || trigger->tginitdeferred || trigger->tgnargs!=0 ||
           trigger->tgnattr!=0 || trigger->tgqual!=NULL || trigger->tgenabled!=TRIGGER_FIRES_ON_ORIGIN) LakeBadConstraint();
        HeapTuple tuple=SearchSysCache1(CONSTROID,ObjectIdGetDatum(trigger->tgconstraint));
        if(!HeapTupleIsValid(tuple)) LakeBadConstraint();
        int fk=LakeValidateControlFK(tuple,RelationGetNamespace(relation),owner);
        Form_pg_constraint con=(Form_pg_constraint)GETSTRUCT(tuple);
        bool child=RelationGetRelid(relation)==con->conrelid;
        bool parent=RelationGetRelid(relation)==con->confrelid;
        if((!child && !parent) || trigger->tgconstrrelid!=(child?con->confrelid:con->conrelid) || trigger->tgconstrindid!=con->conindid) LakeBadConstraint();
        int which=-1;uint16 event=0;
        if(child && trigger->tgfoid==F_RI_FKEY_CHECK_INS) {which=0;event=TRIGGER_TYPE_INSERT;}
        if(child && trigger->tgfoid==F_RI_FKEY_CHECK_UPD) {which=1;event=TRIGGER_TYPE_UPDATE;}
        Oid deleteFunction=(fk==1 || fk==2 || fk==6)?F_RI_FKEY_CASCADE_DEL:(fk==3?F_RI_FKEY_SETNULL_DEL:F_RI_FKEY_NOACTION_DEL);
        if(parent && trigger->tgfoid==deleteFunction) {which=2;event=TRIGGER_TYPE_DELETE;}
        if(parent && trigger->tgfoid==F_RI_FKEY_NOACTION_UPD) {which=3;event=TRIGGER_TYPE_UPDATE;}
        if(which<0 || trigger->tgtype!=(TRIGGER_TYPE_ROW|event)) LakeBadConstraint();
        uint32 bit=1U<<(4*fk+which);if((triggers&bit)!=0) LakeBadConstraint();triggers|=bit;
        ReleaseSysCache(tuple);
    }
    uint32 expectedTriggers[]={0x00c,0xcc3,0x030,0x300,0,0,0xc3000,0xc30000,0x300000,0,0,0x03000000};
    if(kind==1 && LakeNativePartitionVersionEnabled())expectedTriggers[1]|=0x0c000000;
    if(kind==1 && LakeHasIndexCatalogVersion()) expectedTriggers[1]|=0xc000;
    if(kind==6 && indexBootstrap) expectedTriggers[6]=0xc0000;
    if(triggers!=expectedTriggers[kind]) LakeBadConstraint();
}

static void LakeValidatePrivateRelationImpl(Relation relation, Oid owner, bool extensionControl, bool indexBootstrap)
{
    Oid oid=RelationGetRelid(relation);
    if(relation->rd_rel->relowner!=owner || relation->rd_rel->relkind!=RELKIND_RELATION ||
       relation->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT || RelationIsPartitioned(relation) ||
       relation->rd_rel->relhassubclass || RelationIsColStore(relation) || RowRelationIsCompressed(relation))
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native lake metadata relation has untrusted ownership or storage")));
    if(relation->rd_rules!=NULL || RelationHasRlspolicy(oid) || RelationEnableRowSecurity(relation) ||
       RelationForceRowSecurity(relation))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake internal relations cannot have rules or row-security policies")));
    HeapTuple classTuple=SearchSysCache1(RELOID,ObjectIdGetDatum(oid));
    if(!HeapTupleIsValid(classTuple))
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_TABLE),errmsg("native lake internal relation disappeared")));
    bool isnull=false;
    Datum acl=SysCacheGetAttr(RELOID,classTuple,Anum_pg_class_relacl,&isnull);
    LakeCheckPrivateAcl(acl,isnull,owner); ReleaseSysCache(classTuple);
    TupleDesc desc=RelationGetDescr(relation);
    for(int i=0;i<desc->natts;++i) {
        Form_pg_attribute attr=&desc->attrs[i];
        if(attr->attisdropped || !LakeInternalType(attr->atttypid) ||
           (OidIsValid(attr->attcollation) && attr->attcollation!=DEFAULT_COLLATION_OID &&
            attr->attcollation!=C_COLLATION_OID && attr->attcollation!=POSIX_COLLATION_OID))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake internal relations require reviewed built-in physical types")));
        HeapTuple at=SearchSysCache2(ATTNUM,ObjectIdGetDatum(oid),Int16GetDatum(i+1));
        if(!HeapTupleIsValid(at))
            ereport(ERROR,(errcode(ERRCODE_UNDEFINED_COLUMN),errmsg("native lake internal column disappeared")));
        Datum columnAcl=SysCacheGetAttr(ATTNUM,at,Anum_pg_attribute_attacl,&isnull);
        LakeCheckPrivateAcl(columnAcl,isnull,owner); ReleaseSysCache(at);
    }
    TupleConstr* constraints=desc->constr;
    if(constraints!=NULL && (constraints->num_defval!=0 || (!extensionControl && constraints->num_check!=0)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake external metadata cannot execute defaults or CHECK expressions")));
    if(extensionControl) LakeValidateControlConstraints(relation,owner,indexBootstrap);
    else if(relation->trigdesc!=NULL && relation->trigdesc->numtriggers!=0)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata cannot execute triggers")));
    List* indexes=RelationGetIndexList(relation);ListCell* cell=NULL;
    foreach(cell,indexes) {
        Relation index=!extensionControl&&LakeIsAnnInlineIndex(lfirst_oid(cell))?
            LakeOpenTrustedAnnInlineIndex(lfirst_oid(cell),owner):
            LakeOpenTrustedIndex(lfirst_oid(cell),owner,!extensionControl&&RelationIsUstoreFormat(relation));
        index_close(index,NoLock);
    }
    list_free(indexes);
}

void LakeValidatePrivateRelation(Relation relation, Oid owner, bool extensionControl)
{
    LakeValidatePrivateRelationImpl(relation,owner,extensionControl,false);
}

/* Only explicit administrator attachment/configuration can populate this map.
 * Runtime never records a missing object merely because its name matches. */
static Relation LakeOpenIdentityMap(Oid owner, LOCKMODE mode)
{
    Oid ns=get_namespace_oid("og_lake_internal",false);
    Oid oid=get_relname_relid("metadata_relation",ns);
    if(!OidIsValid(oid) || getExtensionOfObject(RelationRelationId,oid)!=get_extension_oid("og_lake",false))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake metadata identity registry is not trusted")));
    Relation relation=heap_open(oid,mode);
    LakeValidatePrivateRelation(relation,owner,true);
    TupleDesc desc=RelationGetDescr(relation);
    const char* names[]={"metadata_schema_oid","relation_oid","relation_name"};
    const Oid types[]={OIDOID,OIDOID,NAMEOID};
    if(desc->natts!=3)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake metadata identity registry layout changed")));
    for(int i=0;i<3;++i)
        if(strcmp(NameStr(desc->attrs[i].attname),names[i])!=0 || desc->attrs[i].atttypid!=types[i] || !desc->attrs[i].attnotnull)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake metadata identity registry layout changed")));
    return relation;
}

void LakeCheckMetadataObjectIdentity(Oid namespaceOid, Oid relationOid, Oid owner)
{
    if(namespaceOid==get_namespace_oid("og_lake_internal",false)) return;
    Relation registry=LakeOpenIdentityMap(owner,AccessShareLock);
    SysScanDesc scan=systable_beginscan(registry,InvalidOid,false,SnapshotNow,0,NULL);
    HeapTuple tuple; int matches=0; char* name=get_rel_name(relationOid);
    if(name==NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake registered relation disappeared")));
    while(HeapTupleIsValid(tuple=systable_getnext(scan))) {
        bool nulls[3]; Datum values[3];
        for(int i=0;i<3;++i) values[i]=tableam_tops_tuple_getattr(tuple,i+1,RelationGetDescr(registry),&nulls[i]);
        if(nulls[0]||nulls[1]||nulls[2])
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake metadata identity registry contains NULL")));
        if(DatumGetObjectId(values[0])!=namespaceOid || DatumGetObjectId(values[1])!=relationOid) continue;
        if(strcmp(NameStr(*DatumGetName(values[2])),name)!=0)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake registered metadata relation was renamed")));
        ++matches;
    }
    systable_endscan(scan);heap_close(registry,NoLock);pfree(name);
    if(matches!=1)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake metadata relation has no exact registered OID identity")));
}

static void LakeRegisterMetadataObjectsInternal(Oid namespaceOid, Oid owner, Oid controlOwner)
{
    LakeInlineRequireAdministrator();
    LakeValidatePrivateNamespace(namespaceOid,owner);
    /* Both callers hold catalog_binding ShareRowExclusiveLock. This function
     * only inserts MVCC rows and maintains the PK/UNIQUE indexes; it does not
     * rewrite the registry. RowExclusive preserves concurrent runtime AS
     * readers, including a transaction that will later request registration. */
    Relation registry=LakeOpenIdentityMap(controlOwner,RowExclusiveLock);
    /* Re-attachment never silently repairs a disappeared/renamed/replaced OID. */
    SysScanDesc old=systable_beginscan(registry,InvalidOid,false,SnapshotNow,0,NULL);
    HeapTuple tuple;
    while(HeapTupleIsValid(tuple=systable_getnext(old))) {
        bool isnull=false;
        Oid ns=DatumGetObjectId(tableam_tops_tuple_getattr(tuple,1,RelationGetDescr(registry),&isnull));
        if(isnull || ns!=namespaceOid) continue;
        Oid oid=DatumGetObjectId(tableam_tops_tuple_getattr(tuple,2,RelationGetDescr(registry),&isnull));
        if(isnull)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake metadata registry contains NULL identity")));
        Datum nameValue=tableam_tops_tuple_getattr(tuple,3,RelationGetDescr(registry),&isnull);
        if(isnull || get_rel_namespace(oid)!=namespaceOid ||
           get_relname_relid(NameStr(*DatumGetName(nameValue)),namespaceOid)!=oid)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake existing metadata identity cannot be silently rebound")));
    }
    systable_endscan(old);
    Relation classes=heap_open(RelationRelationId,AccessShareLock);
    SysScanDesc relations=systable_beginscan(classes,InvalidOid,false,SnapshotNow,0,NULL);
    HeapTuple row;
    while(HeapTupleIsValid(row=systable_getnext(relations))) {
        Form_pg_class cls=(Form_pg_class)GETSTRUCT(row);
        if(cls->relnamespace!=namespaceOid || cls->relkind==RELKIND_INDEX) continue;
        Oid oid=HeapTupleGetOid(row);
        Relation relation=heap_open(oid,AccessShareLock);
        LakeValidatePrivateRelation(relation,owner,false);
        bool found=false;
        SysScanDesc entries=systable_beginscan(registry,InvalidOid,false,SnapshotNow,0,NULL);
        HeapTuple entry;
        while(HeapTupleIsValid(entry=systable_getnext(entries))) {
            bool isnull=false;
            Oid registered=DatumGetObjectId(tableam_tops_tuple_getattr(entry,2,RelationGetDescr(registry),&isnull));
            if(!isnull && registered==oid) { found=true;break; }
        }
        systable_endscan(entries);
        if(!found) {
            NameData name;namestrcpy(&name,RelationGetRelationName(relation));
            Datum values[]={ObjectIdGetDatum(namespaceOid),ObjectIdGetDatum(oid),NameGetDatum(&name)};
            bool nulls[]={false,false,false};
            HeapTuple added=heap_form_tuple(RelationGetDescr(registry),values,nulls);
            simple_heap_insert(registry,added);CatalogUpdateIndexes(registry,added);heap_freetuple(added);
            CommandCounterIncrement();
        }
        heap_close(relation,NoLock);
    }
    systable_endscan(relations);heap_close(classes,AccessShareLock);heap_close(registry,NoLock);
}

/* Only the just-created exact relation can enter through this internal helper.
 * Caller performed a closed CREATE under the configured private owner. */
void LakeRegisterCreatedCarrier(Oid namespaceOid, Oid relationOid)
{
    Oid owner=LakeMetadataOwner();
    if(!OidIsValid(owner))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("new lake carrier needs a private owner")));
    LakeValidatePrivateNamespace(namespaceOid,owner);
    Relation carrier=heap_open(relationOid,AccessShareLock);
    if(carrier->rd_createSubid==InvalidSubTransactionId || RelationGetNamespace(carrier)!=namespaceOid ||
       carrier->rd_rel->relowner!=owner || carrier->rd_rel->relkind!=RELKIND_RELATION)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake may register only its newly created private carrier")));
    LakeValidatePrivateRelation(carrier,owner,false);
    Relation registry=LakeOpenIdentityMap(owner,RowExclusiveLock);
    NameData name;namestrcpy(&name,RelationGetRelationName(carrier));
    Datum values[]={ObjectIdGetDatum(namespaceOid),ObjectIdGetDatum(relationOid),NameGetDatum(&name)};
    bool nulls[]={false,false,false};
    HeapTuple added=heap_form_tuple(RelationGetDescr(registry),values,nulls);
    simple_heap_insert(registry,added);CatalogUpdateIndexes(registry,added);heap_freetuple(added);
    CommandCounterIncrement();heap_close(registry,NoLock);heap_close(carrier,NoLock);
}

void LakeRegisterMetadataObjects(Oid namespaceOid)
{
    LakeInlineRequireAdministrator();
    LakeLockCatalogRegistration(); /* Defensive for any future admin call site. */
    Oid owner=LakeMetadataOwner();
    if(!OidIsValid(owner)) return; /* No new objects required for 0.1--0.4/unconfigured admin paths. */
    LakeRegisterMetadataObjectsInternal(namespaceOid,owner,owner);
}

/* Native access to a two-column, extension-owned control table. Never resolve
 * or execute user SQL before proving its extension membership and admin owner. */
static Oid LakeMetadataOwnerImpl(void)
{
    Oid extension=InvalidOid; bool supports=false;
    Oid administrator=LakeExtensionOwner(&extension,&supports);
    if(!supports) return InvalidOid; /* Installed 0.2/0.3/0.4 keep admin-only access. */
    Oid ns=get_namespace_oid("og_lake_internal",false);
    if(getExtensionOfObject(NamespaceRelationId,ns)!=extension)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake private namespace is not extension-owned")));
    Oid policy=get_relname_relid("access_policy",ns);
    if(!OidIsValid(policy) || getExtensionOfObject(RelationRelationId,policy)!=extension)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake access policy is missing or untrusted")));
    Relation rel=heap_open(policy,AccessShareLock);
    LakeValidatePrivateRelation(rel,administrator,true);
    TupleDesc desc=RelationGetDescr(rel);
    if(desc->natts!=2 || strcmp(NameStr(desc->attrs[0].attname),"singleton")!=0 || desc->attrs[0].atttypid!=BOOLOID ||
       strcmp(NameStr(desc->attrs[1].attname),"internal_owner_oid")!=0 || desc->attrs[1].atttypid!=OIDOID)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake access policy layout changed")));
    SysScanDesc scan=systable_beginscan(rel,InvalidOid,false,SnapshotNow,0,NULL);
    HeapTuple tuple; Oid owner=InvalidOid; int rows=0;
    while(HeapTupleIsValid(tuple=systable_getnext(scan))) {
        bool keyNull=false,ownerNull=false;
        Datum key=tableam_tops_tuple_getattr(tuple,1,desc,&keyNull);
        Datum value=tableam_tops_tuple_getattr(tuple,2,desc,&ownerNull);
        if(++rows!=1 || keyNull || !DatumGetBool(key) || ownerNull || !OidIsValid(DatumGetObjectId(value)))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake internal owner registration")));
        owner=DatumGetObjectId(value);
    }
    systable_endscan(scan); heap_close(rel,NoLock);
    if(OidIsValid(owner)) LakeCheckOwnerRole(owner);
    Oid expectedOwner=OidIsValid(owner)?owner:administrator;
    LakeValidatePrivateNamespace(ns,expectedOwner);
    const char* controls[]={"catalog_binding","table_binding","relation_binding","column_binding","metadata_relation"};
    for(Size i=0;i<lengthof(controls);++i) {
        Oid oid=get_relname_relid(controls[i],ns);
        if(!OidIsValid(oid) || getExtensionOfObject(RelationRelationId,oid)!=extension)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake binding is not an extension object")));
        Relation control=heap_open(oid,AccessShareLock);
        LakeValidatePrivateRelation(control,expectedOwner,true);
        heap_close(control,NoLock);
    }
    if(LakeRowLocksEnabled()) {
        Oid oid=get_relname_relid("row_lock_binding",ns);
        if(!OidIsValid(oid) || getExtensionOfObject(RelationRelationId,oid)!=extension)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake row-head catalog is not an extension object")));
        Relation control=heap_open(oid,AccessShareLock);
        LakeValidatePrivateRelation(control,expectedOwner,true);heap_close(control,NoLock);
    }
    if(LakeAutoAnalyzeCatalogAvailable()) {
        Relation control=heap_open(get_relname_relid("analyze_state",ns),AccessShareLock);
        LakeValidatePrivateRelation(control,expectedOwner,true);heap_close(control,NoLock);
    }
    if(LakeNativePartitionVersionEnabled()) {
        Relation control=heap_open(get_relname_relid("partition_binding",ns),AccessShareLock);
        LakeValidatePrivateRelation(control,expectedOwner,true);heap_close(control,NoLock);
    }
    return owner;
}

Oid LakeMetadataOwner(void)
{
    Oid owner=InvalidOid;
    if (LakeFastOwnerLookup(&owner))
        return owner;
    owner=LakeMetadataOwnerImpl();
    LakeFastOwnerStore(owner);
    return owner;
}

void LakeCheckMaskingAdmission(void)
{
    if(!u_sess->attr.attr_security.Enable_Security_Policy) return;
    /* Actual CREATE/ALTER MASKING POLICY writers use RowExclusiveLock. Keep
     * ShareLock to transaction end, so a checked empty policy set cannot gain
     * an enabled policy between this check and parsing under the private role.
     */
    Relation policies=heap_open(GsMaskingPolicyRelationId,ShareLock);
    SysScanDesc scan=systable_beginscan(policies,InvalidOid,false,SnapshotNow,0,NULL);
    HeapTuple tuple;bool enabled=false;
    while(HeapTupleIsValid(tuple=systable_getnext(scan)))
        if(((Form_gs_masking_policy)GETSTRUCT(tuple))->polenabled) { enabled=true;break; }
    systable_endscan(scan);heap_close(policies,NoLock);
    if(enabled)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake access with enabled masking policies is not supported")));
}

void LakeRequireAccessPolicy(void)
{
    /* The supported host isolation contract covers reads as well as writes.
     * Check every entry, including reuse of a previously prepared lake plan. */
    if(u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake access requires READ COMMITTED")));
    Oid owner=LakeMetadataOwner();
    if(!OidIsValid(owner) && !superuser())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("ordinary native lake access requires installed extension 0.5 or later and a configured private owner")));
}

bool LakeTrustedForeignServer(Oid serverOid)
{
    ForeignServer* server=GetForeignServer(serverOid);
    ForeignDataWrapper* fdw=GetForeignDataWrapper(server->fdwid);
    if(strcmp(fdw->fdwname,"og_lake_fdw")!=0) return false;
    Oid extension=get_extension_oid("og_lake",false);
    if(getExtensionOfObject(ForeignDataWrapperRelationId,fdw->fdwid)!=extension)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake wrapper lacks the trusted extension dependency")));
    const Oid functions[]={fdw->fdwhandler,fdw->fdwvalidator};
    const char* symbols[]={"og_lake_fdw_handler","og_lake_fdw_validator"};
    for(Size i=0;i<lengthof(functions);++i) {
        HeapTuple tuple=SearchSysCache1(PROCOID,ObjectIdGetDatum(functions[i]));
        if(!HeapTupleIsValid(tuple))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake FDW function disappeared")));
        Form_pg_proc function=(Form_pg_proc)GETSTRUCT(tuple);
        bool sourceNull=false,libraryNull=false;
        Datum source=SysCacheGetAttr(PROCOID,tuple,Anum_pg_proc_prosrc,&sourceNull);
        Datum library=SysCacheGetAttr(PROCOID,tuple,Anum_pg_proc_probin,&libraryNull);
        bool valid=function->prolang==ClanguageId && superuser_arg(function->proowner) &&
            !sourceNull && !libraryNull && strcmp(TextDatumGetCString(source),symbols[i])==0 &&
            strcmp(TextDatumGetCString(library),"$libdir/og_lake")==0 &&
            getExtensionOfObject(ProcedureRelationId,functions[i])==extension;
        ReleaseSysCache(tuple);
        if(!valid)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake FDW function is not trusted")));
    }
    return true;
}

static bool LakeHasLakeQuery(Node* node, void*)
{
    if(node==NULL) return false;
    if(IsA(node,Query)) {
        Query* query=(Query*)node; ListCell* cell=NULL;
        foreach(cell,query->rtable) {
            RangeTblEntry* rte=(RangeTblEntry*)lfirst(cell);
            if(rte->rtekind==RTE_RELATION && get_rel_relkind(rte->relid)==RELKIND_FOREIGN_TABLE &&
               LakeTrustedForeignServer(GetForeignTable(rte->relid)->serverid)) return true;
            if(rte->rtekind==RTE_RELATION && get_rel_relkind(rte->relid)==RELKIND_RELATION) {
                Relation relation=heap_open(rte->relid,AccessShareLock);
                bool marked=LakeHasNativePartitionMarker(relation);heap_close(relation,NoLock);
                if(marked)return true;
            }
        }
        return query_tree_walker(query,(bool (*)())LakeHasLakeQuery,NULL,0);
    }
    return expression_tree_walker(node,(bool (*)())LakeHasLakeQuery,NULL);
}

static void LakeRejectSecurityRtes(List* rangeTable)
{
    ListCell* cell=NULL;
    foreach(cell,rangeTable) {
        RangeTblEntry* rte=(RangeTblEntry*)lfirst(cell);
        if(rte->security_barrier || rte->securityQuals!=NIL ||
           (rte->rtekind==RTE_RELATION && RelationHasRlspolicy(rte->relid)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake row-security and security-barrier combinations are not enabled")));
    }
}

static bool LakeCheckPermissionQuery(Node* node, void*)
{
    if(node==NULL) return false;
    if(IsA(node,Query)) {
        Query* query=(Query*)node;
        (void)ExecCheckRTPerms(query->rtable,true);
        LakeRejectSecurityRtes(query->rtable);
        return query_tree_walker(query,(bool (*)())LakeCheckPermissionQuery,NULL,0);
    }
    return expression_tree_walker(node,(bool (*)())LakeCheckPermissionQuery,NULL);
}

void LakeCheckMetadataParseRelation(Relation relation)
{
    LakeMetadataScope* scope=LakeGetMetadataScope();
    if(scope==NULL) {
        if(RelationGetRelkind(relation)==RELKIND_FOREIGN_TABLE &&
           LakeTrustedForeignServer(GetForeignTable(RelationGetRelid(relation))->serverid)) LakeCheckMaskingAdmission();
        return;
    }
    if(scope->require_oid_binding) LakeCheckMaskingAdmission();
    if(RelationGetNamespace(relation)!=scope->namespace_oid)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake metadata resolved outside its bound namespace")));
    if(!scope->require_oid_binding) return; /* Legacy admin path: no identity elevation or 0.5 constraint assumptions. */
    LakeValidatePrivateNamespace(scope->namespace_oid,scope->owner_oid);
    bool control=scope->namespace_oid==get_namespace_oid("og_lake_internal",false);
    if(control && !OidIsValid(getExtensionOfObject(RelationRelationId,RelationGetRelid(relation))) &&
       LakeRowLocksEnabled() && LakeIsRegisteredRowLockHead(relation,scope->owner_oid)) return;
    if(control && getExtensionOfObject(RelationRelationId,RelationGetRelid(relation))!=get_extension_oid("og_lake",false))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake internal relation is not extension-owned")));
    LakeValidatePrivateRelation(relation,scope->owner_oid,control);
    if(scope->require_oid_binding && !control)
        LakeCheckMetadataObjectIdentity(scope->namespace_oid,RelationGetRelid(relation),scope->owner_oid);
}

static bool LakeCheckMetadataQueryNode(Node* node, void*)
{
    if(node==NULL) return false;
    if(IsA(node,RangeTblEntry)) {
        RangeTblEntry* rte=(RangeTblEntry*)node;
        if(rte->rtekind==RTE_RELATION) {
            Relation relation=heap_open(rte->relid,AccessShareLock);
            LakeCheckMetadataParseRelation(relation); heap_close(relation,NoLock);
        }
        return false;
    }
    bool strict=LakeGetMetadataScope()->require_oid_binding;
    if(strict && IsA(node,FuncExpr) && ((FuncExpr*)node)->funcid>=FirstNormalObjectId)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata cannot execute user functions")));
    if(strict && IsA(node,OpExpr) && get_opcode(((OpExpr*)node)->opno)>=FirstNormalObjectId)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata cannot execute user operators")));
    if(strict && IsA(node,Aggref) && ((Aggref*)node)->aggfnoid>=FirstNormalObjectId)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata cannot execute user aggregates")));
    if(IsA(node,Query)) {
        Query* query=(Query*)node;
        if(query->utilityStmt!=NULL)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("utility commands cannot execute in a lake metadata identity scope")));
        return query_tree_walker(query,(bool (*)())LakeCheckMetadataQueryNode,NULL,QTW_EXAMINE_RTES);
    }
    return expression_tree_walker(node,(bool (*)())LakeCheckMetadataQueryNode,NULL);
}

void LakeCheckMetadataExecutor(PlannedStmt* plan,Snapshot snapshot)
{
    LakeCheckNativePartitionExecutor(plan);
    if(LakeGetMetadataScope()==NULL) {
        LakeCheckBoundSnapshotPlan(plan);
        LakeCheckNotNullPlanSnapshot(plan,snapshot);
        return;
    }
    if(plan==NULL || plan->utilityStmt!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake metadata requires a regular trusted plan")));
    ListCell* cell=NULL;
    foreach(cell,plan->rtable) (void)LakeCheckMetadataQueryNode((Node*)lfirst(cell),NULL);
}

void LakeCheckQueryPermissions(Query* query)
{
    if(LakeGetMetadataScope()!=NULL) {
        (void)LakeCheckMetadataQueryNode((Node*)query,NULL);
        return;
    }
    if(!LakeHasLakeQuery((Node*)query,NULL)) return;
    LakeCheckMaskingAdmission();
    /* Preserve requiredPerms, all column bitmaps, and view checkAsUser exactly. */
    (void)LakeCheckPermissionQuery((Node*)query,NULL);
    LakeRequireAccessPolicy();
    LakeCheckBoundSnapshotQuery(query);
    LakeCheckNotNullQuerySnapshot(query,ActiveSnapshotSet()?GetActiveSnapshot():NULL);
}

void LakeCheckExecutorPermissions(EState* estate)
{
    if(estate==NULL)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake requires an executor permission context")));
    LakeCheckMaskingAdmission();
    (void)ExecCheckRTPerms(estate->es_range_table,true);
    LakeRejectSecurityRtes(estate->es_range_table);
    LakeRequireAccessPolicy();
}

/* Administrator explicitly prepares metadata ownership before this call. The
 * fixed extension binding objects are handed over in the same user transaction;
 * this function neither creates a cluster role nor commits internally. */
Datum og_lake_configure_internal_owner(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator(); LakeTxnRejectBindingDdl();
    LakeInvalidateFastValidationCache();
    if(PG_ARGISNULL(0))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("native lake internal owner name is required")));
    Oid extension=InvalidOid; bool supports=false;
    Oid administrator=LakeExtensionOwner(&extension,&supports);
    if(!supports)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("internal owner registration requires installed extension 0.5 or later")));
    LakeLockCatalogRegistration(); /* Same order as create/attach/drop, before policy AX. */
    Oid owner=get_role_oid(NameStr(*PG_GETARG_NAME(0)),false);
    LakeCheckOwnerRole(owner);
    Oid current=LakeMetadataOwner();
    if(OidIsValid(current)) {
        if(current!=owner)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake internal owner is immutable after registration")));
        PG_RETURN_VOID();
    }
    Oid ns=get_namespace_oid("og_lake_internal",false);
    Oid policy=get_relname_relid("access_policy",ns);
    /* Ownership change truly requires AX; never weaken it. A reader can hold
     * policy/control AS and later request the registration mutex we now own.
     * Acquire every required AX conditionally before changing any ownership or
     * recording any metadata identities, and fail atomically rather than wait
     * in that lock-upgrade cycle. The caller may retry after readers finish. */
    const char* lockNames[]={"access_policy","catalog_binding","table_binding",
        "relation_binding","column_binding","metadata_relation","index_definition","index_segment","index_coverage","row_lock_binding","analyze_state","partition_binding"};
    Size lockCount=(LakeRowLocksEnabled()?10:(LakeHasIndexCatalogVersion()?9:6))+(LakeAutoAnalyzeCatalogAvailable()?1:0);
    if(LakeNativePartitionVersionEnabled()) {
        if(!LakeRowLocksEnabled()||!LakeAutoAnalyzeCatalogAvailable())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent SQL must inherit the complete prior control schema")));
        lockCount=lengthof(lockNames);
    }
    for(Size i=0;i<lockCount;++i) {
        Oid relid=get_relname_relid(lockNames[i],ns);
        if(!OidIsValid(relid) || getExtensionOfObject(RelationRelationId,relid)!=extension)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("native lake owner handover target is not extension-owned")));
        if(!ConditionalLockRelationOid(relid,AccessExclusiveLock))
            ereport(ERROR,(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                errmsg("native lake owner registration conflicts with active catalog users"),
                errhint("Roll back this transaction or savepoint and retry after active catalog users finish.")));
    }
    List* rowHeads=LakeRowLocksEnabled()?LakeRowLockHeadRelations(administrator):NIL;
    ListCell* headCell=NULL;
    foreach(headCell,rowHeads)
        if(!ConditionalLockRelationOid(lfirst_oid(headCell),AccessExclusiveLock))
            ereport(ERROR,(errcode(ERRCODE_LOCK_NOT_AVAILABLE),errmsg("native lake owner registration conflicts with active row-head users")));
    Relation control=heap_open(policy,NoLock);
    /* Registration mutex and all ownership locks are now held. Revalidate. */
    if(OidIsValid(LakeMetadataOwner()))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake internal owner was concurrently registered")));
    Relation bindings=heap_open(get_relname_relid("catalog_binding",ns),AccessShareLock);
    SysScanDesc scan=systable_beginscan(bindings,InvalidOid,false,SnapshotNow,0,NULL); HeapTuple row;
    AttrNumber schemaAtt=get_attnum(RelationGetRelid(bindings),"metadata_schema_oid");
    if(schemaAtt<=0) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake catalog binding has no metadata identity")));
    while(HeapTupleIsValid(row=systable_getnext(scan))) {
        bool isnull=false;
        Datum value=tableam_tops_tuple_getattr(row,schemaAtt,RelationGetDescr(bindings),&isnull);
        if(isnull) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake metadata namespace is null")));
        Oid metadataNamespace=DatumGetObjectId(value);
        LakeValidatePrivateNamespace(metadataNamespace,owner);
        LakeRegisterMetadataObjectsInternal(metadataNamespace,owner,administrator);
    }
    systable_endscan(scan); heap_close(bindings,NoLock);
    const char* names[]={"catalog_binding","table_binding","relation_binding","column_binding","metadata_relation",
        "index_definition","index_segment","index_coverage","row_lock_binding","analyze_state","partition_binding"};
    Size controlCount=(LakeRowLocksEnabled()?9:(LakeHasIndexCatalogVersion()?8:5))+(LakeAutoAnalyzeCatalogAvailable()?1:0);
    if(LakeNativePartitionVersionEnabled())controlCount=lengthof(names);
    for(Size i=0;i<controlCount;++i) {
        Oid relid=get_relname_relid(names[i],ns);
        if(!OidIsValid(relid) || getExtensionOfObject(RelationRelationId,relid)!=extension)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake binding relation is not extension-owned")));
        Relation rel=heap_open(relid,AccessExclusiveLock);
        LakeValidatePrivateRelation(rel,administrator,true); heap_close(rel,NoLock);
    }
    for(Size i=0;i<controlCount;++i)
        ATExecChangeOwner(get_relname_relid(names[i],ns),owner,false,AccessExclusiveLock);
    foreach(headCell,rowHeads) ATExecChangeOwner(lfirst_oid(headCell),owner,false,AccessExclusiveLock);
    list_free(rowHeads);
    AlterSchemaOwner_oid(ns,owner);
    CommandCounterIncrement();
    Datum values[2]={BoolGetDatum(true),ObjectIdGetDatum(owner)}; bool nulls[2]={false,false};
    HeapTuple tuple=heap_form_tuple(RelationGetDescr(control),values,nulls);
    simple_heap_insert(control,tuple); CatalogUpdateIndexes(control,tuple); heap_freetuple(tuple);
    CommandCounterIncrement(); heap_close(control,NoLock);
    LakeInvalidateFastValidationCache();
    (void)LakeMetadataOwner(); /* Validate the finished identity/namespace trust chain. */
    PG_RETURN_VOID();
}


PG_FUNCTION_INFO_V1(og_lake_index_catalog_upgrade);
PG_FUNCTION_INFO_V1(og_lake_index_catalog_validate);

void LakeValidateIndexControlConstraints(Relation relation,Oid owner)
{
    int kind=LakeControlNumber(RelationGetRelationName(relation));
    if(kind<6 || kind>8) LakeBadConstraint();
    LakeValidateControlConstraints(relation,owner);
}

void LakeIndexAdoptCatalogForUpgrade(void)
{
    LakeInlineRequireAdministrator();
    if(!creating_extension)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("index catalog handover is restricted to the extension script")));
    LakeTxnRejectBindingDdl();
    Oid extension=InvalidOid;bool supports=false;Oid administrator=LakeExtensionOwner(&extension,&supports);
    if(!supports) LakeBadConstraint();
    Oid ns=get_namespace_oid("og_lake_internal",false);
    if(getExtensionOfObject(NamespaceRelationId,ns)!=extension) LakeBadConstraint();
    Oid policy=get_relname_relid("access_policy",ns);
    if(!OidIsValid(policy) || getExtensionOfObject(RelationRelationId,policy)!=extension) LakeBadConstraint();
    Relation control=heap_open(policy,AccessShareLock);
    LakeValidatePrivateRelation(control,administrator,true);
    SysScanDesc scan=systable_beginscan(control,InvalidOid,false,SnapshotNow,0,NULL);
    Oid owner=InvalidOid;HeapTuple tuple;int count=0;
    while(HeapTupleIsValid(tuple=systable_getnext(scan))) {
        bool keyNull=false,ownerNull=false;
        Datum key=tableam_tops_tuple_getattr(tuple,1,RelationGetDescr(control),&keyNull);
        Datum value=tableam_tops_tuple_getattr(tuple,2,RelationGetDescr(control),&ownerNull);
        if(++count!=1 || keyNull || !DatumGetBool(key) || ownerNull || !OidIsValid(DatumGetObjectId(value))) LakeBadConstraint();
        owner=DatumGetObjectId(value);
    }
    systable_endscan(scan);heap_close(control,NoLock);
    if(OidIsValid(owner)) LakeCheckOwnerRole(owner);
    LakeValidatePrivateNamespace(ns,OidIsValid(owner)?owner:administrator);
    const char* names[]={"index_definition","index_segment","index_coverage"};
    for(Size i=0;i<lengthof(names);++i) {
        Oid oid=get_relname_relid(names[i],ns);
        if(!OidIsValid(oid) || getExtensionOfObject(RelationRelationId,oid)!=extension) LakeBadConstraint();
        if(!ConditionalLockRelationOid(oid,AccessExclusiveLock))
            ereport(ERROR,(errcode(ERRCODE_LOCK_NOT_AVAILABLE),errmsg("index catalog handover conflicts with active users")));
    }
    for(Size i=0;i<lengthof(names);++i) {
        Relation relation=heap_open(get_relname_relid(names[i],ns),NoLock);
        LakeValidatePrivateRelationImpl(relation,administrator,true,true);
        SysScanDesc rows=systable_beginscan(relation,InvalidOid,false,SnapshotNow,0,NULL);
        bool empty=!HeapTupleIsValid(systable_getnext(rows));systable_endscan(rows);
        if(!empty) ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("new index catalog must be empty before owner handover")));
        heap_close(relation,NoLock);
    }
    if(OidIsValid(owner)) {
        for(Size i=0;i<lengthof(names);++i) ATExecChangeOwner(get_relname_relid(names[i],ns),owner,false,AccessExclusiveLock);
        CommandCounterIncrement();
    }
}

Datum og_lake_index_catalog_upgrade(PG_FUNCTION_ARGS)
{
    LakeIndexAdoptCatalogForUpgrade();PG_RETURN_VOID();
}

void LakeAdoptRowLockCatalogForUpgrade(void)
{
    LakeInlineRequireAdministrator();
    if(!creating_extension || !LakeRowLocksEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("row-head catalog handover is restricted to the extension 0.17 script")));
    LakeTxnRejectBindingDdl();
    Oid extension=InvalidOid;bool supports=false;
    Oid administrator=LakeExtensionOwner(&extension,&supports);
    if(!supports) LakeBadConstraint();
    Oid ns=get_namespace_oid("og_lake_internal",false);
    if(getExtensionOfObject(NamespaceRelationId,ns)!=extension) LakeBadConstraint();
    Oid registration=get_relname_relid("catalog_binding",ns);
    if(!OidIsValid(registration) || getExtensionOfObject(RelationRelationId,registration)!=extension) LakeBadConstraint();
    /* Preserve registration -> policy -> new map order without calling the
     * normal owner resolver until this newly created table has been adopted. */
    LockRelationOid(registration,ShareRowExclusiveLock);
    Oid policy=get_relname_relid("access_policy",ns);
    if(!OidIsValid(policy) || getExtensionOfObject(RelationRelationId,policy)!=extension) LakeBadConstraint();
    Relation control=heap_open(policy,AccessShareLock);
    LakeValidatePrivateRelation(control,administrator,true);
    SysScanDesc scan=systable_beginscan(control,InvalidOid,false,SnapshotNow,0,NULL);
    Oid owner=InvalidOid;HeapTuple tuple;int count=0;
    while(HeapTupleIsValid(tuple=systable_getnext(scan))) {
        bool keyNull=false,ownerNull=false;
        Datum key=tableam_tops_tuple_getattr(tuple,1,RelationGetDescr(control),&keyNull);
        Datum value=tableam_tops_tuple_getattr(tuple,2,RelationGetDescr(control),&ownerNull);
        if(++count!=1 || keyNull || !DatumGetBool(key) || ownerNull || !OidIsValid(DatumGetObjectId(value))) LakeBadConstraint();
        owner=DatumGetObjectId(value);
    }
    systable_endscan(scan);heap_close(control,NoLock);
    if(OidIsValid(owner)) LakeCheckOwnerRole(owner);
    LakeValidatePrivateNamespace(ns,OidIsValid(owner)?owner:administrator);
    Oid map=get_relname_relid("row_lock_binding",ns);
    if(!OidIsValid(map) || getExtensionOfObject(RelationRelationId,map)!=extension) LakeBadConstraint();
    if(!ConditionalLockRelationOid(map,AccessExclusiveLock))
        ereport(ERROR,(errcode(ERRCODE_LOCK_NOT_AVAILABLE),errmsg("row-head catalog handover conflicts with active users")));
    Relation relation=heap_open(map,NoLock);
    LakeValidatePrivateRelation(relation,administrator,true);
    scan=systable_beginscan(relation,InvalidOid,false,SnapshotNow,0,NULL);
    bool empty=!HeapTupleIsValid(systable_getnext(scan));
    systable_endscan(scan);heap_close(relation,NoLock);
    if(!empty)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("new row-head catalog must be empty before owner handover")));
    if(OidIsValid(owner)) ATExecChangeOwner(map,owner,false,AccessExclusiveLock);
    CommandCounterIncrement();
}

Datum og_lake_index_catalog_validate(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();
    if(!creating_extension || !LakeHasIndexCatalogVersion())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("index catalog validation is restricted to its extension script")));
    Oid owner=LakeMetadataOwner();
    if(!OidIsValid(owner)) {Oid extension=InvalidOid;bool supports=false;owner=LakeExtensionOwner(&extension,&supports);}
    Oid ns=get_namespace_oid("og_lake_internal",false);const char* names[]={"index_definition","index_segment","index_coverage"};
    for(Size i=0;i<lengthof(names);++i) {
        Relation relation=heap_open(get_relname_relid(names[i],ns),AccessShareLock);
        LakeValidatePrivateRelation(relation,owner,true);heap_close(relation,NoLock);
    }
    PG_RETURN_VOID();
}
