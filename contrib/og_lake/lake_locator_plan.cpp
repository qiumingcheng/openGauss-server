/* Native DML locator marker, PHV placement and final executable-plan check. */
#include "postgres.h"
#include "access/heapam.h"
#include "knl/knl_variable.h"
#include "access/og_lake.h"
#include "lake_txn_private.h"
#include "lake_native_partition.h"
#include "catalog/dependency.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_language.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/acl.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/placeholder.h"
#include "optimizer/planner.h"
#include "optimizer/planmem_walker.h"
#include "optimizer/var.h"
#include "parser/parse_func.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

extern "C" Datum og_lake_row_locator(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_row_locator);
Datum og_lake_row_locator(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("native lake row locator marker must be supplied by a lake scan slot")));
    PG_RETURN_NULL();
}

Oid LakeLocatorMarkerOid(bool missingOk)
{
    Oid argument = ANYELEMENTOID;
    Oid namespaceOid=get_namespace_oid("og_lake_internal",missingOk);
    if(!OidIsValid(namespaceOid)) return InvalidOid;
    oidvector* arguments=buildoidvector(&argument,1);
    HeapTuple tuple=SearchSysCache3(PROCNAMEARGSNSP,CStringGetDatum("row_locator"),
        PointerGetDatum(arguments),ObjectIdGetDatum(namespaceOid));
    pfree(arguments);
    if(!HeapTupleIsValid(tuple) && missingOk) return InvalidOid;
    Oid marker=HeapTupleIsValid(tuple)?HeapTupleGetOid(tuple):InvalidOid;
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("native lake row locator marker disappeared")));
    Form_pg_proc function = (Form_pg_proc)GETSTRUCT(tuple);
    bool sourceNull = false, libraryNull = false;
    Datum source = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &sourceNull);
    Datum library = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_probin, &libraryNull);
    bool builtin = function->prolang == ClanguageId && superuser_arg(function->proowner) && !sourceNull && !libraryNull &&
        strcmp(TextDatumGetCString(source), "og_lake_row_locator") == 0 &&
        strcmp(TextDatumGetCString(library), "$libdir/og_lake") == 0;
    ReleaseSysCache(tuple);
    Oid extension = get_extension_oid("og_lake", false);
    if (!builtin || getExtensionOfObject(ProcedureRelationId, marker) != extension ||
        get_func_rettype(marker) != BYTEAOID || !func_strict(marker) || func_volatile(marker) != PROVOLATILE_STABLE)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("invalid native lake row locator marker binding")));
    return marker;
}

static bool LakeIsMarker(Node* node, Oid marker, Index target)
{
    if (node == NULL || !IsA(node, FuncExpr)) return false;
    FuncExpr* function = (FuncExpr*)node;
    if (function->funcid != marker || list_length(function->args) != 1) return false;
    Node* argument = (Node*)linitial(function->args);
    return IsA(argument, Var) && ((Var*)argument)->varno == target &&
        ((Var*)argument)->varattno == 0 && ((Var*)argument)->varlevelsup == 0;
}

bool LakeIsLocatorPlaceholder(Expr* expression, Index target)
{
    if (expression == NULL || !IsA(expression, PlaceHolderVar)) return false;
    PlaceHolderVar* phv = (PlaceHolderVar*)expression;
    if (phv->phlevelsup != 0 || !bms_is_member(target, phv->phrels) || bms_num_members(phv->phrels) != 1 ||
        !IsA(phv->phexpr, FuncExpr) || ((FuncExpr*)phv->phexpr)->funcresulttype != BYTEAOID) return false;
    /* Ordinary SELECT and installed 0.3 must not require the 0.4 marker. */
    Oid marker = LakeLocatorMarkerOid(true);
    return OidIsValid(marker) && LakeIsMarker((Node*)phv->phexpr, marker, target);
}

void LakePrepareDmlLocatorPlaceholders(PlannerInfo* root, List* tlist)
{
    Query* query = root->parse;
    if (query->commandType != CMD_UPDATE && query->commandType != CMD_DELETE) return;
    if (list_length(query->resultRelations) != 1) return;
    Index target = linitial_int(query->resultRelations);
    RangeTblEntry* rte = rt_fetch(target, query->rtable);
    if(rte->rtekind!=RTE_RELATION)return;
    bool nativeParent=false;
    if(get_rel_relkind(rte->relid)==RELKIND_RELATION) {
        Relation targetRelation=heap_open(rte->relid,AccessShareLock);
        nativeParent=LakeHasNativePartitionMarker(targetRelation);heap_close(targetRelation,NoLock);
    }
    if(nativeParent) {
        if(!LakeNativePartitionMutationVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent mutation locator is not activated")));
    } else {
        if(get_rel_relkind(rte->relid)!=RELKIND_FOREIGN_TABLE||!IsSpecifiedFDWFromRelid(rte->relid,"og_lake_fdw"))return;
        LakeRequireWriteVersion(query->commandType);
    }
    if (query->hasModifyingCTE || query->hasAggs || query->hasWindowFuncs || query->groupClause != NIL ||
        query->groupingSets != NIL || query->setOperations != NULL || query->rowMarks != NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported native lake modification query shape")));
    Oid marker = LakeLocatorMarkerOid(false);
    PlaceHolderVar* placeholder = NULL;
    ListCell* cell = NULL;
    foreach(cell, tlist) {
        TargetEntry* entry = (TargetEntry*)lfirst(cell);
        if (entry->resjunk && entry->resname != NULL && strcmp(entry->resname, "og_lake_row_locator") == 0) {
            if (LakeIsLocatorPlaceholder(entry->expr, target)) continue;
            if (!LakeIsMarker((Node*)entry->expr, marker, target))
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("invalid native lake locator target")));
            if (placeholder == NULL)
                placeholder = make_placeholder_expr(root, entry->expr, bms_make_singleton(target));
            entry->expr = (Expr*)copyObject(placeholder);
        }
    }
}

struct LakeLocatorPlanCheck { MethodPlanWalkerContext walker; Oid marker; };
static bool LakeCheckPlanNode(Node* node, void* argument)
{
    if (node == NULL) return false;
    LakeLocatorPlanCheck* context = (LakeLocatorPlanCheck*)argument;
    if (IsA(node, BaseResult) && outerPlan((Plan*)node) == NULL && innerPlan((Plan*)node) == NULL &&
        is_dummy_plan((Plan*)node)) {
        /* Empty Append paths become a leaf Result gated by constant FALSE,
         * without a ForeignScan that could supply the locator. ExecResult
         * checks that gate before its projection. Preserve the descriptor,
         * but do not leave a non-executable marker in the dummy targetlist.
         * All other fields, including initplans, are still checked below. */
        ListCell* cell = NULL;
        foreach(cell, ((Plan*)node)->targetlist) {
            TargetEntry* entry = (TargetEntry*)lfirst(cell);
            if (IsA(entry->expr, FuncExpr) && ((FuncExpr*)entry->expr)->funcid == context->marker)
                entry->expr = (Expr*)makeNullConst(BYTEAOID, -1, InvalidOid);
        }
    }
    if (IsA(node, FuncExpr) && ((FuncExpr*)node)->funcid == context->marker)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake locator was not rewritten to a scan slot")));
    if (IsA(node, Stream) || IsA(node, VecStream) || IsA(node, RemoteQuery) || IsA(node, VecRemoteQuery) ||
        IsA(node, VecForeignScan) || IsA(node, VecModifyTable) || IsA(node, RowToVec) || IsA(node, VecToRow))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake modification requires a local row execution plan")));
    if (IsA(node, ForeignScan)) {
        ForeignScan* scan = (ForeignScan*)node;
        (void)LakeCheckPlanNode((Node*)scan->fdw_recheck_quals, argument);
        /* fdw_scan_tlist describes physical slots; its marker is not evaluated. */
    }
    if (IsA(node, ModifyTable)) {
        ModifyTable* modify = (ModifyTable*)node;
        (void)LakeCheckPlanNode((Node*)modify->returningLists, argument);
        (void)LakeCheckPlanNode((Node*)modify->withCheckOptionLists, argument);
    }
    return plan_tree_walker(node, (MethodWalker)LakeCheckPlanNode, argument);
}

void LakeValidateDmlLocatorPlan(PlannedStmt* plan)
{
    /* A SELECT/INSERT parent can contain a modifying CTE whose child Query
     * has no hasModifyingCTE flag of its own. Inspect its final target groups
     * too; reject only if one of those actual targets is a lake table. */
    if (plan->commandType != CMD_UPDATE && plan->commandType != CMD_DELETE && !plan->hasModifyingCTE) return;
    bool lake = false;
    int targetCount = 0;
    ListCell* cell = NULL;
    ListCell* group = NULL;
    /* This host stores PlannedStmt targets as List<IntList>, unlike Query's
     * flat IntList. Mirror execMain's initialization of result relations;
     * interpreting the group pointer as an RT index crashes ordinary DML. */
    if (plan->resultRelations != NIL && !IsA(plan->resultRelations, List))
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("invalid planned result-relation groups")));
    foreach(group, plan->resultRelations) {
        List* targets = (List*)lfirst(group);
        if (targets != NIL && !IsA(targets, IntList))
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("invalid planned result-relation indexes")));
        foreach(cell, targets) {
            int target = lfirst_int(cell);
            if (target <= 0 || target > list_length(plan->rtable))
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("planned result relation is outside the range table")));
            ++targetCount;
            RangeTblEntry* rte = rt_fetch(target, plan->rtable);
            if (rte->rtekind == RTE_RELATION && get_rel_relkind(rte->relid) == RELKIND_FOREIGN_TABLE &&
                IsSpecifiedFDWFromRelid(rte->relid, "og_lake_fdw")) lake = true;
            if(rte->rtekind==RTE_RELATION&&get_rel_relkind(rte->relid)==RELKIND_RELATION) {
                Relation parent=heap_open(rte->relid,AccessShareLock);bool native=LakeHasNativePartitionMarker(parent);heap_close(parent,NoLock);
                if(native) {
                    if(!LakeNativePartitionMutationVersionEnabled())
                        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent mutation plan is not activated")));
                    lake=true;
                }
            }
        }
    }
    if (!lake) return;
    if (targetCount != 1 || plan->hasModifyingCTE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake modification supports one target and no modifying CTE")));
    LakeLocatorPlanCheck context;
    errno_t rc = memset_s(&context, sizeof(context), 0, sizeof(context));
    securec_check(rc, "", "");
    exec_init_plan_tree_base(&context.walker.base, plan);
    context.marker = LakeLocatorMarkerOid(false);
    (void)LakeCheckPlanNode((Node*)plan->planTree, &context);
    foreach(cell, plan->subplans) (void)LakeCheckPlanNode((Node*)lfirst(cell), &context);
    list_free(context.walker.base.init_plans);
    if (context.walker.base.traverse_flag != NULL) pfree(context.walker.base.traverse_flag);
}
