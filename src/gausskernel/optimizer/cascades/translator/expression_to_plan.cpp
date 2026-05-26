#include "expression_to_plan.h"
#include "../memo/opt_expression.h"
#include "../operator/operators.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "nodes/plannodes.h"
#include "nodes/value.h"
#include "lib/stringinfo.h"
#include "parser/parsetree.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "utils/palloc.h"
#include "utils/lsyscache.h"
#include <stddef.h>

namespace cascades {

/*
 * Assign plan_node_id to each node in the plan tree.
 * This is needed by the executor to identify plan nodes.
 */
static void assign_plan_node_ids(Plan* plan, int* next_id)
{
    if (plan == nullptr) {
        return;
    }

    plan->plan_node_id = (*next_id)++;

    /* Recurse into children */
    if (plan->lefttree != nullptr) {
        assign_plan_node_ids(plan->lefttree, next_id);
    }
    if (plan->righttree != nullptr) {
        assign_plan_node_ids(plan->righttree, next_id);
    }

    /* Note: initPlan handling omitted for simplicity - Cascades doesn't generate initPlans yet */
}

ExprToPlanConverter::ExprToPlanConverter() : root_(nullptr) {}

ExprToPlanConverter::ExprToPlanConverter(PlannerInfo* root) : root_(root) {}

PlannedStmt* ExprToPlanConverter::convert(OptExpression* expr) {
    if (expr == nullptr) {
        return nullptr;
    }

    ereport(WARNING, (errmsg("[CASCADES] ExprToPlanConverter::convert: expr=%s", expr->to_string().c_str())));

    Plan* plan = translate_expression(expr);
    if (plan == nullptr) {
        return nullptr;
    }

    ereport(WARNING, (errmsg("[CASCADES] ExprToPlanConverter::convert: plan created, type=%d", (int)plan->type)));

    /* Assign plan_node_id to each node in the plan tree */
    int next_id = 1;
    assign_plan_node_ids(plan, &next_id);

    /*
     * NOTE: Var reference conversion is NOT done here.
     * The caller should call set_plan_references() after getting the plan,
     * which properly handles Var reference conversion, RTE offset adjustment,
     * and opfuncid computation. This matches the old optimizer's approach:
     *   create_plan() → set_plan_references()
     */

    PlannedStmt* result = makeNode(PlannedStmt);
    result->commandType = CMD_SELECT;
    result->queryId = 0;
    result->uniqueSQLId = 0;
    result->hasReturning = false;
    result->hasModifyingCTE = false;
    result->canSetTag = true;
    result->transientPlan = false;
    result->dependsOnRole = false;
    result->planTree = plan;

    if (root_ != nullptr && root_->glob != nullptr) {
        result->rtable = root_->glob->finalrtable;
    } else {
        result->rtable = NIL;
    }

    result->resultRelations = NIL;
    result->utilityStmt = nullptr;
    result->subplans = NIL;
    result->rewindPlanIDs = nullptr;
    result->rowMarks = NIL;
    result->relationOids = NIL;
    result->invalItems = NIL;
    result->nParamExec = (root_ != nullptr && root_->glob != nullptr) ? root_->glob->nParamExec : 0;
    result->noanalyze_rellist = NIL;
    result->hasIgnore = false;
    result->num_nodes = 0;
    result->num_streams = 0;
    result->max_push_sql_num = 0;
    result->gather_count = 0;
    result->num_plannodes = 0;
    result->query_string = nullptr;
    result->nodesDefinition = nullptr;

    return result;
}

/*
 * Translate an optimized expression tree to a plan tree.
 * After Cascades optimization, the expression tree should contain only physical operators.
 * Logical operators should have been transformed to physical operators during optimization.
 */
Plan* ExprToPlanConverter::translate_expression(OptExpression* expr) {
    if (expr == nullptr) {
        return nullptr;
    }

    OptExprOp* op = expr->get_op();

    // Should only receive physical operators from Cascades optimizer
    if (!op->is_physical()) {
        ereport(WARNING, (errmsg("[CASCADES] translate_expression: unexpected logical op type=%d",
                             (int)op->get_type())));
        assert(false);
        return nullptr;
    }

    PhysicalOptExprOp* phys_op = static_cast<PhysicalOptExprOp*>(op);

    // Handle PhysicalSubqueryScan specially
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_SUBQUERY_SCAN) {
        return translate_subquery_scan(expr);
    }

    // Handle PhysicalLimit specially (it has no Path)
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_LIMIT) {
        return translate_limit(expr);
    }

    // Handle PhysicalProject specially (it has no Path, uses target list)
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_PROJECT) {
        return translate_project(expr);
    }

    // Handle PhysicalScan specially to preserve column pruning
    // create_plan() uses build_physical_tlist() which reads all table columns,
    // ignoring path->pathtarget. We build the scan node directly here.
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_SEQ_SCAN ||
        phys_op->get_type() == OptExprOpType::PHYSICAL_INDEX_SCAN) {
        return translate_scan(expr);
    }

    // Handle PhysicalFilter - create Result node with qual
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_FILTER) {
        return translate_filter(expr);
    }

    // Handle PhysicalHashAgg - create Agg node
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_HASH_AGG) {
        return translate_hash_agg(expr);
    }

    // Handle PhysicalSort - create Sort node
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_SORT) {
        return translate_sort(expr);
    }

    // Handle PhysicalNestLoop - may have placeholder path (nullptr)
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_NESTLOOP_JOIN) {
        return translate_nestloop_join(expr);
    }

    // Handle PhysicalHashJoin - may have placeholder path (nullptr)
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_HASH_JOIN) {
        return translate_hash_join(expr);
    }

    // Handle PhysicalMergeJoin - may have placeholder path (nullptr)
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_MERGE_JOIN) {
        return translate_merge_join(expr);
    }

    // Handle PhysicalPassthrough - no physical node, just pass through child's plan
    // PassThrough is used by SortImplRule to represent "sort elimination via index"
    // The child must already produce sorted output, so no additional node needed
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_PASSTHROUGH) {
        if (expr->arity() >= 1) {
            return translate_expression(expr->input_at(0));
        }
        return nullptr;
    }

    // Handle other physical operators with Path
    Path* path = phys_op->get_path();
    if (path != nullptr && root_ != nullptr) {
        return create_plan(root_, path);
    }

    // Fallback: recursively translate child
    if (expr->arity() >= 1) {
        return translate_expression(expr->input_at(0));
    }

    return nullptr;
}

/*
 * Translate PhysicalScan to scan plan node.
 *
 * We build the target list from path->pathtarget to preserve column pruning
 * from Cascades optimizer. This is different from create_plan() which calls
 * build_physical_tlist() and reads all table columns.
 *
 * 根据 Path 类型创建相应的执行计划节点：
 * - IndexPath → IndexScan
 * - BitmapHeapPath → BitmapHeapScan
 * - 其他 Path → SeqScan
 */
Plan* ExprToPlanConverter::translate_scan(OptExpression* expr) {
    if (expr == nullptr || root_ == nullptr) {
        return nullptr;
    }

    PhysicalOptExprOp* scan_op = dynamic_cast<PhysicalOptExprOp*>(expr->get_op());
    if (scan_op == nullptr) {
        return nullptr;
    }

    // Check if it's PhysicalSeqScan or PhysicalIndexScan
    Path* path = scan_op->get_path();
    if (path == nullptr) {
        return nullptr;
    }

    RelOptInfo* rel = path->parent;
    if (rel == nullptr) {
        return nullptr;
    }

    /* Build target list from path->pathtarget to preserve column pruning */
    List* tlist = NIL;
    if (path->pathtarget != nullptr && path->pathtarget->exprs != NIL) {
        int resno = 1;
        int idx = 0;
        ListCell* lc;
        foreach(lc, path->pathtarget->exprs) {
            Var* var = (Var*)lfirst(lc);
            // Keep original var varno, varattno from the path target
            // The resno in TargetEntry represents position in output
            Var* new_var = (Var*)copyObject(var);
            TargetEntry* tle = makeTargetEntry((Expr*)new_var, resno, NULL, false);

            // Try to get sortgroupref from path->pathtarget first
            Index sortgroupref = get_pathtarget_sortgroupref(path->pathtarget, idx);

            // If pathtarget doesn't have sortgroupref, try to find it from root_->parse->targetList
            if (sortgroupref == 0 && root_ != nullptr && root_->parse != nullptr) {
                ListCell* tl_cell;
                foreach(tl_cell, root_->parse->targetList) {
                    TargetEntry* parse_tle = (TargetEntry*)lfirst(tl_cell);
                    if (parse_tle->expr != nullptr && equal(parse_tle->expr, (Node*)var)) {
                        sortgroupref = parse_tle->ressortgroupref;
                        break;
                    }
                }
            }

            tle->ressortgroupref = sortgroupref;
            tlist = lappend(tlist, tle);
            resno++;
            idx++;
        }
    }

    Plan* plan = nullptr;
    /* 根据 Path 类型创建相应的 Scan 节点 */
    if (IsA(path, IndexPath)) {
        plan = translate_index_scan((IndexPath*)path, rel, tlist);
    } else if (IsA(path, BitmapHeapPath)) {
        plan = translate_bitmap_scan((BitmapHeapPath*)path, rel, tlist);
    } else {
        plan = translate_seq_scan(rel, tlist);
    }

    /* Copy cost estimates from path */
    plan->startup_cost = path->startup_cost;
    plan->total_cost = path->total_cost;
    plan->plan_rows = path->rows;
    plan->plan_width = path->pathtarget ? path->pathtarget->width : 0;

    /* Attach restriction clauses from rel->baserestrictinfo */
    if (rel->baserestrictinfo != NIL) {
        List* qual_list = NIL;
        ListCell* lc;
        foreach(lc, rel->baserestrictinfo) {
            RestrictInfo* ri = (RestrictInfo*)lfirst(lc);
            if (ri != nullptr && ri->clause != nullptr) {
                qual_list = lappend(qual_list, ri->clause);
            }
        }
        plan->qual = qual_list;
    }

    return plan;
}

/*
 * 初始化 Scan 节点的基本字段
 * SeqScan is typedef for Scan, so we use Scan* for all scan types
 */
void ExprToPlanConverter::init_scan_plan(Scan* scan_plan, RelOptInfo* rel, List* tlist) {
    Plan* plan = &scan_plan->plan;
    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->isDeltaTable = false;

    scan_plan->scanrelid = rel->relid;
    scan_plan->scanBatchMode = false;
    scan_plan->isPartTbl = false;
    scan_plan->itrs = 0;
    scan_plan->pruningInfo = nullptr;
    scan_plan->bucketInfo = nullptr;
    scan_plan->partScanDirection = ForwardScanDirection;
    scan_plan->tablesample = nullptr;
}

Plan* ExprToPlanConverter::translate_index_scan(IndexPath* index_path, RelOptInfo* rel, List* tlist) {
    if (index_path->path.pathtype == T_IndexOnlyScan) {
        IndexOnlyScan* index_only_scan = makeNode(IndexOnlyScan);
        Scan* scan = &index_only_scan->scan;

        init_scan_plan(scan, rel, tlist);

        index_only_scan->indexid = index_path->indexinfo->indexoid;
        index_only_scan->indexqual = fix_indexqual_references(root_, index_path, nullptr);
        index_only_scan->indexqualorig = get_actual_clauses(index_path->indexquals);
        index_only_scan->indexorderby = fix_indexorderby_references(root_, index_path);
        index_only_scan->indexorderdir = index_path->indexscandir;
        index_only_scan->selectivity = index_path->indexselectivity;
        index_only_scan->is_partial = (index_path->indexinfo->indpred != NIL);

        /* Build indextlist from index columns */
        List* indextlist = NIL;
        IndexOptInfo* indexinfo = index_path->indexinfo;
        List* indexprs = indexinfo->indexprs;
        ListCell* lc_expr = list_head(indexprs);
        for (int i = 0; i < indexinfo->ncolumns; i++) {
            if (indexinfo->indexkeys[i] != 0) {
                /* Simple column reference - create Var referencing the base table column */
                Var* var = (Var*)makeNode(Var);
                var->varno = rel->relid;
                var->varattno = indexinfo->indexkeys[i];
                var->vartype = indexinfo->opcintype[i];
                var->vartypmod = -1;
                var->varcollid = indexinfo->indexcollations[i];
                var->varlevelsup = 0;
                var->varnoold = rel->relid;
                var->varoattno = indexinfo->indexkeys[i];
                TargetEntry* tle = makeTargetEntry((Expr*)var, i + 1, NULL, false);
                indextlist = lappend(indextlist, tle);
            } else {
                /* Expression column - get from indexprs */
                if (lc_expr == NULL) {
                    continue;
                }
                Node* expr = (Node*)lfirst(lc_expr);
                lc_expr = lnext(lc_expr);
                TargetEntry* tle = makeTargetEntry((Expr*)copyObject(expr), i + 1, NULL, false);
                indextlist = lappend(indextlist, tle);
            }
        }
        index_only_scan->indextlist = indextlist;

        ereport(WARNING, (errmsg("[CASCADES] translate_scan: created IndexOnlyScan, indexid=%u, indexqual=%d",
                                 index_only_scan->indexid, list_length(index_only_scan->indexqual))));

        return &scan->plan;
    } else {
        IndexScan* index_scan = makeNode(IndexScan);
        Scan* scan = &index_scan->scan;

        init_scan_plan(scan, rel, tlist);

        index_scan->indexid = index_path->indexinfo->indexoid;
        index_scan->indexname = (char*)get_rel_name(index_scan->indexid);
        index_scan->indexqual = fix_indexqual_references(root_, index_path, nullptr);
        index_scan->indexqualorig = get_actual_clauses(index_path->indexquals);
        index_scan->indexorderby = fix_indexorderby_references(root_, index_path);
        index_scan->indexorderbyorig = get_actual_clauses(index_path->indexorderbys);
        index_scan->indexorderbyops = nullptr;
        index_scan->indexorderdir = index_path->indexscandir;
        index_scan->selectivity = index_path->indexselectivity;
        index_scan->is_partial = (index_path->indexinfo->indpred != NIL);
        index_scan->is_ustore = index_path->is_ustore;

        ereport(WARNING, (errmsg("[CASCADES] translate_scan: created IndexScan, indexid=%u, indexqual=%d",
                                 index_scan->indexid, list_length(index_scan->indexqual))));

        return &scan->plan;
    }
}

Plan* ExprToPlanConverter::translate_bitmap_scan(BitmapHeapPath* bitmap_path, RelOptInfo* rel, List* tlist) {
    BitmapHeapScan* bitmap_scan = makeNode(BitmapHeapScan);
    Scan* scan = &bitmap_scan->scan;

    init_scan_plan(scan, rel, tlist);

    /* Extract bitmapqualorig from bitmap_path->bitmapqual (which is a Path*, not a List*).
     * bitmapqual can be IndexPath, BitmapAndPath, or BitmapOrPath.
     * For a simple IndexPath, extract actual clauses and create BitmapIndexScan child. */
    Path* bitmapqual = bitmap_path->bitmapqual;
    if (bitmapqual != nullptr && IsA(bitmapqual, IndexPath)) {
        IndexPath* ipath = (IndexPath*)bitmapqual;

        /* Build BitmapIndexScan as child node (matching old optimizer: create_bitmap_subplan) */
        BitmapIndexScan* bitmap_index_scan = makeNode(BitmapIndexScan);
        Plan* bindex_plan = &bitmap_index_scan->scan.plan;
        bindex_plan->targetlist = NIL;
        bindex_plan->qual = NIL;
        bindex_plan->lefttree = nullptr;
        bindex_plan->righttree = nullptr;
        bindex_plan->startup_cost = 0.0;
        bindex_plan->total_cost = ipath->indextotalcost;
        bindex_plan->plan_rows = clamp_row_est(ipath->indexselectivity * ipath->path.parent->tuples);
        bindex_plan->plan_width = 0;
        bitmap_index_scan->scan.scanrelid = ipath->indexinfo->rel->relid;
        bitmap_index_scan->indexid = ipath->indexinfo->indexoid;
        bitmap_index_scan->indexqual = fix_indexqual_references(root_, ipath, nullptr);
        bitmap_index_scan->indexqualorig = get_actual_clauses(ipath->indexquals);
        bitmap_index_scan->selectivity = ipath->indexselectivity;
        bitmap_index_scan->is_partial = (ipath->indexinfo->indpred != NIL);
        bitmap_index_scan->is_ustore = ipath->is_ustore;
        bitmap_index_scan->scan.bucketInfo = bitmapqual->parent->bucketInfo;
        bitmap_index_scan->scan.isPartTbl = false;
        bitmap_index_scan->scan.itrs = 0;
        bitmap_index_scan->scan.pruningInfo = nullptr;

        scan->plan.lefttree = bindex_plan;

        bitmap_scan->bitmapqualorig = get_actual_clauses(ipath->indexclauses);
    } else {
        /* For nested bitmap quals (BitmapAndPath/BitmapOrPath), we fall back to NIL
         * since Cascades does not generate complex bitmap plans yet. */
        bitmap_scan->bitmapqualorig = NIL;
    }

    ereport(WARNING, (errmsg("[CASCADES] translate_scan: created BitmapHeapScan")));

    return &scan->plan;
}

Plan* ExprToPlanConverter::translate_seq_scan(RelOptInfo* rel, List* tlist) {
    SeqScan* seq_scan = makeNode(SeqScan);

    init_scan_plan(seq_scan, rel, tlist);

    ereport(WARNING, (errmsg("[CASCADES] translate_scan: created SeqScan")));

    return &seq_scan->plan;
}

/* Forward declarations for static helper functions */
static bool is_projection_capable_plan_node(Plan* plan);
static List* build_tlist_from_tlist(List* target_list);

/*
 * translate_project - Translate Project expression to plan node
 *
 * Following create_projection_plan pattern from old optimizer:
 * 1. Build tlist using build_path_tlist() for PhysicalProject (preserves full expressions)
 *    or build_tlist_from_tlist() for LogicalProject
 * 2. Check if child plan can project directly (is_projection_capable_plan_node)
 *    or if tlist matches child's targetlist (tlist_same_exprs)
 * 3. If either condition is true, assign tlist to child plan directly (no Result node)
 * 4. Otherwise, create BaseResult/Result node with the tlist
 *
 * This matches the old optimizer's approach and properly handles expression
 * comparison in set_plan_references via tlist_same_exprs.
 */
Plan* ExprToPlanConverter::translate_project(OptExpression* expr) {
    if (expr->arity() < 1) {
        return nullptr;
    }

    /* Build child plan first */
    Plan* child_plan = translate_expression(expr->input_at(0));
    if (child_plan == nullptr) {
        return nullptr;
    }

    List* tlist = NIL;

    /*
     * Get target list based on operator type.
     * PhysicalProject and LogicalProject both store tlist directly,
     * so we always use build_tlist_from_tlist() to build the plan tlist.
     * This matches the old optimizer's approach for projection plans.
     */
    PhysicalProject* physical_proj = dynamic_cast<PhysicalProject*>(expr->get_op());
    Assert(physical_proj != nullptr);
    tlist = build_tlist_from_tlist(physical_proj->get_target_list());

    /* For SELECT * (tlist is NIL), no projection needed */
    if (tlist == NIL) {
        return child_plan;
    }

    /*
     * Projection elimination check (matching create_projection_plan):
     *
     * 1. is_projection_capable_plan: check if child plan type can project
     *    - SeqScan, IndexScan, etc. can project
     *    - Sort, Material, Hash, etc. cannot project
     *
     * 2. tlist_same_exprs: check if our tlist matches child's targetlist exactly
     *    - If so, we can just assign tlist to child without creating Result
     */
    bool can_project_directly = is_projection_capable_plan_node(child_plan) ||
                                 tlist_same_exprs(tlist, child_plan->targetlist);

    if (can_project_directly) {
        /*
         * Child plan can project directly - just assign tlist to child.
         * This eliminates the need for a separate Result node.
         */
        child_plan->targetlist = tlist;
        return child_plan;
    }

    /*
     * Child cannot project directly - create BaseResult/Result node.
     * The tlist will be processed by set_upper_references() which uses
     * search_indexed_tlist_for_var() to match Vars with child's indexed tlist.
     */
#ifdef USE_SPQ
    Result* result_plan = makeNode(Result);
    result_plan->numHashFilterCols = 0;
    result_plan->hashFilterColIdx = nullptr;
    result_plan->hashFilterFuncs = nullptr;
#else
    BaseResult* result_plan = makeNode(BaseResult);
#endif

    result_plan->plan.lefttree = child_plan;
    result_plan->plan.qual = NIL;
    result_plan->resconstantqual = nullptr;
    result_plan->plan.targetlist = tlist;
    return (Plan*)result_plan;
}

/*
 * is_projection_capable_plan_node - Check if a plan node can do projection
 *
 * Similar to is_projection_capable_path() but operates on Plan nodes
 * instead of Path nodes. Most plan types can project; only specific
 * types cannot.
 */
static bool is_projection_capable_plan_node(Plan* plan)
{
    if (plan == nullptr) {
        return false;
    }

    switch (plan->type) {
        case T_Hash:
        case T_Material:
        case T_Sort:
        case T_SortGroup:
        case T_Unique:
        case T_SetOp:
        case T_LockRows:
        case T_Limit:
        case T_ModifyTable:
        case T_MergeAppend:
        case T_RecursiveUnion:
            return false;
        case T_ProjectSet:
            /* ProjectSet can't have its tlist replaced arbitrarily */
            return false;
        default:
            break;
    }
    return true;
}

/*
 * build_tlist_from_tlist - Build plan target list from LogicalProject's target_list
 *
 * For LogicalProject, we build TargetEntry list preserving the expressions
 * from the original target_list. This is used when there's no path->pathtarget
 * (i.e., for LogicalProject which doesn't have an associated Path).
 */
static List* build_tlist_from_tlist(List* target_list)
{
    List* tlist = NIL;
    int resno = 1;
    ListCell* lc;

    foreach (lc, target_list) {
        TargetEntry* src_tle = (TargetEntry*)lfirst(lc);
        TargetEntry* new_tle = makeTargetEntry((Expr*)copyObject(src_tle->expr),
                                              resno++, NULL, false);
        new_tle->ressortgroupref = src_tle->ressortgroupref;
        new_tle->resname = src_tle->resname;
        new_tle->resorigtbl = src_tle->resorigtbl;
        new_tle->resorigcol = src_tle->resorigcol;
        tlist = lappend(tlist, new_tle);
    }

    return tlist;
}

/*
 * Translate PhysicalFilter to Result node with qual.
 * PhysicalFilter represents a runtime filter operation in the plan.
 */
Plan* ExprToPlanConverter::translate_filter(OptExpression* expr) {
    if (expr->arity() < 1) {
        return nullptr;
    }

    Plan* child_plan = translate_expression(expr->input_at(0));
    if (child_plan == nullptr) {
        return nullptr;
    }

    PhysicalFilter* filter_op = dynamic_cast<PhysicalFilter*>(expr->get_op());
    List* qual_list = filter_op ? (List*)filter_op->get_qual_list() : nullptr;

    // Build qual from qual_list (List* of RestrictInfo)
    List* qual = NIL;
    if (qual_list != nullptr) {
        // Extract clauses from RestrictInfo list
        ListCell* lc;
        foreach(lc, qual_list) {
            RestrictInfo* ri = (RestrictInfo*)lfirst(lc);
            if (ri != nullptr && ri->clause != nullptr) {
                qual = lappend(qual, ri->clause);
            }
        }
    }

#ifdef USE_SPQ
    Result* result_plan = makeNode(Result);
    result_plan->numHashFilterCols = 0;
    result_plan->hashFilterColIdx = nullptr;
    result_plan->hashFilterFuncs = nullptr;
#else
    BaseResult* result_plan = makeNode(BaseResult);
#endif
    result_plan->plan.lefttree = child_plan;
    result_plan->plan.targetlist = (List*)copyObject(child_plan->targetlist);
    result_plan->plan.qual = qual;
    result_plan->resconstantqual = nullptr;

    return (Plan*)result_plan;
}

/* Stub join translation - should be implemented with proper physical join operators */
Plan* ExprToPlanConverter::translate_join(OptExpression* expr) {
    return translate_expression(expr);
}

Plan* ExprToPlanConverter::translate_hash_join(OptExpression* expr) {
    PhysicalHashJoin* hashjoin = dynamic_cast<PhysicalHashJoin*>(expr->get_op());
    if (hashjoin == nullptr) {
        return translate_expression(expr);
    }

    ereport(WARNING, (errmsg("[CASCADES] translate_hash_join: created hashjoin plan")));
    ereport(WARNING, (errmsg("[CASCADES] translate_hash_join: qual nodeTag=%d",
         hashjoin->get_qual() == nullptr ? 0 : (int)nodeTag(hashjoin->get_qual()))));
    if (expr->arity() < 2) {
        ereport(WARNING, (errmsg("[CASCADES] translate_hash_join: insufficient children")));
        return nullptr;
    }

    // Get paths from child operators
    PhysicalOptExprOp* left_op = static_cast<PhysicalOptExprOp*>(expr->input_at(0)->get_op());
    PhysicalOptExprOp* right_op = static_cast<PhysicalOptExprOp*>(expr->input_at(1)->get_op());
    Path* left_path = left_op->get_path();
    Path* right_path = right_op->get_path();
    if (left_path == nullptr || right_path == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_hash_join: child path is null")));
        return nullptr;
    }

    // Translate children
    Plan* left_plan = translate_expression(expr->input_at(0));
    if (left_plan == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_hash_join: left plan is null")));
        return nullptr;
    }

    Plan* right_plan = translate_expression(expr->input_at(1));
    if (right_plan == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_hash_join: right plan is null")));
        return nullptr;
    }

    JoinType jointype = hashjoin->get_join_type();

    // Build HashPath for cost/size computation
    JoinPath hash_path_node;
    MemSet(&hash_path_node, 0, sizeof(JoinPath));
    JoinPath* hp = &hash_path_node;
    hp->path.pathtype = T_HashJoin;
    hp->path.parent = left_path->parent;

    // Build join pathtarget from both sides
    PathTarget* join_pathtarget = create_empty_pathtarget();
    join_pathtarget->exprs = (List*)copyObject(left_path->pathtarget->exprs);
    join_pathtarget->cost.startup = 0;
    join_pathtarget->cost.per_tuple = 0;
    join_pathtarget->width = left_path->pathtarget->width + right_path->pathtarget->width;
    join_pathtarget->sortgrouprefs = nullptr;

    // Add inner side expressions not in outer
    ListCell* lc;
    foreach(lc, right_path->pathtarget->exprs) {
        Node* inner_expr = (Node*)lfirst(lc);
        bool found = false;
        ListCell* lc2;
        foreach(lc2, join_pathtarget->exprs) {
            if (equal(inner_expr, (Node*)lfirst(lc2))) {
                found = true;
                break;
            }
        }
        if (!found) {
            join_pathtarget->exprs = lappend(join_pathtarget->exprs, copyObject(inner_expr));
        }
    }

    hp->path.pathtarget = join_pathtarget;
    hp->path.pathkeys = NIL;
    hp->path.param_info = nullptr;
    hp->path.rows = 0;
    hp->path.startup_cost = 0;
    hp->path.total_cost = 0;
    hp->path.dop = 0;
    hp->jointype = jointype;
    hp->inner_unique = false;
    hp->outerjoinpath = left_path;
    hp->innerjoinpath = right_path;
    hp->joinrestrictinfo = NIL;
    hp->skewoptimize = 0;

    // Compute join cost
    JoinCostWorkspace workspace;
    initial_cost_hashjoin(root_, &workspace, jointype, NIL, left_path, right_path, nullptr, 1);

    // Build tlist
    List* tlist = build_path_tlist(root_, &hp->path);

    // Build hashclauses from join qual (matching old optimizer: get_actual_clauses)
    List* hashclauses = NIL;
    Node* qual = hashjoin->get_qual();
    if (qual != nullptr) {
        if (IsA(qual, List)) {
            ListCell* lc3;
            foreach(lc3, (List*)qual) {
                hashclauses = lappend(hashclauses, copyObject(lfirst(lc3)));
            }
        } else if (IsA(qual, OpExpr)) {
            hashclauses = list_make1(copyObject(qual));
        } else if (IsA(qual, BoolExpr)) {
            BoolExpr* boolExpr = (BoolExpr*)qual;
            if (boolExpr->boolop == AND_EXPR) {
                ListCell* lc3;
                foreach(lc3, boolExpr->args) {
                    hashclauses = lappend(hashclauses, copyObject(lfirst(lc3)));
                }
            } else {
                hashclauses = list_make1(copyObject(qual));
            }
        }
    }
    ereport(WARNING, (errmsg("[CASCADES] translate_hash_join: hashclauses=%d items",
         list_length(hashclauses))));

    // Ensure OUTER var is on the left side of each hashclause OpExpr
    // (matching old optimizer: get_switched_clauses)
    // ExecInitHashJoin reads linitial(args) as OUTER hashkey and lsecond(args) as INNER hashkey.
    {
        Relids outerrelids = left_path->parent->relids;
        ListCell* lc3;
        foreach(lc3, hashclauses) {
            OpExpr* hclause = (OpExpr*)lfirst(lc3);
            if (IsA(hclause, OpExpr) && list_length(hclause->args) == 2) {
                Node* left_arg = (Node*)linitial(hclause->args);
                Node* right_arg = (Node*)lsecond(hclause->args);
                bool left_is_outer = false;
                if (IsA(left_arg, Var)) {
                    left_is_outer = bms_is_member(((Var*)left_arg)->varno, outerrelids);
                }
                if (!left_is_outer && IsA(right_arg, Var)) {
                    // RIGHT side is outer → need to swap args (commute)
                    OpExpr* temp = (OpExpr*)copyObject(hclause);
                    CommuteOpExpr(temp);
                    lfirst(lc3) = temp;
                }
            }
        }
    }

    // Create Hash child node wrapping inner plan (matching old optimizer: make_hash)
    Hash* hash_plan = makeNode(Hash);
    copy_plan_costsize(&hash_plan->plan, right_plan);
    hash_plan->plan.startup_cost = hash_plan->plan.total_cost;
    hash_plan->plan.targetlist = right_plan->targetlist;
    hash_plan->plan.qual = NIL;
    hash_plan->plan.lefttree = right_plan;
    hash_plan->plan.righttree = NULL;
    hash_plan->plan.hasUniqueResults = right_plan->hasUniqueResults;
    hash_plan->skewTable = InvalidOid;
    hash_plan->skewColumn = InvalidAttrNumber;
    hash_plan->skewInherit = false;
    hash_plan->skewColType = InvalidOid;
    hash_plan->skewColTypmod = -1;

    // Create HashJoin plan node
    HashJoin* join_plan = makeNode(HashJoin);
    Plan* plan = &join_plan->join.plan;
    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = left_plan;
    plan->righttree = (Plan*)hash_plan;
    join_plan->join.joinqual = NIL;
    join_plan->join.jointype = jointype;
    join_plan->join.inner_unique = false;
    join_plan->hashclauses = hashclauses;

    // Copy cost and size
    plan->startup_cost = workspace.startup_cost;
    plan->total_cost = workspace.total_cost;
    plan->plan_rows = hp->path.rows > 0 ? hp->path.rows : 1;
    plan->plan_width = hp->path.pathtarget ? hp->path.pathtarget->width : 0;

    return plan;
}

Plan* ExprToPlanConverter::translate_merge_join(OptExpression* expr) {
    PhysicalMergeJoin* mergejoin = dynamic_cast<PhysicalMergeJoin*>(expr->get_op());
    if (mergejoin == nullptr) {
        return translate_expression(expr);
    }

    ereport(WARNING, (errmsg("[CASCADES] translate_merge_join: created mergejoin plan")));
    if (expr->arity() < 2) {
        ereport(WARNING, (errmsg("[CASCADES] translate_merge_join: insufficient children")));
        return nullptr;
    }

    // Get paths from child operators
    PhysicalOptExprOp* left_op = static_cast<PhysicalOptExprOp*>(expr->input_at(0)->get_op());
    PhysicalOptExprOp* right_op = static_cast<PhysicalOptExprOp*>(expr->input_at(1)->get_op());
    Path* left_path = left_op->get_path();
    Path* right_path = right_op->get_path();
    if (left_path == nullptr || right_path == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_merge_join: child path is null")));
        return nullptr;
    }

    // Translate children
    Plan* left_plan = translate_expression(expr->input_at(0));
    if (left_plan == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_merge_join: left plan is null")));
        return nullptr;
    }

    Plan* right_plan = translate_expression(expr->input_at(1));
    if (right_plan == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_merge_join: right plan is null")));
        return nullptr;
    }

    JoinType jointype = JOIN_INNER;

    // Build MergePath for cost/size computation
    JoinPath merge_path_node;
    MemSet(&merge_path_node, 0, sizeof(JoinPath));
    JoinPath* mp = &merge_path_node;
    mp->path.pathtype = T_MergeJoin;
    mp->path.parent = left_path->parent;

    // Build join pathtarget from both sides
    PathTarget* join_pathtarget = create_empty_pathtarget();
    join_pathtarget->exprs = (List*)copyObject(left_path->pathtarget->exprs);
    join_pathtarget->cost.startup = 0;
    join_pathtarget->cost.per_tuple = 0;
    join_pathtarget->width = left_path->pathtarget->width + right_path->pathtarget->width;
    join_pathtarget->sortgrouprefs = nullptr;

    // Add inner side expressions not in outer
    ListCell* lc;
    foreach(lc, right_path->pathtarget->exprs) {
        Node* inner_expr = (Node*)lfirst(lc);
        bool found = false;
        ListCell* lc2;
        foreach(lc2, join_pathtarget->exprs) {
            if (equal(inner_expr, (Node*)lfirst(lc2))) {
                found = true;
                break;
            }
        }
        if (!found) {
            join_pathtarget->exprs = lappend(join_pathtarget->exprs, copyObject(inner_expr));
        }
    }

    mp->path.pathtarget = join_pathtarget;
    mp->path.pathkeys = NIL;
    mp->path.param_info = nullptr;
    mp->path.rows = 0;
    mp->path.startup_cost = 0;
    mp->path.total_cost = 0;
    mp->path.dop = 0;
    mp->jointype = jointype;
    mp->inner_unique = false;
    mp->outerjoinpath = left_path;
    mp->innerjoinpath = right_path;
    mp->joinrestrictinfo = NIL;
    mp->skewoptimize = 0;

    // Compute join cost
    JoinCostWorkspace workspace;
    initial_cost_mergejoin(root_, &workspace, jointype, NIL, left_path, right_path, NIL, NIL, nullptr);

    // Build tlist
    List* tlist = build_path_tlist(root_, &mp->path);

    // Create MergeJoin plan node
    MergeJoin* join_plan = makeNode(MergeJoin);
    Plan* plan = &join_plan->join.plan;
    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = left_plan;
    plan->righttree = right_plan;
    join_plan->join.joinqual = NIL;
    join_plan->join.jointype = jointype;
    join_plan->join.inner_unique = false;
    join_plan->mergeclauses = NIL;

    // Copy cost and size
    plan->startup_cost = workspace.startup_cost;
    plan->total_cost = workspace.total_cost;
    plan->plan_rows = mp->path.rows > 0 ? mp->path.rows : 1;
    plan->plan_width = mp->path.pathtarget ? mp->path.pathtarget->width : 0;

    return plan;
}

Plan* ExprToPlanConverter::translate_nestloop_join(OptExpression* expr) {
    PhysicalNestLoop* nestloop = dynamic_cast<PhysicalNestLoop*>(expr->get_op());
    if (nestloop == nullptr) {
        return translate_expression(expr);
    }

    ereport(WARNING, (errmsg("[CASCADES] translate_nestloop_join: created nestloop plan")));
    if (expr->arity() < 2) {
        ereport(WARNING, (errmsg("[CASCADES] translate_nestloop_join: insufficient children")));
        return nullptr;
    }

    // Get paths from child operators (before translation, paths are already set)
    PhysicalOptExprOp* left_op = static_cast<PhysicalOptExprOp*>(expr->input_at(0)->get_op());
    PhysicalOptExprOp* right_op = static_cast<PhysicalOptExprOp*>(expr->input_at(1)->get_op());
    Path* left_path = left_op->get_path();
    Path* right_path = right_op->get_path();
    if (left_path == nullptr || right_path == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_nestloop_join: child path is null")));
        return nullptr;
    }

    JoinType jointype = JOIN_INNER;

    // Translate outer child first (no curOuterRels needed - it's the outer side)
    Plan* outer_plan = translate_expression(expr->input_at(0));
    if (outer_plan == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_nestloop_join: outer plan is null")));
        return nullptr;
    }

    // Set curOuterRels before translating inner child, matching old optimizer:
    // create_join_plan: root->curOuterRels = bms_union(root->curOuterRels,
    //     best_path->outerjoinpath->parent->relids);
    // This ensures replace_nestloop_params can identify outer Vars when
    // processing the inner plan's index quals.
    Relids saveOuterRels = root_->curOuterRels;
    root_->curOuterRels = bms_union(root_->curOuterRels, left_path->parent->relids);
    root_->curOuterParams = NIL;
    root_->plan_params = NIL;

    // Translate inner child with curOuterRels set
    Plan* inner_plan = translate_expression(expr->input_at(1));

    // Restore curOuterRels; save curOuterParams as nestParams
    bms_free_ext(root_->curOuterRels);
    root_->curOuterRels = saveOuterRels;
    List* nestParams = root_->curOuterParams;
    root_->curOuterParams = NIL;
    root_->plan_params = NIL;

    if (inner_plan == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_nestloop_join: inner plan is null")));
        return nullptr;
    }

    // Build NestPath for cost/size computation (matching old optimizer)
    JoinPath nest_path_node;
    MemSet(&nest_path_node, 0, sizeof(JoinPath));
    JoinPath* np = &nest_path_node;
    np->path.pathtype = T_NestLoop;
    np->path.parent = left_path->parent;
    /*
     * Build the join's pathtarget by combining expressions from both outer
     * and inner pathtargets, matching the old optimizer which sets
     * pathnode->path.pathtarget = joinrel->reltarget (containing both sides).
     * The outer side comes first, then any unique expressions from the inner side.
     */
    PathTarget* join_pathtarget = create_empty_pathtarget();
    join_pathtarget->exprs = (List*)copyObject(left_path->pathtarget->exprs);
    join_pathtarget->cost.startup = 0;
    join_pathtarget->cost.per_tuple = 0;
    join_pathtarget->width = left_path->pathtarget->width + right_path->pathtarget->width;
    join_pathtarget->sortgrouprefs = nullptr;

    /* Add inner side expressions that are not already in the outer side */
    {
        ListCell* lc;
        foreach(lc, right_path->pathtarget->exprs) {
            Node* inner_expr = (Node*)lfirst(lc);
            bool found = false;
            ListCell* lc2;
            foreach(lc2, join_pathtarget->exprs) {
                if (equal(inner_expr, (Node*)lfirst(lc2))) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                join_pathtarget->exprs = lappend(join_pathtarget->exprs, copyObject(inner_expr));
            }
        }
    }

    np->path.pathtarget = join_pathtarget;
    np->path.pathkeys = NIL;
    np->path.param_info = right_path->param_info;
    np->path.rows = 0;
    np->path.startup_cost = 0;
    np->path.total_cost = 0;
    np->path.dop = 0;
    np->jointype = jointype;
    np->inner_unique = false;
    np->outerjoinpath = left_path;
    np->innerjoinpath = right_path;
    np->joinrestrictinfo = NIL;
    np->skewoptimize = 0;

    // Compute join cost
    JoinCostWorkspace workspace;
    initial_cost_nestloop(root_, &workspace, jointype,
        left_path, right_path, nullptr, 1);

    /*
     * Build result tlist from the join pathtarget, matching old optimizer:
     *   List* tlist = build_path_tlist(root, &best_path->path);
     */
    List* tlist = build_path_tlist(root_, &np->path);

    // Create NestLoop node (matching old optimizer make_nestloop)
    NestLoop* join_plan = makeNode(NestLoop);
    Plan* plan = &join_plan->join.plan;
    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = outer_plan;
    plan->righttree = inner_plan;
    join_plan->join.joinqual = NIL;
    join_plan->join.jointype = jointype;
    join_plan->join.inner_unique = false;
    join_plan->nestParams = nestParams;

    // Copy cost and size from computed workspace
    plan->startup_cost = workspace.startup_cost;
    plan->total_cost = workspace.total_cost;
    plan->plan_rows = np->path.rows > 0 ? np->path.rows : 1;
    plan->plan_width = np->path.pathtarget ? np->path.pathtarget->width : 0;

    return plan;
}

Plan* ExprToPlanConverter::translate_hash_agg(OptExpression* expr) {
    if (expr->arity() < 1) {
        return nullptr;
    }

    Plan* child_plan = translate_expression(expr->input_at(0));
    if (child_plan == nullptr) {
        return nullptr;
    }

    PhysicalHashAgg* hash_agg = dynamic_cast<PhysicalHashAgg*>(expr->get_op());
    if (hash_agg == nullptr) {
        return child_plan;
    }

    const std::vector<int>& group_keys = hash_agg->get_group_keys();

    Agg* agg_plan = makeNode(Agg);
    agg_plan->plan.lefttree = child_plan;
    agg_plan->plan.targetlist = (List*)copyObject(hash_agg->get_target_list());

    if (hash_agg->has_having_qual()) {
        agg_plan->plan.qual = (List*)hash_agg->get_having_qual();
    } else {
        agg_plan->plan.qual = NIL;
    }

    if (group_keys.empty()) {
        agg_plan->aggstrategy = AGG_PLAIN;
        agg_plan->numCols = 0;
        agg_plan->grpColIdx = nullptr;
        agg_plan->grpOperators = nullptr;
        agg_plan->grp_collations = nullptr;
    } else {
        agg_plan->aggstrategy = AGG_HASHED;
        agg_plan->numCols = (int)group_keys.size();
        agg_plan->grpColIdx = (AttrNumber*)palloc(agg_plan->numCols * sizeof(AttrNumber));
        agg_plan->grpOperators = (Oid*)palloc(agg_plan->numCols * sizeof(Oid));
        agg_plan->grp_collations = (Oid*)palloc(agg_plan->numCols * sizeof(Oid));

        Oid* src_ops = hash_agg->get_grp_operators();
        Oid* src_collations = hash_agg->get_grp_collations();

        List* agg_tlist = agg_plan->plan.targetlist;
        for (size_t i = 0; i < group_keys.size(); i++) {
            AttrNumber target_resno = group_keys[i];
            TargetEntry* group_tle = get_tle_by_resno(agg_tlist, target_resno);
            if (group_tle == nullptr) {
                return nullptr;
            }

            TargetEntry* child_tle = tlist_member((Node*)group_tle->expr, child_plan->targetlist);
            if (child_tle == nullptr) {
                return nullptr;
            }

            agg_plan->grpColIdx[i] = child_tle->resno;
            agg_plan->grpOperators[i] = (src_ops != nullptr) ? src_ops[i] : InvalidOid;
            agg_plan->grp_collations[i] = (src_collations != nullptr) ? src_collations[i] : InvalidOid;;
        }
    }

    agg_plan->numGroups = 1;
    agg_plan->groupingSets = NIL;
    agg_plan->chain = NIL;

    double total_cost = expr->get_cost();
    if (total_cost > 0) {
        agg_plan->plan.startup_cost = total_cost;
        agg_plan->plan.total_cost = total_cost;
    }

    Statistics* stats = expr->get_statistics();
    if (stats != nullptr) {
        agg_plan->plan.plan_rows = stats->row_count();
    } else if (child_plan != nullptr) {
        agg_plan->plan.plan_rows = child_plan->plan_rows;
    }

    agg_plan->plan.plan_width = child_plan != nullptr ? child_plan->plan_width : 0;

    return (Plan*)agg_plan;
}

Plan* ExprToPlanConverter::translate_sort(OptExpression* expr) {
    if (expr->arity() < 1) {
        return nullptr;
    }

    Plan* child_plan = translate_expression(expr->input_at(0));
    if (child_plan == nullptr) {
        return nullptr;
    }

    OptExprOp* op = expr->get_op();
    List* sort_clause = nullptr;

    // Handle PhysicalSort - extract sort_clause directly
    if (op->get_type() == OptExprOpType::PHYSICAL_SORT) {
        PhysicalSort* physical_sort = static_cast<PhysicalSort*>(op);
        sort_clause = (List*)physical_sort->get_sort_clause();
    } else {
        assert(false);
    }

    if (sort_clause == nullptr || list_length(sort_clause) == 0) {
        return child_plan;
    }

    // Create Sort plan node
    Sort* sort_plan = makeNode(Sort);
    sort_plan->plan.lefttree = child_plan;
    sort_plan->plan.targetlist = child_plan->targetlist;

    // Extract sort key information from sort_clause
    // sort_clause is a list of SortGroupClause nodes
    int numCols = list_length(sort_clause);
    sort_plan->numCols = numCols;
    sort_plan->sortColIdx = (AttrNumber*)palloc(numCols * sizeof(AttrNumber));
    sort_plan->sortOperators = (Oid*)palloc(numCols * sizeof(Oid));
    sort_plan->collations = (Oid*)palloc(numCols * sizeof(Oid));
    sort_plan->nullsFirst = (bool*)palloc(numCols * sizeof(bool));

    int col_idx = 0;
    ListCell* lc;
    foreach(lc, sort_clause) {
        SortGroupClause* sgcl = (SortGroupClause*)lfirst(lc);

        // Use tleSortGroupRef to find the corresponding targetlist entry
        // tleSortGroupRef is a reference that matches the targetlist entry's ressortgroupref
        AttrNumber sort_col_idx = 0;
        Index ref = sgcl->tleSortGroupRef;

        ListCell* tl_cell;
        foreach(tl_cell, child_plan->targetlist) {
            TargetEntry* te = (TargetEntry*)lfirst(tl_cell);
            if (te->ressortgroupref == ref) {
                sort_col_idx = te->resno;
                break;
            }
        }

        Assert(sort_col_idx > 0);
        sort_plan->sortColIdx[col_idx] = sort_col_idx;

        // Get sort operator OID
        sort_plan->sortOperators[col_idx] = sgcl->sortop;

        // Get collation directly from expression type using get_typcollation
        // This avoids issues with collation getting lost through Var transformation
        Oid collation = InvalidOid;
        ListCell* tl_cell2;
        foreach(tl_cell2, child_plan->targetlist) {
            TargetEntry* te = (TargetEntry*)lfirst(tl_cell2);
            if (te->ressortgroupref == ref) {
                Oid expr_type = exprType((Node*)te->expr);
                collation = get_typcollation(expr_type);
                break;
            }
        }
        sort_plan->collations[col_idx] = collation;

        // Use nulls_first from SortGroupClause
        sort_plan->nullsFirst[col_idx] = sgcl->nulls_first;

        col_idx++;
    }

// Initialize memory info (use default values)
    sort_plan->mem_info.opMem = 0;
    sort_plan->mem_info.minMem = 0;
    sort_plan->mem_info.maxMem = 0;
    sort_plan->mem_info.regressCost = 0;

// Copy cost from path
    Path* path = static_cast<PhysicalOptExprOp*>(op)->get_path();
    if (path != nullptr) {
        sort_plan->plan.startup_cost = path->startup_cost;
        sort_plan->plan.total_cost = path->total_cost;
        sort_plan->plan.plan_rows = path->rows;
        sort_plan->plan.plan_width = path->pathtarget ? (int)path->pathtarget->width : 0;
    }

    return (Plan*)sort_plan;
}

Plan* ExprToPlanConverter::translate_material(OptExpression* expr) {
    return translate_expression(expr);
}

Plan* ExprToPlanConverter::translate_exchange(OptExpression* expr) {
    if (expr->arity() >= 1) {
        OptExpression* child_expr = expr->input_at(0);
        return translate_expression(child_expr);
    }
    return nullptr;
}

Plan* ExprToPlanConverter::translate_default(OptExpression* expr) {
    return translate_expression(expr);
}

Plan* ExprToPlanConverter::translate_limit(OptExpression* expr) {
    if (expr->arity() < 1) {
        return nullptr;
    }

    Plan* child_plan = translate_expression(expr->input_at(0));
    if (child_plan == nullptr) {
        return nullptr;
    }

    Node* limit_count = nullptr;
    Node* limit_offset = nullptr;

    // Try LogicalLimit first
    LogicalLimit* logical_limit = dynamic_cast<LogicalLimit*>(expr->get_op());
    if (logical_limit != nullptr) {
        limit_count = logical_limit->get_limit_count();
        limit_offset = logical_limit->get_limit_offset();
    } else {
        // Try PhysicalLimit
        PhysicalLimit* physical_limit = dynamic_cast<PhysicalLimit*>(expr->get_op());
        if (physical_limit != nullptr) {
            limit_count = physical_limit->get_limit_count();
            limit_offset = physical_limit->get_limit_offset();
        }
    }

    Limit* limit_plan = makeNode(Limit);
    limit_plan->plan.lefttree = child_plan;
    limit_plan->plan.targetlist = child_plan->targetlist;
    limit_plan->limitCount = limit_count;
    limit_plan->limitOffset = limit_offset;

    /* Copy cost estimates from child plan */
    limit_plan->plan.startup_cost = child_plan->startup_cost;
    limit_plan->plan.total_cost = child_plan->total_cost;
    limit_plan->plan.plan_rows = child_plan->plan_rows;
    limit_plan->plan.plan_width = child_plan->plan_width;

    return (Plan*)limit_plan;
}

Plan* ExprToPlanConverter::translate_subquery_scan(OptExpression* expr) {
    if (expr == nullptr || root_ == nullptr) {
        return nullptr;
    }

    OptExprOp* op = expr->get_op();
    PhysicalSubqueryScan* subquery_scan_op = dynamic_cast<PhysicalSubqueryScan*>(op);
    if (subquery_scan_op == nullptr) {
        return nullptr;
    }

    // Get subquery path from PhysicalSubqueryScan
    Path* subquery_path = subquery_scan_op->get_subquery_path();

    if (subquery_path == nullptr) {
        return nullptr;
    }

    // Check if the path is a SubqueryScanPath
    // Note: openGauss sets pathtype = T_SubqueryScan (the Plan type) for SubqueryScanPath.
    // The exact value of T_SubqueryScan may vary depending on compile-time macros (e.g., USE_SPQ).
    // We trust that create_subqueryscan_path returns a valid SubqueryScanPath.
    SubqueryScanPath* sq_path = (SubqueryScanPath*)subquery_path;

    // Create SubqueryScan plan node using make_subqueryscan
    // scanrelid = 0 indicates this is a subquery (not a table scan)
    SubqueryScan* scan_node = make_subqueryscan(
        NIL,  // target list will be inherited from subplan
        NIL,  // no restriction clauses for stranslate_target_listbquery scan
        0,
        sq_path->subplan
    );

    // Handle child expression if any (typically there's no child for subquery scan)
    if (expr->arity() > 0) {
        scan_node->scan.plan.lefttree = translate_expression(expr->input_at(0));
    }

    return (Plan*)scan_node;
}

}  // namespace cascades
