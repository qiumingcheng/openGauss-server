#include "expression_to_plan.h"
#include "../memo/opt_expression.h"
#include "../operator/operators.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "nodes/plannodes.h"
#include "nodes/value.h"
#include "parser/parsetree.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
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
     * Fix Var references in the plan tree.
     * This is called once after the entire plan tree is built.
     * It converts table column Var references (varno=RTE, varattno=col) to
     * reference child plan outputs (varno=OUTER_VAR, varattno=resno).
     *
     * The algorithm:
     * 1. Build a mapping from table column numbers to child output positions for each node
     * 2. Walk the plan tree, converting Var references to OUTER_VAR using the mappings
     * 3. Update column references like grpColIdx, sortColIdx to use output positions
     */
    if (root_ != nullptr) {
        fix_var_references(plan, root_);
    }

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
    result->nParamExec = 0;
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
    if (phys_op->get_type() == OptExprOpType::PHYSICAL_SCAN) {
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

    PhysicalScan* scan_op = dynamic_cast<PhysicalScan*>(expr->get_op());
    if (scan_op == nullptr) {
        return nullptr;
    }

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

            // Try to get sortgroupref from pathtarget first
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
    IndexScan* index_scan = makeNode(IndexScan);
    Scan* scan = &index_scan->scan;

    init_scan_plan(scan, rel, tlist);

    /* Index-specific fields */
    index_scan->indexid = index_path->indexinfo->indexoid;
    index_scan->indexname = (char*)get_rel_name(index_scan->indexid);
    index_scan->indexqual = index_path->indexquals;
    index_scan->indexqualorig = get_actual_clauses(index_path->indexquals);
    index_scan->indexorderby = index_path->indexorderbys;
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

Plan* ExprToPlanConverter::translate_bitmap_scan(BitmapHeapPath* bitmap_path, RelOptInfo* rel, List* tlist) {
    BitmapHeapScan* bitmap_scan = makeNode(BitmapHeapScan);
    Scan* scan = &bitmap_scan->scan;

    init_scan_plan(scan, rel, tlist);

    /* Bitmap-specific: use the bitmapqual from the path */
    bitmap_scan->bitmapqualorig = (List*)bitmap_path->bitmapqual;

    ereport(WARNING, (errmsg("[CASCADES] translate_scan: created BitmapHeapScan")));

    return &scan->plan;
}

Plan* ExprToPlanConverter::translate_seq_scan(RelOptInfo* rel, List* tlist) {
    SeqScan* seq_scan = makeNode(SeqScan);

    init_scan_plan(seq_scan, rel, tlist);

    ereport(WARNING, (errmsg("[CASCADES] translate_scan: created SeqScan")));

    return &seq_scan->plan;
}

/*
 * Convert Vars in expression to reference child plan output.
 * Original Vars have varno = RTE index (e.g., 1), we keep varno=1.
 * varattno is changed to position in child targetlist.
 * Executor will fetch tuple from ecxt_scantuple which should point to child's output slot.
 */
typedef struct {
    List* child_tlist;  /* child plan's targetlist */
} ConvertVarContext;

static Node* convert_var_references_mutator(Node* node, void* context)
{
    ConvertVarContext* ctx = (ConvertVarContext*)context;
    List* child_tlist = ctx->child_tlist;

    if (node == NULL)
        return NULL;

    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        /* Find corresponding TargetEntry in child targetlist by varattno */
        TargetEntry* child_tle = NULL;
        ListCell* lc;
        foreach(lc, child_tlist) {
            TargetEntry* ctle = (TargetEntry*)lfirst(lc);
            if (IsA(ctle->expr, Var)) {
                Var* cvar = (Var*)ctle->expr;
                if (cvar->varattno == var->varattno) {
                    child_tle = ctle;
                    break;
                }
            }
        }

        if (child_tle != NULL && IsA(child_tle->expr, Var)) {
            Var* child_var = (Var*)child_tle->expr;
            /* Create new Var with varno=OUTER_VAR and varattno=child_tle->resno */
            /* Use child's vartypmod to match child's targetlist */
            return (Node*)makeVar(OUTER_VAR, child_tle->resno,
                                  child_var->vartype, child_var->vartypmod, child_var->varcollid, 0);
        }
        /* If not found, return original Var (shouldn't happen) */
        return node;
    }

    /* Recurse into expression tree */
    return expression_tree_mutator(node, convert_var_references_mutator, context);
}

static List* convert_var_references(List* target_list, List* child_tlist)
{
    List* new_target_list = NIL;
    ListCell* lc;
    ConvertVarContext context;

    context.child_tlist = child_tlist;

    foreach(lc, target_list) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        Node* new_expr = convert_var_references_mutator((Node*)tle->expr, &context);
        TargetEntry* new_tle = makeTargetEntry((Expr*)new_expr,
                                               tle->resno,
                                               tle->resname,
                                               tle->resjunk);
        new_tle->ressortgroupref = tle->ressortgroupref;
        new_target_list = lappend(new_target_list, new_tle);
    }

    return new_target_list;
}

Plan* ExprToPlanConverter::translate_project(OptExpression* expr) {
    if (expr->arity() < 1) {
        return nullptr;
    }

    Plan* child_plan = translate_expression(expr->input_at(0));
    if (child_plan == nullptr) {
        return nullptr;
    }

    List* target_list = NIL;

    // Try PhysicalProject first
    PhysicalProject* physical_proj = dynamic_cast<PhysicalProject*>(expr->get_op());
    if (physical_proj != nullptr) {
        target_list = physical_proj->get_target_list();
    } else {
        // Try LogicalProject
        LogicalProject* logical_proj = dynamic_cast<LogicalProject*>(expr->get_op());
        if (logical_proj != nullptr) {
            target_list = logical_proj->get_target_list();
        }
    }

    // For SELECT * (target_list is NIL), no projection needed, return child directly
    if (target_list == NIL) {
        return child_plan;
    }

    /*
     * Project elimination check: compare original target_list with child's targetlist
     * We keep the original varattno values (table column numbers) here.
     * The Var -> OUTER_VAR conversion will be done in fix_var_references().
     */
    bool can_eliminate = false;
    if (list_length(target_list) == list_length(child_plan->targetlist)) {
        bool all_vars_match = true;
        ListCell *lc1 = list_head(target_list);
        ListCell *lc2 = list_head(child_plan->targetlist);
        for (int i = 0; i < list_length(target_list) && lc1 != NULL && lc2 != NULL; i++) {
            TargetEntry *tle1 = (TargetEntry *)lfirst(lc1);
            TargetEntry *tle2 = (TargetEntry *)lfirst(lc2);
            if (tle1 == NULL || tle2 == NULL || tle1->expr == NULL || tle2->expr == NULL) {
                all_vars_match = false;
                break;
            }
            if (!IsA(tle1->expr, Var) || !IsA(tle2->expr, Var)) {
                all_vars_match = false;
                break;
            }
            Var *v1 = (Var *)tle1->expr;
            Var *v2 = (Var *)tle2->expr;
            // Compare varattno and vartype (original table column numbers)
            if (v1->varattno != v2->varattno || v1->vartype != v2->vartype) {
                all_vars_match = false;
                break;
            }
            lc1 = lnext(lc1);
            lc2 = lnext(lc2);
        }
        if (all_vars_match) {
            can_eliminate = true;
        }
    }

    if (can_eliminate) {
        // Project can be eliminated - child produces same output
        return child_plan;
    }

    // Need to create Result node with the targetlist
    // Keep original varattno - conversion happens in fix_var_references()
    List* new_targetlist = (List*)copyObject(target_list);

    // If child is an Agg node, update its targetlist
    if (nodeTag(child_plan) == T_Agg) {
        Agg* agg = (Agg*)child_plan;
        agg->plan.targetlist = new_targetlist;
        return child_plan;
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
    result_plan->plan.qual = NIL;
    result_plan->resconstantqual = nullptr;
    result_plan->plan.targetlist = new_targetlist;

    return (Plan*)result_plan;
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
    return translate_expression(expr);
}

Plan* ExprToPlanConverter::translate_merge_join(OptExpression* expr) {
    return translate_expression(expr);
}

Plan* ExprToPlanConverter::translate_nestloop_join(OptExpression* expr) {
    return translate_expression(expr);
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

    // Use convert_var_references to build proper targetlist with Var varno=OUTER_VAR
    List* agg_tlist = convert_var_references(hash_agg->get_target_list(), child_plan->targetlist);
    agg_plan->plan.targetlist = agg_tlist;

    // Set HAVING clause - parse->havingQual is already preprocessed into List* format
    if (hash_agg->has_having_qual()) {
        agg_plan->plan.qual = (List*)hash_agg->get_having_qual();
    } else {
        agg_plan->plan.qual = NIL;
    }

    if (group_keys.empty()) {
        // Global aggregate (no GROUP BY)
        agg_plan->aggstrategy = AGG_PLAIN;
        agg_plan->numCols = 0;
        agg_plan->grpColIdx = nullptr;
        agg_plan->grpOperators = nullptr;
        ereport(WARNING, (errmsg("[CASCADES] translate_hash_agg: global aggregate")));
    } else {
        // Grouped aggregate - use hashed aggregation
        agg_plan->aggstrategy = AGG_HASHED;
        agg_plan->numCols = (int)group_keys.size();
        agg_plan->grpColIdx = (AttrNumber*)palloc(agg_plan->numCols * sizeof(AttrNumber));
        agg_plan->grpOperators = (Oid*)palloc(agg_plan->numCols * sizeof(Oid));

        // Use grpOperators extracted by HashAggImplRule if available
        Oid* src_ops = hash_agg->get_grp_operators();
        ereport(WARNING, (errmsg("[CASCADES] translate_hash_agg: numCols=%d, src_ops=%p",
                             agg_plan->numCols, (void*)src_ops)));

        // Extract varattno from the converted targetlist Var expressions
        // grpColIdx should be the column position in the CHILD output
        for (size_t i = 0; i < group_keys.size(); i++) {
            AttrNumber target_resno = group_keys[i];
            AttrNumber child_varattno = InvalidAttrNumber;

            ListCell* lc;
            foreach(lc, agg_plan->plan.targetlist) {
                TargetEntry* te = (TargetEntry*)lfirst(lc);
                if (te->resno == target_resno && IsA(te->expr, Var)) {
                    Var* v = (Var*)te->expr;
                    child_varattno = v->varattno;
                    break;
                }
            }

            if (child_varattno == InvalidAttrNumber) {
                ereport(ERROR, (errmsg("[CASCADES] translate_hash_agg: failed to find Var for group key %zu", i)));
                return nullptr;
            }

            agg_plan->grpColIdx[i] = child_varattno;
            agg_plan->grpOperators[i] = (src_ops != nullptr) ? src_ops[i] : InvalidOid;
            ereport(WARNING, (errmsg("[CASCADES] translate_hash_agg: key[%zu] colIdx=%d, op=%u",
                                 i, agg_plan->grpColIdx[i], agg_plan->grpOperators[i])));
        }
    }

agg_plan->numGroups = 1;
    agg_plan->groupingSets = NIL;
    agg_plan->chain = NIL;

    // Copy cost from path
    Path* path = hash_agg->get_path();
    if (path != nullptr) {
        agg_plan->plan.startup_cost = path->startup_cost;
        agg_plan->plan.total_cost = path->total_cost;
        agg_plan->plan.plan_rows = path->rows;
        agg_plan->plan.plan_width = path->pathtarget ? (int)path->pathtarget->width : 0;
    }

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
    } else if (op->is_physical()) {
        // For other physical operators with path, use create_plan
        PhysicalOptExprOp* phys_op = static_cast<PhysicalOptExprOp*>(op);
        Path* path = phys_op->get_path();
        if (path != nullptr && root_ != nullptr) {
            return create_plan(root_, path);
        }
    } else {
        // For logical sort, extract sort keys
        LogicalSort* logical_sort = dynamic_cast<LogicalSort*>(op);
        if (logical_sort != nullptr) {
            sort_clause = (List*)logical_sort->get_sort_clause();
        }
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
        int pos = 1;
        foreach(tl_cell, child_plan->targetlist) {
            TargetEntry* te = (TargetEntry*)lfirst(tl_cell);
            if (te->ressortgroupref == ref) {
                sort_col_idx = pos;
                break;
            }
            pos++;
        }

        Assert(sort_col_idx > 0);
        sort_plan->sortColIdx[col_idx] = sort_col_idx;

        // Get sort operator OID
        sort_plan->sortOperators[col_idx] = sgcl->sortop;
        // Collation: use default (InvalidOid)
        sort_plan->collations[col_idx] = InvalidOid;
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

/*
 * fix_var_references
 *
 * Convert table column Var references to reference child plan outputs.
 * This is called once after the entire plan tree is built.
 *
 * Algorithm:
 * 1. Build output column mappings for each node (table col -> output position)
 * 2. Walk the plan tree, converting Vars to OUTER_VAR using the mappings
 * 3. Update column references (grpColIdx, sortColIdx, etc.) to use output positions
 */
void ExprToPlanConverter::fix_var_references(Plan* plan, PlannerInfo* root)
{
    if (plan == nullptr) {
        return;
    }

    ereport(WARNING, (errmsg("[CASCADES] fix_var_references: plan type=%d", (int)nodeTag(plan))));

    // Process child nodes first (bottom-up)
    if (plan->lefttree != nullptr) {
        fix_var_references(plan->lefttree, root);
    }
    if (plan->righttree != nullptr) {
        fix_var_references(plan->righttree, root);
    }

    // Build mapping for this node based on its type
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_IndexScan:
        case T_BitmapHeapScan:
            // Scan nodes produce table columns as-is (varattno = table column number)
            // No conversion needed for targetlist - leaf nodes
            break;

        case T_Agg: {
            // Agg nodes: grpColIdx references child output, targetlist output references Agg output
            Agg* agg = (Agg*)plan;
            fix_agg_var_references(agg, root);
            break;
        }

        case T_Sort: {
            // Sort nodes: sortColIdx references child output
            Sort* sort = (Sort*)plan;
            fix_sort_var_references(sort, root);
            break;
        }

        case T_BaseResult:
        case T_Result: {
            // Result/Project nodes: targetlist references child output
            Plan* p = (Plan*)plan;
            fix_result_var_references(p, root);
            break;
        }

        default:
            // For other node types, process targetlist and qual
            fix_generic_var_references(plan, root);
            break;
    }
}

/*
 * Forward declarations for static helpers defined later
 */
static Node* fix_var_mutator(Node* node, List* child_tlist);
static Node* convert_expr_vars(Node* node, List* child_tlist, Index newvarno);

void ExprToPlanConverter::fix_agg_var_references(Agg* agg, PlannerInfo* root)
{
    ereport(WARNING, (errmsg("[CASCADES] ignore fix_agg_var_references: numCols=%d", agg->numCols)));
}

void ExprToPlanConverter::fix_sort_var_references(Sort* sort, PlannerInfo* root)
{
    Plan* child = sort->plan.lefttree;
    if (child == nullptr) {
        return;
    }

    // Fix sortColIdx: these reference child's output columns
    if (sort->sortColIdx != nullptr && sort->numCols > 0) {
        List* child_tlist = child->targetlist;

        for (int i = 0; i < sort->numCols; i++) {
            AttrNumber old_idx = sort->sortColIdx[i];
            AttrNumber new_idx = map_varattno_to_output_pos(old_idx, child_tlist);
            ereport(WARNING, (errmsg("[CASCADES] fix_sort_var_references: sortColIdx[%d] %d -> %d",
                                     i, old_idx, new_idx)));
            sort->sortColIdx[i] = new_idx;
        }
    }

    // Fix targetlist: sort doesn't evaluate targetlist, but we need correct varrefs
    // For safety, convert vars in targetlist to reference child output
    if (sort->plan.targetlist != NIL) {
        sort->plan.targetlist = (List*)convert_expr_vars((Node*)sort->plan.targetlist, child->targetlist, OUTER_VAR);
    }
}

void ExprToPlanConverter::fix_result_var_references(Plan* plan, PlannerInfo* root)
{
    Plan* child = plan->lefttree;
    if (child == nullptr) {
        return;
    }

    ereport(WARNING, (errmsg("[CASCADES] fix_result_var_references")));

    // Result/Project targetlist references child's output
    if (plan->targetlist != NIL) {
        plan->targetlist = (List*)fix_var_mutator((Node*)plan->targetlist, child->targetlist);
    }

    // Fix qual if any
    if (plan->qual != NIL) {
        plan->qual = (List*)fix_var_mutator((Node*)plan->qual, child->targetlist);
    }
}

void ExprToPlanConverter::fix_generic_var_references(Plan* plan, PlannerInfo* root)
{
    Plan* child = plan->lefttree;
    if (child == nullptr) {
        return;
    }

    // For other node types, process targetlist and qual
    if (plan->targetlist != NIL) {
        plan->targetlist = (List*)fix_var_mutator((Node*)plan->targetlist, child->targetlist);
    }

    if (plan->qual != NIL) {
        plan->qual = (List*)fix_var_mutator((Node*)plan->qual, child->targetlist);
    }
}

/*
 * Map from original table column number to child's output position.
 * Child targetlist has entries like: Var(varattno=3, resno=1) meaning
 * the 3rd column of the original table is the 1st output column.
 */
AttrNumber ExprToPlanConverter::map_varattno_to_output_pos(AttrNumber varattno, List* child_tlist)
{
    int pos = 1;
    ListCell* lc;
    foreach (lc, child_tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (IsA(tle->expr, Var)) {
            Var* v = (Var*)tle->expr;
            if (v->varattno == varattno) {
                return (AttrNumber)pos;
            }
        }
        pos++;
    }
    // If not found, return original value (shouldn't happen for valid plans)
    return varattno;
}

/*
 * Try to find a Var in child targetlist.
 * Returns new Var with (newvarno, resno) if found, NULL otherwise.
 * Matches on (varno, varattno) pair like the standard optimizer's search_indexed_tlist_for_var.
 */
static Var* match_var_in_tlist(Var* var, List* child_tlist, Index newvarno)
{
    int pos = 1;
    ListCell* lc;
    foreach(lc, child_tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (tle->expr != nullptr && IsA(tle->expr, Var)) {
            Var* child_var = (Var*)tle->expr;
            if (child_var->varno == var->varno && child_var->varattno == var->varattno) {
                Var* newvar = makeVar(newvarno, (AttrNumber)pos,
                    var->vartype, var->vartypmod, var->varcollid, var->varlevelsup);
                return newvar;
            }
        }
        pos++;
    }
    return nullptr;
}

/* Simple context for fix_var_mutator */
typedef struct {
    List* child_tlist;
    Index newvarno;
} FixVarContext;

/*
 * Mutator: convert Vars matching (varno,varattno) in child_tlist to (newvarno, resno).
 * For all node types (including Aggref), use generic expression_tree_mutator which
 * creates copies — never modifies the original node in-place.
 */
static Node* fix_expr_mutator(Node* node, FixVarContext* ctx)
{
    if (node == nullptr) {
        return nullptr;
    }

    if (IsA(node, Var)) {
        Var* var = (Var*)node;
        Var* newvar = match_var_in_tlist(var, ctx->child_tlist, ctx->newvarno);
        return newvar != nullptr ? (Node*)newvar : node;
    }

    /* For all other node types (including Aggref), use generic mutator */
    return expression_tree_mutator(node,
        (Node* (*)(Node*, void*))fix_expr_mutator, (void*)ctx);
}

/*
 * Convert Vars in expression using child targetlist.
 * New varno for converted Vars is given by newvarno (typically OUTER_VAR).
 */
static Node* convert_expr_vars(Node* node, List* child_tlist, Index newvarno)
{
    if (node == nullptr || child_tlist == NIL) {
        return node;
    }
    FixVarContext ctx;
    ctx.child_tlist = child_tlist;
    ctx.newvarno = newvarno;
    return fix_expr_mutator(node, &ctx);
}

/*
 * Legacy wrapper: convert Vars using child_tlist with newvarno=OUTER_VAR.
 * Used by fix_result_var_references and fix_generic_var_references.
 */
static Node* fix_var_mutator(Node* node, List* child_tlist)
{
    return convert_expr_vars(node, child_tlist, OUTER_VAR);
}

}  // namespace cascades
