#include "expression_to_plan.h"
#include "../memo/opt_expression.h"
#include "../operator/operators.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "nodes/plannodes.h"
#include "nodes/value.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
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
 * Translate PhysicalScan to SeqScan plan node.
 *
 * We build the target list from path->pathtarget to preserve column pruning
 * from Cascades optimizer. This is different from create_plan() which calls
 * build_physical_tlist() and reads all table columns.
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
        ListCell* lc;
        foreach(lc, path->pathtarget->exprs) {
            Var* var = (Var*)lfirst(lc);
            TargetEntry* tle = makeTargetEntry((Expr*)var, resno, NULL, false);
            tlist = lappend(tlist, tle);
            resno++;
        }
    }

    /* Create SeqScan node */
    SeqScan* scan_plan = makeNode(SeqScan);
    Plan* plan = &scan_plan->plan;

    /* Initialize Plan fields like make_seqscan does */
    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->isDeltaTable = false;

    /* Initialize SeqScan fields */
    scan_plan->scanrelid = rel->relid;
    scan_plan->scanBatchMode = false;
    scan_plan->isPartTbl = false;
    scan_plan->itrs = 0;
    scan_plan->pruningInfo = nullptr;
    scan_plan->bucketInfo = nullptr;
    scan_plan->partScanDirection = ForwardScanDirection;
    scan_plan->tablesample = nullptr;

    /* Copy cost estimates from path */
    plan->startup_cost = path->startup_cost;
    plan->total_cost = path->total_cost;
    plan->plan_rows = path->rows;
    plan->plan_width = path->pathtarget ? path->pathtarget->width : 0;

    /* Attach restriction clauses from rel->baserestrictinfo */
    /* These are the WHERE conditions that were pushed down to this scan */
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

    return (Plan*)scan_plan;
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

        if (child_tle != NULL) {
            /* Create new Var with varno=OUTER_VAR and varattno=child_tle->resno */
            /* Executor will fetch from ecxt_outertuple (standard PostgreSQL semantics) */
            return (Node*)makeVar(OUTER_VAR, child_tle->resno,
                                  var->vartype, var->vartypmod, var->varcollid, 0);
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

    BaseResult* result_plan = makeNode(BaseResult);

    result_plan->plan.lefttree = child_plan;
    result_plan->plan.qual = NIL;
    result_plan->resconstantqual = nullptr;

    /*
     * Build new target list: convert Vars to reference child plan output
     * Original Vars have varno = RTE index, we need varno = position in child targetlist
     * This supports expressions like "id+id" by preserving the expression tree
     */
    if (target_list != NIL) {
        result_plan->plan.targetlist = convert_var_references(target_list, child_plan->targetlist);
    } else {
        result_plan->plan.targetlist = child_plan->targetlist;
    }

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
    List* predicate_ids = filter_op ? (List*)filter_op->get_predicate_ids() : nullptr;

    // Build qual from predicate_ids using rel->baserestrictinfo
    List* qual = NIL;
    if (predicate_ids != nullptr && root_ != nullptr) {
        // Get the rel from child plan's relation info
        // For now, use the root's baserestrictinfo directly
        if (root_->parse && root_->parse->rtable && list_length(root_->parse->rtable) > 0) {
            int rtindex = 1;
            RelOptInfo* rel = root_->simple_rel_array[rtindex];
            if (rel != nullptr && rel->baserestrictinfo != NIL) {
                ListCell* lc;
                foreach(lc, rel->baserestrictinfo) {
                    RestrictInfo* ri = (RestrictInfo*)lfirst(lc);
                    if (ri != nullptr && ri->clause != nullptr) {
                        qual = lappend(qual, ri->clause);
                    }
                }
            }
        }
    }

    BaseResult* result_plan = makeNode(BaseResult);
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
    return translate_expression(expr);
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

    // If it's a physical sort, use the path directly
    if (op->is_physical()) {
        PhysicalOptExprOp* phys_op = static_cast<PhysicalOptExprOp*>(op);
        Path* path = phys_op->get_path();
        if (path != nullptr && root_ != nullptr) {
            return create_plan(root_, path);
        }
    }

    // For logical sort, extract sort keys and create a Sort plan node
    LogicalSort* logical_sort = dynamic_cast<LogicalSort*>(op);
    List* sort_clause = logical_sort ? (List*)logical_sort->get_sort_clause() : nullptr;

    if (sort_clause == nullptr || list_length(sort_clause) == 0) {
        // No sort keys, return child plan
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

        // Get the sort key's varattno from the target list
        // tleSortGroupRef links to SortGroupClause's tleSortGroupRef
        if (sgcl->tleSortGroupRef > 0 && (int)sgcl->tleSortGroupRef <= list_length(child_plan->targetlist)) {
            sort_plan->sortColIdx[col_idx] = sgcl->tleSortGroupRef;
        } else {
            // Fallback: use position in target list
            sort_plan->sortColIdx[col_idx] = col_idx + 1;
        }

        // Get sort operator OID
        sort_plan->sortOperators[col_idx] = sgcl->sortop;
        // Collation: use default (InvalidOid) - can be extended to extract from operator
        sort_plan->collations[col_idx] = InvalidOid;
        // Use nulls_first from SortGroupClause (note: different case convention)
        sort_plan->nullsFirst[col_idx] = sgcl->nulls_first;

        col_idx++;
    }

    // Initialize memory info (use default values)
    sort_plan->mem_info.opMem = 0;
    sort_plan->mem_info.minMem = 0;
    sort_plan->mem_info.maxMem = 0;
    sort_plan->mem_info.regressCost = 0;

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
            long count = physical_limit->get_limit_count();
            long offset = physical_limit->get_limit_offset();
            if (count >= 0) {
                limit_count = (Node*)makeInteger(count);
            }
            if (offset > 0) {
                limit_offset = (Node*)makeInteger(offset);
            }
        }
    }

    Limit* limit_plan = makeNode(Limit);
    limit_plan->plan.lefttree = child_plan;
    limit_plan->plan.targetlist = child_plan->targetlist;
    limit_plan->limitCount = limit_count;
    limit_plan->limitOffset = limit_offset;

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
