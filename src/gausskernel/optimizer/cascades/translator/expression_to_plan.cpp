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
#include "optimizer/planmain.h"
#include "utils/palloc.h"
#include "utils/lsyscache.h"

namespace cascades {

ExprToPlanConverter::ExprToPlanConverter() : root_(nullptr) {}

ExprToPlanConverter::ExprToPlanConverter(PlannerInfo* root) : root_(root) {}

PlannedStmt* ExprToPlanConverter::convert(OptExpression* expr) {
    if (expr == nullptr) {
        return nullptr;
    }

    Plan* plan = translate_expression(expr);
    if (plan == nullptr) {
        return nullptr;
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

Plan* ExprToPlanConverter::translate_expression(OptExpression* expr) {
    if (expr == nullptr) {
        return nullptr;
    }

    OptExprOp* op = expr->get_op();

    if (op->is_logical()) {
        LogicalOptExprOp* logical_op = dynamic_cast<LogicalOptExprOp*>(op);
        if (logical_op != nullptr) {
            switch (logical_op->get_type()) {
                case OptExprOpType::LOGICAL_SCAN:
                    return translate_scan(expr);
                case OptExprOpType::LOGICAL_PROJECT:
                    return translate_project(expr);
                case OptExprOpType::LOGICAL_FILTER:
                    return translate_filter(expr);
                case OptExprOpType::LOGICAL_JOIN:
                case OptExprOpType::LOGICAL_AGGREGATE:
                case OptExprOpType::LOGICAL_SUBQUERY:
                    return translate_join(expr);
                case OptExprOpType::LOGICAL_SORT:
                    return translate_sort(expr);
                case OptExprOpType::LOGICAL_LIMIT:
                    return translate_limit(expr);
                default:
                    return translate_default(expr);
            }
        }
    } else if (op->is_physical()) {
        PhysicalOptExprOp* phys_op = static_cast<PhysicalOptExprOp*>(op);

        // Handle PhysicalSubqueryScan specially
        if (phys_op->get_type() == OptExprOpType::PHYSICAL_SUBQUERY_SCAN) {
            return translate_subquery_scan(expr);
        }

        Path* path = phys_op->get_path();
        if (path != nullptr && root_ != nullptr) {
            return create_plan(root_, path);
        }
    }

    // Fallback: recursively translate children
    if (expr->arity() >= 1) {
        return translate_expression(expr->input_at(0));
    }

    return nullptr;
}

Plan* ExprToPlanConverter::translate_scan(OptExpression* expr) {
    if (expr == nullptr || root_ == nullptr) {
        return nullptr;
    }

    LogicalScan* scan_op = dynamic_cast<LogicalScan*>(expr->get_op());
    if (scan_op == nullptr) {
        return nullptr;
    }

    int relid = scan_op->get_table_id();

    // simple_rel_array is 1-based in PostgreSQL/openGauss
    if (relid <= 0 || relid >= root_->simple_rel_array_size) {
        return nullptr;
    }

    RelOptInfo* rel = root_->simple_rel_array[relid];
    if (rel == nullptr) {
        return nullptr;
    }

    Path* best_path = (Path*)linitial(rel->cheapest_total_path);
    if (best_path == nullptr) {
        return nullptr;
    }

    return create_plan(root_, best_path);
}

Plan* ExprToPlanConverter::translate_project(OptExpression* expr) {
    if (expr->arity() < 1) {
        return nullptr;
    }

    Plan* child_plan = translate_expression(expr->input_at(0));
    if (child_plan == nullptr) {
        return nullptr;
    }

    LogicalProject* proj_op = dynamic_cast<LogicalProject*>(expr->get_op());
    List* target_list = proj_op ? proj_op->get_target_list() : NIL;

    BaseResult* result_plan = (BaseResult*)palloc0(sizeof(BaseResult));
    result_plan->plan.type = T_BaseResult;
    result_plan->plan.lefttree = child_plan;
    result_plan->plan.targetlist = target_list ? target_list : child_plan->targetlist;
    result_plan->plan.qual = NIL;
    result_plan->resconstantqual = nullptr;

    return (Plan*)result_plan;
}

Plan* ExprToPlanConverter::translate_filter(OptExpression* expr) {
    if (expr->arity() < 1) {
        return nullptr;
    }

    Plan* child_plan = translate_expression(expr->input_at(0));
    if (child_plan == nullptr) {
        return nullptr;
    }

    LogicalFilter* filter_op = dynamic_cast<LogicalFilter*>(expr->get_op());
    Node* qual = filter_op ? filter_op->get_qual() : nullptr;

    SeqScan* scan = makeNode(SeqScan);
    scan->plan.lefttree = child_plan;
    scan->plan.targetlist = (List*)copyObject(child_plan->targetlist);
    scan->plan.qual = qual ? list_make1(qual) : NIL;
    scan->scanrelid = 0;

    return (Plan*)scan;
}

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
    List* sort_clause = logical_sort ? logical_sort->get_sort_clause() : nullptr;

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

    LogicalLimit* limit_op = dynamic_cast<LogicalLimit*>(expr->get_op());
    long count = -1;
    long offset = 0;

    if (limit_op != nullptr) {
        if (limit_op->get_limit_count() != nullptr) {
            if (IsA(limit_op->get_limit_count(), Integer)) {
                count = (long)intVal(limit_op->get_limit_count());
            }
        }
        if (limit_op->get_limit_offset() != nullptr) {
            if (IsA(limit_op->get_limit_offset(), Integer)) {
                offset = (long)intVal(limit_op->get_limit_offset());
            }
        }
    }

    Limit* limit_plan = makeNode(Limit);
    limit_plan->plan.lefttree = child_plan;
    limit_plan->plan.targetlist = child_plan->targetlist;
    limit_plan->limitCount = limit_op->get_limit_count();
    limit_plan->limitOffset = limit_op->get_limit_offset();

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
    if (subquery_path->pathtype != T_SubqueryScan) {
        // If not already a SubqueryScan path, use create_plan to handle it
        return create_plan(root_, subquery_path);
    }

    SubqueryScanPath* sq_path = (SubqueryScanPath*)subquery_path;

    // Create SubqueryScan plan node using make_subqueryscan
    // scanrelid = 0 indicates this is a subquery (not a table scan)
    SubqueryScan* scan_node = make_subqueryscan(
        NIL,  // target list will be inherited from subplan
        NIL,  // no restriction clauses for subquery scan
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
