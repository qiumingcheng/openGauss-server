#include "expression_to_plan.h"
#include "../memo/opt_expression.h"
#include "../operator/operators.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "nodes/plannodes.h"
#include "optimizer/planmain.h"
#include "utils/palloc.h"

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
                default:
                    return translate_default(expr);
            }
        }
    } else if (op->is_physical()) {
        PhysicalOptExprOp* phys_op = static_cast<PhysicalOptExprOp*>(op);
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
    return translate_expression(expr);
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

}  // namespace cascades
