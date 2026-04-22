#include "../base/memory_utils.h"
#include "predicate_push_down_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"

namespace cascades {

// Static singleton instance
PredicatePushDownRule* PredicatePushDownRule::instance() {
    static PredicatePushDownRule inst;
    return &inst;
}

PredicatePushDownRule::PredicatePushDownRule()
    : Rule(RuleType::TF_PREDICATE_PUSH_DOWN,
           Pattern::create(OptExprOpType::LOGICAL_FILTER,
                          Pattern::any())) {}

std::vector<OptExpression*> PredicatePushDownRule::transform(
    OptExpression* input, OptimizerContext* context) {
    ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, input=%s",
                         input->to_string().c_str())));

    Assert(input->arity() > 0);  // Pattern must match expression with child

    // Pattern: Filter on top of any expression
    LogicalFilter* filter = static_cast<LogicalFilter*>(input->get_op());
    OptExpression* child = input->input_at(0);
    OptExprOp* child_op = child->get_op();

    std::vector<OptExpression*> result;

    // Get predicate IDs from the filter (supports multiple predicates)
    List* predicate_ids = filter->get_predicate_ids();
    if (predicate_ids == nullptr || list_length(predicate_ids) == 0) {
        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, no predicate to push")));
        // No predicate to push, return input as-is
        result.push_back(input);
        return result;
    }

    ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, predicate_ids has %d elements, child_op=%d",
                         list_length(predicate_ids), (int)child_op->get_type())));

    // Case 1: Push filter down through joins
    if (child_op->get_type() == OptExprOpType::LOGICAL_JOIN) {
        // Create filtered versions of join children
        // The predicate can be pushed to one or both sides based on join type

        LogicalJoin* join = static_cast<LogicalJoin*>(child_op);
        JoinType join_type = join->get_join_type();

        OptExpression* left_child = child->input_at(0);
        OptExpression* right_child = child->input_at(1);

        // For inner join, push predicate to both sides
        // For left/right outer join, push to the outer side only
        if (join_type == JOIN_INNER) {
            // Create filters for both children with same predicate_ids
            LogicalFilter* left_filter = NEW_EX(LogicalFilter);
            left_filter->set_predicate_ids(predicate_ids);

            LogicalFilter* right_filter = NEW_EX(LogicalFilter);
            right_filter->set_predicate_ids(predicate_ids);

            OptExpression* left_filtered = OptExpression::create(left_filter, left_child);
            OptExpression* right_filtered = OptExpression::create(right_filter, right_child);

            // Reconstruct join with filtered children
            LogicalJoin* new_join = static_cast<LogicalJoin*>(join->clone());
            OptExpression* new_expr = OptExpression::create(new_join, left_filtered, right_filtered);
            result.push_back(new_expr);
        } else if (join_type == JOIN_LEFT) {
            // Push to right side only for left outer join
            LogicalFilter* right_filter = NEW_EX(LogicalFilter);
            right_filter->set_predicate_ids(predicate_ids);

            OptExpression* right_filtered = OptExpression::create(right_filter, right_child);

            LogicalJoin* new_join = static_cast<LogicalJoin*>(join->clone());
            OptExpression* new_expr = OptExpression::create(new_join, left_child, right_filtered);
            result.push_back(new_expr);
        } else if (join_type == JOIN_RIGHT) {
            // Push to left side only for right outer join
            LogicalFilter* left_filter = NEW_EX(LogicalFilter);
            left_filter->set_predicate_ids(predicate_ids);

            OptExpression* left_filtered = OptExpression::create(left_filter, left_child);

            LogicalJoin* new_join = static_cast<LogicalJoin*>(join->clone());
            OptExpression* new_expr = OptExpression::create(new_join, left_filtered, right_child);
            result.push_back(new_expr);
        }
        // JOIN_FULL - complex, skip for now
        else {
            ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, skipping JOIN_FULL")));
        }
    }
    // Case 2: Push filter down to scan
    else if (child_op->get_type() == OptExprOpType::LOGICAL_SCAN) {
        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, pushing to SCAN")));
        // Push filter predicates to scan - attach predicate_id to scan
        LogicalScan* scan = static_cast<LogicalScan*>(child_op->clone());

        // Get existing predicate_id from scan
        int existing_id = scan->get_predicate_id();

        // For now, use the first predicate_id from the list
        // TODO: Support multiple predicates by merging or using list
        int new_predicate_id = linitial_int(predicate_ids);

        // Merge with existing predicate if present
        if (scan->has_predicate()) {
            ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, scan already has predicate_id=%d, new=%d",
                                 existing_id, new_predicate_id)));
            // For simplicity, keep existing - a proper implementation would merge
        } else {
            scan->set_predicate_id(new_predicate_id);
        }

        // Return scan directly without filter wrapper
        OptExpression* scan_expr = OptExpression::create(scan);
        result.push_back(scan_expr);

        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, pushed filter to scan, result=%s",
                             scan_expr->to_string().c_str())));
    }
    // Case 3: Push filter through projection
    else if (child_op->get_type() == OptExprOpType::LOGICAL_PROJECT) {
        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, pushing through PROJECT")));
        // Create a new filter on top of the projection's child
        // The predicate needs to be expressed in terms of child's columns
        // For now, just push the filter down to the projection's child
        LogicalProject* project = static_cast<LogicalProject*>(child_op);
        OptExpression* project_child = child->input_at(0);

        LogicalFilter* pushed_filter = NEW_EX(LogicalFilter);
        pushed_filter->set_predicate_ids(predicate_ids);

        OptExpression* pushed_expr = OptExpression::create(pushed_filter, project_child);

        // Create new projection on top of the pushed filter
        LogicalProject* new_project = NEW_EX(LogicalProject);
        new_project->set_target_list(project->get_target_list());

        OptExpression* result_expr = OptExpression::create(new_project, pushed_expr);
        result.push_back(result_expr);
    }
    // Case 4: Push filter through another filter - merge predicates
    else if (child_op->get_type() == OptExprOpType::LOGICAL_FILTER) {
        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, merging FILTERs")));
        // Merge with child's filter - combine predicate_ids
        LogicalFilter* child_filter = static_cast<LogicalFilter*>(child_op);
        List* child_predicate_ids = child_filter->get_predicate_ids();

        // Combine predicate_ids from both filters
        List* merged_ids = predicate_ids;
        if (child_predicate_ids != nullptr) {
            for (ListCell* cell = list_head(child_predicate_ids); cell != nullptr; cell = lnext(cell)) {
                merged_ids = lappend_int(merged_ids, lfirst_int(cell));
            }
        }

        LogicalFilter* merged_filter = NEW_EX(LogicalFilter);
        merged_filter->set_predicate_ids(merged_ids);

        OptExpression* grandchild = child->input_at(0);
        OptExpression* merged_expr = OptExpression::create(merged_filter, grandchild);
        result.push_back(merged_expr);
    }
    // Case 5: Cannot push through other operators
    else {
        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, cannot push through operator type %d",
                             (int)child_op->get_type())));
        result.push_back(input);
    }

    ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, returning %d results",
                         (int)result.size())));

    return result;
}


}  // namespace cascades
