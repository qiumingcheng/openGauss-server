#include "predicate_push_down_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"

namespace cascades {
PredicatePushDownRule::PredicatePushDownRule()
    : Rule(RuleType::TF_PREDICATE_PUSH_DOWN,
           Pattern::create(OptExprOpType::LOGICAL_FILTER,
                          Pattern::any())) {}

std::vector<OptExpression*> PredicatePushDownRule::transform(
    OptExpression* input, OptimizerContext* context) {
    // Pattern: Filter on top of any expression
    LogicalFilter* filter = static_cast<LogicalFilter*>(input->get_op());
    OptExpression* child = input->input_at(0);
    OptExprOp* child_op = child->get_op();

    std::vector<OptExpression*> result;

    // Get the predicate ID from the filter
    int predicate_id = filter->get_predicate_id();
    if (predicate_id < 0) {
        // No predicate to push, return input as-is
        result.push_back(input);
        return result;
    }

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
            // Create filters for both children
            LogicalFilter* left_filter = new LogicalFilter();
            left_filter->set_predicate_id(predicate_id);

            LogicalFilter* right_filter = new LogicalFilter();
            right_filter->set_predicate_id(predicate_id);

            OptExpression* left_filtered = OptExpression::create(left_filter, left_child);
            OptExpression* right_filtered = OptExpression::create(right_filter, right_child);

            // Reconstruct join with filtered children
            LogicalJoin* new_join = static_cast<LogicalJoin*>(join->clone());
            OptExpression* new_expr = OptExpression::create(new_join, left_filtered, right_filtered);
            result.push_back(new_expr);
        } else if (join_type == JOIN_LEFT) {
            // Push to right side only for left outer join
            LogicalFilter* right_filter = new LogicalFilter();
            right_filter->set_predicate_id(predicate_id);

            OptExpression* right_filtered = OptExpression::create(right_filter, right_child);

            LogicalJoin* new_join = static_cast<LogicalJoin*>(join->clone());
            OptExpression* new_expr = OptExpression::create(new_join, left_child, right_filtered);
            result.push_back(new_expr);
        } else if (join_type == JOIN_RIGHT) {
            // Push to left side only for right outer join
            LogicalFilter* left_filter = new LogicalFilter();
            left_filter->set_predicate_id(predicate_id);

            OptExpression* left_filtered = OptExpression::create(left_filter, left_child);

            LogicalJoin* new_join = static_cast<LogicalJoin*>(join->clone());
            OptExpression* new_expr = OptExpression::create(new_join, left_filtered, right_child);
            result.push_back(new_expr);
        }
        // JOIN_FULL - complex, skip for now
    }
    // Case 2: Push filter down to scan
    else if (child_op->get_type() == OptExprOpType::LOGICAL_SCAN) {
        // Push filter directly to scan - attach predicate to scan
        LogicalScan* scan = static_cast<LogicalScan*>(child_op->clone());

        // If scan already has a predicate, combine them
        if (scan->has_predicate()) {
            // Keep existing predicate, for now just return input
            result.push_back(input);
        } else {
            scan->set_predicate_id(predicate_id);
            // LOGICAL_SCAN has no inputs
            OptExpression* filtered_scan = OptExpression::create(scan);
            result.push_back(filtered_scan);
        }
    }
    // Case 3: Push filter through projection
    else if (child_op->get_type() == OptExprOpType::LOGICAL_PROJECT) {
        // Create a new filter on top of the projection's child
        // The predicate needs to be expressed in terms of child's columns
        // For now, just push the filter down to the projection's child
        LogicalProject* project = static_cast<LogicalProject*>(child_op);
        OptExpression* project_child = child->input_at(0);

        LogicalFilter* pushed_filter = new LogicalFilter();
        pushed_filter->set_predicate_id(predicate_id);

        OptExpression* pushed_expr = OptExpression::create(pushed_filter, project_child);

        // Create new projection on top of the pushed filter
        LogicalProject* new_project = new LogicalProject();
        new_project->set_projections(project->get_projections());

        OptExpression* result_expr = OptExpression::create(new_project, pushed_expr);
        result.push_back(result_expr);
    }
    // Case 4: Push filter through another filter - merge predicates
    else if (child_op->get_type() == OptExprOpType::LOGICAL_FILTER) {
        // Merge with child's filter
        LogicalFilter* child_filter = static_cast<LogicalFilter*>(child_op);

        // Create a new filter with combined predicate
        LogicalFilter* merged_filter = new LogicalFilter();
        // For now, just use the parent's predicate
        merged_filter->set_predicate_id(predicate_id);

        OptExpression* grandchild = child->input_at(0);
        OptExpression* merged_expr = OptExpression::create(merged_filter, grandchild);
        result.push_back(merged_expr);
    }
    // Case 5: Cannot push through other operators
    else {
        result.push_back(input);
    }

    return result;
}


}  // namespace cascades