#include "../base/memory_utils.h"
#include "filter_merge_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"

namespace cascades {

// Static singleton instance
FilterMergeRule* FilterMergeRule::instance() {
    static FilterMergeRule inst;
    return &inst;
}

FilterMergeRule::FilterMergeRule()
    : Rule(RuleType::TF_FILTER_MERGE,
           Pattern::create(OptExprOpType::LOGICAL_FILTER,
                          Pattern::any())) {}

std::vector<OptExpression*> FilterMergeRule::transform(
    OptExpression* input, OptimizerContext* context) {
    // Pattern: Filter on top of any expression
    LogicalFilter* filter = static_cast<LogicalFilter*>(input->get_op());
    OptExpression* child = input->input_at(0);
    OptExprOp* child_op = child->get_op();

    std::vector<OptExpression*> result;

    int predicate_id = filter->get_predicate_id();

    // Case 1: Merge consecutive filters
    // Filter(Filter(child)) -> Filter(child)
    if (child_op->get_type() == OptExprOpType::LOGICAL_FILTER) {
        LogicalFilter* child_filter = static_cast<LogicalFilter*>(child_op);
        int child_predicate_id = child_filter->get_predicate_id();

        // If predicates are equivalent, the child filter is redundant
        if (predicate_id == child_predicate_id) {
            LogicalFilter* merged_filter = NEW_EX(LogicalFilter);
            merged_filter->set_predicate_id(predicate_id);

            OptExpression* grandchild = child->input_at(0);
            OptExpression* merged_expr = OptExpression::create(merged_filter, grandchild);
            result.push_back(merged_expr);
        }
        // If parent predicate is a subset of child predicate, parent is redundant
        else if (is_predicate_subset(predicate_id, child_predicate_id)) {
            OptExpression* grandchild = child->input_at(0);
            LogicalFilter* new_filter = NEW_EX(LogicalFilter);
            new_filter->set_predicate_id(predicate_id);
            OptExpression* new_expr = OptExpression::create(new_filter, grandchild);
            result.push_back(new_expr);
        }
        // If child predicate is a subset of parent predicate, child is redundant
        else if (is_predicate_subset(child_predicate_id, predicate_id)) {
            LogicalFilter* new_filter = NEW_EX(LogicalFilter);
            new_filter->set_predicate_id(predicate_id);
            OptExpression* new_expr = OptExpression::create(new_filter, child->input_at(0));
            result.push_back(new_expr);
        }
        // Otherwise, both predicates are needed - merge them (AND them together)
        else {
            LogicalFilter* merged_filter = NEW_EX(LogicalFilter);
            // Use the more restrictive predicate (lower ID typically means simpler)
            merged_filter->set_predicate_id(predicate_id < child_predicate_id ?
                                            predicate_id : child_predicate_id);

            OptExpression* grandchild = child->input_at(0);
            OptExpression* merged_expr = OptExpression::create(merged_filter, grandchild);
            result.push_back(merged_expr);
        }
    }
    // Case 2: Push filter down through LogicalProject
    // Filter(Project(child)) -> Project(Filter(child))
    else if (child_op->get_type() == OptExprOpType::LOGICAL_PROJECT) {
        LogicalProject* project = static_cast<LogicalProject*>(child_op);
        OptExpression* project_child = child->input_at(0);

        // Create a new filter on top of the projection's child
        LogicalFilter* pushed_filter = NEW_EX(LogicalFilter);
        pushed_filter->set_predicate_id(predicate_id);

        OptExpression* pushed_expr = OptExpression::create(pushed_filter, project_child);

        // Create new projection on top of the pushed filter
        LogicalProject* new_project = NEW_EX(LogicalProject);
        new_project->set_projections(project->get_projections());

        OptExpression* result_expr = OptExpression::create(new_project, pushed_expr);
        result.push_back(result_expr);
    }
    // Case 3: Filter on top of scan - nothing to merge
    else if (child_op->get_type() == OptExprOpType::LOGICAL_SCAN) {
        // Cannot merge with scan, return as-is
        result.push_back(input);
    }
    // Case 4: Filter with join child - push down to applicable sides
    else if (child_op->get_type() == OptExprOpType::LOGICAL_JOIN) {
        // Try to push filter down to one or both children of the join
        LogicalJoin* join = static_cast<LogicalJoin*>(child_op->clone());
        OptExpression* left_child = child->input_at(0);
        OptExpression* right_child = child->input_at(1);
        JoinType join_type = join->get_join_type();

        if (join_type == JOIN_INNER) {
            // Create filters for both children
            LogicalFilter* left_filter = NEW_EX(LogicalFilter);
            left_filter->set_predicate_id(predicate_id);

            LogicalFilter* right_filter = NEW_EX(LogicalFilter);
            right_filter->set_predicate_id(predicate_id);

            OptExpression* left_filtered = OptExpression::create(left_filter, left_child);
            OptExpression* right_filtered = OptExpression::create(right_filter, right_child);

            // Reconstruct join with filtered children
            OptExpression* new_expr = OptExpression::create(join, left_filtered, right_filtered);
            result.push_back(new_expr);
        } else if (join_type == JOIN_LEFT) {
            LogicalFilter* right_filter = NEW_EX(LogicalFilter);
            right_filter->set_predicate_id(predicate_id);

            OptExpression* right_filtered = OptExpression::create(right_filter, right_child);

            OptExpression* new_expr = OptExpression::create(join, left_child, right_filtered);
            result.push_back(new_expr);
        } else if (join_type == JOIN_RIGHT) {
            LogicalFilter* left_filter = NEW_EX(LogicalFilter);
            left_filter->set_predicate_id(predicate_id);

            OptExpression* left_filtered = OptExpression::create(left_filter, left_child);

            OptExpression* new_expr = OptExpression::create(join, left_filtered, right_child);
            result.push_back(new_expr);
        } else {
            // JOIN_FULL or other - cannot push safely
            result.push_back(input);
        }
    }
    // Case 5: Cannot merge or push - return as-is
    else {
        result.push_back(input);
    }

    return result;
}

bool FilterMergeRule::are_predicates_equivalent(int pred_id1, int pred_id2) const {
    // Predicates are considered equivalent if they have the same ID
    // A full implementation would compare the actual predicate expressions
    return pred_id1 == pred_id2;
}

bool FilterMergeRule::is_predicate_subset(int pred_id1, int pred_id2) const {
    // Predicate1 is a subset of predicate2 if:
    // - They are equivalent (same ID)
    // - A full implementation would check if pred1 is more restrictive
    //
    // For now, we assume that if pred_id1 <= pred_id2, pred_id1 is simpler/more restrictive
    // This is a simplification - real implementation would analyze predicate structure
    return pred_id1 <= pred_id2;
}


}  // namespace cascades