#include "column_prune_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/expression_context.h"

namespace cascades {
ColumnPruneRule::ColumnPruneRule()
    : Rule(RuleType::TF_COLUMN_PRUNE,
           Pattern::create(OptExprOpType::LOGICAL_PROJECT,
                          Pattern::any())) {}

std::vector<OptExpression*> ColumnPruneRule::transform(
    OptExpression* input, OptimizerContext* context) {
    // Get the projection operator and its child
    LogicalOptExprOp* project_op = static_cast<LogicalOptExprOp*>(input->get_op());
    OptExpression* child = input->input_at(0);
    OptExprOp* child_op = child->get_op();

    // Get the output columns of the projection
    const ColumnRefSet& project_output = input->get_logical_property()->get_output_columns();

    // Get the output columns of the child
    const ColumnRefSet& child_output = child->get_logical_property()->get_output_columns();

    // Find columns that are needed by parent but not produced by projection
    // For now, we assume all projection columns are needed
    // A full implementation would analyze the parent to find required columns

    // Check if we can prune any columns from the child
    std::vector<int> needed_columns;
    for (int col : project_output) {
        needed_columns.push_back(col);
    }

    // If projection output equals child output, no pruning needed
    if (project_output.size() == child_output.size()) {
        return {};
    }

    std::vector<OptExpression*> result;

    // Create a pruned projection that only outputs needed columns
    LogicalProject* pruned_project = new LogicalProject();
    pruned_project->set_projections(needed_columns);

    OptExpression* pruned_expr = OptExpression::create(pruned_project, child);

    result.push_back(pruned_expr);

    // Also try to push down column requirements to child if it's a projection
    if (child_op->get_type() == OptExprOpType::LOGICAL_PROJECT) {
        // Recursively prune the child projection
        LogicalProject* child_project = static_cast<LogicalProject*>(child_op);
        const std::vector<int>& child_projections = child_project->get_projections();

        // Create mapping from parent needed columns to child columns
        // For now, assume direct mapping (column i in parent corresponds to column i in child)
        // A full implementation would analyze the projection expressions

        // Push down required columns to child
        LogicalProject* pushed_project = new LogicalProject();
        std::vector<int> pushed_columns;

        for (int needed_col : needed_columns) {
            if (needed_col < static_cast<int>(child_projections.size())) {
                pushed_columns.push_back(child_projections[needed_col]);
            } else {
                pushed_columns.push_back(needed_col);
            }
        }

        pushed_project->set_projections(pushed_columns);

        OptExpression* left_child = child->input_at(0);
        OptExpression* pushed_child = OptExpression::create(pushed_project, left_child);

        // Update our pruned expression's child
        pruned_expr->set_input(0, pushed_child);
    }

    return result;
}


}  // namespace cascades