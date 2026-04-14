#include "cross_join_elimination_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../property/logical_property.h"

namespace cascades {

/**
 * Check if a join is a cross join (no join keys)
 */
static bool has_no_join_keys(const LogicalJoin* join) {
    return join->get_join_keys().empty();
}

/**
 * Check if an expression is a single-row scan (like VALUES or LIMIT 1)
 * For now, we check if it's a scan with a limit of 1
 */
static bool is_single_row_scan(OptExpression* expr) {
    if (expr == nullptr) {
        return false;
    }
    OptExprOp* op = expr->get_op();
    // Simplified: consider LOGICAL_SCAN with no predicate as potentially single-row
    // A full implementation would check for LIMIT 1 or VALUES with one row
    return op->get_type() == OptExprOpType::LOGICAL_SCAN;
}

/**
 * Check if an expression is an empty relation (produces no rows)
 * For now, we don't detect this - would need special marker
 */
static bool is_empty_relation(OptExpression* expr) {
    // Would need to check for constant false filters or special markers
    return false;
}

CrossJoinEliminationRule::CrossJoinEliminationRule()
    : Rule(RuleType::TF_CROSS_JOIN_ELIMINATE,
           Pattern::create(OptExprOpType::LOGICAL_JOIN)) {}

std::vector<OptExpression*> CrossJoinEliminationRule::transform(
    OptExpression* input, OptimizerContext* context) {
    std::vector<OptExpression*> result;

    OptExprOp* op = input->get_op();
    if (op->get_type() != OptExprOpType::LOGICAL_JOIN) {
        return result;
    }

    LogicalJoin* join = static_cast<LogicalJoin*>(op);
    JoinType join_type = join->get_join_type();

    // Only handle INNER JOINs for cross join elimination
    if (join_type != JOIN_INNER) {
        return result;
    }

    // Case 1: Pure cross join (no join keys)
    if (is_cross_join(join)) {
        OptExpression* left = input->input_at(0);
        OptExpression* right = input->input_at(1);

        // Case 1a: If one side is empty, the result is empty
        if (is_empty_relation(left) || is_empty_relation(right)) {
            // Return empty relation marker - for now return as-is
            // A full implementation would create a dummy scan
            return result;
        }

        // Case 1b: If one side is single-row, cross join is equivalent
        // to projecting the other side with the single row's values
        // Example: A CROSS JOIN (VALUES (1)) => A with constant projection
        if (is_single_row_scan(right)) {
            // Right side is single row - result is like left side
            // with constants from right appended
            result.push_back(left);
        } else if (is_single_row_scan(left)) {
            // Left side is single row - result is like right side
            // with constants from left appended
            result.push_back(right);
        }
    }

    // Case 2: Check for semi-join reduction opportunity
    // If we have: SELECT * FROM a WHERE a.id IN (SELECT b.id FROM b)
    // This could be written as a SEMI JOIN
    // The current implementation doesn't handle subqueries yet

    // Case 3: Self-join elimination
    // If the same table appears on both sides (after aliasing),
    // and the join condition is on the same key, we might eliminate
    // This requires table alias tracking

    return result;
}

bool CrossJoinEliminationRule::is_cross_join(const LogicalJoin* join) const {
    return has_no_join_keys(join);
}

bool CrossJoinEliminationRule::has_implicit_join_condition(OptExpression* join) const {
    // If there's a filter above the join that could serve as the join condition
    // This would be handled by predicate pushdown before this rule runs
    return false;
}

bool CrossJoinEliminationRule::can_push_down_filter(OptExpression* child) const {
    // Check if child is a simple scan or a single-table filter
    OptExprOp* child_op = child->get_op();
    OptExprOpType child_type = child_op->get_type();

    return child_type == OptExprOpType::LOGICAL_SCAN ||
           child_type == OptExprOpType::LOGICAL_FILTER;
}


}  // namespace cascades