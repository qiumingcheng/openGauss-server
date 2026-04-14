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

    // Case 1: Pure cross join (no join keys) with filter above
    if (is_cross_join(join)) {
        // Case 1a: If one side produces no output columns (empty relation),
        // the whole join result is empty - we could mark it as dummy
        // This would require a separate dummy relation marker

        // Case 1b: If one side is a single-row relation (e.g., VALUES clause),
        // the cross join is equivalent to a simple projection of the other side
        // This optimization is more complex and requires row count estimation

        // Case 1c: If there's a filter on top of the cross join that references
        // only one side, we might be able to push it down and eliminate the cross join
        // This requires checking if the filter can be decomposed

        // For now, if the cross join has no keys and there's no way to simplify,
        // we could consider it as a potential performance issue and keep it
        // A production optimizer would use statistics to decide
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