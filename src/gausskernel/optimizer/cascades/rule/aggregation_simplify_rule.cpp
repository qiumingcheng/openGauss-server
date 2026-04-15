#include "../base/memory_utils.h"
#include "aggregation_simplify_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../property/logical_property.h"
#include <algorithm>

namespace cascades {

/**
 * Check if a column ref is a constant value
 * In a full implementation, we would analyze the expression context
 * For now, we use a heuristic: column_id < 0 indicates a constant
 */
static bool is_constant_column(int column_id) {
    // Column IDs >= 0 are real columns
    // Column IDs < 0 represent constants in this simplified model
    return column_id < 0;
}

/**
 * Check if all group keys are constants
 */
bool AggregationSimplifyRule::is_group_by_constant(
    const std::vector<int>& group_keys) const {
    if (group_keys.empty()) {
        return false;  // No GROUP BY, not constant GROUP BY
    }
    return std::all_of(group_keys.begin(), group_keys.end(),
                       [](int key) { return is_constant_column(key); });
}

/**
 * Check if aggregation can be pushed below a join
 *
 * For INNER JOIN: aggregation can be pushed below if join keys
 * are a superset of the aggregation group keys
 *
 * For LEFT/RIGHT JOIN: aggregation can only be pushed to the
 * inner side if it's grouped on the outer key
 */
bool AggregationSimplifyRule::can_push_aggregate_below_join(
    OptExpression* agg, OptExpression* join, bool& push_to_left) const {
    // Get the join operator
    OptExprOp* join_op = join->get_op();
    if (join_op->get_type() != OptExprOpType::LOGICAL_JOIN) {
        return false;
    }

    LogicalJoin* logical_join = static_cast<LogicalJoin*>(join_op);
    JoinType join_type = logical_join->get_join_type();

    // Get aggregation group keys
    LogicalAggregate* aggregate = static_cast<LogicalAggregate*>(agg->get_op());
    const std::vector<int>& agg_group_keys = aggregate->get_group_keys();

    // Get join keys
    const std::vector<int>& join_keys = logical_join->get_join_keys();

    // For INNER JOIN: we can push aggregation below if join keys
    // are a superset of aggregation group keys
    if (join_type == JOIN_INNER) {
        // Check if agg_group_keys is subset of join_keys
        // Simplified: just check if we have join keys
        if (!join_keys.empty()) {
            push_to_left = true;  // Can push to left child
            return true;
        }
    }
    // For LEFT JOIN: can push aggregation to right (inner) side
    // if grouped on left join key
    else if (join_type == JOIN_LEFT) {
        // Aggregation on left side can be pushed below left child
        if (!agg_group_keys.empty() &&
            std::includes(join_keys.begin(), join_keys.end(),
                         agg_group_keys.begin(), agg_group_keys.end())) {
            push_to_left = true;
            return true;
        }
    }
    // For RIGHT JOIN: can push aggregation to left (inner) side
    else if (join_type == JOIN_RIGHT) {
        if (!agg_group_keys.empty() &&
            std::includes(join_keys.begin(), join_keys.end(),
                         agg_group_keys.begin(), agg_group_keys.end())) {
            push_to_left = false;  // Push to right child
            return true;
        }
    }

    return false;
}

/**
 * Check if outer aggregate is redundant given inner aggregate
 * This happens when inner aggregate groups on a superset of outer keys
 */
bool AggregationSimplifyRule::is_redundant_aggregate(
    OptExpression* outer_agg, OptExpression* inner_agg) const {
    OptExprOp* inner_op = inner_agg->get_op();
    if (inner_op->get_type() != OptExprOpType::LOGICAL_AGGREGATE) {
        return false;
    }

    LogicalAggregate* outer = static_cast<LogicalAggregate*>(outer_agg->get_op());
    LogicalAggregate* inner = static_cast<LogicalAggregate*>(inner_op);

    const std::vector<int>& outer_keys = outer->get_group_keys();
    const std::vector<int>& inner_keys = inner->get_group_keys();

    // If outer has no GROUP BY (global aggregate) and inner has GROUP BY,
    // outer is NOT redundant - it's computing a different thing
    if (outer_keys.empty()) {
        return false;
    }

    // If inner groups on a superset of outer keys, outer might be redundant
    // Actually, if inner groups on same keys, then outer agg is redundant
    // because inner already does the grouping
    if (outer_keys == inner_keys) {
        return true;
    }

    // If inner groups on a superset of outer keys, then the inner result
    // has fewer groups than outer would expect - not redundant
    // (this is actually an error in the original query if it expected same result)
    if (std::includes(inner_keys.begin(), inner_keys.end(),
                      outer_keys.begin(), outer_keys.end())) {
        // Inner has more groups (superset), outer is not redundant
        return false;
    }

    return false;
}

/**
 * Remove constant group keys from aggregation
 */
OptExpression* AggregationSimplifyRule::remove_constant_group_keys(
    OptExpression* agg) const {
    LogicalAggregate* aggregate = static_cast<LogicalAggregate*>(agg->get_op());
    const std::vector<int>& group_keys = aggregate->get_group_keys();

    // Filter out constant keys
    std::vector<int> non_constant_keys;
    for (int key : group_keys) {
        if (!is_constant_column(key)) {
            non_constant_keys.push_back(key);
        }
    }

    // If no non-constant keys remain, this is a global aggregate
    // (no GROUP BY needed)
    LogicalAggregate* new_agg = NEW_EX(LogicalAggregate, non_constant_keys);
    OptExpression* new_expr = OptExpression::create(new_agg, agg->input_at(0));

    return new_expr;
}

/**
 * Create local aggregate for push-down below join
 */
OptExpression* AggregationSimplifyRule::create_local_aggregate(
    OptExpression* agg, OptExpression* child,
    const std::vector<int>& local_keys) const {
    // Create new aggregate with local keys (subset of original)
    LogicalAggregate* local_agg = NEW_EX(LogicalAggregate, local_keys);

    OptExpression* result = OptExpression::create(local_agg, child);
    return result;
}

AggregationSimplifyRule* AggregationSimplifyRule::instance() {
    static AggregationSimplifyRule inst;
    return &inst;
}

AggregationSimplifyRule::AggregationSimplifyRule()
    : Rule(RuleType::TF_AGGREGATION_SIMPLIFY,
           Pattern::create(OptExprOpType::LOGICAL_AGGREGATE)) {}

std::vector<OptExpression*> AggregationSimplifyRule::transform(
    OptExpression* input, OptimizerContext* context) {
    std::vector<OptExpression*> result;

    OptExprOp* op = input->get_op();
    if (op->get_type() != OptExprOpType::LOGICAL_AGGREGATE) {
        return result;
    }

    LogicalAggregate* agg = static_cast<LogicalAggregate*>(op);
    const std::vector<int>& group_keys = agg->get_group_keys();

    // =========================================================================
    // Case 1: Eliminate unnecessary GROUP BY (GROUP BY constants)
    // =========================================================================
    // Example: SELECT COUNT(*) FROM t GROUP BY 1, 2, 3
    // If columns 1, 2, 3 are all constants, the GROUP BY is unnecessary
    if (!group_keys.empty() && is_group_by_constant(group_keys)) {
        OptExpression* simplified = remove_constant_group_keys(input);
        result.push_back(simplified);
    }

    // =========================================================================
    // Case 2: Remove global aggregate if input is already aggregated
    // =========================================================================
    // Example: SELECT SUM(x) FROM (SELECT SUM(x) FROM t GROUP BY y) sub
    // The outer SUM is redundant if y is the same as the group key
    if (input->arity() > 0) {
        OptExpression* child = input->input_at(0);
        OptExprOp* child_op = child->get_op();

        if (child_op->get_type() == OptExprOpType::LOGICAL_AGGREGATE) {
            if (is_redundant_aggregate(input, child)) {
                // The inner aggregate already groups by the same keys,
                // so the outer aggregate is redundant
                // We can just return the child directly (with identity projection)
                LogicalProject* identity_proj_op = NEW_EX(LogicalProject);
                OptExpression* identity_proj = OptExpression::create(identity_proj_op, child);
                result.push_back(identity_proj);
            }
        }
    }

    // =========================================================================
    // Case 3: Push aggregation below join
    // =========================================================================
    // Example: SELECT COUNT(*) FROM a INNER JOIN b ON a.id = b.id GROUP BY a.id
    // Can be transformed to push the aggregation below the join
    if (input->arity() > 0) {
        OptExpression* child = input->input_at(0);
        OptExprOp* child_op = child->get_op();

        if (child_op->get_type() == OptExprOpType::LOGICAL_JOIN) {
            bool push_to_left = true;
            if (can_push_aggregate_below_join(input, child, push_to_left)) {
                OptExpression* left_child = child->input_at(0);
                OptExpression* right_child = child->input_at(1);

                // Create local aggregates on both sides
                // The local keys should be the join keys that are on that side
                LogicalAggregate* orig_agg =
                    static_cast<LogicalAggregate*>(input->get_op());
                const std::vector<int>& orig_keys = orig_agg->get_group_keys();

                // Simplified: use all group keys for both sides
                // In a full implementation, would partition keys by side
                OptExpression* left_local_agg = create_local_aggregate(
                    input, left_child, orig_keys);
                OptExpression* right_local_agg = create_local_aggregate(
                    input, right_child, orig_keys);

                // Re-create the join with local aggregates
                LogicalJoin* orig_join =
                    static_cast<LogicalJoin*>(child->get_op());
                LogicalInnerJoin* new_join = NEW_EX(LogicalInnerJoin);
                new_join->set_join_keys(orig_join->get_join_keys());
                new_join->set_join_type(orig_join->get_join_type());

                OptExpression* new_join_expr = OptExpression::create(
                    new_join, left_local_agg, right_local_agg);

                // Create final aggregate on top
                LogicalAggregate* final_agg = NEW_EX(LogicalAggregate, orig_keys);
                OptExpression* final_expr = OptExpression::create(
                    final_agg, new_join_expr);

                result.push_back(final_expr);
            }
        }
    }

    // =========================================================================
    // Case 4: Convert global aggregate to local aggregate when possible
    // =========================================================================
    // Example: SELECT SUM(x) FROM t
    // If t is partitioned, we can compute partial sums locally
    // This requires checking the distribution property of the child
    // For now, we just mark this as a potential optimization point

    return result;
}


}  // namespace cascades