#include "../base/memory_utils.h"
#include "outer_join_simplify_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../property/logical_property.h"

namespace cascades {

/**
 * Check if an expression contains only strict operators
 * For a simple implementation, we check:
 * 1. The predicate ID references the inner side of an outer join
 * 2. The predicate contains a strict comparison (=, <>, <, >, <=, >= with constant)
 *
 * In a full implementation, this would analyze the actual expression tree
 */
static bool is_strict_comparison(int predicate_id) {
    // Simplified heuristic:
    // For a real implementation, we would analyze the expression tree
    // to determine if it's strict. Here we assume predicates with
    // predicate_id >= 100 are comparisons with constants
    return predicate_id >= 100;
}

/**
 * Check if predicate is on the inner side of an outer join
 * and contains strict operators
 */
static bool can_reduce_left_join(int predicate_id, bool predicate_on_right) {
    // For LEFT JOIN: if WHERE condition references right side and is strict,
    // we can reduce to INNER JOIN
    // Simplified: assume predicate_id > 0 means there's a meaningful predicate
    if (predicate_id > 0 && predicate_on_right) {
        // Check if predicate is strict
        if (is_strict_comparison(predicate_id)) {
            return true;
        }
    }
    return false;
}

OuterJoinSimplifyRule* OuterJoinSimplifyRule::instance() {
    static OuterJoinSimplifyRule inst;
    return &inst;
}

OuterJoinSimplifyRule::OuterJoinSimplifyRule()
    : Rule(RuleType::TF_OUTER_JOIN_SIMPLIFY,
           Pattern::create(OptExprOpType::LOGICAL_JOIN, Pattern::any(), Pattern::any())) {}

std::vector<OptExpression*> OuterJoinSimplifyRule::transform(
    OptExpression* input, OptimizerContext* context) {
    std::vector<OptExpression*> result;

    OptExprOp* op = input->get_op();
    if (op->get_type() != OptExprOpType::LOGICAL_JOIN) {
        return result;
    }

    LogicalJoin* join = static_cast<LogicalJoin*>(op);
    JoinType join_type = join->get_join_type();

    OptExpression* left = input->input_at(0);
    OptExpression* right = input->input_at(1);

    // Use join's own qual condition for simplification check.
    // Note: Parent Filter's predicate would require accessing parent context.
    // For now, check if join has a qual condition.
    Node* qual = join->get_qual();
    int predicate_id = (qual != nullptr) ? 1 : -1;

    // Case 1: LEFT JOIN with strict predicate on right side -> INNER JOIN
    if (join_type == JOIN_LEFT) {
        // Check if we have a WHERE clause that can reduce this
        // In a full implementation, we would analyze the parent's filter
        // For now, use heuristics based on the predicate

        // If join has a condition (predicate_id >= 0), we can consider reduction
        // if the predicate is strict
        if (can_reduce_left_join(predicate_id, true)) {
            LogicalInnerJoin* new_join = NEW_EX(LogicalInnerJoin);
            new_join->set_join_keys(join->get_join_keys());
            new_join->set_join_type(JOIN_INNER);

            OptExpression* new_expr = OptExpression::create(new_join, left, right);
            result.push_back(new_expr);
        }
    }
    // Case 2: RIGHT JOIN -> flip to LEFT JOIN, then reduce
    else if (join_type == JOIN_RIGHT) {
        // Flip RIGHT JOIN to LEFT JOIN: (A RIGHT JOIN B) -> (B LEFT JOIN A)
        LogicalInnerJoin* flipped_join = NEW_EX(LogicalInnerJoin);
        flipped_join->set_join_keys(join->get_join_keys());
        flipped_join->set_join_type(JOIN_LEFT);

        OptExpression* new_expr = OptExpression::create(flipped_join, right, left);
        result.push_back(new_expr);
    }
    // Case 3: FULL OUTER JOIN - complex, currently not simplified
    // In a full implementation, if one side is provably non-null,
    // FULL JOIN could reduce to LEFT or RIGHT JOIN
    else if (join_type == JOIN_FULL) {
        // For now, skip FULL JOIN simplification
        // Full implementation would check:
        // 1. If WHERE clause references only one side with strict predicates
        // 2. If one side's join key is a primary key (guaranteed non-null)
    }
    // Case 4: LEFT ANTI JOIN - check if can reduce to regular ANTI
    else if (join_type == JOIN_LEFT_ANTI_FULL) {
        // If the join qual is strict for inner side, could simplify
        // For now, return as-is
    }
    // INNER JOIN - already simplified, no action needed
    else if (join_type == JOIN_INNER) {
        // INNER JOINs don't need simplification
    }
    // SEMI JOIN - currently not simplified
    else {
        // JOIN_SEMI, JOIN_ANTI, etc. - skip for now
    }

    return result;
}


}  // namespace cascades