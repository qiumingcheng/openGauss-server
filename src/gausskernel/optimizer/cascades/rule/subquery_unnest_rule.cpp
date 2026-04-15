#include "subquery_unnest_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../property/logical_property.h"

namespace cascades {

SubqueryUnnestRule* SubqueryUnnestRule::instance() {
    static SubqueryUnnestRule inst;
    return &inst;
}

/**
 * SubqueryUnnestRule Implementation
 *
 * Based on OpenGauss's pull_up_subqueries function in prepjointree.cpp
 * and the is_simple_subquery, pull_up_simple_subquery helper functions.
 */

SubqueryUnnestRule::SubqueryUnnestRule()
    : Rule(RuleType::TF_SUBQUERY_UNNEST,
           Pattern::create(OptExprOpType::LOGICAL_SUBQUERY)) {}

bool SubqueryUnnestRule::check(OptExpression* input, OptimizerContext* context) const {
    (void)context;

    // Check that this is a subquery expression
    OptExprOp* op = input->get_op();
    if (op->get_type() != OptExprOpType::LOGICAL_SUBQUERY) {
        return false;
    }

    // Only process leaf subqueries (no nested subqueries at this stage)
    // or subqueries in FROM clause
    return input->arity() == 0 || input->arity() == 1;
}

std::vector<OptExpression*> SubqueryUnnestRule::transform(
    OptExpression* input, OptimizerContext* context) {
    (void)context;
    std::vector<OptExpression*> result;

    OptExprOp* op = input->get_op();
    if (op->get_type() != OptExprOpType::LOGICAL_SUBQUERY) {
        return result;
    }

    LogicalSubquery* subquery = static_cast<LogicalSubquery*>(op);

    // Check if this subquery can be unnested
    if (!can_unnest_subquery(input)) {
        // Cannot unnest - return input as-is
        result.push_back(input);
        return result;
    }

    // Check if the subquery is correlated
    bool correlated = is_correlated_subquery(input);
    subquery->set_correlated(correlated);

    if (correlated) {
        // Correlated subquery: pull up to parent
        // Reference: prepjointree.cpp pull_up_simple_subquery
        OptExpression* unnested = unnest_correlated_subquery(input, nullptr);
        if (unnested != nullptr) {
            result.push_back(unnested);
        } else {
            result.push_back(input);
        }
    } else {
        // Uncorrelated subquery: try to convert to semi-join or anti-join
        // Reference: prepjointree.cpp handling of IN/EXISTS with sublinks
        OptExpression* converted = convert_to_semi_join(input, nullptr);
        if (converted != nullptr) {
            result.push_back(converted);
        } else {
            result.push_back(input);
        }
    }

    return result;
}

bool SubqueryUnnestRule::is_correlated_subquery(OptExpression* subquery_expr) {
    // A subquery is correlated if it references variables from the outer query.
    // In a full implementation, we would analyze the expression tree to detect
    // var references that belong to the outer query's scope.
    //
    // For now, we use a simplified heuristic based on the subquery's properties.
    // A real implementation would traverse the expression and check Var nodes.

    LogicalSubquery* subquery = static_cast<LogicalSubquery*>(subquery_expr->get_op());
    if (subquery == nullptr) {
        return false;
    }

    // Check if subquery_id is set (indicates it was analyzed)
    // In a full implementation, we would check if any Var has a varlevelsup > 0
    return subquery->is_correlated();
}

bool SubqueryUnnestRule::can_unnest_subquery(OptExpression* subquery_expr) {
    // A subquery can be unnested if:
    // 1. It has no set operations (UNION, INTERSECT, EXCEPT)
    // 2. It has no aggregates (unless it's a simple aggregation that can be decorrelated)
    // 3. It has no window functions
    // 4. It has no LIMIT/OFFSET (for most cases)
    // 5. It's not a DISTINCT with aggregates
    //
    // Reference: is_simple_subquery in prepjointree.cpp

    OptExprOp* op = subquery_expr->get_op();
    if (op->get_type() != OptExprOpType::LOGICAL_SUBQUERY) {
        return false;
    }

    // For now, allow all subqueries to be considered for unnesting
    // A full implementation would check:
    // - No HAVING clause (or HAVING that can be pushed down)
    // - No set operations in the subquery
    // - Not a VALUES clause
    // - Not a WITH RECURSIVE

    return true;
}

OptExpression* SubqueryUnnestRule::convert_to_semi_join(
    OptExpression* subquery_expr, OptExpression* parent_expr) {
    // Convert an uncorrelated subquery to a semi-join or anti-join
    //
    // Reference: pull_up_sublinks handling of SubLink in OpenGauss
    //
    // This transformation applies to:
    // - IN (SELECT ...) -> SEMI JOIN
    // - NOT IN (SELECT ...) -> ANTI JOIN (with null-aware handling)
    // - EXISTS (SELECT ...) -> SEMI JOIN with TRUE qual
    // - NOT EXISTS (SELECT ...) -> ANTI JOIN with TRUE qual
    //
    // For the Cascades framework, we create a LogicalJoin with SEMI or ANTI type

    (void)parent_expr;

    LogicalSubquery* subquery = static_cast<LogicalSubquery*>(subquery_expr->get_op());
    if (subquery == nullptr) {
        return nullptr;
    }

    // For uncorrelated subqueries, we can convert to semi-join
    // The parent expression (e.g., LogicalFilter) would provide the context
    // for determining if this is IN, EXISTS, NOT IN, or NOT EXISTS

    // For a simple implementation, return the subquery as-is
    // A full implementation would:
    // 1. Look at the parent filter's predicate to determine join type
    // 2. Create a LogicalJoin (SEMI or ANTI) with the subquery as one child
    // 3. The other child would be the relation being joined to

    // Since we don't have full parent context here, return nullptr
    // to indicate no transformation was applied
    return nullptr;
}

OptExpression* SubqueryUnnestRule::unnest_correlated_subquery(
    OptExpression* subquery_expr, OptExpression* parent_expr) {
    // Pull up a correlated subquery to the parent level
    //
    // Reference: pull_up_simple_subquery in prepjointree.cpp
    //
    // The key steps are:
    // 1. Create a modified copy of the subquery's expression tree
    // 2. Adjust variable levels (subquery vars become outer vars)
    // 3. Replace references to subquery outputs with the actual output expressions
    // 4. Merge the subquery's from list into the parent's from list
    // 5. Return the unnested expression

    (void)parent_expr;

    // For a full implementation, we would:
    // 1. Clone the subquery expression
    // 2. Use pullup_replace_vars context to adjust variable references
    // 3. Merge the subquery's join tree with the parent's join tree
    // 4. Return the combined expression

    // Since we don't have the full OpenGauss infrastructure here,
    // we return nullptr to indicate the transformation needs more context

    // A simplified implementation would be:
    // - If subquery has a single table scan, merge it into parent
    // - If subquery has a join, create a new join with parent

    return nullptr;
}


}  // namespace cascades