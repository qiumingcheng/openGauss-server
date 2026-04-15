#pragma once
#include "rule.h"
#include "../memo/opt_expression.h"

namespace cascades {


/**
 * SubqueryUnnestRule - Transforms subqueries in the join tree
 *
 * Based on OpenGauss's pull_up_subqueries optimization:
 * 1. Correlated subquery unnesting: pulls up subqueries that reference
 *    outer query variables to the parent level
 * 2. Uncorrelated subquery conversion: converts non-correlated subqueries
 *    to semi-join or anti-join where appropriate
 * 3. FROM clause subquery handling: flattens subqueries in FROM clause
 *
 * This rule operates on LOGICAL_SUBQUERY operators in the expression tree.
 * The transformation can:
 * - Reduce query execution time by eliminating nested loop execution
 * - Enable more efficient join ordering
 * - Allow push-down of predicates and projections
 *
 * Examples:
 *   SELECT * FROM a WHERE a.id IN (SELECT b.id FROM b)
 *   -> converted to SEMI JOIN if subquery is uncorrelated
 *
 *   SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id)
 *   -> correlated subquery pulled up and joined with a
 */
class SubqueryUnnestRule : public Rule {
public:
    static SubqueryUnnestRule* instance();
    SubqueryUnnestRule();

    int promise() const override { return 2; }  // medium-high priority

    bool check(OptExpression* input, OptimizerContext* context) const override;

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

private:
    /**
     * Check if the subquery is correlated (references outer query vars)
     */
    static bool is_correlated_subquery(OptExpression* subquery_expr);

    /**
     * Try to convert uncorrelated subquery to semi-join or anti-join
     */
    static OptExpression* convert_to_semi_join(
        OptExpression* subquery_expr, OptExpression* parent_expr);

    /**
     * Unnest a correlated subquery by pulling it up to the parent level
     */
    static OptExpression* unnest_correlated_subquery(
        OptExpression* subquery_expr, OptExpression* parent_expr);

    /**
     * Check if the subquery can be safely unnested
     * (no aggregates, window functions, set operations, etc.)
     */
    static bool can_unnest_subquery(OptExpression* subquery_expr);
};

}