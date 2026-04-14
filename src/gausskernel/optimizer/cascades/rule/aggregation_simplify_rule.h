#pragma once
#include "rule.h"
#include "../memo/opt_expression.h"

namespace cascades {


/**
 * AggregationSimplifyRule - Simplifies aggregate operations
 *
 * This rule implements several aggregation simplification optimizations:
 *
 * 1. Eliminate unnecessary GROUP BY:
 *    - GROUP BY on constant values can be removed
 *    - When query has no GROUP BY but has aggregate functions,
 *      it's a global aggregate (single group)
 *
 * 2. Convert global aggregate to local aggregate:
 *    - When aggregation is on a joined column that is already
 *      grouped by the join's inner relation
 *
 * 3. Push aggregation below join:
 *    - For INNER JOIN, aggregation can be pushed below join
 *      if the join keys are superset of group keys
 *    - For LEFT JOIN, aggregation can be pushed to the inner side
 *      if it's grouped on the outer key
 *
 * 4. Remove duplicate aggregations:
 *    - If an aggregate's input is already aggregated on the same
 *      or superset of keys, the outer aggregate is redundant
 *
 * Examples:
 *   SELECT COUNT(*) FROM t GROUP BY 1  -- if 1 is constant
 *   => SELECT COUNT(*) FROM t          -- remove GROUP BY
 *
 *   SELECT COUNT(*) FROM a, b GROUP BY a.id
 *   => SELECT COUNT(*) FROM a INNER JOIN b ON ... GROUP BY a.id
 *      (can be pushed down with proper grouping)
 *
 *   SELECT SUM(x) FROM (SELECT SUM(x) FROM t GROUP BY y) sub
 *   => SELECT SUM(x) FROM (SELECT SUM(x) FROM t GROUP BY y) sub
 *      (inner aggregation is redundant if y is same)
 */
class AggregationSimplifyRule : public Rule {
public:
    AggregationSimplifyRule();

    static AggregationSimplifyRule* instance();

    int promise() const override { return 2; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

private:
    /**
     * Check if all group keys are constants
     */
    bool is_group_by_constant(const std::vector<int>& group_keys) const;

    /**
     * Check if we can push aggregation below join
     */
    bool can_push_aggregate_below_join(OptExpression* agg,
                                        OptExpression* join,
                                        bool& push_to_left) const;

    /**
     * Check if outer aggregate is redundant given inner aggregate
     */
    bool is_redundant_aggregate(OptExpression* outer_agg,
                                 OptExpression* inner_agg) const;

    /**
     * Remove constant group keys from aggregation
     */
    OptExpression* remove_constant_group_keys(OptExpression* agg) const;

    /**
     * Create local aggregate from global aggregate
     */
    OptExpression* create_local_aggregate(OptExpression* agg,
                                           OptExpression* child,
                                           const std::vector<int>& local_keys) const;
};

}