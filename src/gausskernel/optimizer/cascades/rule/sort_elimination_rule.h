#pragma once
#include "rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"

namespace cascades {


/**
 * SortEliminationRule - Eliminates unnecessary Sort operators
 *
 * A Sort operator can be eliminated when the child already produces data
 * in the required sorted order. This happens in several scenarios:
 *
 * 1. Child is already sorted:
 *    - Child is a PhysicalSort with the same or a prefix of the required order
 *    - The child's sort order satisfies the parent's required order
 *
 * 2. Child produces inherently sorted output:
 *    - PhysicalMergeJoin produces sorted output on join keys
 *    - Index scan (PhysicalScan with index) preserves index order
 *    - PhysicalHashAgg produces sorted output on group keys
 *
 * 3. No ordering change in between:
 *    - LogicalProject doesn't change the order of rows
 *    - LogicalFilter doesn't change the order of rows
 *
 * Examples:
 *   SELECT * FROM t ORDER BY a  -- if t has index on 'a', scan produces sorted
 *   SELECT * FROM (SELECT * FROM t ORDER BY a) ORDER BY a  -- inner sort redundant
 *   SELECT * FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a  -- merge join output sorted
 */
class SortEliminationRule : public Rule {
public:
    SortEliminationRule();

    int promise() const override { return 2; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

private:
    /**
     * Check if the child's output order satisfies the required sort order
     */
    bool is_child_order_satisfying(PhysicalSort* sort, OptExpression* child,
                                   const OrderSpec& required_spec) const;

    /**
     * Check if child is a PhysicalSort whose order satisfies required order
     */
    bool can_eliminate_child_sort(PhysicalSort* sort, OptExpression* child,
                                   const OrderSpec& required_spec) const;

    /**
     * Check if child is a MergeJoin which produces sorted output
     */
    bool can_eliminate_merge_join(PhysicalSort* sort, OptExpression* child,
                                   const OrderSpec& required_spec) const;

    /**
     * Check if child is a HashAgg which produces sorted output
     */
    bool can_eliminate_hash_agg(PhysicalSort* sort, OptExpression* child,
                                const OrderSpec& required_spec) const;

    /**
     * Check if child is a scan that produces sorted output (e.g., index scan)
     */
    bool can_eliminate_scan(PhysicalSort* sort, OptExpression* child,
                           const OrderSpec& required_spec) const;

    /**
     * Check if the required order is a prefix of the child's order
     */
    bool is_prefix_order(const OrderSpec& required, const OrderSpec& child) const;

    /**
     * Extract sort order from a PhysicalSort operator
     */
    OrderSpec get_sort_order(const PhysicalSort* sort) const;
};

}