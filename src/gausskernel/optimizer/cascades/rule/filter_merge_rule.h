#pragma once
#include "rule.h"

namespace cascades {


/**
 * FilterMergeRule - Merges consecutive filter operators
 *
 * This rule handles several cases:
 * 1. Merge consecutive filters: Filter(Filter(child)) -> Filter(child)
 * 2. Merge equivalent filters: Filter(A), Filter(A) -> Filter(A)
 * 3. Eliminate redundant filters: when child filter is a superset of parent
 * 4. Handle filter push-down through LogicalProject
 */
class FilterMergeRule : public Rule {
public:
    FilterMergeRule();

    int promise() const override { return 3; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

private:
    /**
     * Check if two predicates are equivalent
     */
    bool are_predicates_equivalent(int pred_id1, int pred_id2) const;

    /**
     * Check if predicate1 is a subset of predicate2
     */
    bool is_predicate_subset(int pred_id1, int pred_id2) const;
};

}