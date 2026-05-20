#pragma once
#include "cost_estimate.h"
#include <vector>
#include <cmath>

namespace cascades {


class GroupExpression;
class PhysicalPropertySet;
class Statistics;

class CostModel {
public:
    CostModel() = default;
    ~CostModel() = default;

    CostEstimate compute_cost(GroupExpression* group_expr,
                              const PhysicalPropertySet& required_property,
                              const std::vector<CostEstimate>& children_costs);

    static constexpr double DEFAULT_CPU_TUPLE_COST = 0.01;
    static constexpr double DEFAULT_RANDOM_PAGE_COST = 4.0;
    static constexpr double DEFAULT_SEQ_PAGE_COST = 1.0;

private:
    CostEstimate compute_hash_join_cost(const GroupExpression* expr, Statistics* stats,
                                         const std::vector<CostEstimate>& children);
    CostEstimate compute_merge_join_cost(const GroupExpression* expr, Statistics* stats,
                                         const std::vector<CostEstimate>& children);
    CostEstimate compute_nestloop_join_cost(const GroupExpression* expr, Statistics* stats,
                                            const std::vector<CostEstimate>& children);
    CostEstimate compute_scan_cost_impl(Statistics* stats);
    CostEstimate compute_agg_cost_impl(Statistics* stats, const std::vector<CostEstimate>& children);
    CostEstimate compute_sort_cost_impl(Statistics* stats, const std::vector<CostEstimate>& children);
};

}