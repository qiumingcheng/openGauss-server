#include "cost_model.h"
#include "../memo/group_expression.h"
#include "../memo/group.h"
#include "../statistics/statistics.h"
#include "../operator/operator.h"
#include "optimizer/pathnode.h"

namespace cascades {

CostEstimate* CostModel::compute_cost(GroupExpression* group_expr,
                                      const PhysicalPropertySet& required_property,
                                      const std::vector<CostEstimate*>& children_costs) {
    CostEstimate* cost = new CostEstimate();

    OptExprOp* op = group_expr->get_op();

    // Try to use OpenGauss Path cost first
    if (op->is_physical()) {
        PhysicalOptExprOp* phys_op = static_cast<PhysicalOptExprOp*>(op);
        Path* path = phys_op->get_path();
        if (path != nullptr) {
            // Use OpenGauss computed cost
            cost->set_cpu_cost(path->total_cost);
            cost->set_memory_cost(0);
            cost->set_network_cost(0);
            return cost;
        }
    }

    // Fallback: use cascades own cost estimation
    Statistics* stats = group_expr->get_group()->get_statistics();

    // Add children's costs
    for (auto* child_cost : children_costs) {
        *cost += *child_cost;
    }

    // Compute operator-specific cost based on type
    if (op->is_logical()) {
        // Logical operators have no execution cost
        return cost;
    }

    // Physical operators have execution cost
    switch (op->get_type()) {
        case OptExprOpType::PHYSICAL_HASH_JOIN:
            *cost += compute_hash_join_cost(group_expr, stats, children_costs);
            break;
        case OptExprOpType::PHYSICAL_MERGE_JOIN:
            *cost += compute_merge_join_cost(group_expr, stats, children_costs);
            break;
        case OptExprOpType::PHYSICAL_NESTLOOP_JOIN:
            *cost += compute_nestloop_join_cost(group_expr, stats, children_costs);
            break;
        case OptExprOpType::PHYSICAL_SCAN:
            *cost += compute_scan_cost_impl(stats);
            break;
        case OptExprOpType::PHYSICAL_HASH_AGG:
            *cost += compute_agg_cost_impl(stats, children_costs);
            break;
        case OptExprOpType::PHYSICAL_SORT:
            *cost += compute_sort_cost_impl(stats, children_costs);
            break;
        default:
            // Default per-tuple cost
            if (stats != nullptr) {
                *cost += CostEstimate(stats->row_count() * DEFAULT_CPU_TUPLE_COST, 0, 0);
            }
            break;
    }

    return cost;
}

CostEstimate CostModel::compute_hash_join_cost(const GroupExpression* expr, Statistics* stats,
                                              const std::vector<CostEstimate*>& children) {
    // Hash join cost = cost of building hash table + cost of probing
    double cost = 0;
    if (stats != nullptr) {
        double rows = stats->row_count();
        // Build phase: O(n) hash computation
        cost += rows * DEFAULT_CPU_TUPLE_COST;
        // Memory cost for hash table (estimated at 2x row size)
        cost += rows * 0.001;  // memory cost factor
    }
    return CostEstimate(cost, 0, 0);
}

CostEstimate CostModel::compute_merge_join_cost(const GroupExpression* expr, Statistics* stats,
                                                const std::vector<CostEstimate*>& children) {
    // Merge join cost = sort cost + merge cost
    double cost = 0;
    if (stats != nullptr) {
        double rows = stats->row_count();
        // Sort both inputs: O(n log n)
        cost += rows * std::log2(rows > 1 ? rows : 2) * DEFAULT_CPU_TUPLE_COST;
    }
    return CostEstimate(cost, 0, 0);
}

CostEstimate CostModel::compute_nestloop_join_cost(const GroupExpression* expr, Statistics* stats,
                                                   const std::vector<CostEstimate*>& children) {
    // Nested loop join cost = outer_rows * inner_rows (cartesian product)
    double cost = 0;
    if (stats != nullptr && children.size() >= 2) {
        double outer_rows = children[0] != nullptr ? children[0]->cpu_cost() : 1;
        double inner_rows = children[1] != nullptr ? children[1]->cpu_cost() : 1;
        cost = outer_rows + outer_rows * inner_rows * DEFAULT_CPU_TUPLE_COST;
    }
    return CostEstimate(cost, 0, 0);
}

CostEstimate CostModel::compute_scan_cost_impl(Statistics* stats) {
    if (stats == nullptr) {
        return CostEstimate(0, 0, 0);
    }
    double rows = stats->row_count();
    // Sequential scan: cheap per tuple, plus page fetch cost
    double cpu_cost = rows * DEFAULT_CPU_TUPLE_COST;
    double memory_cost = 0;  // No significant memory for scan
    return CostEstimate(cpu_cost, memory_cost, 0);
}

CostEstimate CostModel::compute_agg_cost_impl(Statistics* stats,
                                             const std::vector<CostEstimate*>& children) {
    double cost = 0;
    if (children.size() > 0 && children[0] != nullptr) {
        cost = children[0]->cpu_cost();
    }
    if (stats != nullptr) {
        // Aggregation overhead: O(n)
        cost += stats->row_count() * DEFAULT_CPU_TUPLE_COST;
    }
    return CostEstimate(cost, 0, 0);
}

CostEstimate CostModel::compute_sort_cost_impl(Statistics* stats,
                                               const std::vector<CostEstimate*>& children) {
    double cost = 0;
    if (children.size() > 0 && children[0] != nullptr) {
        cost = children[0]->cpu_cost();
    }
    if (stats != nullptr) {
        // Sort overhead: O(n log n)
        double rows = stats->row_count();
        cost += rows * std::log2(rows > 1 ? rows : 2) * DEFAULT_CPU_TUPLE_COST;
    }
    return CostEstimate(cost, 0, 0);
}

}  // namespace cascades
