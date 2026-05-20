#include "cost_model.h"
#include "../memo/group_expression.h"
#include "../memo/group.h"
#include "../statistics/statistics.h"
#include "../operator/operator.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"

namespace cascades {

static std::vector<CostEstimate> deal_children_costs(GroupExpression* group_expr,
                                const PhysicalPropertySet& required_property,
                                const std::vector<CostEstimate>& children_costs) {
        
    if(!children_costs.empty()) {
        return children_costs;
    }

    std::vector<CostEstimate> ret;
    for(int i=0; i<group_expr->arity(); i++) {
        Group* children = group_expr->input_at(i);
        GroupExpression* ge = children->get_best_expression(required_property);
        double cost = ge ? ge->get_cost(required_property) : 0.0;
        ret.push_back(CostEstimate(cost, 0, 0));
    }
    return ret;
}

CostEstimate CostModel::compute_cost(GroupExpression* group_expr,
                                      const PhysicalPropertySet& required_property,
                                      const std::vector<CostEstimate>& in_costs) {
    CostEstimate cost;

    OptExprOp* op = group_expr->get_op();

    // Try to use OpenGauss Path cost first
    if (op->is_physical()) {
        PhysicalOptExprOp* phys_op = static_cast<PhysicalOptExprOp*>(op);
        Path* path = phys_op->get_path();
        if (path != nullptr) {
            // Use OpenGauss computed cost
            cost.set_cpu_cost(path->total_cost);
            cost.set_memory_cost(0);
            cost.set_network_cost(0);
            return cost;
        }
    }

    // Fallback: use cascades own cost estimation
    Statistics* stats = group_expr->get_group()->get_statistics();
    std::vector<CostEstimate> children_costs = deal_children_costs(group_expr, required_property, in_costs);

    // Physical operators have execution cost
    switch (op->get_type()) {
        case OptExprOpType::PHYSICAL_HASH_JOIN:
            cost += compute_hash_join_cost(group_expr, stats, children_costs);
            break;
        case OptExprOpType::PHYSICAL_MERGE_JOIN:
            cost += compute_merge_join_cost(group_expr, stats, children_costs);
            break;
        case OptExprOpType::PHYSICAL_NESTLOOP_JOIN:
            cost += compute_nestloop_join_cost(group_expr, stats, children_costs);
            break;
        case OptExprOpType::PHYSICAL_SEQ_SCAN:
        case OptExprOpType::PHYSICAL_INDEX_SCAN:
            cost += compute_scan_cost_impl(stats);
            break;
        case OptExprOpType::PHYSICAL_HASH_AGG:
            cost += compute_agg_cost_impl(stats, children_costs);
            break;
        case OptExprOpType::PHYSICAL_SORT:
            cost += compute_sort_cost_impl(stats, children_costs);
            break;
        default:
            // Default per-tuple cost
            if (stats != nullptr) {
                cost += CostEstimate(stats->row_count() * DEFAULT_CPU_TUPLE_COST, 0, 0);
            }
            break;
    }

    return cost;
}

CostEstimate CostModel::compute_hash_join_cost(const GroupExpression* expr, Statistics* stats,
                                              const std::vector<CostEstimate>& children) {
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
                                                const std::vector<CostEstimate>& children) {
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
                                                   const std::vector<CostEstimate>& children) {
    /*
     * NestLoop join cost = outer_rows * inner_cost
     * where inner_cost is the cost to scan/process inner relation once per outer row.
     *
     * Get row counts directly from child groups' statistics.
     */
    const auto& inputs = expr->get_inputs();
    double outer_rows = 1.0;
    double inner_rows = 1.0;

    // Get outer group (left child, index 0) row count
    if (inputs.size() >= 1 && inputs[0] != nullptr) {
        Statistics* outer_stats = inputs[0]->get_statistics();
        if (outer_stats != nullptr) {
            outer_rows = outer_stats->row_count();
            if (outer_rows < 1) outer_rows = 1;
        }
    }

    // Get inner group (right child, index 1) row count
    Statistics* inner_stats = inputs[1]->get_statistics();
    if (inner_stats != nullptr) {
        inner_rows = inner_stats->row_count();
        if (inner_rows < 1) inner_rows = 1;
    }

    
    // Inner cost = cost to process one inner row (CPU per tuple)
    double inner_cost = children[1].cpu_cost() / inner_rows;
    // NestLoop: for each outer row, scan inner once
    double cost = outer_rows * inner_cost;
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
                                             const std::vector<CostEstimate>& children) {
    double cost = 0;
    if (children.size() > 0) {
        cost = children[0].cpu_cost();
    }
    if (stats != nullptr) {
        // Aggregation overhead: O(n)
        cost += stats->row_count() * DEFAULT_CPU_TUPLE_COST;
    }
    return CostEstimate(cost, 0, 0);
}

CostEstimate CostModel::compute_sort_cost_impl(Statistics* stats,
                                               const std::vector<CostEstimate>& children) {
    double cost = 0;
    if (children.size() > 0) {
        cost = children[0].cpu_cost();
    }
    if (stats != nullptr) {
        // Sort overhead: O(n log n)
        double rows = stats->row_count();
        cost += rows * std::log2(rows > 1 ? rows : 2) * DEFAULT_CPU_TUPLE_COST;
    }
    return CostEstimate(cost, 0, 0);
}

}  // namespace cascades
