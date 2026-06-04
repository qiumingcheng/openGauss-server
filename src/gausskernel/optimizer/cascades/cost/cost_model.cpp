#include "cost_model.h"
#include "../memo/group_expression.h"
#include "../memo/group.h"
#include "../statistics/statistics.h"
#include "../operator/operator.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"
#include "pg_config.h"
#include <algorithm>

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
        case OptExprOpType::PHYSICAL_PASSTHROUGH:
            cost += CostEstimate(DEFAULT_CPU_TUPLE_COST, 0, 0);
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
    /*
     * Hash join cost = cost_build + cost_probe + cost_output
     *
     * cost_build  = inner_rows * (DEFAULT_CPU_TUPLE_COST + 0.001)  // Build hash table from right child
     * cost_probe  = outer_rows * DEFAULT_CPU_TUPLE_COST * (1 + innerbucketsize * 0.5)  // Probe with left child
     * cost_output = output_rows * DEFAULT_CPU_TUPLE_COST  // Output result tuples
     *
     * innerbucketsize ≈ inner_rows / default_numbuckets (hash collision factor)
     * 0.5 because hash miss skips qual evaluation
     */
    const auto& inputs = expr->get_inputs();
    double inner_rows = 1.0;  // right child (index 1)
    double outer_rows = 1.0;  // left child (index 0)

    if (inputs.size() >= 2 && inputs[1] != nullptr) {
        Statistics* inner_stats = inputs[1]->get_statistics();
        if (inner_stats != nullptr) {
            inner_rows = inner_stats->row_count();
            if (inner_rows < 1) inner_rows = 1;
        }
    }

    if (inputs.size() >= 1 && inputs[0] != nullptr) {
        Statistics* outer_stats = inputs[0]->get_statistics();
        if (outer_stats != nullptr) {
            outer_rows = outer_stats->row_count();
            if (outer_rows < 1) outer_rows = 1;
        }
    }

    double output_rows = (stats != nullptr) ? stats->row_count() : outer_rows;
    if (output_rows < 1) output_rows = 1;

    // Default hash table bucket count
    static const int DEFAULT_NUMBUCKETS = 64;
    double innerbucketsize = std::max(1.0, inner_rows / DEFAULT_NUMBUCKETS);

    // Build phase: right table builds hash table
    double cost_build = inner_rows * (DEFAULT_CPU_TUPLE_COST + 0.001);

    // Probe phase: left table probes hash table
    double cost_probe = outer_rows * DEFAULT_CPU_TUPLE_COST * (1.0 + innerbucketsize * 0.5);

    // Output phase: output result tuples
    double cost_output = output_rows * DEFAULT_CPU_TUPLE_COST;

    return CostEstimate(cost_build + cost_probe + cost_output, 0, 0);
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

    double output_rows = (stats != nullptr) ? stats->row_count() : outer_rows;
    if (output_rows < 1) output_rows = 1;

    // Output phase: output result tuples
    double cost_output = output_rows * DEFAULT_CPU_TUPLE_COST;

    // NestLoop: for each outer row, scan inner once
    double inner_cost = children[1].cpu_cost(); 
    double cost = outer_rows * inner_cost + cost_output;
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
    double startup_cost = 0;
    if (children.size() > 0) {
        startup_cost = children[0].cpu_cost();
    }
    if (stats != nullptr) {
        double rows = stats->row_count();
        if (rows < 2.0) {
            rows = 2.0;
        }

        // Estimate row width (average ~30 bytes for typical tuples)
        double width = 30.0;
        double input_bytes = rows * width;

        // sort_mem in KB, typical default is 16384KB (16MB)
        long sort_mem_bytes = 16384L * 1024L;

        // comparison_cost = 2.0 * cpu_operator_cost
        double comparison_cost = 2.0 * DEFAULT_CPU_TUPLE_COST;

        // CPU cost: N log2 N comparisons
        startup_cost += comparison_cost * rows * (std::log(rows) / std::log(2.0));

        // Per-tuple cost (like cpu_operator_cost * tuples in cost_sort)
        cost += DEFAULT_CPU_TUPLE_COST * rows + startup_cost;

        // Disk I/O cost (only if data exceeds sort_mem)
        if (input_bytes > sort_mem_bytes) {
            double npages = std::ceil(input_bytes / (double)BLCKSZ);
            double nruns = (input_bytes / sort_mem_bytes) * 0.5;
            double mergeorder = 6.0;  // tuplesort_merge_order approximation
            double log_runs = (nruns > mergeorder) ?
                std::ceil(std::log(nruns) / std::log(mergeorder)) : 1.0;
            double npageaccesses = 2.0 * npages * log_runs;
            // Assume 3/4 sequential, 1/4 random access
            double disk_cost = npageaccesses * (DEFAULT_SEQ_PAGE_COST * 0.75 + DEFAULT_RANDOM_PAGE_COST * 0.25);
            startup_cost += disk_cost;
            cost += disk_cost;
        }
    }
    return CostEstimate(cost, 0, 0);
}

}  // namespace cascades
