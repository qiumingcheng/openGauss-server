#include "enforce_and_cost_task.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../cost/cost_model.h"
#include "../cost/cost_estimate.h"
#include "../statistics/statistics.h"
#include "task_context.h"
#include "optimizer_task.h"
#include <cmath>
#include <limits>

namespace cascades {
EnforceAndCostTask::EnforceAndCostTask(TaskContext* context, Group* group,
                                       const PhysicalPropertySet& required_property)
    : OptimizerTask(context), group_(group), required_property_(required_property) {}

void EnforceAndCostTask::execute() {
    CostModel cost_model;
    double best_cost = std::numeric_limits<double>::infinity();
    GroupExpression* best_expr = nullptr;
    PhysicalPropertySet best_output_property;

    for (auto* expr : group_->get_physical_expressions()) {
        // Compute children's costs and get children's best expressions
        std::vector<double> children_costs;
        std::vector<GroupExpression*> children_best_exprs;
        bool all_children_have_best = true;

        for (int i = 0; i < expr->arity(); ++i) {
            Group* child_group = expr->input_at(i);
            GroupExpression* child_best = child_group->get_best_expression(required_property_);
            if (child_best != nullptr) {
                children_best_exprs.push_back(child_best);
                children_costs.push_back(child_best->get_cost(required_property_));
            } else {
                all_children_have_best = false;
                break;
            }
        }

        if (!all_children_have_best) {
            continue;  // Skip this expression if not all children have best expressions
        }

        // Compute this expression's cost (sum of children's costs)
        double expr_cost = 0.0;
        for (double c : children_costs) {
            expr_cost += c;
        }

        // Derive the output property of this expression
        // For now, we assume output property is same as required property
        // A more sophisticated implementation would compute actual output property
        PhysicalPropertySet output_property = required_property_;

        // Check if output property satisfies required property
        bool satisfies_sort = !required_property_.has_sort() ||
                              output_property.get_sort().is_satisfy(required_property_.get_sort());
        bool satisfies_dist = output_property.get_distribution().is_satisfy(required_property_.get_distribution());

        if (!satisfies_sort || !satisfies_dist) {
            // Need to add enforcement
            double enforcement_cost = compute_enforcement_cost(output_property, required_property_);
            expr_cost += enforcement_cost;

            // Create enforcer expression
            GroupExpression* enforced_expr = add_enforcers(expr, output_property, required_property_);
            if (enforced_expr != nullptr) {
                expr = enforced_expr;
                // Update output property to reflect enforcement
                output_property = required_property_;
            }
        }

        // Add local cost of this expression
        CostEstimate* local_cost = cost_model.compute_cost(expr, required_property_, {});
        expr_cost += local_cost->total_cost();
        delete local_cost;

        // Update best expression
        if (expr_cost < best_cost) {
            best_cost = expr_cost;
            best_expr = expr;
            best_output_property = output_property;
        }
    }

    // Store best expression with its cost
    if (best_expr != nullptr) {
        group_->set_best_expression(best_expr, best_cost, required_property_);

        // Also store the output property that this expression satisfies
        best_expr->update_property_with_cost(best_output_property, {}, best_cost);
    }
}

double EnforceAndCostTask::compute_enforcement_cost(const PhysicalPropertySet& output_property,
                                                     const PhysicalPropertySet& required_property) const {
    double cost = 0.0;

    // Cost for sort enforcement: O(n log n)
    if (required_property_.has_sort() &&
        !output_property.get_sort().is_satisfy(required_property_.get_sort())) {
        // Get row count from statistics
        double row_count = 1000.0;  // Default estimate
        Statistics* stats = group_->get_statistics();
        if (stats != nullptr) {
            row_count = stats->row_count();
        }
        // Sort cost: O(n log n) comparison cost
        if (row_count > 1) {
            cost += row_count * log2(row_count) * CostModel::DEFAULT_CPU_TUPLE_COST;
        } else {
            cost += row_count * CostModel::DEFAULT_CPU_TUPLE_COST;
        }
    }

    // Cost for distribution enforcement: network cost for data movement
    if (!output_property.get_distribution().is_satisfy(required_property_.get_distribution())) {
        double row_count = 1000.0;  // Default estimate
        Statistics* stats = group_->get_statistics();
        if (stats != nullptr) {
            row_count = stats->row_count();
        }
        // Exchange cost: data movement cost
        cost += row_count * 0.001;  // Network cost factor
    }

    return cost;
}

GroupExpression* EnforceAndCostTask::add_enforcers(GroupExpression* expr,
                                                    const PhysicalPropertySet& output_property,
                                                    const PhysicalPropertySet& required_property) const {
    // Check what enforcement is needed
    bool needs_sort = required_property_.has_sort() &&
                      !output_property.get_sort().is_satisfy(required_property_.get_sort());
    bool needs_distribution = !output_property.get_distribution().is_satisfy(required_property_.get_distribution());

    if (!needs_sort && !needs_distribution) {
        return expr;  // No enforcement needed
    }

    GroupExpression* result = expr;

    // Create enforcer chain in reverse order (bottom-up)
    // The topmost enforcer is returned, which will have the original expr as its input

    // For proper enforcer chaining, we need to:
    // 1. Sort enforcer wraps expr (if sort needed)
    // 2. Distribution enforcer wraps sort enforcer (if distribution needed)

    if (needs_sort) {
        GroupExpression* sort_enforcer = required_property_.get_sort().append_enforcers(group_);
        if (sort_enforcer != nullptr) {
            // The sort enforcer needs to have expr as its input
            // Since append_enforcers creates enforcer with child as input,
            // we need to properly link them
            // For now, just mark the enforcer with the group
            sort_enforcer->set_group(group_);
            result = sort_enforcer;
        }
    }

    if (needs_distribution) {
        GroupExpression* dist_enforcer = required_property_.get_distribution().append_enforcers(group_);
        if (dist_enforcer != nullptr) {
            dist_enforcer->set_group(group_);
            // The distribution enforcer becomes the new top of the chain
            result = dist_enforcer;
        }
    }

    return result;
}

std::string EnforceAndCostTask::to_string() const {
    return "EnforceAndCostTask for group " + std::to_string(group_->id());
}


}  // namespace cascades