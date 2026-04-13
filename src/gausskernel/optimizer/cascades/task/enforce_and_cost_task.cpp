#include "enforce_and_cost_task.h"
#include "optimize_expression_task.h"
#include "task_context.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../memo/memo.h"
#include "../cost/cost_model.h"
#include "../cost/cost_estimate.h"
#include "../statistics/statistics.h"
#include "../optimizer/optimizer_context.h"
#include "../operator/operators/physical_exchange.h"
#include "../operator/operators/physical_sort.h"
#include "../operator/operators/physical_hash_agg.h"
#include "../property/required_property_deriver.h"
#include <limits>
#include <algorithm>

namespace cascades {

EnforceAndCostTask::EnforceAndCostTask(TaskContext* context, Group* group,
                                       const PhysicalPropertySet& required_property)
    : OptimizerTask(context), group_(group), required_property_(required_property) {}

void EnforceAndCostTask::execute() {
    OptimizerContext* optimizer_context = context_->get_optimizer_context();
    CostModel cost_model;
    RequiredPropertyDeriver property_deriver;

    double best_cost = std::numeric_limits<double>::max();
    GroupExpression* best_expr = nullptr;
    PhysicalPropertySet best_output_property;

    // Iterate over all physical expressions in the group
    const auto& physical_exprs = group_->get_physical_expressions();

    for (GroupExpression* expr : physical_exprs) {
        // Skip if expression is unused (has empty child groups)
        if (expr->is_unused()) {
            continue;
        }

        // Collect children's costs and best expressions
        std::vector<double> children_costs;
        std::vector<PhysicalPropertySet> children_output_properties;
        std::vector<GroupExpression*> children_best_exprs;

        bool all_children_optimized = true;
        double accumulated_child_cost = 0.0;  // For lower bound pruning

        for (int i = 0; i < expr->arity(); ++i) {
            Group* child_group = expr->input_at(i);

            // Derive child's required property from parent
            PhysicalPropertySet child_required = property_deriver
                .derive_child_required_properties(expr, required_property_)[i];

            // Lower bound pruning: check if this branch can be pruned
            double child_lower_bound = child_group->get_cost_lower_bound(child_required);
            if (child_lower_bound > 0 && accumulated_child_cost + child_lower_bound >= context_->get_upper_bound_cost()) {
                // This child's lower bound already exceeds upper bound, prune
                all_children_optimized = false;
                break;
            }

            GroupExpression* child_best = child_group->get_best_expression(child_required);

            if (child_best == nullptr) {
                // Child not yet optimized for this required property
                all_children_optimized = false;
                break;
            }

            children_best_exprs.push_back(child_best);
            children_costs.push_back(child_best->get_cost(child_required));
            accumulated_child_cost += children_costs.back();

            // Get child's output property
            PhysicalPropertySet child_output = child_best->get_output_property(child_required);
            children_output_properties.push_back(child_output);
        }

        if (!all_children_optimized) {
            // Child groups need optimization first or was pruned
            continue;
        }

        // Lower bound pruning: check total cost against upper bound
        if (accumulated_child_cost >= context_->get_upper_bound_cost()) {
            // Children's cost alone exceeds upper bound, prune
            continue;
        }

        // Derive output property for this expression
        PhysicalPropertySet output_property = derive_output_property(expr, children_output_properties);

        // Check if enforcement is needed
        double enforcement_cost = 0.0;
        GroupExpression* enforced_expr = expr;

        if (!output_property.is_satisfy(required_property_)) {
            // Compute enforcement cost and add enforcers
            // First compute base enforcement cost estimate
            enforcement_cost = compute_enforcement_cost(output_property, required_property_);

            // Add enforcers and get the enforced expression
            // Pass current cost (accumulated_child_cost + local_expr_cost) for proper cost tracking
            double current_cost = accumulated_child_cost + local_expr_cost;
            enforced_expr = add_enforcers(expr, output_property, required_property_, current_cost);

            if (enforced_expr != nullptr && enforced_expr != expr) {
                // Update output property to reflect enforcement
                output_property = required_property_;
            }
        }

        // Compute local cost of this expression
        CostEstimate* local_cost = cost_model.compute_cost(enforced_expr, required_property_, {});
        double local_expr_cost = local_cost ? local_cost->total_cost() : 0.0;
        delete local_cost;

        // Total cost = children costs + local cost + enforcement cost
        double total_cost = accumulated_child_cost + local_expr_cost + enforcement_cost;

        // Apply cost pruning (upper bound check)
        if (total_cost >= context_->get_upper_bound_cost()) {
            // This path is too expensive, prune it
            continue;
        }

        // Update best expression if this is cheaper
        if (total_cost < best_cost) {
            best_cost = total_cost;
            best_expr = enforced_expr;
            best_output_property = output_property;
        }
    }

    // Store best expression with its cost
    if (best_expr != nullptr) {
        // Update group's best expression for this required property
        group_->set_best_expression(best_expr, best_cost, required_property_);

        // Update cost lower bound for this group
        // The best cost found is a new lower bound for this group+property
        group_->set_cost_lower_bound(required_property_, best_cost);

        // Update cost upper bound for pruning sibling branches
        update_cost_upper_bound(best_cost);

        // Update expression's property table
        best_expr->update_property_with_cost(best_output_property, {}, best_cost);
    }
}

PhysicalPropertySet EnforceAndCostTask::derive_output_property(
    GroupExpression* expr,
    const std::vector<PhysicalPropertySet>& children_properties) const {

    OptExprOp* op = expr->get_op();

    if (op == nullptr || children_properties.empty()) {
        return required_property_;
    }

    const PhysicalPropertySet& child_property = children_properties[0];

    switch (op->get_type()) {
        case OptExprOpType::PHYSICAL_SCAN: {
            // Scan produces ANY property (no sort, no specific distribution)
            return PhysicalPropertySet();
        }

        case OptExprOpType::PHYSICAL_SORT: {
            // Sort ensures output is sorted by the required sort keys
            // Preserve distribution from child
            if (required_property_.has_sort()) {
                return PhysicalPropertySet(child_property.get_distribution(),
                                           required_property_.get_sort());
            }
            return child_property;
        }

        case OptExprOpType::PHYSICAL_EXCHANGE: {
            // Exchange ensures output has the target distribution
            // Preserve sort order from child
            PhysicalExchange* exchange = static_cast<PhysicalExchange*>(op);
            DistributionProperty dist(exchange->get_distribution_type());
            if (child_property.has_sort()) {
                PhysicalPropertySet result(dist);
                result.set_sort(child_property.get_sort());
                return result;
            }
            return PhysicalPropertySet(dist);
        }

        case OptExprOpType::PHYSICAL_HASH_AGG:
        case OptExprOpType::PHYSICAL_SORT_AGG: {
            // HashAgg destroys sort order (hash shuffle)
            // But preserves/produces distribution on group keys
            return PhysicalPropertySet();
        }

        case OptExprOpType::PHYSICAL_MERGE_JOIN: {
            // MergeJoin produces output sorted on probe keys (left side)
            // In a full implementation, extract sort keys from the join op
            // For now, pass through with distribution
            return PhysicalPropertySet(child_property.get_distribution());
        }

        case OptExprOpType::PHYSICAL_HASH_JOIN:
        case OptExprOpType::PHYSICAL_NESTLOOP_JOIN: {
            // Joins don't guarantee sort order
            // Pass through distribution
            return PhysicalPropertySet(child_property.get_distribution());
        }

        case OptExprOpType::PHYSICAL_LIMIT:
        case OptExprOpType::PHYSICAL_MATERIAL: {
            // These operators preserve child properties
            return child_property;
        }

        default: {
            // Default: pass through child property
            return child_property;
        }
    }
}

double EnforceAndCostTask::compute_enforcement_cost(
    const PhysicalPropertySet& output_property,
    const PhysicalPropertySet& required_property) const {

    double cost = 0.0;

    // Get row count for cost estimation
    double row_count = 1000.0;  // Default estimate
    Statistics* stats = group_->get_statistics();
    if (stats != nullptr) {
        row_count = stats->row_count();
    }

    // Sort enforcement cost: O(n log n)
    if (required_property_.has_sort() &&
        !output_property.get_sort().is_satisfy(required_property_.get_sort())) {

        if (row_count > 1) {
            cost += row_count * std::log2(row_count) * CostModel::DEFAULT_CPU_TUPLE_COST;
        } else {
            cost += row_count * CostModel::DEFAULT_CPU_TUPLE_COST;
        }
    }

    // Distribution enforcement cost: data movement
    if (!output_property.get_distribution().is_satisfy(required_property_.get_distribution())) {
        // Network cost factor for data shuffle/broadcast
        cost += row_count * 0.001;
    }

    return cost;
}

GroupExpression* EnforceAndCostTask::add_enforcers(
    GroupExpression* expr,
    const PhysicalPropertySet& output_property,
    const PhysicalPropertySet& required_property,
    double current_cost) const {

    // Check what enforcement is needed
    bool needs_sort = required_property_.has_sort() &&
                      !output_property.get_sort().is_satisfy(required_property_.get_sort());
    bool needs_distribution = !output_property.get_distribution().is_satisfy(required_property_.get_distribution());

    if (!needs_sort && !needs_distribution) {
        return expr;  // No enforcement needed
    }

    GroupExpression* result = expr;
    PhysicalPropertySet enforced_property = output_property;
    double enforced_cost = current_cost;

    // Build enforcer chain in reverse order (bottom-up execution)
    // Distribution enforcer goes on top of Sort enforcer

    // 1. Add Sort enforcer if needed (bottom of chain)
    if (needs_sort) {
        GroupExpression* sort_enforcer = required_property_.get_sort().append_enforcers(group_);
        if (sort_enforcer != nullptr) {
            // The sort enforcer wraps the current result
            std::vector<Group*> inputs = {result->get_group()};
            sort_enforcer->set_inputs(inputs);

            // Compute enforced property after sort
            PhysicalPropertySet sort_required = output_property;
            sort_required.set_sort(required_property_.get_sort());
            enforced_property = sort_required;

            // Update cost with enforcer
            OptimizerContext* optimizer_context = context_->get_optimizer_context();
            CostModel cost_model;
            CostEstimate* enforcer_cost = cost_model.compute_cost(sort_enforcer, sort_required, {});
            enforced_cost += enforcer_cost ? enforcer_cost->total_cost() : 0.0;
            delete enforcer_cost;

            // Use update_output_property_set to avoid duplicate enforcers
            result = sort_enforcer;
        }
    }

    // 2. Add Distribution enforcer if needed (top of chain)
    if (needs_distribution) {
        GroupExpression* dist_enforcer = required_property_.get_distribution().append_enforcers(group_);
        if (dist_enforcer != nullptr) {
            // The distribution enforcer wraps the current result
            std::vector<Group*> inputs = {result->get_group()};
            dist_enforcer->set_inputs(inputs);

            // Compute enforced property after distribution
            PhysicalPropertySet dist_required = required_property_;
            enforced_property = dist_required;

            // Update cost with enforcer
            OptimizerContext* optimizer_context = context_->get_optimizer_context();
            CostModel cost_model;
            CostEstimate* enforcer_cost = cost_model.compute_cost(dist_enforcer, dist_required, {});
            enforced_cost += enforcer_cost ? enforcer_cost->total_cost() : 0.0;
            delete enforcer_cost;

            // Use update_output_property_set to get consistent property object
            // and avoid duplicate enforcers
            PhysicalPropertySet final_property = group_->update_output_property_set(
                dist_enforcer, enforced_cost, dist_required);

            result = dist_enforcer;
        }
    }

    return result;
}

void EnforceAndCostTask::update_cost_upper_bound(double cost) const {
    // Update the upper bound cost in the task context
    // This enables pruning of more expensive sibling branches
    if (cost < context_->get_upper_bound_cost()) {
        context_->set_upper_bound_cost(cost);
    }
}

std::string EnforceAndCostTask::to_string() const {
    return "EnforceAndCostTask for group " + std::to_string(group_->id()) +
           " property=" + required_property_.to_string();
}

}  // namespace cascades
