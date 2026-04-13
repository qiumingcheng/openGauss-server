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
            accumulated_child_cost += child_best->get_cost(child_required);

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

        // Compute local cost of this expression FIRST (before enforcement)
        CostEstimate local_cost_estimate = cost_model.compute_cost(expr, required_property_, {});
        double local_expr_cost = local_cost_estimate.total_cost();

        // Check if enforcement is needed
        GroupExpression* enforced_expr = expr;
        double enforcement_cost = 0.0;

        if (!output_property.is_satisfy(required_property_)) {
            // Add enforcers and get the enforced expression with actual cost
            double current_cost = accumulated_child_cost + local_expr_cost;
            std::pair<GroupExpression*, double> enforcement_result = add_enforcers(expr, output_property, required_property_, current_cost);
            enforced_expr = enforcement_result.first;
            enforcement_cost = enforcement_result.second;

            if (enforced_expr != nullptr && enforced_expr != expr) {
                // Update output property to reflect enforcement
                output_property = required_property_;
            }
        }

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

        case OptExprOpType::PHYSICAL_LIMIT: {
            // LIMIT preserves both distribution and sort order
            return child_property;
        }

        case OptExprOpType::PHYSICAL_MATERIAL: {
            // Materialize destroys sort order but preserves distribution
            return PhysicalPropertySet(child_property.get_distribution());
        }

        default: {
            // Default: pass through child property
            return child_property;
        }
    }
}

std::pair<GroupExpression*, double> EnforceAndCostTask::add_enforcers(
    GroupExpression* expr,
    const PhysicalPropertySet& output_property,
    const PhysicalPropertySet& required_property,
    double current_cost) const {

    // Check what enforcement is needed
    bool needs_sort = required_property_.has_sort() &&
                      !output_property.get_sort().is_satisfy(required_property_.get_sort());
    bool needs_distribution = !output_property.get_distribution().is_satisfy(required_property_.get_distribution());

    if (!needs_sort && !needs_distribution) {
        return {expr, 0.0};  // No enforcement needed
    }

    GroupExpression* result = expr;
    double total_enforcement_cost = 0.0;

    // Build enforcer chain following StarRocks pattern:
    // - For Gather distribution: Sort first, then Exchange (sort-then-gather)
    // - For other distributions: Exchange first, then Sort (shuffle-then-sort)
    bool is_gather = required_property_.get_distribution().get_type() == DistributionType::GATHER;

    if (is_gather && needs_sort && needs_distribution) {
        // Gather case: Sort first, then Exchange
        // 1. Add Sort enforcer
        std::pair<GroupExpression*, double> sort_result = add_sort_enforcer(result, output_property, required_property);
        result = sort_result.first;
        total_enforcement_cost += sort_result.second;

        // 2. Add Distribution enforcer on top of sort
        PhysicalPropertySet after_sort_property = output_property;
        after_sort_property.set_sort(required_property_.get_sort());

        std::pair<GroupExpression*, double> dist_result = add_distribution_enforcer(result, after_sort_property, required_property);
        result = dist_result.first;
        total_enforcement_cost += dist_result.second;

    } else {
        // Default case: Exchange first (if needed), then Sort (if needed)

        // 1. Add Distribution enforcer first
        if (needs_distribution) {
            std::pair<GroupExpression*, double> dist_result = add_distribution_enforcer(result, output_property, required_property);
            result = dist_result.first;
            total_enforcement_cost += dist_result.second;
        }

        // 2. Add Sort enforcer on top
        if (needs_sort) {
            // After distribution, the intermediate property has the required distribution
            // but may not have the required sort
            PhysicalPropertySet after_dist_property = required_property;
            after_dist_property.set_sort(SortProperty());  // Clear sort - will be added by sort enforcer

            std::pair<GroupExpression*, double> sort_result = add_sort_enforcer(result, after_dist_property, required_property);
            result = sort_result.first;
            total_enforcement_cost += sort_result.second;
        }
    }

    return {result, total_enforcement_cost};
}

std::pair<GroupExpression*, double> EnforceAndCostTask::add_sort_enforcer(
    GroupExpression* expr,
    const PhysicalPropertySet& input_property,
    const PhysicalPropertySet& required_property) const {

    GroupExpression* sort_enforcer = required_property_.get_sort().append_enforcers(group_);
    if (sort_enforcer == nullptr) {
        return {expr, 0.0};
    }

    // Set the input of sort enforcer to the current result's group
    std::vector<Group*>& inputs = sort_enforcer->get_inputs();
    inputs.clear();
    inputs.push_back(expr->get_group());

    // Compute property after sort enforcement
    PhysicalPropertySet sort_required = input_property;
    sort_required.set_sort(required_property_.get_sort());

    // Compute actual enforcer cost using CostModel
    CostModel cost_model;
    CostEstimate enforcer_cost_estimate = cost_model.compute_cost(sort_enforcer, sort_required, {});
    double enforcer_cost = enforcer_cost_estimate.total_cost();

    // Use update_output_property_set to avoid duplicate enforcers
    // This returns the consistent property object
    PhysicalPropertySet final_property = group_->update_output_property_set(
        sort_enforcer, enforcer_cost, sort_required);

    // If the returned property is different, it means there's already a cheaper enforcer
    if (final_property != sort_required) {
        // Use the existing cheaper enforcer
        GroupExpression* existing_enforcer = group_->get_best_expression(final_property);
        if (existing_enforcer != nullptr) {
            double existing_cost = existing_enforcer->get_cost(final_property);
            return {existing_enforcer, existing_cost};
        }
    }

    // New enforcer is the best, store it
    group_->set_best_expression(sort_enforcer, enforcer_cost, sort_required);

    return {sort_enforcer, enforcer_cost};
}

std::pair<GroupExpression*, double> EnforceAndCostTask::add_distribution_enforcer(
    GroupExpression* expr,
    const PhysicalPropertySet& input_property,
    const PhysicalPropertySet& required_property) const {

    GroupExpression* dist_enforcer = required_property_.get_distribution().append_enforcers(group_);
    if (dist_enforcer == nullptr) {
        return {expr, 0.0};
    }

    // Set the input of distribution enforcer to the current result's group
    std::vector<Group*>& inputs = dist_enforcer->get_inputs();
    inputs.clear();
    inputs.push_back(expr->get_group());

    // Compute property after distribution enforcement
    PhysicalPropertySet dist_required = required_property;

    // Compute actual enforcer cost using CostModel
    CostModel cost_model;
    CostEstimate enforcer_cost_estimate = cost_model.compute_cost(dist_enforcer, dist_required, {});
    double enforcer_cost = enforcer_cost_estimate.total_cost();

    // Use update_output_property_set to avoid duplicate enforcers
    // This returns the consistent property object
    PhysicalPropertySet final_property = group_->update_output_property_set(
        dist_enforcer, enforcer_cost, dist_required);

    // If the returned property is different, it means there's already a cheaper enforcer
    if (final_property != dist_required) {
        // Use the existing cheaper enforcer
        GroupExpression* existing_enforcer = group_->get_best_expression(final_property);
        if (existing_enforcer != nullptr) {
            double existing_cost = existing_enforcer->get_cost(final_property);
            return {existing_enforcer, existing_cost};
        }
    }

    // New enforcer is the best, store it
    group_->set_best_expression(dist_enforcer, enforcer_cost, dist_required);

    return {dist_enforcer, enforcer_cost};
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
