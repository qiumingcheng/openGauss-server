#include "enforce_and_cost_task.h"
#include "optimize_expression_task.h"
#include "task_context.h"
#include "../base/memory_utils.h"
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
#include "../operator/operators/physical_merge_join.h"
#include "../property/required_property_deriver.h"
#include <limits>
#include <algorithm>

namespace cascades {

EnforceAndCostTask::EnforceAndCostTask(TaskContext* context, Group* group,
                                       const PhysicalPropertySet& required_property)
    : OptimizerTask(context), group_(group), required_property_(required_property) {}

void EnforceAndCostTask::execute() {
    ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: group id=%d, property=%s",
                         group_->id(), required_property_.to_string().c_str())));

    CostModel cost_model;
    RequiredPropertyDeriver property_deriver;

    double best_cost = std::numeric_limits<double>::max();
    GroupExpression* best_expr = nullptr;
    PhysicalPropertySet best_output_property;
    std::vector<PhysicalPropertySet> best_children_output_properties;  // For plan reconstruction

    // Iterate over all physical expressions in the group
    const auto& physical_exprs = group_->get_physical_expressions();

    ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: %d physical expressions",
                         (int)physical_exprs.size())));

    for (GroupExpression* expr : physical_exprs) {
        ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: processing expr=%s",
                             expr->to_string().c_str())));

        // Skip if expression is unused (has empty child groups)
        if (expr->is_unused()) {
            ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: skipped, expression unused")));
            continue;
        }

        if(expr->is_implement(required_property_)){
            ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: skipped, expression has implemented")));
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

            ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: child %d, group id=%d, child_required=%s",
                                 i, child_group->id(), child_required.to_string().c_str())));

            // Lower bound pruning: check if this branch can be pruned
            // Use > instead of >= to allow equal cost plans
            double child_lower_bound = child_group->get_cost_lower_bound(child_required);
            if (child_lower_bound > 0 && accumulated_child_cost + child_lower_bound > context_->get_upper_bound_cost()) {
                // This child's lower bound already exceeds upper bound, prune
                ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: pruned by lower bound, child_lower_bound=%.2f, accumulated=%.2f, upper_bound=%.2f",
                                     child_lower_bound, accumulated_child_cost, context_->get_upper_bound_cost())));
                all_children_optimized = false;
                break;
            }

            GroupExpression* child_best = child_group->get_best_expression(child_required);

            if (child_best == nullptr) {
                // Check if child group has any physical expressions
                const auto& child_phys_exprs = child_group->get_physical_expressions();
                if (child_phys_exprs.empty()) {
                    // No physical expressions available, cannot optimize - skip
                    ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: child %d has no physical expressions, skipping", i)));
                    all_children_optimized = false;
                    break;
                }
                // Has physical expressions but not yet optimized for this property - push tasks
                push_task(MakeTask<EnforceAndCostTask>(context_, group_, required_property_));
                push_task(MakeTask<EnforceAndCostTask>(context_, child_group, child_required));
                return;
            }

            ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: child %d best_expr found, cost=%.2f",
                                 i, child_best->get_cost(child_required))));

            children_best_exprs.push_back(child_best);
            accumulated_child_cost += child_best->get_cost(child_required);

            // Set child_path for PhysicalHashAgg after optimization
            if (i == 0) {
                PhysicalHashAgg* hash_agg = dynamic_cast<PhysicalHashAgg*>(expr->get_op());
                if (hash_agg != nullptr && hash_agg->get_child_path() == nullptr) {
                    PhysicalOptExprOp* child_op = static_cast<PhysicalOptExprOp*>(child_best->get_op());
                    hash_agg->set_child_path(child_op->get_path());
                }
            }

            // Get child's output property
            PhysicalPropertySet child_output = child_best->get_output_property(child_required);
            children_output_properties.push_back(child_output);
            ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: child %d output_property=%s",
                                 i, child_output.to_string().c_str())));
        }

        if (!all_children_optimized) {
            // Child groups need optimization first or was pruned
            ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: all_children_optimized=false, skipping expr")));
            continue;
        }

        // Lower bound pruning: check total cost against upper bound
        // Upper bound is only set by root group, so this pruning is safe
        ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: accumulated_child_cost=%.2f, upper_bound=%.2f",
                             accumulated_child_cost, context_->get_upper_bound_cost())));
        if (accumulated_child_cost > context_->get_upper_bound_cost()) {
            // Children's cost alone exceeds upper bound, prune
            continue;
        }

        // Derive output property for this expression
        PhysicalPropertySet output_property = derive_output_property(expr, children_output_properties);
        ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: output_property=%s", output_property.to_string().c_str())));

        // Compute local cost of this expression FIRST (before enforcement)
        CostEstimate local_cost_estimate = cost_model.compute_cost(expr, required_property_, {});
        double local_expr_cost = local_cost_estimate.total_cost();
        ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: local_expr_cost=%.2f", local_expr_cost)));

        // Check if enforcement is needed
        GroupExpression* enforced_expr = expr;
        double enforcement_cost = 0.0;

        if (!output_property.is_satisfy(required_property_)) {
            // Add enforcers and get the enforced expression with actual cost
            // output_property is updated in-place to the actual output (superset)
            double current_cost = accumulated_child_cost + local_expr_cost;
            std::pair<GroupExpression*, double> enforcement_result = add_enforcers(expr, output_property, required_property_, current_cost);
            enforced_expr = enforcement_result.first;
            enforcement_cost = enforcement_result.second;
        }

        // Total cost = children costs + local cost + enforcement cost
        double total_cost = accumulated_child_cost + local_expr_cost + enforcement_cost;
        ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: total_cost=%.2f (accumulated=%.2f, local=%.2f, enforcement=%.2f)",
                             total_cost, accumulated_child_cost, local_expr_cost, enforcement_cost)));

        // Apply cost pruning (upper bound check)
        // Upper bound is only set by root group, so this pruning is safe
        if (total_cost >= context_->get_upper_bound_cost()) {
            // This path is too expensive, prune
            ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: pruned by total_cost >= upper_bound")));
            continue;
        }

        // Update best expression if this is cheaper
        ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: comparing total_cost=%.2f with best_cost=%.2f",
                             total_cost, best_cost)));
        if (total_cost < best_cost) {
            best_cost = total_cost;
            best_expr = enforced_expr;
            best_output_property = output_property;
            best_children_output_properties = children_output_properties;  // Save for plan reconstruction
            ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: updated best_expr, cost=%.2f", best_cost)));
        }
    }

    // Store best expression with its cost
    ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: final best_expr=%p, best_cost=%.2f",
                         (void*)best_expr, best_cost)));
    if (best_expr != nullptr) {
        // Update group's best expression for this required property
        group_->set_best_expression(best_expr, best_cost, required_property_);

        // Update cost lower bound for this group
        // The best cost found is a new lower bound for this group+property
        group_->set_cost_lower_bound(required_property_, best_cost);

        // Update cost upper bound ONLY for root group
        // The root group's best cost is the total plan cost, which can be used for pruning
        Memo* memo = context_->get_optimizer_context()->get_memo();
        if (group_ == memo->get_root_group()) {
            ereport(WARNING, (errmsg("[CASCADES] EnforceAndCostTask: updating upper_bound=%.2f (root group)",
                                 best_cost)));
            update_cost_upper_bound(best_cost);
        }

        // Update expression's property table
        // Save children's output properties as input properties for plan reconstruction
        best_expr->update_property_with_cost(required_property_, best_output_property, best_children_output_properties, best_cost);
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

        case OptExprOpType::PHYSICAL_HASH_AGG: {
            // HashAgg does NOT guarantee sorted output - it uses hashing
            // Only return sort property if explicitly required
            return PhysicalPropertySet(child_property.get_distribution());
        }

        case OptExprOpType::PHYSICAL_SORT_AGG: {
            // SortAgg produces sorted output on group keys
            PhysicalHashAgg* sort_agg = static_cast<PhysicalHashAgg*>(op);
            const std::vector<int>& group_keys = sort_agg->get_group_keys();
            OrderSpec spec;
            for (int key : group_keys) {
                spec.add_ordering(key, true);  // Assume ascending
            }
            SortProperty sort_prop(spec);
            return PhysicalPropertySet(child_property.get_distribution(), sort_prop);
        }

        case OptExprOpType::PHYSICAL_MERGE_JOIN: {
            // MergeJoin produces output sorted on probe keys (left side)
            PhysicalMergeJoin* merge_join = static_cast<PhysicalMergeJoin*>(op);
            const std::vector<int>& probe_keys = merge_join->get_probe_keys();
            OrderSpec spec;
            for (int key : probe_keys) {
                spec.add_ordering(key, true);  // Assume ascending
            }
            SortProperty sort_prop(spec);
            return PhysicalPropertySet(child_property.get_distribution(), sort_prop);
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
    PhysicalPropertySet& output_property,
    const PhysicalPropertySet& required_property,
    double current_cost) const {

    // Check what enforcement is needed
    bool needs_sort = required_property.has_sort() &&
                      !output_property.get_sort().is_satisfy(required_property.get_sort());
    bool needs_distribution = !output_property.get_distribution().is_satisfy(required_property.get_distribution());

    if (!needs_sort && !needs_distribution) {
        return {expr, 0.0};  // No enforcement needed
    }

    GroupExpression* result = expr;
    double total_enforcement_cost = 0.0;

    // Build enforcer chain following StarRocks pattern:
    // - For Gather distribution: Sort first, then Exchange (sort-then-gather)
    // - For other distributions: Exchange first, then Sort (shuffle-then-sort)
    bool is_gather = required_property.get_distribution().get_type() == DistributionType::GATHER;

    if (is_gather && needs_sort && needs_distribution) {
        // Gather case: Sort first, then Exchange
        // 1. Add Sort enforcer - preserves distribution, adds sort
        std::pair<GroupExpression*, double> sort_result = add_sort_enforcer(result, output_property, required_property);
        result = sort_result.first;
        total_enforcement_cost += sort_result.second;
        // Update output_property: preserves distribution, adds required sort
        output_property.set_sort(required_property.get_sort());

        // 2. Add Distribution enforcer on top of sort - preserves sort, adds distribution
        std::pair<GroupExpression*, double> dist_result = add_distribution_enforcer(result, output_property, required_property);
        result = dist_result.first;
        total_enforcement_cost += dist_result.second;
        // Update output_property: adds required distribution
        output_property.set_distribution(required_property.get_distribution());

    } else {
        // Default case: Exchange first (if needed), then Sort (if needed)

        // 1. Add Distribution enforcer first - preserves sort, adds distribution
        if (needs_distribution) {
            std::pair<GroupExpression*, double> dist_result = add_distribution_enforcer(result, output_property, required_property);
            result = dist_result.first;
            total_enforcement_cost += dist_result.second;
            // Update output_property: adds required distribution
            output_property.set_distribution(required_property.get_distribution());
        }

        // 2. Add Sort enforcer on top - preserves distribution, adds sort
        if (needs_sort) {
            std::pair<GroupExpression*, double> sort_result = add_sort_enforcer(result, output_property, required_property);
            result = sort_result.first;
            total_enforcement_cost += sort_result.second;
            // Update output_property: adds required sort
            output_property.set_sort(required_property.get_sort());
        }
    }

    return {result, total_enforcement_cost};
}

std::pair<GroupExpression*, double> EnforceAndCostTask::add_sort_enforcer(
    GroupExpression* expr,
    const PhysicalPropertySet& input_property,
    const PhysicalPropertySet& required_property) const {

    GroupExpression* sort_enforcer = required_property.get_sort().append_enforcers(group_);
    if (sort_enforcer == nullptr) {
        return {expr, 0.0};
    }

    // Set the input of sort enforcer to the current result's group
    std::vector<Group*>& inputs = sort_enforcer->get_inputs();
    inputs.clear();
    inputs.push_back(expr->get_group());

    // Compute property after sort enforcement
    PhysicalPropertySet sort_required = input_property;
    sort_required.set_sort(required_property.get_sort());

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

    GroupExpression* dist_enforcer = required_property.get_distribution().append_enforcers(group_);
    if (dist_enforcer == nullptr) {
        return {expr, 0.0};
    }

    // Set the input of distribution enforcer to the current result's group
    std::vector<Group*>& inputs = dist_enforcer->get_inputs();
    inputs.clear();
    inputs.push_back(expr->get_group());

    // Compute property after distribution enforcement
    PhysicalPropertySet dist_required = input_property;
    dist_required.set_distribution(required_property.get_distribution());

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
