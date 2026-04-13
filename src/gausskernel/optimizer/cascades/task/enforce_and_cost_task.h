#pragma once
#include "optimizer_task.h"
#include "../property/physical_property_set.h"

namespace cascades {

class Group;
class GroupExpression;

/**
 * EnforceAndCostTask - Physical optimization core task
 *
 * This task is the heart of physical optimization in the Cascades framework.
 * It performs the following functions:
 *
 * 1. Cost Computation: Calculates the cost of each GroupExpression in the group
 *    for a specific required physical property (sort, distribution).
 *
 * 2. Property Enforcement: When an expression's output property doesn't satisfy
 *    the required property, adds enforcement operators (Sort, Exchange) to bridge
 *    the gap.
 *
 * 3. Best Expression Selection: Selects the lowest-cost expression for each
 *    required property and stores it in the Group.
 *
 * 4. Cost Pruning: Uses upper/lower bound pruning to eliminate suboptimal paths.
 *
 * Task Flow:
 * 1. For each physical expression in the group:
 *    a. Recursively optimize children with their required properties
 *    b. Compute child output properties and enforce if needed
 *    c. Compute local cost of this expression
 *    d. Add enforcement cost if output doesn't satisfy required
 *    e. Update best expression if this is cheaper
 *
 * 2. Store best expression and cost in the group
 *
 * This implementation follows the StarRocks pattern:
 * - Property enforcement chain: Distribution -> Sort (top to bottom)
 * - Cost model integration via CostModel class
 * - Upper/lower bound pruning for optimization efficiency
 */
class EnforceAndCostTask : public OptimizerTask {
public:
    EnforceAndCostTask(TaskContext* context, Group* group,
                       const PhysicalPropertySet& required_property);

    void execute() override;
    std::string to_string() const override;

private:
    /**
     * Compute the cost of enforcing property requirements
     * Called when expression output doesn't satisfy required property
     *
     * @param output_property The actual output property of the expression
     * @param required_property The required physical property
     * @return The enforcement cost (sort + exchange)
     */
    double compute_enforcement_cost(const PhysicalPropertySet& output_property,
                                    const PhysicalPropertySet& required_property) const;

    /**
     * Add enforcer operators when output property doesn't satisfy required property
     *
     * Enforcer chain (bottom-up):
     * 1. Sort enforcer (if sort property doesn't match)
     * 2. Distribution enforcer (if distribution doesn't match)
     *
     * @param expr The original expression
     * @param output_property The actual output property
     * @param required_property The required property
     * @return New expression with enforcers, or original if no enforcement needed
     */
    GroupExpression* add_enforcers(GroupExpression* expr,
                                   const PhysicalPropertySet& output_property,
                                   const PhysicalPropertySet& required_property) const;

    /**
     * Derive the output property for an expression based on its operator type
     * and children's output properties
     *
     * @param expr The expression
     * @param children_properties Children's output properties
     * @return The derived output property
     */
    PhysicalPropertySet derive_output_property(
        GroupExpression* expr,
        const std::vector<PhysicalPropertySet>& children_properties) const;

    /**
     * Update cost upper bound based on current best cost
     * Used for pruning sibling branches
     *
     * @param cost The current best cost
     */
    void update_cost_upper_bound(double cost) const;

private:
    Group* group_;
    PhysicalPropertySet required_property_;
};

}
