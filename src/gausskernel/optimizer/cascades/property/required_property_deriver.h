#pragma once
#include "physical_property_set.h"
#include <vector>

namespace cascades {

class GroupExpression;
class OptExprOp;

/**
 * RequiredPropertyDeriver - Derives child required properties from parent
 *
 * This class is responsible for deriving what physical properties each child
 * of an expression must satisfy. Different operators have different requirements:
 *
 * - PhysicalMergeJoin: Requires children to be sorted on join keys
 * - PhysicalHashJoin: No special distribution requirement (builds hash table internally)
 * - PhysicalSort: Requires child with any property (sort adds ordering)
 * - PhysicalExchange: Requires child with any distribution (exchange redistributes)
 * - PhysicalSeqScan/PhysicalIndexScan/PhysicalHashAgg: No special requirements (ANY property)
 *
 * The derived child required properties are used by EnforceAndCostTask to:
 * 1. Look up or compute the best child plan for that specific requirement
 * 2. Add enforcers if child's output doesn't satisfy the derived requirement
 *
 * Example:
 *   Parent: PhysicalMergeJoin (requires sorted input)
 *   Child 0 required property: Sort on [t1.a, t1.b]
 *   Child 1 required property: Sort on [t2.c, t2.d]
 */
class RequiredPropertyDeriver {
public:
    RequiredPropertyDeriver() = default;
    ~RequiredPropertyDeriver() = default;

    /**
     * Derive required properties for all children of an expression
     *
     * @param parent_expr The parent expression
     * @param parent_required The parent's required property
     * @return Vector of required properties, one per child (in arity order)
     */
    std::vector<PhysicalPropertySet> derive_child_required_properties(
        GroupExpression* parent_expr,
        const PhysicalPropertySet& parent_required);

private:
    /**
     * Derive child required property for PhysicalMergeJoin
     * Both children need to be sorted on their respective join keys
     */
    PhysicalPropertySet derive_merge_join_child_property(
        GroupExpression* parent_expr,
        int child_index,
        const PhysicalPropertySet& parent_required);

    /**
     * Derive child required property for PhysicalSort
     * Child can have any property (sort adds ordering)
     */
    PhysicalPropertySet derive_sort_child_property(
        const PhysicalPropertySet& parent_required);

    /**
     * Derive child required property for PhysicalPassthrough
     * Child inherits the parent's required property (pass through)
     */
    PhysicalPropertySet derive_passthrough_child_property(
        const PhysicalPropertySet& parent_required);

    /**
     * Derive child required property for PhysicalExchange
     * Child can have any distribution (exchange redistributes)
     */
    PhysicalPropertySet derive_exchange_child_property(
        const PhysicalPropertySet& parent_required);

    /**
     * Default: child inherits parent's required property
     * Used for operators that pass through requirements
     */
    PhysicalPropertySet derive_default_child_property(
        const PhysicalPropertySet& parent_required);

    std::vector<PhysicalPropertySet> deal_specified_required(
    GroupExpression* parent_expr,
    const PhysicalPropertySet& parent_required,
    const PhysicalPropertySet* specified_required,
    OptExprOp* op);
};

}  // namespace cascades
