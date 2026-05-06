#include "required_property_deriver.h"
#include "../memo/group_expression.h"
#include "../operator/operator.h"
#include "../operator/operators/physical_merge_join.h"
#include "../operator/operators/physical_hash_join.h"
#include "../operator/operators/physical_sort.h"
#include "../operator/operators/physical_exchange.h"
#include "sort_property.h"

namespace cascades {

std::vector<PhysicalPropertySet> RequiredPropertyDeriver::derive_child_required_properties(
    GroupExpression* parent_expr,
    const PhysicalPropertySet& parent_required) {

    std::vector<PhysicalPropertySet> result;
    OptExprOp* op = parent_expr->get_op();

    if (op == nullptr) {
        return result;
    }

    // Derive child required properties based on operator type
    switch (op->get_type()) {
        case OptExprOpType::PHYSICAL_MERGE_JOIN: {
            // MergeJoin requires both children to be sorted on join keys
            for (int i = 0; i < parent_expr->arity(); ++i) {
                result.push_back(derive_merge_join_child_property(parent_expr, i, parent_required));
            }
            break;
        }

        case OptExprOpType::PHYSICAL_HASH_JOIN: {
            // HashJoin requires children to be hash distributed on join keys
            for (int i = 0; i < parent_expr->arity(); ++i) {
                result.push_back(derive_hash_join_child_property(parent_expr, i, parent_required));
            }
            break;
        }

        case OptExprOpType::PHYSICAL_SORT: {
            // Sort requires child with any property (sort adds ordering)
            result.push_back(derive_sort_child_property(parent_required));
            break;
        }

        case OptExprOpType::PHYSICAL_PASSTHROUGH: {
            // PassThrough operator passes through the required property to child
            // It does not modify or enforce any property, just a placeholder
            // Child must satisfy whatever the parent requires
            result.push_back(derive_passthrough_child_property(parent_required));
            break;
        }

        case OptExprOpType::PHYSICAL_EXCHANGE: {
            // Exchange requires child with any distribution (redistributes)
            result.push_back(derive_exchange_child_property(parent_required));
            break;
        }

        default: {
            // Default: child inherits parent's required property
            for (int i = 0; i < parent_expr->arity(); ++i) {
                result.push_back(derive_default_child_property(parent_required));
            }
            break;
        }
    }

    return result;
}

PhysicalPropertySet RequiredPropertyDeriver::derive_merge_join_child_property(
    GroupExpression* parent_expr,
    int child_index,
    const PhysicalPropertySet& parent_required) {

    PhysicalMergeJoin* merge_join = static_cast<PhysicalMergeJoin*>(parent_expr->get_op());

    // Get join keys for the appropriate child
    // For MergeJoin: build_keys for left child (build side), probe_keys for right child (probe side)
    const std::vector<int>* keys = nullptr;

    if (child_index == 0) {
        // Left child (build side) - needs to be sorted on build keys
        keys = &merge_join->get_build_keys();
    } else {
        // Right child (probe side) - needs to be sorted on probe keys
        keys = &merge_join->get_probe_keys();
    }

    // Build sort property from join keys
    OrderSpec spec;
    for (int key : *keys) {
        // Assuming ascending order for merge join keys
        // In a full implementation, this could support DESC order
        spec.add_ordering(key, true);  // true = ascending
    }

    // Child needs to satisfy the sort requirement
    // Distribution can be ANY (MergeJoin doesn't require specific distribution)
    SortProperty sort_prop(spec);
    DistributionProperty any_dist(DistributionType::ANY);
    return PhysicalPropertySet(any_dist, sort_prop);
}

PhysicalPropertySet RequiredPropertyDeriver::derive_hash_join_child_property(
    GroupExpression* parent_expr,
    int child_index,
    const PhysicalPropertySet& parent_required) {

    PhysicalHashJoin* hash_join = static_cast<PhysicalHashJoin*>(parent_expr->get_op());

    // Get join keys for the appropriate child
    // For HashJoin: build_keys for build side (left), probe_keys for probe side (right)
    const std::vector<int>* keys = nullptr;

    if (child_index == 0) {
        // Left child (build side) - needs to be hash distributed on build keys
        keys = &hash_join->get_build_keys();
    } else {
        // Right child (probe side) - needs to be hash distributed on probe keys
        keys = &hash_join->get_probe_keys();
    }

    // Child needs to be hash distributed on join keys
    // Sort property can be ANY (HashJoin doesn't require sorted input)
    DistributionProperty dist(DistributionType::HASH, *keys);
    return PhysicalPropertySet(dist);
}

PhysicalPropertySet RequiredPropertyDeriver::derive_sort_child_property(
    const PhysicalPropertySet& parent_required) {

    // Sort operator adds ordering to its child
    // Child can have ANY property - the sort will add the required ordering
    return PhysicalPropertySet();
}

PhysicalPropertySet RequiredPropertyDeriver::derive_passthrough_child_property(
    const PhysicalPropertySet& parent_required) {

    // PassThrough operator passes through the required property
    // Child must satisfy whatever the parent requires
    return parent_required;
}

PhysicalPropertySet RequiredPropertyDeriver::derive_exchange_child_property(
    const PhysicalPropertySet& parent_required) {

    // Exchange operator redistributes data
    // Child can have any distribution - the exchange will redistribute
    // However, we should preserve the sort requirement (if child is already sorted)
    return PhysicalPropertySet();  // ANY property
}

PhysicalPropertySet RequiredPropertyDeriver::derive_default_child_property(
    const PhysicalPropertySet& parent_required) {

    // Default: child inherits parent's required property
    // This is appropriate for operators that pass through requirements
    // (e.g., PhysicalFilter, PhysicalProject, PhysicalHashAgg)
    return parent_required;
}

}  // namespace cascades
