#include "required_property_deriver.h"
#include "../memo/group_expression.h"
#include "../operator/operator.h"
#include "../operator/operators/physical_merge_join.h"
#include "../operator/operators/physical_sort.h"
#include "../operator/operators/physical_exchange.h"
#include "sort_property.h"

namespace cascades {

std::vector<PhysicalPropertySet> RequiredPropertyDeriver::derive_child_required_properties(
    GroupExpression* parent_expr, const PhysicalPropertySet& parent_required,
    PhysicalPropertySet** top_required) {

    std::vector<PhysicalPropertySet> result;
    OptExprOp* op = parent_expr->get_op();
    const PhysicalPropertySet* specified_required = parent_expr->get_children_required_property();
    if(specified_required != nullptr) {
        *top_required = const_cast<PhysicalPropertySet*>(&parent_required);
    }

    PhysicalPropertySet* p_top_required = *top_required;
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
            // HashJoin does not require children to be hash distributed
            // It builds a hash table internally during execution
            if(p_top_required) {
                result.push_back(specified_required ? *specified_required : parent_required);
                // HashJoin does not require children to be hash distributed
                // It builds a hash table internally during execution
                for (int i = 1; i < parent_expr->arity(); ++i) {
                    result.push_back(derive_default_child_property(*p_top_required));
                }
            } else {
                for (int i = 0; i < parent_expr->arity(); ++i) {
                    result.push_back(derive_default_child_property(parent_required));
                }
            }
            break;
        }

        case OptExprOpType::PHYSICAL_NESTLOOP_JOIN: {
            // Nested loop: left child (outer) gets ANY property
            // right child (inner) needs parameterized scan for index access
            if(p_top_required) {
                // left child (outer)
                result.push_back(specified_required ? *specified_required : parent_required);
                // right child (inner)  
                PhysicalPropertySet child_prop = derive_default_child_property(*p_top_required);
                child_prop.set_parameterized_scan_req(true);
                result.push_back(child_prop);
            } else {
                for (int i = 0; i < parent_expr->arity(); ++i) {
                    PhysicalPropertySet child_prop = derive_default_child_property(parent_required);
                    if (i == 1) {
                        child_prop.set_parameterized_scan_req(true);
                    }
                    result.push_back(child_prop);
                }
            }
            break;
        }

        case OptExprOpType::PHYSICAL_SORT: {
            // Sort requires child with any property (sort adds ordering)
            result.push_back(derive_sort_child_property(parent_required));
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
                result.push_back(derive_default_child_property(specified_required ? *specified_required : parent_required));
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
        spec.add_ordering(key, true, -1);  // table_id unknown for merge join keys
    }

    // Child needs to satisfy the sort requirement
    // Distribution can be ANY (MergeJoin doesn't require specific distribution)
    SortProperty sort_prop(spec);
    DistributionProperty any_dist(DistributionType::ANY);
    return PhysicalPropertySet(any_dist, sort_prop);
}

PhysicalPropertySet RequiredPropertyDeriver::derive_sort_child_property(
    const PhysicalPropertySet& parent_required) {

    // Sort operator adds ordering to its child
    // Child can have ANY property - the sort will add the required ordering
    return PhysicalPropertySet();
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
