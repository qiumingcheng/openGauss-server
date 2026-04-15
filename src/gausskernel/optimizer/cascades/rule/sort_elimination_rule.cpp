#include "sort_elimination_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/group_expression.h"
#include "../property/sort_property.h"
#include "nodes/relation.h"
#include "nodes/nodes.h"
#include "utils/lsyscache.h"

namespace cascades {

SortEliminationRule* SortEliminationRule::instance() {
    static SortEliminationRule inst;
    return &inst;
}

SortEliminationRule::SortEliminationRule()
    : Rule(RuleType::TF_SORT_ELIMINATION,
           Pattern::create(OptExprOpType::PHYSICAL_SORT,
                           Pattern::any())) {}

std::vector<OptExpression*> SortEliminationRule::transform(
    OptExpression* input, OptimizerContext* context) {
    Assert(input->arity() > 0);  // Pattern must match expression with child

    std::vector<OptExpression*> result;

    OptExprOp* op = input->get_op();
    if (op->get_type() != OptExprOpType::PHYSICAL_SORT) {
        return result;
    }

    PhysicalSort* sort = static_cast<PhysicalSort*>(op);
    if (sort->get_child_path() == nullptr) {
        // Cannot eliminate sort without child path information
        return result;
    }

    OptExpression* child = input->input_at(0);
    Assert(child != nullptr);  // Guaranteed by arity check above

    OptExprOp* child_op = child->get_op();
    Assert(child_op != nullptr);  // Must have valid operator

    // Get required sort order from the PhysicalSort's path
    OrderSpec required_spec = get_sort_order(sort);
    if (required_spec.get_orderings().empty()) {
        // No specific order required, sort can be eliminated
        result.push_back(child);
        return result;
    }

    // Case 1: Child is already a PhysicalSort
    if (child_op->get_type() == OptExprOpType::PHYSICAL_SORT) {
        if (can_eliminate_child_sort(sort, child, required_spec)) {
            // The child's sort already satisfies our requirement
            // Replace Sort->ChildSort with just ChildSort
            OptExpression* grandchild = child->input_at(0);
            if (grandchild != nullptr) {
                result.push_back(grandchild);
            }
            return result;
        }
    }

    // Case 2: Child is a MergeJoin (produces sorted output on join keys)
    if (child_op->get_type() == OptExprOpType::PHYSICAL_MERGE_JOIN) {
        if (can_eliminate_merge_join(sort, child, required_spec)) {
            result.push_back(child);
            return result;
        }
    }

    // Case 3: Child is a HashAgg (produces sorted output on group keys)
    if (child_op->get_type() == OptExprOpType::PHYSICAL_HASH_AGG) {
        if (can_eliminate_hash_agg(sort, child, required_spec)) {
            result.push_back(child);
            return result;
        }
    }

    // Case 4: Child is a PhysicalScan (index scan may preserve order)
    if (child_op->get_type() == OptExprOpType::PHYSICAL_SCAN) {
        if (can_eliminate_scan(sort, child, required_spec)) {
            result.push_back(child);
            return result;
        }
    }

    // Case 5: Child is a LogicalProject (doesn't change row order)
    if (child_op->get_type() == OptExprOpType::LOGICAL_PROJECT) {
        // Recursively check if the project can be eliminated
        OptExpression* grandchild = child->input_at(0);
        if (grandchild != nullptr && !required_spec.get_orderings().empty()) {
            // Check if grandchild already satisfies the order
            OptExprOp* grandchild_op = grandchild->get_op();
            if (grandchild_op != nullptr) {
                // Create a temporary sort to check against
                OrderSpec grandchild_spec;
                if (grandchild_op->get_type() == OptExprOpType::PHYSICAL_SORT) {
                    grandchild_spec = get_sort_order(static_cast<PhysicalSort*>(grandchild_op));
                    if (grandchild_spec.is_satisfy(required_spec)) {
                        // Skip the project and sort, use grandchild directly
                        result.push_back(grandchild);
                        return result;
                    }
                }
            }
        }
    }

    // Case 6: Child is a LogicalFilter (doesn't change row order)
    if (child_op->get_type() == OptExprOpType::LOGICAL_FILTER) {
        OptExpression* grandchild = child->input_at(0);
        if (grandchild != nullptr) {
            OptExprOp* grandchild_op = grandchild->get_op();
            if (grandchild_op != nullptr &&
                grandchild_op->get_type() == OptExprOpType::PHYSICAL_SORT) {
                // Filter on top of sort - check if we can push the sort down
                // and eliminate the top sort
                OrderSpec grandchild_spec = get_sort_order(static_cast<PhysicalSort*>(grandchild_op));
                if (grandchild_spec.is_satisfy(required_spec)) {
                    // The grandchild sort satisfies our requirement
                    // Return grandchild directly, bypassing both filter and top sort
                    result.push_back(grandchild);
                    return result;
                }
            }
        }
    }

    // Could not eliminate the sort
    return result;
}

bool SortEliminationRule::can_eliminate_child_sort(PhysicalSort* sort,
                                                   OptExpression* child,
                                                   const OrderSpec& required_spec) const {
    if (child->get_op()->get_type() != OptExprOpType::PHYSICAL_SORT) {
        return false;
    }

    PhysicalSort* child_sort = static_cast<PhysicalSort*>(child->get_op());
    OrderSpec child_spec = get_sort_order(child_sort);

    // Child's order must satisfy the required order
    return child_spec.is_satisfy(required_spec);
}

bool SortEliminationRule::can_eliminate_merge_join(PhysicalSort* sort,
                                                   OptExpression* child,
                                                   const OrderSpec& required_spec) const {
    if (child->get_op()->get_type() != OptExprOpType::PHYSICAL_MERGE_JOIN) {
        return false;
    }

    PhysicalMergeJoin* merge_join = static_cast<PhysicalMergeJoin*>(child->get_op());
    const std::vector<int>& probe_keys = merge_join->get_probe_keys();

    // MergeJoin produces sorted output on probe keys (left side)
    const auto& required_orderings = required_spec.get_orderings();

    // Check if all required orderings match probe keys in order
    if (probe_keys.size() < required_orderings.size()) {
        return false;
    }

    for (size_t i = 0; i < required_orderings.size(); ++i) {
        // Assuming ascending order for probe keys (could be extended for DESC)
        if (probe_keys[i] != required_orderings[i].column_id) {
            return false;
        }
        // For now, assume probe keys are ascending; could check direction too
    }

    return true;
}

bool SortEliminationRule::can_eliminate_hash_agg(PhysicalSort* sort,
                                                 OptExpression* child,
                                                 const OrderSpec& required_spec) const {
    if (child->get_op()->get_type() != OptExprOpType::PHYSICAL_HASH_AGG) {
        return false;
    }

    PhysicalHashAgg* hash_agg = static_cast<PhysicalHashAgg*>(child->get_op());
    const std::vector<int>& group_keys = hash_agg->get_group_keys();

    // HashAgg produces sorted output on group keys
    const auto& required_orderings = required_spec.get_orderings();

    // Check if all required orderings match group keys in order
    if (group_keys.size() < required_orderings.size()) {
        return false;
    }

    for (size_t i = 0; i < required_orderings.size(); ++i) {
        if (group_keys[i] != required_orderings[i].column_id) {
            return false;
        }
    }

    return true;
}

bool SortEliminationRule::can_eliminate_scan(PhysicalSort* sort,
                                            OptExpression* child,
                                            const OrderSpec& required_spec) const {
    // PhysicalScan with an index scan can produce sorted output
    // This requires checking if the scan path has an index that matches
    // the required sort order. For now, we return false as this
    // requires more detailed path analysis.
    //
    // In a full implementation, you would:
    // 1. Get the scan path from PhysicalScan
    // 2. Check if it's an index scan (vs. sequential scan)
    // 3. Check if the index key matches the required sort order
    // 4. Check if the index preserves the required order (e.g., B-tree index)

    return false;
}

bool SortEliminationRule::is_prefix_order(const OrderSpec& required,
                                         const OrderSpec& child) const {
    const auto& required_orderings = required.get_orderings();
    const auto& child_orderings = child.get_orderings();

    if (child_orderings.size() < required_orderings.size()) {
        return false;
    }

    for (size_t i = 0; i < required_orderings.size(); ++i) {
        if (required_orderings[i].column_id != child_orderings[i].column_id ||
            required_orderings[i].ascending != child_orderings[i].ascending) {
            return false;
        }
    }

    return true;
}

OrderSpec SortEliminationRule::get_sort_order(const PhysicalSort* sort) const {
    // Get sort order from the PhysicalSort's path
    Path* sort_path = sort->get_path();
    if (sort_path == nullptr) {
        return OrderSpec();
    }

    // Extract sort order from path->pathkeys
    // pathkeys is a List* of PathKey nodes
    OrderSpec spec;
    List* pathkeys = sort_path->pathkeys;

    foreach_cell(cell, pathkeys) {
        PathKey* pk = lfirst_node(PathKey, cell);

        // PathKey contains equivalence class and sort direction
        // pk_strategy: BTLessStrategyNumber (1) = ASC, BTGreaterStrategyNumber (4) = DESC
        bool ascending = (pk->pk_strategy == 1);  // BTLessStrategyNumber

        // Get column ID from equivalence class
        // In a full implementation, we would extract the Var from eclass members
        int column_id = -1;
        if (pk->pk_eclass != nullptr && pk->pk_eclass->ec_members != NIL) {
            // Get first member's varattno as column reference
            // This is simplified - real implementation needs proper column extraction
            column_id = 0;  // Placeholder for actual column extraction
        }

        spec.add_ordering(column_id, ascending);
    }

    return spec;
}


}  // namespace cascades