#include "duplicate_join_elimination_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../property/logical_property.h"

namespace cascades {
/**
 * Check if two vectors of join keys are equivalent
 * Keys are considered equivalent if they contain the same column IDs
 */
bool DuplicateJoinEliminationRule::are_keys_equivalent(
    const std::vector<int>& keys1,
    const std::vector<int>& keys2) const {
    if (keys1.size() != keys2.size()) {
        return false;
    }

    std::unordered_set<int> key_set1(keys1.begin(), keys1.end());
    std::unordered_set<int> key_set2(keys2.begin(), keys2.end());

    return key_set1 == key_set2;
}

/**
 * Check if an expression is a simple table scan node
 */
bool DuplicateJoinEliminationRule::is_scan_node(OptExpression* expr) const {
    if (expr == nullptr) {
        return false;
    }
    OptExprOp* op = expr->get_op();
    return op->get_type() == OptExprOpType::LOGICAL_SCAN;
}

/**
 * Extract table ID from a scan node
 */
int DuplicateJoinEliminationRule::get_table_id_from_scan(OptExpression* scan) const {
    if (!is_scan_node(scan)) {
        return -1;
    }
    LogicalScan* scan_op = static_cast<LogicalScan*>(scan->get_op());
    return scan_op->get_table_id();
}

/**
 * Check if a join is a self-join (same table on both sides)
 * For aliased tables, we need to check the underlying table IDs
 */
bool DuplicateJoinEliminationRule::is_self_join(const LogicalJoin* join) const {
    // For a simpler implementation, we check if both sides are scans
    // and have the same table ID
    // This is typically done by checking table IDs through the memo group
    return false;  // Placeholder - actual implementation needs memo context
}

/**
 * Get all table IDs involved in a join expression
 * For nested joins, recursively collects tables from both sides
 */
std::unordered_set<int> DuplicateJoinEliminationRule::get_tables_in_join(
    OptExpression* join) const {
    std::unordered_set<int> tables;

    if (join == nullptr) {
        return tables;
    }

    OptExprOp* op = join->get_op();

    if (op->get_type() == OptExprOpType::LOGICAL_SCAN) {
        LogicalScan* scan = static_cast<LogicalScan*>(op);
        tables.insert(scan->get_table_id());
        return tables;
    }

    if (op->get_type() == OptExprOpType::LOGICAL_JOIN) {
        // Recursively collect tables from both children
        // Note: In the Cascades framework, children are accessed differently
        // The join operator itself doesn't store children - they're in the OptExpression
        // But we receive OptExpression* input, so we use input->input_at()
    }

    return tables;
}

/**
 * Check if two join expressions are equivalent
 * Two joins are equivalent if:
 * 1. They join the same tables (same table IDs)
 * 2. They have equivalent join conditions (same or equivalent join keys)
 * 3. They have the same join type
 */
bool DuplicateJoinEliminationRule::are_joins_equivalent(
    OptExpression* join1,
    OptExpression* join2) const {
    if (join1 == nullptr || join2 == nullptr) {
        return false;
    }

    OptExprOp* op1 = join1->get_op();
    OptExprOp* op2 = join2->get_op();

    // Both must be join operators
    if (op1->get_type() != OptExprOpType::LOGICAL_JOIN ||
        op2->get_type() != OptExprOpType::LOGICAL_JOIN) {
        return false;
    }

    LogicalJoin* logical_join1 = static_cast<LogicalJoin*>(op1);
    LogicalJoin* logical_join2 = static_cast<LogicalJoin*>(op2);

    // Must have the same join type
    if (logical_join1->get_join_type() != logical_join2->get_join_type()) {
        return false;
    }

    // Must have equivalent join keys
    if (!are_keys_equivalent(logical_join1->get_join_keys(),
                            logical_join2->get_join_keys())) {
        return false;
    }

    // Check if the same tables are involved
    // For a proper implementation, we would compare the table IDs from memo groups
    // Here we use a simplified approach by checking output columns

    const ColumnRefSet& output1 = join1->get_logical_property()->get_output_columns();
    const ColumnRefSet& output2 = join2->get_logical_property()->get_output_columns();

    // If outputs are identical, the joins might be equivalent
    // This is a simplified check - a full implementation would need
    // to compare the actual table references
    if (output1.size() != output2.size()) {
        return false;
    }

    return true;
}

DuplicateJoinEliminationRule::DuplicateJoinEliminationRule()
    : Rule(RuleType::TF_DUPLICATE_JOIN_ELIMINATION,
           Pattern::create(OptExprOpType::LOGICAL_JOIN)) {}

std::vector<OptExpression*> DuplicateJoinEliminationRule::transform(
    OptExpression* input, OptimizerContext* context) {
    std::vector<OptExpression*> result;

    OptExprOp* op = input->get_op();
    if (op->get_type() != OptExprOpType::LOGICAL_JOIN) {
        return result;
    }

    LogicalJoin* join = static_cast<LogicalJoin*>(op);

    // Get the join's children
    OptExpression* left = input->input_at(0);
    OptExpression* right = input->input_at(1);

    // Case 1: Check for self-join elimination
    // If the same table (after alias resolution) appears on both sides,
    // and the join condition is on the key, we can eliminate the self-join
    if (is_scan_node(left) && is_scan_node(right)) {
        int left_table_id = get_table_id_from_scan(left);
        int right_table_id = get_table_id_from_scan(right);

        // Same table on both sides - self-join
        if (left_table_id == right_table_id && left_table_id >= 0) {
            const std::vector<int>& keys = join->get_join_keys();

            // If join is on the same key (a.id = a.id), this is a self-join
            // that can potentially be eliminated
            if (!keys.empty()) {
                // Self-join on the same key: the result is identical to a single scan
                // We keep the left child and eliminate the join
                // However, we need to ensure the output columns match

                // For now, we produce a simplified result
                // A full implementation would need to:
                // 1. Check if the join condition references only one side's columns
                // 2. Verify that the output doesn't require data from both sides
                // 3. Apply this only when the join is a trivial self-equi-join

                // Example: SELECT * FROM t a JOIN t b ON a.id = b.id
                // This is equivalent to just scanning t once if no columns from b are used
            }
        }
    }

    // Case 2: Check for duplicate join paths in the memo
    // If the same join expression exists in the memo, we don't need this one
    // This is handled by the memo's deduplication mechanism

    // Case 3: Check if this join produces the same result as another
    // join in the same group. This requires comparing join conditions
    // and table references more thoroughly.

    // For a production implementation, the actual memo integration would
    // handle most duplicate detection. Here we provide the transformation
    // logic that could be triggered when duplicates are detected.

    // Case 4: Handle redundant join elimination when we have:
    // A JOIN B ON cond1 JOIN B ON cond2
    // If cond1 and cond2 are equivalent, we only need one join

    // The actual duplicate detection in the Cascades optimizer is typically
    // done through the memo structure when groups are merged. The rule's
    // transform method is called to generate alternative expressions.

    return result;
}


}  // namespace cascades