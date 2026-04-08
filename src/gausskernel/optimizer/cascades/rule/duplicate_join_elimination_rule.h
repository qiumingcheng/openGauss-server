#pragma once
#include "rule.h"
#include "../memo/opt_expression.h"
#include "../operator/operators.h"
#include <unordered_set>

namespace cascades {


/**
 * DuplicateJoinEliminationRule - Eliminates duplicate join operations
 *
 * A duplicate join occurs when:
 * 1. The same two tables are joined multiple times with equivalent conditions
 * 2. A self-join is performed where the join condition is on the same key(s)
 * 3. Multiple join paths produce the same result set
 *
 * This rule detects and eliminates such redundant joins to improve query performance.
 *
 * Examples:
 *   SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id
 *   SELECT * FROM a t1 JOIN a t2 ON t1.id = t2.id  -- self-join elimination
 *
 *   When two joins are equivalent (same tables, same join keys), only one is kept.
 */
class DuplicateJoinEliminationRule : public Rule {
public:
    DuplicateJoinEliminationRule();

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

private:
    /**
     * Check if two join expressions are equivalent
     * Equivalence is determined by:
     * 1. Same tables on both sides (considering aliases for self-join)
     * 2. Equivalent join conditions (same join keys)
     */
    bool are_joins_equivalent(OptExpression* join1, OptExpression* join2) const;

    /**
     * Check if the join is a self-join (same table on both sides)
     * Handles aliased tables by comparing underlying table IDs
     */
    bool is_self_join(const LogicalJoin* join) const;

    /**
     * Extract the set of tables involved in a join expression
     * For self-joins, the table appears only once in the set
     */
    std::unordered_set<int> get_tables_in_join(OptExpression* join) const;

    /**
     * Check if two sets of join keys are equivalent
     * Keys are equivalent if they reference the same columns
     */
    bool are_keys_equivalent(const std::vector<int>& keys1,
                             const std::vector<int>& keys2) const;

    /**
     * Check if an expression is a simple table scan
     */
    bool is_scan_node(OptExpression* expr) const;

    /**
     * Get table ID from a scan node
     */
    int get_table_id_from_scan(OptExpression* scan) const;
};

}