#pragma once
#include "../operator/operator.h"
#include "../property/logical_property.h"
#include "../statistics/statistics.h"
#include <vector>

namespace cascades {


class OptExpression;
class GroupExpression;

/**
 * ExpressionContext - Context for deriving properties from an expression
 *
 * Provides a unified interface for accessing operator-specific information
 * during logical property and statistics derivation.
 */
class ExpressionContext {
public:
    explicit ExpressionContext(OptExpression* expression);
    explicit ExpressionContext(GroupExpression* group_expression);
    ~ExpressionContext();

    // Basic expression access
    OptExpression* get_opt_expression() const { return expression_; }
    GroupExpression* get_group_expression() const { return group_expression_; }
    OptExprOp* get_op() const;

    // Logical property access
    LogicalProperty* get_root_property() const { return root_property_; }
    LogicalProperty* get_child_property(int index) const;
    void derive_logical_property();

    // Statistics access
    Statistics* get_statistics() const;
    Statistics* get_child_statistics(int index) const;

    // Column access
    ColumnRefSet get_child_output_columns(int index) const;

    // Arity (number of children)
    int arity() const;

    // ========================================================================
    // Operator-specific property access helpers
    // These provide type-safe access to operator-specific properties
    // ========================================================================

    /**
     * Get join keys from a join operator
     * @param left_keys Output: left join keys
     * @param right_keys Output: right join keys
     * @return true if this is a join operator with valid keys
     */
    bool get_join_keys(std::vector<int>& left_keys, std::vector<int>& right_keys) const;

    /**
     * Get group keys from an aggregate operator
     * @return Group keys, or empty vector if not an aggregate
     */
    std::vector<int> get_group_keys() const;

    /**
     * Get filter qual list for selectivity estimation
     * @return Qual list (List* in openGauss terms), or nullptr
     */
    void* get_filter_qual_list() const;

    /**
     * Get table OID from a scan operator
     * @return Table OID, or InvalidOid if not a scan or OID not available
     */
    Oid get_scan_table_oid() const;

private:
    void init_from_expression(OptExpression* expression);
    void init_from_group_expression(GroupExpression* group_expression);

    OptExpression* expression_;
    GroupExpression* group_expression_;
    LogicalProperty* root_property_;
    bool owns_root_property_;  // Track if we own the root_property_
    std::vector<LogicalProperty*> children_properties_;
    Statistics* statistics_;
    std::vector<Statistics*> children_statistics_;
};

}