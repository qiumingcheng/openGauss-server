#pragma once
#include "../base/memory_utils.h"
#include "../property/physical_property_set.h"
#include "../statistics/statistics.h"
#include "../property/logical_property.h"
#include "group_expression.h"
#include <vector>
#include <unordered_map>
#include <set>
#include <string>
#include <limits>

namespace cascades {

class GroupExpression;

/**
 * Group - A set of logically equivalent GroupExpressions
 *
 * A Group represents all possible ways to produce the same logical output.
 * It contains:
 * 1. Logical expressions (different ways to compute the same result)
 * 2. Physical expressions (different execution strategies)
 * 3. Best expression tracking per required physical property
 * 4. Cost lower bounds for pruning
 * 5. Statistics for cost estimation
 */
class Group {
public:
    explicit Group(int id);
    ~Group();

    /**
     * Get the unique group ID.
     * @return Group ID
     */
    int id() const { return id_; }

    /**
     * Add a GroupExpression to this group.
     * @param expr The expression to add
     */
    void add_expression(GroupExpression* expr);

    /**
     * Remove a GroupExpression from this group.
     * @param expr The expression to remove
     */
    void remove_expression(GroupExpression* expr);

    /**
     * Remove a GroupExpression from this group (alias for remove_expression).
     * @param expr The expression to remove
     */
    void remove_group_expression(GroupExpression* expr) { remove_expression(expr); }

    /**
     * Get all logical expressions in this group.
     * @return Vector of logical expressions
     */
    const std::vector<GroupExpression*>& get_logical_expressions() const {
        return logical_expressions_;
    }

    /**
     * Get all physical expressions in this group.
     * @return Vector of physical expressions
     */
    const std::vector<GroupExpression*>& get_physical_expressions() const {
        return physical_expressions_;
    }

    /**
     * Get the first logical expression (used for property derivation).
     * @return The first logical expression
     */
    GroupExpression* get_first_logical_expression() const;

    /**
     * Set the logical property for this group.
     * All expressions in a group share the same logical property.
     * @param property The logical property
     */
    void set_logical_property(LogicalProperty* property);

    /**
     * Get the logical property for this group.
     * @return The logical property
     */
    LogicalProperty* get_logical_property() const { return logical_property_; }

    /**
     * Set the statistics for this group.
     * @param stats The statistics
     */
    void set_statistics(Statistics* stats);

    /**
     * Get the statistics for this group.
     * @return The statistics
     */
    Statistics* get_statistics() const { return statistics_; }

    /**
     * Set the best expression for a given required property.
     * @param expr The best expression
     * @param cost The cost of this expression
     * @param property The required physical property
     */
    void set_best_expression(GroupExpression* expr, double cost,
                            const PhysicalPropertySet& property);

    /**
     * Get the best expression for a given required property.
     * @param property The required physical property
     * @return The best expression, or nullptr if not found
     */
    GroupExpression* get_best_expression(const PhysicalPropertySet& property) const;

    /**
     * Check if this group has a best expression for a given property.
     * @param property The required physical property
     * @return true if a best expression exists
     */
    bool has_best_expression(const PhysicalPropertySet& property) const;

    /**
     * Get the cost lower bound for a given required property.
     * Used for pruning during optimization.
     * @param property The required physical property
     * @return The cost lower bound
     */
    double get_cost_lower_bound(const PhysicalPropertySet& property) const;

    /**
     * Set the cost lower bound for a given required property.
     * @param property The required physical property
     * @param cost The cost lower bound
     */
    void set_cost_lower_bound(const PhysicalPropertySet& property, double cost);

    /**
     * Check if this group has been fully explored.
     * @return true if explored
     */
    bool is_explored() const { return is_explored_; }

    /**
     * Mark this group as explored.
     */
    void set_explored(bool explored) { is_explored_ = explored; }

    /**
     * Merge another group into this group.
     * Absorbs all expressions and state from the other group.
     * @param other The group to merge from
     */
    void merge_group(Group* other);

    /**
     * Check if this group is empty (no logical expressions).
     * @return true if empty
     */
    bool is_empty() const { return logical_expressions_.empty(); }

    /**
     * Check if this group is in valid initial state.
     * Valid state: exactly one logical expression, no physical expressions.
     * @return true if valid initial state
     */
    bool is_valid_init_state() const;

    /**
     * Update the output property set for a given required property.
     * @param expression The expression providing the output property
     * @param cost The cost
     * @param required_property_set The required property set
     * @return The actual output property set
     */
    PhysicalPropertySet update_output_property_set(GroupExpression* expression,
                                                   double cost,
                                                   const PhysicalPropertySet& required_property_set);

    /**
     * Delete a best expression from the group.
     * Also removes dependent enforcers.
     * @param group_expression The expression to delete
     */
    void delete_best_expression(GroupExpression* group_expression);

    /**
     * Replace the best expression for a property.
     * @param old_expr The old best expression
     * @param new_expr The new best expression
     */
    void replace_best_expression(GroupExpression* old_expr, GroupExpression* new_expr);

    /**
     * Replace best expression property.
     * @param old_property The old property
     * @param new_property The new property
     * @param cost The cost
     */
    void replace_best_expression_property(const PhysicalPropertySet& old_property,
                                         const PhysicalPropertySet& new_property,
                                         double cost);

    /**
     * String representation for debugging.
     * @return String description
     */
    std::string to_string() const;

    /**
     * Debug string with indentation.
     * @param headline_indent Indent for group header
     * @param detail_indent Indent for expression details
     * @return Debug string
     */
    std::string debug_string(const std::string& headline_indent,
                            const std::string& detail_indent) const;

private:
    int id_;
    std::vector<GroupExpression*> logical_expressions_;
    std::vector<GroupExpression*> physical_expressions_;

    // All expressions in one group have the same logical property
    LogicalProperty* logical_property_;
    Statistics* statistics_;

    bool is_explored_;

    // Best expression table: required property -> (cost, expression)
    std::unordered_map<PhysicalPropertySet,
                       std::pair<double, GroupExpression*>,
                       PhysicalPropertySetHash> lowest_cost_expressions_;

    // Best parameterized scan expression (for PhysicalNestLoop inner table)
    GroupExpression* best_parameterized_scan_ = nullptr;
    double best_parameterized_scan_cost_ = std::numeric_limits<double>::max();

    // Cost lower bounds for pruning: required property -> lower bound
    std::unordered_map<PhysicalPropertySet, double, PhysicalPropertySetHash> cost_lower_bounds_;

    // Satisfy output property group expressions
    std::unordered_map<PhysicalPropertySet,
                       std::set<GroupExpression*>,
                       PhysicalPropertySetHash> satisfy_output_property_group_expressions_;
};

} // namespace cascades
