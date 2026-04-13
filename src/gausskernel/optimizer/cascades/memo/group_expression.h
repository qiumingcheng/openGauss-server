#pragma once
#include "../operator/operator.h"
#include "../rule/rule_type.h"
#include "../property/physical_property_set.h"
#include "../statistics/statistics.h"
#include <vector>
#include <unordered_map>
#include <set>
#include <string>
#include <bitset>

namespace cascades {

class Group;
class ColumnRefSet;

/**
 * GroupExpression - An expression within a Group in the memo structure
 *
 * A GroupExpression represents an operator with references to input Groups.
 * It is the core building block of the Memo data structure.
 *
 * Key features:
 * 1. Operator with input group references (not expressions)
 * 2. Cost tracking per required property
 * 3. Rule application tracking (explored/applied rules)
 * 4. Output property management
 */
class GroupExpression {
public:
    /**
     * Construct a new GroupExpression.
     * @param op The operator
     * @param inputs The input groups
     */
    GroupExpression(OptExprOp* op, const std::vector<Group*>& inputs);
    ~GroupExpression();

    /**
     * Get the operator.
     * @return The operator
     */
    OptExprOp* get_op() const { return op_; }

    /**
     * Get the input groups.
     * @return Vector of input groups
     */
    const std::vector<Group*>& get_inputs() const { return inputs_; }
    std::vector<Group*>& get_inputs() { return inputs_; }  // Non-const version for modification

    /**
     * Get the group this expression belongs to.
     * @return The parent group
     */
    Group* get_group() const { return group_; }

    /**
     * Set the parent group.
     * @param group The parent group
     */
    void set_group(Group* group) { group_ = group; }

    /**
     * Get the number of inputs.
     * @return Arity
     */
    int arity() const { return inputs_.size(); }

    /**
     * Get the input group at a specific index.
     * @param i The index
     * @return The input group
     */
    Group* input_at(int i) const { return inputs_[i]; }

    /**
     * Check if this is a logical expression.
     * @return true if logical
     */
    bool is_logical() const;

    /**
     * Check if this is a physical expression.
     * @return true if physical
     */
    bool is_physical() const;

    /**
     * Derive the logical property for this expression.
     * Assumes child group logical properties are already derived.
     */
    void derive_logical_property_itself();

    /**
     * Derive logical property recursively.
     * First derives child group properties, then derives this expression's property.
     */
    void derive_logical_property_recursively();

    /**
     * Get the cost for a given required property.
     * @param required_property The required property
     * @return The cost, or -1.0 if not computed
     */
    double get_cost(const PhysicalPropertySet& required_property) const;

    /**
     * Get the input properties for a given required property.
     * @param required_property The required property
     * @return Vector of input properties
     */
    std::vector<PhysicalPropertySet> get_input_properties(
        const PhysicalPropertySet& required_property) const;

    /**
     * Update the property with cost.
     * @param output_property The output property
     * @param input_properties The input properties
     * @param cost The cost
     * @return true if updated (new lower cost)
     */
    bool update_property_with_cost(const PhysicalPropertySet& output_property,
                                   const std::vector<PhysicalPropertySet>& input_properties,
                                   double cost);

    /**
     * Get the output property for a given required property.
     * @param required_property The required property
     * @return The output property
     */
    PhysicalPropertySet get_output_property(const PhysicalPropertySet& required_property) const;

    /**
     * Set the output property that satisfies a required property.
     * @param output_property_set The output property set
     * @param required_property_set The required property set
     */
    void set_output_property_satisfy_required_property(
        const PhysicalPropertySet& output_property_set,
        const PhysicalPropertySet& required_property_set);

    /**
     * Mark a rule as explored for this expression.
     * @param rule_type The rule type
     */
    void set_rule_explored(RuleType rule_type);

    /**
     * Check if a rule has been explored.
     * @param rule_type The rule type
     * @return true if explored
     */
    bool has_rule_explored(RuleType rule_type) const;

    /**
     * Mark a rule as applied for this expression.
     * @param rule_type The rule type
     */
    void set_rule_applied(RuleType rule_type);

    /**
     * Check if a rule has been applied.
     * @param rule_type The rule type
     * @return true if applied
     */
    bool has_rule_applied(RuleType rule_type) const;

    /**
     * Get all applied rules.
     * @return The set of applied rules
     */
    const std::set<RuleType>& get_applied_rules() const { return applied_rules_; }

    /**
     * Merge applied rules from another GroupExpression.
     * @param other The other expression
     */
    void merge_applied_rules(const std::set<RuleType>& other);

    /**
     * Add an applied rule.
     * @param rule_type The rule type
     */
    void add_applied_rule(RuleType rule_type);

    /**
     * Get the hash code for this expression.
     * @return Hash code
     */
    size_t hash() const;

    /**
     * Check equality with another GroupExpression.
     * @param other The other expression
     * @return true if equal
     */
    bool operator==(const GroupExpression& other) const;

    /**
     * Merge another GroupExpression's state into this one.
     * @param other The other expression
     */
    void merge_group_expression(const GroupExpression& other);

    /**
     * Check if this expression is unused.
     * An expression is unused if:
     * 1. Its root group is empty, or
     * 2. Any child group is empty
     * @return true if unused
     */
    bool is_unused() const;

    /**
     * Get child group's output columns.
     * @param index The child index
     * @return The column ref set
     */
    ColumnRefSet get_child_output_columns(int index) const;

    /**
     * String representation for debugging.
     * @return String description
     */
    std::string to_string() const;

    /**
     * Debug string with indentation.
     * @param headline_indent Indent for header
     * @param detail_indent Indent for details
     * @return Debug string
     */
    std::string debug_string(const std::string& headline_indent,
                            const std::string& detail_indent) const;

private:
    Group* group_;                      // Parent group
    std::vector<Group*> inputs_;        // Input groups
    OptExprOp* op_;                     // Operator

    // Cost table: output property -> (cost, input properties)
    std::unordered_map<PhysicalPropertySet,
                       std::pair<double, std::vector<PhysicalPropertySet>>,
                       PhysicalPropertySetHash> lowest_cost_table_;

    // Output property map: required property -> output property
    std::unordered_map<PhysicalPropertySet,
                       PhysicalPropertySet,
                       PhysicalPropertySetHash> output_property_map_;

    // Explored rules (for transformation rules)
    std::set<RuleType> explored_rules_;

    // Applied rules (for implementation rules)
    std::set<RuleType> applied_rules_;

    // Valid output property groups (for enumeration plan)
    std::set<std::pair<PhysicalPropertySet, std::vector<PhysicalPropertySet>>> valid_output_property_groups_;
};

} // namespace cascades
