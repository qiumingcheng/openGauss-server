#pragma once
#include "../operator/operator.h"
#include "../rule/rule_type.h"
#include "../property/physical_property_set.h"
#include <vector>
#include <unordered_map>
#include <set>
#include <string>

namespace cascades {


class Group;

/**
 * GroupExpression - An expression within a Group in the memo structure
 * Represents an operator with references to input Groups
 */
class GroupExpression {
public:
    GroupExpression(OptExprOp* op, const std::vector<Group*>& inputs);
    ~GroupExpression();

    OptExprOp* get_op() const { return op_; }
    const std::vector<Group*>& get_inputs() const { return inputs_; }
    Group* get_group() const { return group_; }
    void set_group(Group* group) { group_ = group; }
    int arity() const { return inputs_.size(); }
    Group* input_at(int i) const { return inputs_[i]; }

    bool is_logical() const;
    bool is_physical() const;

    void derive_logical_property();
    void derive_logical_property_recursively();
    void compute_property(const PhysicalPropertySet& required_property);

    double get_cost(const PhysicalPropertySet& required_property) const;
    std::vector<PhysicalPropertySet> get_input_properties(
        const PhysicalPropertySet& required_property) const;
    void update_property_with_cost(const PhysicalPropertySet& output_property,
                                   const std::vector<PhysicalPropertySet>& input_properties,
                                   double cost);

    void set_rule_explored(RuleType rule_type);
    bool has_rule_explored(RuleType rule_type) const;

    size_t hash() const;
    bool operator==(const GroupExpression& other) const;

    std::string to_string() const;

private:
    Group* group_;
    std::vector<Group*> inputs_;
    OptExprOp* op_;
    std::unordered_map<size_t, std::pair<double, std::vector<PhysicalPropertySet>>, std::hash<size_t>> cost_table_;
    std::set<RuleType> explored_rules_;
};

}