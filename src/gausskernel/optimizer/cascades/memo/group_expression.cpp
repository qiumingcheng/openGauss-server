#include "group_expression.h"
#include "group.h"
#include "expression_context.h"
#include <algorithm>

namespace cascades {

GroupExpression::GroupExpression(OptExprOp* op, const std::vector<Group*>& inputs)
    : group_(nullptr), op_(op), inputs_(inputs) {}

GroupExpression::~GroupExpression() {
    // Note: Group and OptExprOp are not owned by GroupExpression
    // They are deleted by their respective owners
}

bool GroupExpression::is_logical() const {
    return op_->is_logical();
}

bool GroupExpression::is_physical() const {
    return op_->is_physical();
}

void GroupExpression::derive_logical_property_itself() {
    ExpressionContext context(this);
    context.derive_logical_property();
    if (group_ != nullptr) {
        // Property is allocated in MemoryContext, just assign it to group
        group_->set_logical_property(context.get_root_property());
    }
}

void GroupExpression::derive_logical_property_recursively() {
    // First, derive logical property for all input groups
    for (Group* input_group : inputs_) {
        GroupExpression* first_logical = input_group->get_first_logical_expression();
        if (first_logical != nullptr) {
            first_logical->derive_logical_property_recursively();
        }
    }
    // Then, derive logical property for this expression
    derive_logical_property_itself();
}

double GroupExpression::get_cost(const PhysicalPropertySet& required_property) const {
    auto it = lowest_cost_table_.find(required_property);
    if (it != lowest_cost_table_.end()) {
        return it->second.first;
    }
    return -1.0;  // indicates not computed
}

std::vector<PhysicalPropertySet> GroupExpression::get_input_properties(
    const PhysicalPropertySet& required_property) const {
    auto it = lowest_cost_table_.find(required_property);
    if (it != lowest_cost_table_.end()) {
        return it->second.second;
    }
    return {};
}

bool GroupExpression::update_property_with_cost(
    const PhysicalPropertySet& required_property,
    const PhysicalPropertySet& output_property,
    const std::vector<PhysicalPropertySet>& input_properties,
    double cost) {
    auto it = lowest_cost_table_.find(required_property);
    if (it != lowest_cost_table_.end()) {
        if (it->second.first > cost) {
            lowest_cost_table_[required_property] = {cost, input_properties};
            output_property_map_[required_property] = output_property;
            return true;
        }
        return false;
    } else {
        lowest_cost_table_[required_property] = {cost, input_properties};
        output_property_map_[required_property] = output_property;
        return true;
    }
}

PhysicalPropertySet GroupExpression::get_output_property(
    const PhysicalPropertySet& required_property) const {
    auto it = output_property_map_.find(required_property);
    if (it != output_property_map_.end()) {
        return it->second;
    }
    return PhysicalPropertySet();  // Return empty property
}

void GroupExpression::set_rule_explored(RuleType rule_type) {
    explored_rules_.insert(rule_type);
}

bool GroupExpression::has_rule_explored(RuleType rule_type) const {
    return explored_rules_.find(rule_type) != explored_rules_.end();
}

void GroupExpression::set_rule_applied(RuleType rule_type) {
    applied_rules_.insert(rule_type);
}

bool GroupExpression::has_rule_applied(RuleType rule_type) const {
    return applied_rules_.find(rule_type) != applied_rules_.end();
}

void GroupExpression::merge_applied_rules(const std::set<RuleType>& other) {
    applied_rules_.insert(other.begin(), other.end());
}

void GroupExpression::add_applied_rule(RuleType rule_type) {
    applied_rules_.insert(rule_type);
}

size_t GroupExpression::hash() const {
    size_t h = op_->hash();
    for (Group* g : inputs_) {
        h = h * 31 + g->id();
    }
    return h;
}

bool GroupExpression::operator==(const GroupExpression& other) const {
    if (this == &other) {
        return true;
    }
    if (!op_->equals(other.op_)) {
        return false;
    }
    if (inputs_.size() != other.inputs_.size()) {
        return false;
    }
    for (size_t i = 0; i < inputs_.size(); ++i) {
        if (inputs_[i]->id() != other.inputs_[i]->id()) {
            return false;
        }
    }
    return true;
}

void GroupExpression::merge_group_expression(const GroupExpression& other) {
    // Merge lowest cost table
    for (const auto& entry : other.lowest_cost_table_) {
        auto it = lowest_cost_table_.find(entry.first);
        if (it == lowest_cost_table_.end()) {
            lowest_cost_table_[entry.first] = entry.second;
        } else if (it->second.first > entry.second.first) {
            it->second = entry.second;
        }
    }

    // Merge output property map
    output_property_map_.insert(other.output_property_map_.begin(),
                                other.output_property_map_.end());
}

bool GroupExpression::is_unused() const {
    // Check if root group is empty
    if (group_ != nullptr && group_->is_empty()) {
        return true;
    }

    // Check if any child group is empty
    for (Group* child_group : inputs_) {
        if (child_group != nullptr && child_group->is_empty()) {
            return true;
        }
    }

    return false;
}

ColumnRefSet GroupExpression::get_child_output_columns(int index) const {
    if (index < 0 || index >= static_cast<int>(inputs_.size())) {
        return ColumnRefSet();
    }
    Group* child_group = inputs_[index];
    if (child_group == nullptr || child_group->get_logical_property() == nullptr) {
        return ColumnRefSet();
    }
    return child_group->get_logical_property()->get_output_columns();
}

std::string GroupExpression::to_string() const {
    return op_->to_string();
}

std::string GroupExpression::debug_string(const std::string& headline_indent,
                                          const std::string& detail_indent) const {
    std::string result = headline_indent + "GroupExpression: " + op_->to_string() + "\n";
    result += detail_indent + "Inputs: ";
    for (size_t i = 0; i < inputs_.size(); ++i) {
        if (i > 0) result += ", ";
        result += "G" + std::to_string(inputs_[i]->id());
    }
    result += "\n";

    if (children_required_property_ != nullptr) {
        result += detail_indent + "Children required property: " + children_required_property_->to_string() + "\n";
    }

    return result;
}

void GroupExpression::set_children_required_property(const PhysicalPropertySet* property) {
    children_required_property_ = property;
}

} // namespace cascades
