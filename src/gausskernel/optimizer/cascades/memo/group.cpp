#include "group.h"
#include <algorithm>

namespace cascades {

Group::Group(int id) : id_(id), logical_property_(nullptr), statistics_(nullptr), is_explored_(false) {}

Group::~Group() {
    for (auto* expr : logical_expressions_) delete expr;
    for (auto* expr : physical_expressions_) delete expr;
    delete logical_property_;
    delete statistics_;
}

void Group::add_expression(GroupExpression* expr) {
    if (expr->is_logical()) {
        logical_expressions_.push_back(expr);
    } else {
        physical_expressions_.push_back(expr);
    }
    expr->set_group(this);
}

void Group::remove_expression(GroupExpression* expr) {
    auto& vec = expr->is_logical() ? logical_expressions_ : physical_expressions_;
    vec.erase(std::remove(vec.begin(), vec.end(), expr), vec.end());
}

GroupExpression* Group::get_first_logical_expression() const {
    return logical_expressions_.front();
}

void Group::set_logical_property(LogicalProperty* property) {
    logical_property_ = property;
}

void Group::set_statistics(Statistics* stats) {
    statistics_ = stats;
}

void Group::set_best_expression(GroupExpression* expr, double cost,
                                const PhysicalPropertySet& property) {
    auto it = best_expressions_.find(property);
    if (it == best_expressions_.end() || it->second.first > cost) {
        best_expressions_[property] = {cost, expr};
    }
}

GroupExpression* Group::get_best_expression(const PhysicalPropertySet& property) const {
    auto it = best_expressions_.find(property);
    if (it != best_expressions_.end()) {
        return it->second.second;
    }
    return nullptr;
}

bool Group::has_best_expression(const PhysicalPropertySet& property) const {
    return best_expressions_.find(property) != best_expressions_.end();
}

double Group::get_cost_lower_bound(const PhysicalPropertySet& property) const {
    auto it = cost_lower_bounds_.find(property);
    return (it != cost_lower_bounds_.end()) ? it->second : 0.0;
}

void Group::set_cost_lower_bound(const PhysicalPropertySet& property, double cost) {
    auto it = cost_lower_bounds_.find(property);
    if (it == cost_lower_bounds_.end() || it->second > cost) {
        cost_lower_bounds_[property] = cost;
    }
}

void Group::merge(Group* other) {
    // Merge expressions
    logical_expressions_.insert(logical_expressions_.end(),
                                other->logical_expressions_.begin(),
                                other->logical_expressions_.end());
    physical_expressions_.insert(physical_expressions_.end(),
                                other->physical_expressions_.begin(),
                                other->physical_expressions_.end());
    other->logical_expressions_.clear();
    other->physical_expressions_.clear();

    // Merge best expressions
    best_expressions_.insert(other->best_expressions_.begin(), other->best_expressions_.end());
    other->best_expressions_.clear();
}

std::string Group::to_string() const {
    return "Group " + std::to_string(id_);
}

}  // namespace cascades
