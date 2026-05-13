#include "group.h"
#include "../operator/operators/physical_scan.h"
#include <algorithm>

namespace cascades {

Group::Group(int id)
    : id_(id),
      logical_property_(nullptr),
      statistics_(nullptr),
      is_explored_(false) {}

Group::~Group() {
    // MemoryContext will automatically free logical_property_ and statistics_
    // No manual delete needed
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
    if (expr->is_logical()) {
        logical_expressions_.erase(
            std::remove(logical_expressions_.begin(), logical_expressions_.end(), expr),
            logical_expressions_.end());
    } else {
        physical_expressions_.erase(
            std::remove(physical_expressions_.begin(), physical_expressions_.end(), expr),
            physical_expressions_.end());
    }
}

GroupExpression* Group::get_first_logical_expression() const {
    if (logical_expressions_.empty()) {
        return nullptr;
    }
    return logical_expressions_.front();
}

void Group::set_logical_property(LogicalProperty* property) {
    // MemoryContext manages memory, no need to delete old property
    logical_property_ = property;
}

void Group::set_statistics(Statistics* stats) {
    // MemoryContext manages memory, no need to delete old statistics
    statistics_ = stats;
}

void Group::set_best_expression(GroupExpression* expr, double cost,
                                const PhysicalPropertySet& property) {
    auto it = lowest_cost_expressions_.find(property);
    if (it == lowest_cost_expressions_.end() || it->second.first > cost) {
        lowest_cost_expressions_[property] = {cost, expr};
    }
}

GroupExpression* Group::get_best_expression(const PhysicalPropertySet& property) const {
    auto it = lowest_cost_expressions_.find(property);
    if (it != lowest_cost_expressions_.end()) {
        return it->second.second;
    }
    return nullptr;
}

bool Group::has_best_expression(const PhysicalPropertySet& property) const {
    return lowest_cost_expressions_.find(property) != lowest_cost_expressions_.end();
}

double Group::get_cost_lower_bound(const PhysicalPropertySet& property) const {
    auto it = cost_lower_bounds_.find(property);
    if (it != cost_lower_bounds_.end()) {
        return it->second;
    }
    return -1000.0;  // Default lower bound
}

void Group::set_cost_lower_bound(const PhysicalPropertySet& property, double cost) {
    auto it = cost_lower_bounds_.find(property);
    if (it == cost_lower_bounds_.end() || it->second > cost) {
        cost_lower_bounds_[property] = cost;
    }
}

PhysicalPropertySet Group::update_output_property_set(GroupExpression* expression,
                                                      double cost,
                                                      const PhysicalPropertySet& required_property_set) {
    for (auto& entry : lowest_cost_expressions_) {
        if (entry.first == required_property_set) {
            if (entry.second.first > cost) {
                lowest_cost_expressions_[required_property_set] = {cost, expression};
                return required_property_set;
            } else {
                // Already has an enforcer, use the original one
                return entry.first;
            }
        }
    }
    lowest_cost_expressions_[required_property_set] = {cost, expression};
    return required_property_set;
}

void Group::delete_best_expression(GroupExpression* group_expression) {
    // Remove from lowest cost expressions
    for (auto it = lowest_cost_expressions_.begin(); it != lowest_cost_expressions_.end(); ) {
        if (it->second.second == group_expression) {
            it = lowest_cost_expressions_.erase(it);
        } else {
            ++it;
        }
    }

    // Delete dependent enforcers
    for (auto it = lowest_cost_expressions_.begin(); it != lowest_cost_expressions_.end(); ) {
        PhysicalPropertySet required_property = it->first;
        GroupExpression* best_expr = it->second.second;

        // Enforcer's child group is same as itself
        if (best_expr->arity() == 1 && best_expr->input_at(0) == best_expr->get_group()) {
            // The enforcer needs to be deleted when its input property cannot be satisfied
            bool can_satisfy = false;
            for (const auto& child_input_prop : best_expr->get_input_properties(required_property)) {
                if (lowest_cost_expressions_.count(child_input_prop) > 0) {
                    can_satisfy = true;
                    break;
                }
            }

            if (!can_satisfy) {
                it = lowest_cost_expressions_.erase(it);
            } else {
                ++it;
            }
        } else {
            ++it;
        }
    }
}

void Group::replace_best_expression(GroupExpression* old_expr, GroupExpression* new_expr) {
    std::unordered_map<PhysicalPropertySet, std::pair<double, GroupExpression*>, PhysicalPropertySetHash> to_replace;

    for (auto& entry : lowest_cost_expressions_) {
        if (entry.second.second == old_expr) {
            to_replace[entry.first] = {entry.second.first, new_expr};
        }
    }

    // Remove old entries
    for (const auto& entry : to_replace) {
        lowest_cost_expressions_.erase(entry.first);
    }

    // Add new entries
    lowest_cost_expressions_.insert(to_replace.begin(), to_replace.end());
}

void Group::replace_best_expression_property(const PhysicalPropertySet& old_property,
                                             const PhysicalPropertySet& new_property,
                                             double cost) {
    auto it = lowest_cost_expressions_.find(old_property);
    if (it != lowest_cost_expressions_.end()) {
        GroupExpression* best_expr = it->second.second;
        lowest_cost_expressions_.erase(it);
        lowest_cost_expressions_[new_property] = {cost, best_expr};
    }
}

void Group::merge_group(Group* other) {
    // StarRocks logic: remove dst expressions from src first, then merge
    // other.getLogicalExpressions().removeAll(logicalExpressions);
    // other.getPhysicalExpressions().removeAll(physicalExpressions);

    // Remove expressions that are already in dst from src
    std::vector<GroupExpression*> new_logical_exprs;
    for (GroupExpression* expr : other->logical_expressions_) {
        bool is_duplicate = false;
        for (GroupExpression* dst_expr : logical_expressions_) {
            if (*dst_expr == *expr) {
                is_duplicate = true;
                break;
            }
        }
        if (!is_duplicate) {
            new_logical_exprs.push_back(expr);
        }
    }

    std::vector<GroupExpression*> new_physical_exprs;
    for (GroupExpression* expr : other->physical_expressions_) {
        bool is_duplicate = false;
        for (GroupExpression* dst_expr : physical_expressions_) {
            if (*dst_expr == *expr) {
                is_duplicate = true;
                break;
            }
        }
        if (!is_duplicate) {
            new_physical_exprs.push_back(expr);
        }
    }

    // Merge logical expressions (only new ones)
    for (GroupExpression* expr : new_logical_exprs) {
        expr->set_group(this);
    }
    logical_expressions_.insert(logical_expressions_.end(),
                                new_logical_exprs.begin(),
                                new_logical_exprs.end());
    other->logical_expressions_.clear();

    // Merge physical expressions (only new ones)
    for (GroupExpression* expr : new_physical_exprs) {
        expr->set_group(this);
    }
    physical_expressions_.insert(physical_expressions_.end(),
                                 new_physical_exprs.begin(),
                                 new_physical_exprs.end());
    other->physical_expressions_.clear();

    // Merge lowest cost expressions
    for (auto& entry : other->lowest_cost_expressions_) {
        GroupExpression* best_expr = entry.second.second;
        // Update enforcer's group reference if it points to other
        if (best_expr->get_group() == other) {
            best_expr->set_group(this);
        }
        // Update input group references
        for (int i = 0; i < best_expr->arity(); ++i) {
            if (best_expr->input_at(i) == other) {
                best_expr->get_inputs()[i] = this;
            }
        }
        // Merge cost: keep the lower one
        auto it = lowest_cost_expressions_.find(entry.first);
        if (it == lowest_cost_expressions_.end() || it->second.first > entry.second.first) {
            lowest_cost_expressions_[entry.first] = entry.second;
        }
    }
    other->lowest_cost_expressions_.clear();

    // Merge statistics
    if (statistics_ == nullptr && other->statistics_ != nullptr) {
        statistics_ = other->statistics_;
        other->statistics_ = nullptr;
    }

    // Merge satisfy output property expressions
    for (auto& entry : other->satisfy_output_property_group_expressions_) {
        for (GroupExpression* expr : entry.second) {
            satisfy_output_property_group_expressions_[entry.first].insert(expr);
        }
    }
    other->satisfy_output_property_group_expressions_.clear();
}

bool Group::is_valid_init_state() const {
    return logical_expressions_.size() == 1 && physical_expressions_.empty();
}

std::string Group::to_string() const {
    std::string result = "Group " + std::to_string(id_) + ":\n";
    for (GroupExpression* expr : logical_expressions_) {
        result += "  Logical: " + expr->to_string() + "\n";
    }
    for (GroupExpression* expr : physical_expressions_) {
        result += "  Physical: " + expr->to_string() + "\n";
    }
    return result;
}

std::string Group::debug_string(const std::string& headline_indent,
                                const std::string& detail_indent) const {
    std::string result = headline_indent + "Group " + std::to_string(id_) + "\n";
    for (GroupExpression* expr : logical_expressions_) {
        result += expr->debug_string(headline_indent, detail_indent);
    }
    for (GroupExpression* expr : physical_expressions_) {
        result += expr->debug_string(headline_indent, detail_indent);
    }
    return result;
}

} // namespace cascades
