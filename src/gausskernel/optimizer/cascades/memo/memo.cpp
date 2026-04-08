#include "memo.h"
#include "opt_expression.h"
#include "../operator/operator.h"
#include <algorithm>

namespace cascades {

Memo::Memo() : next_group_id_(0), root_group_(nullptr) {}

Memo::~Memo() {
    for (auto* g : groups_) delete g;
}

GroupExpression* Memo::init(OptExpression* expression) {
    GroupExpression* root_expr = copy_in(nullptr, expression).second;
    root_group_ = root_expr->get_group();
    return root_expr;
}

std::pair<Group*, GroupExpression*> Memo::copy_in(Group* target_group, OptExpression* expr) {
    // Recursively copy inputs first
    std::vector<Group*> input_groups;
    for (int i = 0; i < expr->arity(); ++i) {
        OptExpression* child_expr = expr->input_at(i);
        auto child_result = copy_in(nullptr, child_expr);
        input_groups.push_back(child_result.first);
    }

    // Create group expression with the operator and input groups
    GroupExpression* group_expr = new GroupExpression(expr->get_op(), input_groups);

    // If no target group specified, create a new one
    if (target_group == nullptr) {
        target_group = new_group();
    }

    // Insert the group expression into the target group
    insert_group_expression(group_expr, target_group);

    return {target_group, group_expr};
}

GroupExpression* Memo::find_equal_expression(const GroupExpression& expr) const {
    size_t h = expr.hash();
    auto range = expression_map_.equal_range(h);
    for (auto it = range.first; it != range.second; ++it) {
        GroupExpression* existing = it->second;
        if (*existing == expr) {
            return existing;
        }
    }
    return nullptr;
}

std::pair<bool, GroupExpression*> Memo::insert_group_expression(
    GroupExpression* expr, Group* target_group) {
    size_t h = expr->hash();

    // Fast O(1) lookup using hash - check if expression already exists
    GroupExpression* existing = find_equal_expression(*expr);
    if (existing != nullptr) {
        if (target_group != nullptr && target_group != existing->get_group()) {
            merge_group(existing->get_group(), target_group);
        }
        return {false, existing};
    }

    if (target_group == nullptr) {
        target_group = new_group();
    }

    // Insert into hash map (handles collisions via multimap)
    expression_map_.insert({h, expr});
    expression_set_.insert(expr);

    expr->set_group(target_group);
    target_group->add_expression(expr);

    return {true, expr};
}

std::pair<bool, GroupExpression*> Memo::insert_transform_result(
    GroupExpression* source_expr, OptExpression* transformed_expr) {
    // Create new GroupExpression using the source's input groups but with transformed operator
    std::vector<Group*> input_groups;
    for (int i = 0; i < source_expr->arity(); ++i) {
        input_groups.push_back(source_expr->input_at(i));
    }

    GroupExpression* new_expr = new GroupExpression(transformed_expr->get_op(), input_groups);

    // Insert into the same group as the source expression
    return insert_group_expression(new_expr, source_expr->get_group());
}

Group* Memo::new_group() {
    Group* g = new Group(next_group_id_++);
    groups_.push_back(g);
    return g;
}

GroupExpression* Memo::get_exist_expression(const GroupExpression& expr) const {
    return find_equal_expression(expr);
}

void Memo::merge_group(Group* src, Group* dst) {
    src->merge(dst);
    groups_.erase(std::remove(groups_.begin(), groups_.end(), src), groups_.end());
    delete src;
}

void Memo::derive_all_group_logical_property() {
    if (root_group_ != nullptr) {
        root_group_->get_first_logical_expression()->derive_logical_property_recursively();
    }
}

OptExpression* Memo::extract_best_plan(const PhysicalPropertySet& required_property) {
    GroupExpression* best_expr = root_group_->get_best_expression(required_property);
    if (best_expr == nullptr) {
        return nullptr;
    }
    return build_best_plan(best_expr, required_property);
}

OptExpression* Memo::build_best_plan(GroupExpression* expr, const PhysicalPropertySet& required_property) {
    // Create an OptExpression from the GroupExpression
    std::vector<OptExpression*> input_plans;
    std::vector<PhysicalPropertySet> input_properties = expr->get_input_properties(required_property);

    for (size_t i = 0; i < expr->arity(); ++i) {
        Group* input_group = expr->input_at(i);
        GroupExpression* input_best = input_group->get_best_expression(input_properties[i]);
        if (input_best != nullptr) {
            input_plans.push_back(build_best_plan(input_best, input_properties[i]));
        } else {
            // Fallback: just use the first logical expression
            input_plans.push_back(nullptr);
        }
    }

    OptExpression* plan = OptExpression::create(expr->get_op());
    for (size_t i = 0; i < input_plans.size(); ++i) {
        if (input_plans[i] != nullptr) {
            plan->set_input(i, input_plans[i]);
        }
    }

    return plan;
}

Group* Memo::duplicate_detection(OptExpression* expr) {
    // O(1) hash-based lookup instead of O(n²) iteration
    // Build a temporary GroupExpression to compute hash
    std::vector<Group*> input_groups;
    for (int i = 0; i < expr->arity(); ++i) {
        OptExpression* child_expr = expr->input_at(i);
        GroupExpression* child_gexpr = child_expr->get_group_expression();
        if (child_gexpr == nullptr) {
            return nullptr;  // Child not yet in memo
        }
        input_groups.push_back(child_gexpr->get_group());
    }

    GroupExpression temp_expr(expr->get_op(), input_groups);
    GroupExpression* existing = find_equal_expression(temp_expr);

    if (existing != nullptr) {
        return existing->get_group();
    }
    return nullptr;
}

void Memo::remove_empty_groups() {
    groups_.erase(
        std::remove_if(groups_.begin(), groups_.end(),
            [](Group* g) {
                return g->get_logical_expressions().empty() &&
                       g->get_physical_expressions().empty();
            }),
        groups_.end());
}

std::string Memo::to_string() const {
    std::string result = "Memo with " + std::to_string(groups_.size()) + " groups\n";
    for (auto* g : groups_) {
        result += g->to_string() + "\n";
    }
    return result;
}

}  // namespace cascades
