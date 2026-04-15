#include "memo.h"
#include "opt_expression.h"
#include "../operator/operator.h"
#include "nodes/memnodes.h"
#include "utils/memutils.h"
#include <algorithm>
#include <set>

namespace cascades {

Memo::Memo(MemoryContext context)
    : memory_context_(context), next_group_id_(0), root_group_(nullptr) {}

Memo::~Memo() {
    // MemoryContext will automatically free all allocated objects
    // expression_set_ contains GroupExpressions allocated in memory_context_
    // groups_ contains Groups allocated in memory_context_
    // No manual delete needed
}

GroupExpression* Memo::init(OptExpression* expression) {
    groups_.clear();
    expression_map_.clear();
    expression_set_.clear();
    root_group_ = nullptr;
    next_group_id_ = 0;

    GroupExpression* root_group_expression = copy_in(nullptr, expression).second;
    root_group_ = root_group_expression->get_group();
    return root_group_expression;
}

std::pair<bool, GroupExpression*> Memo::insert_group_expression(
    GroupExpression* group_expression, Group* target_group) {
    // Check if equivalent expression already exists
    GroupExpression* existed_group_expression = get_existing_expression(*group_expression);
    if (existed_group_expression != nullptr) {
        Group* existed_group = existed_group_expression->get_group();

        // Check if merge is needed
        if (need_merge(target_group, existed_group)) {
            merge_group(existed_group, target_group);
        }

        return {false, existed_group_expression};
    }

    // Create new group if target is null
    if (target_group == nullptr) {
        target_group = new_group();
    }

    // Insert into hash maps
    expression_map_.insert({group_expression->hash(), group_expression});
    expression_set_.insert(group_expression);

    // Add to group
    target_group->add_expression(group_expression);

    return {true, group_expression};
}

std::pair<bool, GroupExpression*> Memo::insert_transform_result(
    GroupExpression* source_expr, OptExpression* transformed_expr) {
    MemoryContext oldcxt = MemoryContextSwitchTo(memory_context_);

    // Create new GroupExpression using the source's input groups but with transformed operator
    std::vector<Group*> input_groups;
    for (int i = 0; i < source_expr->arity(); ++i) {
        input_groups.push_back(source_expr->input_at(i));
    }

    GroupExpression* new_expr = NEW_EX(GroupExpression, transformed_expr->get_op(), input_groups);

    MemoryContextSwitchTo(oldcxt);

    // Insert into the same group as the source expression
    return insert_group_expression(new_expr, source_expr->get_group());
}

void Memo::insert_enforce_expression(GroupExpression* group_expr, Group* target_group) {
    // Enforcer doesn't create new group, just add to target group's physical expressions
    group_expr->set_group(target_group);
    target_group->add_expression(group_expr);
}

Group* Memo::new_group() {
    MemoryContext oldcxt = MemoryContextSwitchTo(memory_context_);
    Group* g = NEW_EX(Group, next_group_id_++);
    MemoryContextSwitchTo(oldcxt);
    groups_.push_back(g);
    return g;
}

std::pair<Group*, GroupExpression*> Memo::copy_in(Group* target_group, OptExpression* expr) {
    MemoryContext oldcxt = MemoryContextSwitchTo(memory_context_);

    // Recursively copy inputs first
    std::vector<Group*> input_groups;
    for (int i = 0; i < expr->arity(); ++i) {
        OptExpression* child_expr = expr->input_at(i);
        GroupExpression* child_group_expr = child_expr->get_group_expression();

        if (child_group_expr != nullptr) {
            // Child already in memo, use its group
            input_groups.push_back(child_group_expr->get_group());
        } else {
            // Recursively copy child
            auto child_result = copy_in(nullptr, child_expr);
            input_groups.push_back(child_result.first);
        }
    }

    // Create group expression using NEW_EX macro
    GroupExpression* group_expr = NEW_EX(GroupExpression, expr->get_op(), input_groups);
    // Insert the group expression
    auto result = insert_group_expression(group_expr, target_group);

    // For new group with logical operator, derive logical property
    // Only derive if the group doesn't have a logical property yet
    if (result.first && target_group == nullptr) {
        assert(result.second->is_logical());
        result.second->derive_logical_property_itself();

        // Copy statistics from original expression if available
        if (expr->get_statistics() != nullptr) {
            Statistics* stats = NEW_EX(Statistics, *expr->get_statistics());
            result.second->get_group()->set_statistics(stats);
        }
    }

    MemoryContextSwitchTo(oldcxt);
    return {result.second->get_group(), group_expr};
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

GroupExpression* Memo::get_existing_expression(const GroupExpression& expr) const {
    return find_equal_expression(expr);
}

bool Memo::need_merge(Group* target_group, Group* existed_group) const {
    return target_group != nullptr && target_group != existed_group;
}

void Memo::merge_group(Group* src, Group* dst) {
    // Merge src into dst (dst absorbs src's content)
    merge_group_impl(src, dst);

    // Remove self-referencing expressions after merge
    remove_self_referencing(dst);

    // Remove empty groups
    auto empty_groups = get_all_empty_groups();
    for (Group* group : empty_groups) {
        remove_one_group(group);
    }
}

void Memo::remove_self_referencing(Group* group) {
    std::vector<GroupExpression*> remove_list;

    // Find logical expressions that reference their own group
    for (GroupExpression* ge : group->get_logical_expressions()) {
        for (Group* input : ge->get_inputs()) {
            if (input == group) {
                remove_list.push_back(ge);
                break;
            }
        }
    }

    // Find physical expressions that reference their own group
    for (GroupExpression* ge : group->get_physical_expressions()) {
        for (Group* input : ge->get_inputs()) {
            if (input == group) {
                remove_list.push_back(ge);
                break;
            }
        }
    }

    // Remove self-referencing expressions
    for (GroupExpression* ge : remove_list) {
        group->remove_group_expression(ge);
        expression_set_.erase(ge);

        // Remove from multimap
        auto range = expression_map_.equal_range(ge->hash());
        for (auto it = range.first; it != range.second; ) {
            if (it->second == ge) {
                it = expression_map_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void Memo::merge_group_impl(Group* src_group, Group* dst_group) {
    // Remove src from groups list
    groups_.erase(std::remove(groups_.begin(), groups_.end(), src_group), groups_.end());

    // Reset root group if src was root
    if (src_group == root_group_) {
        root_group_ = dst_group;
    }

    // If we change the GroupExpression child group, the hash value will change,
    // so we must reinsert the GroupExpression to expression_map_
    std::vector<GroupExpression*> need_modify_expressions;
    std::vector<GroupExpression*> need_reinsert_expressions;

    // Find all GroupExpressions that reference src_group
    for (auto it = expression_map_.begin(); it != expression_map_.end(); ) {
        GroupExpression* group_expr = it->second;
        bool needs_removal = false;

        // 1. Find GroupExpression which refers to src group (as input)
        for (Group* input : group_expr->get_inputs()) {
            if (input == src_group) {
                needs_removal = true;
                need_modify_expressions.push_back(group_expr);
                break;
            }
        }

        // 2. Find GroupExpression which belongs to src group
        if (group_expr->get_group() == src_group) {
            needs_removal = true;
            need_modify_expressions.push_back(group_expr);
        }

        if (needs_removal) {
            it = expression_map_.erase(it);
            expression_set_.erase(group_expr);
        } else {
            ++it;
        }
    }

    // Modify group references
    for (GroupExpression* modify_expr : need_modify_expressions) {
        // Change group if expression belongs to src
        if (modify_expr->get_group() == src_group) {
            modify_expr->set_group(dst_group);
        }

        // Change input references
        for (int i = 0; i < modify_expr->arity(); ++i) {
            if (modify_expr->input_at(i) == src_group) {
                modify_expr->get_inputs()[i] = dst_group;
                need_reinsert_expressions.push_back(modify_expr);
            }
        }
    }

    // Reinsert modified expressions
    for (GroupExpression* reinsert_expr : need_reinsert_expressions) {
        // Check if already exists (duplicate detection)
        GroupExpression* existing = get_existing_expression(*reinsert_expr);
        if (existing == nullptr) {
            // Not a duplicate, insert normally
            expression_map_.insert({reinsert_expr->hash(), reinsert_expr});
            expression_set_.insert(reinsert_expr);
            dst_group->add_expression(reinsert_expr);
        } else {
            // Duplicate found - merge state from reinsert_expr to existing
            // Similar to StarRocks: existing.mergeGroupExpression(reinsert_expr)
            existing->merge_group_expression(*reinsert_expr);
        }
    }

    // Merge src group content into dst
    dst_group->merge_group(src_group);
}

std::unordered_set<Group*> Memo::get_all_empty_groups() const {
    std::unordered_set<Group*> empty_groups;

    for (Group* group : groups_) {
        if (group->is_empty()) {
            empty_groups.insert(group);
            continue;
        }

        // Check if first logical expression has empty child groups
        GroupExpression* first_logical = group->get_first_logical_expression();
        if (first_logical != nullptr) {
            for (Group* child_group : first_logical->get_inputs()) {
                if (child_group->is_empty()) {
                    empty_groups.insert(child_group);
                    break;
                }
            }
        }
    }

    return empty_groups;
}

void Memo::remove_one_group(Group* group) {
    // Remove from groups list
    groups_.erase(std::remove(groups_.begin(), groups_.end(), group), groups_.end());

    // Remove from hash maps and other groups' inputs
    for (auto it = expression_map_.begin(); it != expression_map_.end(); ) {
        GroupExpression* group_expr = it->second;

        if (group_expr->get_group() == group) {
            it = expression_map_.erase(it);
            expression_set_.erase(group_expr);
            continue;
        }

        // Check if any input references this group
        for (int i = 0; i < group_expr->arity(); ++i) {
            if (group_expr->input_at(i) == group) {
                it = expression_map_.erase(it);
                expression_set_.erase(group_expr);
                break;
            }
        }

        if (it != expression_map_.end() && expression_set_.count(group_expr)) {
            ++it;
        }
    }
}

void Memo::remove_all_empty_groups() {
    auto empty_groups = get_all_empty_groups();

    while (!empty_groups.empty()) {
        for (Group* group : empty_groups) {
            remove_one_group(group);
        }
        empty_groups = get_all_empty_groups();
    }
}

void Memo::remove_unreachable_groups() {
    // Only used in logical rewrite phase
    // Mark all groups reachable from root
    std::set<int> touched_groups;

    if (root_group_ != nullptr) {
        std::vector<Group*> to_visit;
        to_visit.push_back(root_group_);

        while (!to_visit.empty()) {
            Group* current = to_visit.back();
            to_visit.pop_back();

            if (touched_groups.count(current->id())) {
                continue;
            }
            touched_groups.insert(current->id());

            GroupExpression* first_logical = current->get_first_logical_expression();
            if (first_logical != nullptr) {
                for (Group* input : first_logical->get_inputs()) {
                    to_visit.push_back(input);
                }
            }
        }
    }

    // Remove unreachable groups
    groups_.erase(
        std::remove_if(groups_.begin(), groups_.end(),
            [&touched_groups](Group* g) {
                return touched_groups.find(g->id()) == touched_groups.end();
            }),
        groups_.end());

    // Rebuild expression map for remaining groups
    expression_map_.clear();
    expression_set_.clear();

    for (Group* group : groups_) {
        for (GroupExpression* ge : group->get_logical_expressions()) {
            expression_map_.insert({ge->hash(), ge});
            expression_set_.insert(ge);
        }
    }
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
    std::vector<OptExpression*> input_plans;
    std::vector<PhysicalPropertySet> input_properties = expr->get_input_properties(required_property);

    for (int i = 0; i < expr->arity(); ++i) {
        Group* input_group = expr->input_at(i);
        GroupExpression* input_best = input_group->get_best_expression(input_properties[i]);
        if (input_best != nullptr) {
            input_plans.push_back(build_best_plan(input_best, input_properties[i]));
        } else {
            // Fallback: use first logical expression
            GroupExpression* first_logical = input_group->get_first_logical_expression();
            if (first_logical != nullptr) {
                input_plans.push_back(nullptr);
            } else {
                input_plans.push_back(nullptr);
            }
        }
    }

    return OptExpression::create_with_childrens(expr->get_op(), input_plans);
}

std::string Memo::to_string() const {
    std::string result = "Memo with " + std::to_string(groups_.size()) + " groups\n";
    for (auto* g : groups_) {
        result += g->to_string() + "\n";
    }
    return result;
}

} // namespace cascades
