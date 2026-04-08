#pragma once
#include "group.h"
#include "group_expression.h"
#include "opt_expression.h"
#include "../property/physical_property_set.h"
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>

namespace cascades {


class Memo {
public:
    Memo();
    ~Memo();

    GroupExpression* init(OptExpression* expression);

    std::pair<bool, GroupExpression*> insert_group_expression(
        GroupExpression* expr, Group* target_group = nullptr);

    // Insert a transformed expression - uses the same input groups as the source
    std::pair<bool, GroupExpression*> insert_transform_result(
        GroupExpression* source_expr, OptExpression* transformed_expr);

    Group* new_group();
    Group* get_root_group() const { return root_group_; }
    const std::vector<Group*>& get_groups() const { return groups_; }

    GroupExpression* get_exist_expression(const GroupExpression& expr) const;

    void merge_group(Group* src, Group* dst);

    void derive_all_group_logical_property();

    OptExpression* extract_best_plan(const PhysicalPropertySet& required_property);

    std::string to_string() const;

private:
    int next_group_id_;
    std::vector<Group*> groups_;
    Group* root_group_;

    // Hash-based lookup for GroupExpressions - maps hash to GroupExpression
    // Uses unordered_multimap to handle hash collisions properly
    std::unordered_multimap<size_t, GroupExpression*> expression_map_;

    // Set of all GroupExpressions for O(1) existence check
    std::unordered_set<GroupExpression*> expression_set_;

    Group* duplicate_detection(OptExpression* expr);
    void remove_empty_groups();

    std::pair<Group*, GroupExpression*> copy_in(Group* target_group, OptExpression* expr);
    OptExpression* build_best_plan(GroupExpression* expr, const PhysicalPropertySet& required_property);

    // Internal helper to find an existing GroupExpression by hash
    GroupExpression* find_equal_expression(const GroupExpression& expr) const;
};

}