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

/**
 * Memo - The core data structure for Cascades optimizer
 *
 * The Memo stores all GroupExpressions generated during optimization.
 * It provides:
 * 1. Duplicate detection via hash-based lookup
 * 2. Group merging when equivalent expressions are found
 * 3. Best expression tracking per required property
 * 4. Cost lower bound management for pruning
 */
class Memo {
public:
    Memo();
    ~Memo();

    /**
     * Initialize the Memo with an OptExpression tree.
     * Recursively copies the expression tree into Group/GroupExpression structure.
     * @param expression The root of the expression tree to copy
     * @return The root GroupExpression in the Memo
     */
    GroupExpression* init(OptExpression* expression);

    /**
     * Insert a GroupExpression into the Memo.
     * If an equivalent expression already exists, merge the groups.
     * @param expr The GroupExpression to insert
     * @param target_group Optional target group (nullptr for new group)
     * @return Pair of (is_new_insert, the GroupExpression)
     */
    std::pair<bool, GroupExpression*> insert_group_expression(
        GroupExpression* expr, Group* target_group = nullptr);

    /**
     * Insert a transformed expression result.
     * Uses the same input groups as the source expression.
     * @param source_expr The source expression being transformed
     * @param transformed_expr The transformed result
     * @return Pair of (is_new_insert, the new GroupExpression)
     */
    std::pair<bool, GroupExpression*> insert_transform_result(
        GroupExpression* source_expr, OptExpression* transformed_expr);

    /**
     * Insert an enforcer expression into a target group.
     * Enforcers don't create new groups - they reference existing ones.
     * @param group_expr The enforcer GroupExpression
     * @param target_group The target group
     */
    void insert_enforce_expression(GroupExpression* group_expr, Group* target_group);

    /**
     * Create a new empty Group.
     * @return The newly created Group
     */
    Group* new_group();

    /**
     * Get the root group of the Memo.
     * @return The root group
     */
    Group* get_root_group() const { return root_group_; }

    /**
     * Get all groups in the Memo.
     * @return Vector of all groups
     */
    const std::vector<Group*>& get_groups() const { return groups_; }

    /**
     * Find an existing GroupExpression equivalent to the given one.
     * @param expr The expression to look up
     * @return The existing expression, or nullptr if not found
     */
    GroupExpression* get_existing_expression(const GroupExpression& expr) const;

    /**
     * Merge two groups when they are found to be equivalent.
     * @param src The source group (will be emptied and removed)
     * @param dst The destination group (will absorb src's content)
     */
    void merge_group(Group* src, Group* dst);

    /**
     * Derive logical properties for all groups starting from root.
     */
    void derive_all_group_logical_property();

    /**
     * Extract the best plan from the Memo for a given required property.
     * @param required_property The required physical property
     * @return The best plan as an OptExpression tree, or nullptr
     */
    OptExpression* extract_best_plan(const PhysicalPropertySet& required_property);

    /**
     * Remove all empty groups from the Memo.
     */
    void remove_all_empty_groups();

    /**
     * Remove unreachable groups (only used in logical rewrite phase).
     */
    void remove_unreachable_groups();

    /**
     * String representation for debugging.
     * @return String description of the Memo
     */
    std::string to_string() const;

private:
    int next_group_id_;
    std::vector<Group*> groups_;
    Group* root_group_;

    // Hash-based lookup for GroupExpressions
    // Uses unordered_multimap to handle hash collisions
    std::unordered_multimap<size_t, GroupExpression*> expression_map_;

    // Set of all GroupExpressions for O(1) existence check
    std::unordered_set<GroupExpression*> expression_set_;

    /**
     * Recursively copy an OptExpression into the Memo structure.
     * @param target_group Optional target group
     * @param expression The expression to copy
     * @return Pair of (created_group, created_group_expression)
     */
    std::pair<Group*, GroupExpression*> copy_in(Group* target_group, OptExpression* expr);

    /**
     * Build the best plan recursively from a GroupExpression.
     * @param expr The GroupExpression
     * @param required_property The required property
     * @return The best plan as OptExpression
     */
    OptExpression* build_best_plan(GroupExpression* expr, const PhysicalPropertySet& required_property);

    /**
     * Internal helper to find an existing GroupExpression by hash.
     * @param expr The expression to find
     * @return The existing expression, or nullptr
     */
    GroupExpression* find_equal_expression(const GroupExpression& expr) const;

    /**
     * Check if two groups need to be merged.
     * @param target_group The target group
     * @param existed_group The existing group
     * @return true if merge is needed
     */
    bool need_merge(Group* target_group, Group* existed_group) const;

    /**
     * Find all empty groups in the Memo.
     * @return Set of empty groups
     */
    std::unordered_set<Group*> get_all_empty_groups() const;

    /**
     * Remove a single group from the Memo.
     * @param group The group to remove
     */
    void remove_one_group(Group* group);

    /**
     * Internal helper to merge two groups.
     * @param src_group The source group
     * @param dst_group The destination group
     */
    void merge_group_impl(Group* src_group, Group* dst_group);

    /**
     * Remove self-referencing expressions from a group.
     * @param group The group to process
     */
    void remove_self_referencing(Group* group);
};

} // namespace cascades
