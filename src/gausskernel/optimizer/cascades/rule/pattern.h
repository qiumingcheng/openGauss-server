#pragma once
#include "../operator/operator.h"
#include <vector>
#include <string>

namespace cascades {


class OptExpression;
class GroupExpression;

/**
 * Pattern - Used for matching expressions during rule application
 *
 * Examples for constructing multi-level join patterns:
 *
 *   // 2 层：A ⋈ B
 *   Pattern::create(LOGICAL_JOIN, leaf(LOGICAL_SCAN), leaf(LOGICAL_SCAN))
 *
 *   // 3 层：(A ⋈ B) ⋈ C
 *   Pattern::create(LOGICAL_JOIN,
 *       Pattern::create(LOGICAL_JOIN, leaf(LOGICAL_SCAN), leaf(LOGICAL_SCAN)),
 *       leaf(LOGICAL_SCAN))
 *
 *   // 4 层：((A ⋈ B) ⋈ C) ⋈ D
 *   Pattern::create(LOGICAL_JOIN,
 *       Pattern::create(LOGICAL_JOIN,
 *           Pattern::create(LOGICAL_JOIN, leaf(LOGICAL_SCAN), leaf(LOGICAL_SCAN)),
 *           leaf(LOGICAL_SCAN)),
 *       leaf(LOGICAL_SCAN))
 *
 *   // 或使用 initializer_list 构造扁平多子节点：
 *   // join(A, B, C, D)
 *   Pattern::create(LOGICAL_JOIN, {LOGICAL_SCAN, LOGICAL_SCAN, LOGICAL_SCAN, LOGICAL_SCAN})
 */
class Pattern {
public:
    enum class Type { LEAF, SINGLE, MULTI, LIST };

    Pattern(Type type, OptExprOpType op_type);
    Pattern(Type type, OptExprOpType op_type, std::initializer_list<Pattern*> children);
    ~Pattern();

    static Pattern* leaf(OptExprOpType op_type);
    static Pattern* create(OptExprOpType op_type);
    static Pattern* create(OptExprOpType op_type, Pattern* child);
    static Pattern* create(OptExprOpType op_type, Pattern* left, Pattern* right);
    static Pattern* create(OptExprOpType op_type, std::initializer_list<OptExprOpType> children);
    static Pattern* any();

    bool match(OptExpression* expression) const;

    /**
     * Match only the root operator without checking children
     * Used during rule filtering to quickly eliminate non-matching rules
     */
    bool match_without_child(OptExpression* expression) const;

    /**
     * Match only the root operator for GroupExpression
     */
    bool match_without_child_group(GroupExpression* group_expr) const;

    Type get_type() const { return type_; }
    OptExprOpType get_operator_type() const { return op_type_; }
    const std::vector<Pattern*>& get_children() const { return children_; }
    std::vector<Pattern*>& get_children() { return children_; }

private:
    Type type_;
    OptExprOpType op_type_;
    std::vector<Pattern*> children_;
};

}
