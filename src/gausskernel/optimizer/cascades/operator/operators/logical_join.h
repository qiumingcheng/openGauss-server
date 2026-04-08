#pragma once
#include "../operator.h"
#include <vector>
#include "nodes/nodes.h"

namespace cascades {

class OptExpression;

/**
 * LogicalJoin - Base class for logical join operators
 * Supports children swapping for join commutativity transformation
 */
class LogicalJoin : public LogicalOptExprOp {
public:
    LogicalJoin(OptExprOpType type, const std::vector<OptExpression*>& children);
    LogicalJoin(OptExprOpType type, OptExpression* left, OptExpression* right);

    OptExprOp* clone() const override = 0;
    void swap_children();

    const std::vector<int>& get_join_keys() const { return join_keys_; }
    void set_join_keys(const std::vector<int>& keys) { join_keys_ = keys; }

    // Overloaded set_join_keys for left/right key separation
    void set_join_keys(const std::vector<int>& left_keys, const std::vector<int>& right_keys) {
        left_join_keys_ = left_keys;
        right_join_keys_ = right_keys;
    }

    const std::vector<int>& get_left_join_keys() const { return left_join_keys_; }
    const std::vector<int>& get_right_join_keys() const { return right_join_keys_; }

    JoinType get_join_type() const { return join_type_; }
    void set_join_type(JoinType type) { join_type_ = type; }

    // Qualifier management for OpenGauss join conditions
    Node* get_qual() const { return qual_; }
    void set_qual(Node* q) { qual_ = q; }

protected:
    LogicalJoin(OptExprOpType type) : LogicalOptExprOp(type) {}

protected:
    std::vector<int> join_keys_;  // join key column indices (for symmetric joins)
    std::vector<int> left_join_keys_;   // left side join keys
    std::vector<int> right_join_keys_;    // right side join keys
    JoinType join_type_ = JOIN_INNER;  // join type (INNER, LEFT, RIGHT, FULL, SEMI)
    Node* qual_ = nullptr;  // OpenGauss join qualifier expression
};

}  // namespace cascades