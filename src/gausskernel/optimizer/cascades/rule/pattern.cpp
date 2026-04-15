#include "../base/memory_utils.h"
#include "pattern.h"
#include "utils/palloc.h"
#include "../memo/opt_expression.h"
#include "../memo/group_expression.h"

namespace cascades {
Pattern::Pattern(Type type, OptExprOpType op_type)
    : type_(type), op_type_(op_type) {}

Pattern::Pattern(Type type, OptExprOpType op_type, std::initializer_list<Pattern*> children)
    : type_(type), op_type_(op_type), children_(children) {}

Pattern::~Pattern() {}

Pattern* Pattern::leaf(OptExprOpType op_type) {
    return NEW_EX(Pattern, Type::LEAF, op_type);
}

Pattern* Pattern::create(OptExprOpType op_type) {
    return NEW_EX(Pattern, Type::SINGLE, op_type);
}

Pattern* Pattern::create(OptExprOpType op_type, Pattern* child) {
    void* mem = palloc0(sizeof(Pattern));
    return new (mem) Pattern(Type::SINGLE, op_type, {child});
}

Pattern* Pattern::create(OptExprOpType op_type, Pattern* left, Pattern* right) {
    void* mem = palloc0(sizeof(Pattern));
    return new (mem) Pattern(Type::MULTI, op_type, {left, right});
}

Pattern* Pattern::create(OptExprOpType op_type, std::initializer_list<OptExprOpType> children) {
    // Build child patterns as LEAF patterns
    Pattern* pattern = NEW_EX(Pattern, Type::MULTI, op_type);
    for (OptExprOpType child_type : children) {
        pattern->get_children().push_back(leaf(child_type));
    }
    return pattern;
}

Pattern* Pattern::any() {
    return NEW_EX(Pattern, Type::LEAF, OptExprOpType::ANY);
}

bool Pattern::match(OptExpression* expression) const {
    if (type_ == Type::LEAF) {
        return true;  // matches anything
    }
    if (expression->get_op()->get_type() != op_type_) {
        return false;
    }
    if (type_ == Type::SINGLE || type_ == Type::MULTI) {
        if (expression->arity() != static_cast<int>(children_.size())) {
            return false;
        }
        for (size_t i = 0; i < children_.size(); ++i) {
            if (!children_[i]->match(expression->input_at(i))) {
                return false;
            }
        }
    }
    return true;
}

bool Pattern::match_without_child(OptExpression* expression) const {
    // Leaf pattern matches any expression
    if (type_ == Type::LEAF) {
        return true;
    }
    // Check only root operator type, ignore children
    return expression->get_op()->get_type() == op_type_;
}

bool Pattern::match_without_child_group(GroupExpression* group_expr) const {
    // Leaf pattern matches any expression
    if (type_ == Type::LEAF) {
        return true;
    }
    // Check only root operator type, ignore children
    return group_expr->get_op()->get_type() == op_type_;
}


}  // namespace cascades
