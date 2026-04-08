#include "pattern.h"
#include "../memo/opt_expression.h"

namespace cascades {
Pattern::Pattern(Type type, OptExprOpType op_type)
    : type_(type), op_type_(op_type) {}

Pattern::Pattern(Type type, OptExprOpType op_type, std::initializer_list<Pattern*> children)
    : type_(type), op_type_(op_type), children_(children) {}

Pattern::~Pattern() {}

Pattern* Pattern::leaf(OptExprOpType op_type) {
    return new Pattern(Type::LEAF, op_type);
}

Pattern* Pattern::create(OptExprOpType op_type) {
    return new Pattern(Type::SINGLE, op_type);
}

Pattern* Pattern::create(OptExprOpType op_type, Pattern* child) {
    return new Pattern(Type::SINGLE, op_type, {child});
}

Pattern* Pattern::create(OptExprOpType op_type, Pattern* left, Pattern* right) {
    return new Pattern(Type::MULTI, op_type, {left, right});
}

Pattern* Pattern::any() {
    return new Pattern(Type::LEAF, OptExprOpType::ANY);
}

bool Pattern::match(OptExpression* expression) const {
    if (type_ == Type::LEAF) {
        return true;  // matches anything
    }
    if (expression->get_op()->get_type() != op_type_) {
        return false;
    }
    if (type_ == Type::SINGLE || type_ == Type::MULTI) {
        if (expression->arity() != children_.size()) {
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


}  // namespace cascades
