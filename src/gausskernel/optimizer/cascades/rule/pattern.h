#pragma once
#include "../operator/operator.h"
#include <vector>
#include <string>

namespace cascades {


class OptExpression;

/**
 * Pattern - Used for matching expressions during rule application
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
    static Pattern* any();

    bool match(OptExpression* expression) const;

    Type get_type() const { return type_; }
    OptExprOpType get_operator_type() const { return op_type_; }
    const std::vector<Pattern*>& get_children() const { return children_; }

private:
    Type type_;
    OptExprOpType op_type_;
    std::vector<Pattern*> children_;
};

}
