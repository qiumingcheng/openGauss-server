#include "opt_expression.h"
#include "group_expression.h"
#include "../base/memory_utils.h"

namespace cascades {

OptExpression::OptExpression(OptExprOp* op)
    : op_(op), group_expression_(nullptr), property_(nullptr), statistics_(nullptr), cost_(0.0) {}

OptExpression::OptExpression(OptExprOp* op, const std::vector<OptExpression*>& inputs)
    : op_(op), inputs_(inputs), group_expression_(nullptr), property_(nullptr), statistics_(nullptr), cost_(0.0) {}

OptExpression::~OptExpression() {
    // MemoryContext will automatically free property_ and statistics_
    // Children are owned by Group, don't delete them
}

OptExpression* OptExpression::create(OptExprOp* op) {
    return NEW_EX(OptExpression, op);
}

OptExpression* OptExpression::create(OptExprOp* op, OptExpression* child) {
    std::vector<OptExpression*> inputs = {child};
    return NEW_EX(OptExpression, op, inputs);
}

OptExpression* OptExpression::create(OptExprOp* op, OptExpression* left, OptExpression* right) {
    std::vector<OptExpression*> inputs = {left, right};
    return NEW_EX(OptExpression, op, inputs);
}

OptExpression* OptExpression::create_with_childrens(OptExprOp* op, const std::vector<OptExpression*>& children) {
    if (children.empty()) {
        return create(op);
    } else if (children.size() == 1) {
        return create(op, children[0]);
    } else if (children.size() == 2) {
        return create(op, children[0], children[1]);
    } else {
        return NEW_EX(OptExpression, op, children);
    }
}

void OptExpression::attach_group_expression(GroupExpression* expr) {
    group_expression_ = expr;
}

ColumnRefSet OptExpression::get_child_output_columns(int index) const {
    return inputs_[index]->get_logical_property()->get_output_columns();
}

std::string OptExpression::to_string() const {
    return op_->to_string();
}

std::string OptExpression::debug_string() const {
    return debug_string(0);
}

std::string OptExpression::debug_string(int indent) const {
    const int indent_step = 4;
    std::string indent_str(indent * indent_step, ' ');

    std::string result = indent_str + op_->to_string();

    // Add statistics info if available
    if (statistics_ != nullptr) {
        result += " [rows=" + std::to_string((int)statistics_->row_count()) + "]";
    }

    // Add cost info if available
    if (cost_ > 0) {
        result += " [cost=" + std::to_string(cost_) + "]";
    }

    result += "\n";

    // Recursively print children
    for (int i = 0; i < (int)inputs_.size(); ++i) {
        OptExpression* child = inputs_[i];
        if (child != nullptr) {
            result += indent_str + "  child[" + std::to_string(i) + "]:\n";
            result += child->debug_string(indent + 1);
        } else {
            result += indent_str + "  child[" + std::to_string(i) + "]: (null)\n";
        }
    }

    return result;
}

}  // namespace cascades