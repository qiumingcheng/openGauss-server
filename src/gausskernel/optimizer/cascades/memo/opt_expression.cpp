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

}  // namespace cascades