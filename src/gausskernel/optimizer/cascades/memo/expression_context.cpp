#include "expression_context.h"
#include "opt_expression.h"
#include "group_expression.h"
#include "group.h"

namespace cascades {

ExpressionContext::ExpressionContext(OptExpression* expression) : expression_(expression) {
    init_from_expression(expression);
}

ExpressionContext::ExpressionContext(GroupExpression* group_expression)
    : expression_(nullptr), group_expression_(group_expression) {
    init_from_group_expression(group_expression);
}

ExpressionContext::~ExpressionContext() {
    if (owns_root_property_) {
        delete root_property_;
    }
}

void ExpressionContext::init_from_expression(OptExpression* expression) {
    root_property_ = expression->get_logical_property();
    owns_root_property_ = false;
    statistics_ = expression->get_statistics();

    for (auto* child : expression->get_inputs()) {
        children_properties_.push_back(child->get_logical_property());
        children_statistics_.push_back(child->get_statistics());
    }
}

void ExpressionContext::init_from_group_expression(GroupExpression* group_expr) {
    root_property_ = group_expr->get_group()->get_logical_property();
    owns_root_property_ = false;
    statistics_ = group_expr->get_group()->get_statistics();

    for (auto* child_group : group_expr->get_inputs()) {
        children_properties_.push_back(child_group->get_logical_property());
        children_statistics_.push_back(child_group->get_statistics());
    }
}

OptExprOp* ExpressionContext::get_op() const {
    if (expression_ != nullptr) return expression_->get_op();
    return group_expression_->get_op();
}

LogicalProperty* ExpressionContext::get_child_property(int index) const {
    return children_properties_[index];
}

void ExpressionContext::derive_logical_property() {
    root_property_ = new LogicalProperty();
    owns_root_property_ = true;
    root_property_->derive(this);
}

Statistics* ExpressionContext::get_statistics() const {
    return statistics_;
}

Statistics* ExpressionContext::get_child_statistics(int index) const {
    return children_statistics_[index];
}

ColumnRefSet ExpressionContext::get_child_output_columns(int index) const {
    return children_properties_[index]->get_output_columns();
}

int ExpressionContext::arity() const {
    if (expression_ != nullptr) return expression_->arity();
    return group_expression_->arity();
}

}  // namespace cascades