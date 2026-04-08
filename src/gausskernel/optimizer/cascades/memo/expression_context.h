#pragma once
#include "../operator/operator.h"
#include "../property/logical_property.h"
#include "../statistics/statistics.h"
#include <vector>

namespace cascades {


class OptExpression;
class GroupExpression;

class ExpressionContext {
public:
    explicit ExpressionContext(OptExpression* expression);
    explicit ExpressionContext(GroupExpression* group_expression);
    ~ExpressionContext();

    OptExpression* get_opt_expression() const { return expression_; }
    GroupExpression* get_group_expression() const { return group_expression_; }

    OptExprOp* get_op() const;

    LogicalProperty* get_root_property() const { return root_property_; }
    LogicalProperty* get_child_property(int index) const;
    void derive_logical_property();

    Statistics* get_statistics() const;
    Statistics* get_child_statistics(int index) const;

    ColumnRefSet get_child_output_columns(int index) const;

    int arity() const;

private:
    void init_from_expression(OptExpression* expression);
    void init_from_group_expression(GroupExpression* group_expression);

    OptExpression* expression_;
    GroupExpression* group_expression_;
    LogicalProperty* root_property_;
    std::vector<LogicalProperty*> children_properties_;
    Statistics* statistics_;
    std::vector<Statistics*> children_statistics_;
};

}