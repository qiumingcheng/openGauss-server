#pragma once
#include <vector>
#include <string>
#include <memory>
#include "../operator/operator.h"
#include "../property/logical_property.h"
#include "../statistics/statistics.h"

namespace cascades {


class GroupExpression;

/**
 * OptExpression - A node in the query expression tree
 * Wraps an Operator with its child expressions and associated metadata
 */
class OptExpression {
public:
    OptExpression(OptExprOp* op);
    OptExpression(OptExprOp* op, const std::vector<OptExpression*>& inputs);
    ~OptExpression();

    static OptExpression* create(OptExprOp* op);
    static OptExpression* create(OptExprOp* op, OptExpression* child);
    static OptExpression* create(OptExprOp* op, OptExpression* left, OptExpression* right);
    static OptExpression* create_with_childrens(OptExprOp* op, const std::vector<OptExpression*>& children);

    OptExprOp* get_op() const { return op_; }
    const std::vector<OptExpression*>& get_inputs() const { return inputs_; }
    int arity() const { return inputs_.size(); }
    OptExpression* input_at(int i) const { return inputs_[i]; }
    void set_input(int i, OptExpression* expr) { inputs_[i] = expr; }

    void attach_group_expression(GroupExpression* expr);
    GroupExpression* get_group_expression() const { return group_expression_; }

    LogicalProperty* get_logical_property() const { return property_; }
    void set_logical_property(LogicalProperty* property) {
        // MemoryContext manages memory, no need to delete old property
        property_ = property;
    }

    Statistics* get_statistics() const { return statistics_; }
    void set_statistics(Statistics* stats) {
        // MemoryContext manages memory, no need to delete old statistics
        statistics_ = stats;
    }

    double get_cost() const { return cost_; }
    void set_cost(double cost) { cost_ = cost; }

    ColumnRefSet get_child_output_columns(int index) const;

    std::string to_string() const;
    std::string debug_string() const;
    std::string debug_string(int indent) const;

private:
    OptExprOp* op_;
    std::vector<OptExpression*> inputs_;
    GroupExpression* group_expression_;
    LogicalProperty* property_;
    Statistics* statistics_;
    double cost_;
};

}