#include "apply_rule_task.h"
#include "task_context.h"
#include "../memo/group_expression.h"
#include "../memo/memo.h"
#include "../rule/rule.h"
#include "../memo/opt_expression.h"
#include "../memo/group.h"

namespace cascades {

// Helper function to build OptExpression tree from GroupExpression
static OptExpression* build_opt_expression(GroupExpression* group_expr) {
    OptExprOp* op = group_expr->get_op();
    std::vector<OptExpression*> children;

    // Recursively build children from input groups
    for (Group* input_group : group_expr->get_inputs()) {
        GroupExpression* child_expr = input_group->get_first_logical_expression();
        if (child_expr != nullptr) {
            children.push_back(build_opt_expression(child_expr));
        }
    }

    OptExpression* opt_expr = OptExpression::create(op);
    for (size_t i = 0; i < children.size(); ++i) {
        opt_expr->set_input(i, children[i]);
    }

    return opt_expr;
}

ApplyRuleTask::ApplyRuleTask(TaskContext* context, GroupExpression* expr, Rule* rule)
    : OptimizerTask(context), expression_(expr), rule_(rule) {}

void ApplyRuleTask::execute() {
    if (expression_->has_rule_explored(rule_->type())) return;

    // Build OptExpression tree from GroupExpression for rule matching
    OptExpression* input = build_opt_expression(expression_);

    // Check if rule matches before applying
    if (!rule_->check(input, context_->get_optimizer_context())) {
        return;
    }

    expression_->set_rule_explored(rule_->type());

    // Transform using the rule
    std::vector<OptExpression*> results = rule_->transform(input, context_->get_optimizer_context());

    // Insert transformed expressions into memo
    Memo* memo = context_->get_optimizer_context()->get_memo();
    for (OptExpression* result : results) {
        memo->insert_transform_result(expression_, result);
    }
}

std::string ApplyRuleTask::to_string() const {
    return "ApplyRuleTask: " + rule_->to_string();
}


}  // namespace cascades