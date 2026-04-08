#include "apply_rule_task.h"
#include "task_context.h"
#include "../memo/group_expression.h"
#include "../memo/memo.h"
#include "../rule/rule.h"
#include "../memo/opt_expression.h"

namespace cascades {
ApplyRuleTask::ApplyRuleTask(TaskContext* context, GroupExpression* expr, Rule* rule)
    : OptimizerTask(context), expression_(expr), rule_(rule) {}

void ApplyRuleTask::execute() {
    if (expression_->has_rule_explored(rule_->type())) return;

    expression_->set_rule_explored(rule_->type());

    // Create OptExpression from GroupExpression for the rule to transform
    OptExpression input(expression_->get_op());
    for (int i = 0; i < expression_->arity(); ++i) {
        // Note: For proper transformation, we need OptExpressions with actual operators
        // This is a simplified approach - in practice, we would build proper OptExpression tree
    }

    // Transform using the rule
    // Note: This simplified version just applies the rule's transform directly
    // A more complete implementation would build proper OptExpression trees
    std::vector<OptExpression*> results = rule_->transform(&input, context_->get_optimizer_context());

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