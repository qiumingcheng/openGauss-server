#include "optimize_expression_task.h"
#include "task_context.h"
#include "../base/memory_utils.h"
#include "apply_rule_task.h"
#include "../memo/group_expression.h"
#include "../optimizer/optimizer_context.h"

namespace cascades {
OptimizeExpressionTask::OptimizeExpressionTask(TaskContext* context, GroupExpression* expr, bool explore)
    : OptimizerTask(context), expression_(expr), explore_(explore) {}

void OptimizeExpressionTask::execute() {
    if (explore_) {
        // Apply transformation rules
        for (auto* rule : context_->get_optimizer_context()->get_rule_set()->get_transformation_rules()) {
            context_->get_optimizer_context()->get_task_scheduler()->push_task(
                MakeTask<ApplyRuleTask>(context_, expression_, rule));
        }
    } else {
        // Apply implementation rules
        for (auto* rule : context_->get_optimizer_context()->get_rule_set()->get_implementation_rules()) {
            context_->get_optimizer_context()->get_task_scheduler()->push_task(
                MakeTask<ApplyRuleTask>(context_, expression_, rule));
        }
    }
}

std::string OptimizeExpressionTask::to_string() const {
    return "OptimizeExpressionTask for expression";
}


}  // namespace cascades