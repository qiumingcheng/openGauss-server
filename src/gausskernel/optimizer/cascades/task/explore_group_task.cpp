#include "explore_group_task.h"
#include "task_context.h"
#include "../base/memory_utils.h"
#include "../memo/group.h"
#include "../optimizer/optimizer_context.h"
#include "optimize_expression_task.h"

namespace cascades {
ExploreGroupTask::ExploreGroupTask(TaskContext* context, Group* group)
    : OptimizerTask(context), group_(group) {}

void ExploreGroupTask::execute() {
    if (group_->is_explored()) return;

    for (auto* logical_expr : group_->get_logical_expressions()) {
        context_->get_optimizer_context()->get_task_scheduler()->push_task(
            MakeTask<OptimizeExpressionTask>(context_, logical_expr, true));
    }
    group_->set_explored(true);
}

std::string ExploreGroupTask::to_string() const {
    return "ExploreGroupTask for group " + std::to_string(group_->id());
}


}  // namespace cascades