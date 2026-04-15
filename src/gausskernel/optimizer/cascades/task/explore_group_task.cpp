#include "explore_group_task.h"
#include "task_context.h"
#include "../base/memory_utils.h"
#include "../memo/group.h"
#include "../optimizer/optimizer_context.h"
#include "optimize_expression_task.h"
#include "optimizer/planner.h"

namespace cascades {
ExploreGroupTask::ExploreGroupTask(TaskContext* context, Group* group)
    : OptimizerTask(context), group_(group) {}

void ExploreGroupTask::execute() {
    ereport(WARNING, (errmsg("[CASCADES] ExploreGroupTask: group id=%d, explored=%d, logical_exprs=%d",
                         group_->id(), group_->is_explored() ? 1 : 0,
                         (int)group_->get_logical_expressions().size())));

    if (group_->is_explored()) return;

    // Push OptimizeExpressionTask with explore_=false
    // This will apply both transformation and implementation rules
    // Tasks are LIFO, so ApplyRuleTask executes first, then ExploreGroupTask for children
    for (auto* logical_expr : group_->get_logical_expressions()) {
        ereport(WARNING, (errmsg("[CASCADES] >>> Pushing OptimizeExpressionTask for expr")));
        push_task(MakeTask<OptimizeExpressionTask>(context_, logical_expr, false));
    }
    group_->set_explored(true);
    ereport(WARNING, (errmsg("[CASCADES] ExploreGroupTask: group id=%d marked as explored", group_->id())));
}

std::string ExploreGroupTask::to_string() const {
    return "ExploreGroupTask for group " + std::to_string(group_->id());
}


}  // namespace cascades