#include "optimizer_task.h"
#include "task_scheduler.h"
#include "task_context.h"
#include "../optimizer/optimizer_context.h"

namespace cascades {

OptimizerTask::OptimizerTask(TaskContext* context) : context_(context) {}

void OptimizerTask::push_task(std::shared_ptr<OptimizerTask> task) {
    context_->get_optimizer_context()->get_task_scheduler()->push_task(task);
}

}  // namespace cascades