#include "task_scheduler.h"
#include "../base/memory_utils.h"

namespace cascades {
TaskScheduler::TaskScheduler() {}

void TaskScheduler::execute_tasks(TaskContext* context) {
    while (!tasks_.empty()) {
        std::shared_ptr<OptimizerTask> task = tasks_.top();
        tasks_.pop();
        task->execute();
    }
}

void TaskScheduler::push_task(std::shared_ptr<OptimizerTask> task) {
    tasks_.push(task);
}

void TaskScheduler::rewrite_iterative(OptExpression* tree, TaskContext* context, Rule* rule) {
    // Iterative rewrite until no changes
}

void TaskScheduler::rewrite_once(OptExpression* tree, TaskContext* context, Rule* rule) {
    // One-time rewrite
}

std::string TaskScheduler::to_string() const {
    return "TaskScheduler with " + std::to_string(tasks_.size()) + " tasks";
}

}  // namespace cascades
