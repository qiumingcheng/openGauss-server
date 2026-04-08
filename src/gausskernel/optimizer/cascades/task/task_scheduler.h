#pragma once
#include "optimizer_task.h"
#include "../base/memory_utils.h"
#include <stack>
#include <memory>

namespace cascades {


// Forward declaration
class Rule;
class OptExpression;

class TaskScheduler {
public:
    TaskScheduler();
    ~TaskScheduler() = default;

    void execute_tasks(TaskContext* context);
    void push_task(std::shared_ptr<OptimizerTask> task);

    void rewrite_iterative(OptExpression* tree, TaskContext* context, Rule* rule);
    void rewrite_once(OptExpression* tree, TaskContext* context, Rule* rule);

    std::string to_string() const;

private:
    std::stack<std::shared_ptr<OptimizerTask>> tasks_;
};

}