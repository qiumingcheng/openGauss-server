#pragma once
#include <string>
#include <memory>

namespace cascades {


// Forward declarations
class TaskContext;
class TaskScheduler;

class OptimizerTask {
public:
    explicit OptimizerTask(TaskContext* context);
    virtual ~OptimizerTask() = default;

    virtual void execute() = 0;
    virtual std::string to_string() const = 0;

protected:
    /**
     * Push a task to the task scheduler.
     * Used for scheduling follow-up tasks after rule application.
     */
    void push_task(std::shared_ptr<OptimizerTask> task);

    TaskContext* context_;
};

}