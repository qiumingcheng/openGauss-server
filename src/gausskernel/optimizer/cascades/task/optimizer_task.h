#pragma once
#include <string>

namespace cascades {


// Forward declaration to break circular dependency
class TaskContext;

class OptimizerTask {
public:
    explicit OptimizerTask(TaskContext* context);
    virtual ~OptimizerTask() = default;

    virtual void execute() = 0;
    virtual std::string to_string() const = 0;

protected:
    TaskContext* context_;
};

}