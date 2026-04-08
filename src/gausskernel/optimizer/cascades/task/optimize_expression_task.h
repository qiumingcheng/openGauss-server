#pragma once
#include "optimizer_task.h"

namespace cascades {


class GroupExpression;

class OptimizeExpressionTask : public OptimizerTask {
public:
    OptimizeExpressionTask(TaskContext* context, GroupExpression* expr, bool explore);
    void execute() override;
    std::string to_string() const override;

private:
    GroupExpression* expression_;
    bool explore_;
};

}