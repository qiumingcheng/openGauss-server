#pragma once
#include "optimizer_task.h"

namespace cascades {


class GroupExpression;

class DeriveStatsTask : public OptimizerTask {
public:
    DeriveStatsTask(TaskContext* context, GroupExpression* expr);
    void execute() override;
    std::string to_string() const override;

private:
    GroupExpression* expression_;
};

}