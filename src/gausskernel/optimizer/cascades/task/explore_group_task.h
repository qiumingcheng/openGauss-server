#pragma once
#include "optimizer_task.h"

namespace cascades {


class Group;

class ExploreGroupTask : public OptimizerTask {
public:
    ExploreGroupTask(TaskContext* context, Group* group);
    void execute() override;
    std::string to_string() const override;

private:
    Group* group_;
};

}