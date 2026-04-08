#pragma once
#include "optimizer_task.h"

namespace cascades {


// Forward declarations
class GroupExpression;
class Rule;

class ApplyRuleTask : public OptimizerTask {
public:
    ApplyRuleTask(TaskContext* context, GroupExpression* expr, Rule* rule);
    void execute() override;
    std::string to_string() const override;

private:
    GroupExpression* expression_;
    Rule* rule_;
};

}