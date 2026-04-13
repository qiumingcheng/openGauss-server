#pragma once
#include "optimizer_task.h"

namespace cascades {


// Forward declarations
class GroupExpression;
class Rule;

/**
 * ApplyRuleTask - Applies a single rule to a GroupExpression
 *
 * Task behavior based on rule type and isExplore flag:
 * 1. Transformation rule + isExplore=true:  Explore mode, push OptimizeExpressionTask(explore)
 * 2. Transformation rule + isExplore=false: Optimize mode, push OptimizeExpressionTask(optimize)
 * 3. Implementation rule:                   Push EnforceAndCostTask for physical optimization
 *
 * StarRocks equivalent: com.starrocks.sql.optimizer.task.ApplyRuleTask
 */
class ApplyRuleTask : public OptimizerTask {
public:
    ApplyRuleTask(TaskContext* context, GroupExpression* expr, Rule* rule, bool is_explore);
    void execute() override;
    std::string to_string() const override;

private:
    GroupExpression* expression_;
    Rule* rule_;
    bool is_explore_;  // Whether this is explore mode (logical only) or optimize mode (logical+physical)
};

}
