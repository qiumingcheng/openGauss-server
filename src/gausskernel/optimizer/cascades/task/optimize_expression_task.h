#pragma once
#include "optimizer_task.h"
#include <vector>

namespace cascades {


class GroupExpression;
class Rule;

/**
 * OptimizeExpressionTask - Core orchestrator for expression optimization
 *
 * If isExplore is true:
 *   - Applies only transformation rules (logical -> logical)
 *   - Used during initial exploration phase
 *
 * If isExplore is false:
 *   - Applies both transformation and implementation rules
 *   - Used during physical optimization phase
 *
 * Task push order (LIFO, so reverse execution order):
 *   1. ExploreGroupTask for each child (pushed last, executed first)
 *   2. DeriveStatsTask
 *   3. ApplyRuleTask for each valid rule (pushed first, executed last)
 */
class OptimizeExpressionTask : public OptimizerTask {
public:
    OptimizeExpressionTask(TaskContext* context, GroupExpression* expr, bool explore);
    void execute() override;
    std::string to_string() const override;

private:
    /**
     * Collect valid rules for this expression
     * Filters out explored rules, unmatched patterns, and disabled rules
     * Sorts by promise (higher promise = earlier execution)
     */
    std::vector<Rule*> get_valid_rules() const;

    GroupExpression* expression_;
    bool explore_;
};

}