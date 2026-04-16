#include "optimize_expression_task.h"
#include "task_context.h"
#include "apply_rule_task.h"
#include "explore_group_task.h"
#include "derive_stats_task.h"
#include "../base/memory_utils.h"
#include "../memo/group_expression.h"
#include "../memo/group.h"
#include "../rule/rule.h"
#include "../rule/rule_set.h"
#include "../optimizer/optimizer_context.h"
#include <algorithm>

namespace cascades {

OptimizeExpressionTask::OptimizeExpressionTask(TaskContext* context, GroupExpression* expr, bool explore)
    : OptimizerTask(context), expression_(expr), explore_(explore) {}

std::vector<Rule*> OptimizeExpressionTask::get_valid_rules() const {
    std::vector<Rule*> valid_rules;
    RuleSet* rule_set = context_->get_optimizer_context()->get_rule_set();

    // Collect transformation rules (always applied)
    const auto& transform_rules = rule_set->get_transformation_rules();
    for (Rule* rule : transform_rules) {
        // Transformation rules only apply to logical expressions
        if (!expression_->is_logical()) {
            continue;
        }

        // Skip if rule has been explored
        if (expression_->has_rule_explored(rule->type())) {
            continue;
        }

        // Skip if rule pattern doesn't match
        if (!rule->get_pattern()->match_without_child_group(expression_)) {
            continue;
        }

        // Skip if rule is exhausted (timeout protection)
        if (rule->exhausted(context_->get_optimizer_context())) {
            continue;
        }

        valid_rules.push_back(rule);
    }

    // Collect implementation rules (only when not in explore mode)
    if (!explore_) {
        const auto& implement_rules = rule_set->get_implementation_rules();
        for (Rule* rule : implement_rules) {
            // Skip if rule has been applied
            if (expression_->has_rule_applied(rule->type())) {
                continue;
            }

            // Skip if rule pattern doesn't match
            if (!rule->get_pattern()->match_without_child_group(expression_)) {
                continue;
            }

            // Skip if rule is exhausted (timeout protection)
            if (rule->exhausted(context_->get_optimizer_context())) {
                continue;
            }

            valid_rules.push_back(rule);
        }
    }

    // Sort by promise (higher promise = earlier execution)
    // StarRocks: validRules.sort(Comparator.comparingInt(Rule::promise))
    std::sort(valid_rules.begin(), valid_rules.end(),
              [](Rule* a, Rule* b) { return a->promise() > b->promise(); });

    return valid_rules;
}

void OptimizeExpressionTask::execute() {
    ereport(WARNING, (errmsg("[CASCADES] OptimizeExpressionTask: expr=%s, explore=%d",
                         expression_->to_string().c_str(), explore_ ? 1 : 0)));

    // Get valid rules sorted by promise
    std::vector<Rule*> valid_rules = get_valid_rules();

    ereport(WARNING, (errmsg("[CASCADES] OptimizeExpressionTask: found %d valid rules",
                         (int)valid_rules.size())));

    // Push ApplyRuleTask for each valid rule
    // Note: Task stack is LIFO, so we push in reverse order
    // Rules with higher promise should be pushed last (executed first)
    for (auto it = valid_rules.rbegin(); it != valid_rules.rend(); ++it) {
        Rule* rule = *it;
        ereport(WARNING, (errmsg("[CASCADES] OptimizeExpressionTask: pushing ApplyRuleTask for rule=%s",
                             rule->to_string().c_str())));
        push_task(MakeTask<ApplyRuleTask>(context_, expression_, rule, explore_));
    }

    // Push DeriveStatsTask to derive statistics after rules are applied
    push_task(MakeTask<DeriveStatsTask>(context_, expression_));

    // Push ExploreGroupTask for each child group (in reverse order)
    // StarRocks: for (int i = groupExpression.arity() - 1; i >= 0; i--)
    for (int i = expression_->arity() - 1; i >= 0; --i) {
        Group* child_group = expression_->input_at(i);
        ereport(WARNING, (errmsg("[CASCADES] OptimizeExpressionTask: pushing ExploreGroupTask for child group id=%d",
                             child_group->id())));
        push_task(MakeTask<ExploreGroupTask>(context_, child_group));
    }
}

std::string OptimizeExpressionTask::to_string() const {
    return "OptimizeExpressionTask" + std::string(explore_ ? "[explore]" : "") +
           " for expression " + expression_->to_string();
}


}  // namespace cascades
