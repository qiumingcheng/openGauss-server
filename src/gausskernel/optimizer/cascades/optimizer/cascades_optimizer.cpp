#include "cascades_optimizer.h"
#include "../base/memory_utils.h"
#include "../task/explore_group_task.h"
#include "../task/optimize_expression_task.h"
#include "../task/enforce_and_cost_task.h"
#include "../task/rewrite_tree_task.h"
#include "../memo/group.h"
#include "../rule/rule_set.h"
#include "../rule/column_prune_rule.h"
#include "../rule/constant_fold_rule.h"
#include <limits>

namespace cascades {
CascadesOptimizer::CascadesOptimizer(PlannerInfo* root)
    : root_(root), context_(root, root->planner_cxt), root_task_context_(nullptr) {}

CascadesOptimizer::~CascadesOptimizer() {
    // root_task_context_ is allocated in MemoryContext, no need to delete
}

OptExpression* CascadesOptimizer::optimize(OptExpression* expression,
                                          const PhysicalPropertySet& required_property,
                                          ColumnRefSet* required_columns) {
    ereport(WARNING, (errmsg("[CASCADES] optimize() called, expression=%p", (void*)expression)));

    // Save current memory context and switch to optimizer's memory context
    // All optimization work will be done in this context
    MemoryContext oldcxt = MemoryContextSwitchTo(context_.get_memory_context());

    // Phase 1: Rewrite - apply logical transformation rules to normalize the tree
    ereport(WARNING, (errmsg("[CASCADES] Phase 1: logical_rewrite")));
    expression = logical_rewrite(expression);

    // Phase 2: Initialize memo
    ereport(WARNING, (errmsg("[CASCADES] Phase 2: init_memo")));
    init_memo(expression);

    // Phase 3: Explore and cost
    ereport(WARNING, (errmsg("[CASCADES] Phase 3: explore")));
    explore();

    // Phase 4: Extract best plan
    ereport(WARNING, (errmsg("[CASCADES] Phase 4: extract_best_plan")));
    OptExpression* result = extract_best_plan(required_property);
    ereport(WARNING, (errmsg("[CASCADES] optimize() returning %p", (void*)result)));

    // Restore original memory context
    MemoryContextSwitchTo(oldcxt);
    return result;
}

OptExpression* CascadesOptimizer::logical_rewrite(OptExpression* expression) {
    // Apply logical transformation rules to normalize the expression tree
    // This includes:
    // - Column pruning (Project -> Scan pattern)
    // - Constant folding

    ereport(WARNING, (errmsg("[CASCADES] logical_rewrite: starting")));

    // Create temporary task context for rewrite phase
    MemoryContext oldcxt = MemoryContextSwitchTo(context_.get_memory_context());

    TaskContext* rewrite_context = NEW_EX(TaskContext, &context_,
                                          PhysicalPropertySet(),
                                          nullptr,
                                          std::numeric_limits<double>::max());

    // Collect transformation rules for logical rewrite
    // Only include rules that are safe to apply in pre-memo phase
    std::vector<Rule*> rewrite_rules;

    // Column pruning: Project -> Scan pattern
    rewrite_rules.push_back(ColumnPruneRule::instance());

    // Constant folding
    // rewrite_rules.push_back(ConstantFoldRule::instance());

    // Note: PredicatePushDownRule is NOT included here
    // It will be applied during explore phase via OptimizeExpressionTask

    // Create and execute RewriteTreeTask
    // only_once=true means we run one pass only
    RewriteTreeTask* rewrite_task = NEW_EX(RewriteTreeTask, rewrite_context, expression, rewrite_rules, true);
    rewrite_task->execute();

    MemoryContextSwitchTo(oldcxt);

    OptExpression* result = rewrite_task->get_result();

    ereport(WARNING, (errmsg("[CASCADES] logical_rewrite: complete, changes=%ld", rewrite_task->has_change() ? 1 : 0)));

    return result;
}

void CascadesOptimizer::init_memo(OptExpression* expression) {
    GroupExpression* root_expr = context_.get_memo()->init(expression);
    root_expr->derive_logical_property_recursively();
}

void CascadesOptimizer::explore() {
    ereport(WARNING, (errmsg("[CASCADES] >>> Entering explore()")));

    // Initialize root task context if not already done
    if (root_task_context_ == nullptr) {
        root_task_context_ = NEW_EX(TaskContext, &context_,
                                              PhysicalPropertySet(),
                                              nullptr,
                                              std::numeric_limits<double>::max());
        ereport(WARNING, (errmsg("[CASCADES] >>> Created root task context")));
    }

    TaskScheduler* scheduler = context_.get_task_scheduler();
    Group* root_group = context_.get_memo()->get_root_group();

    ereport(WARNING, (errmsg("[CASCADES] >>> Root group id=%d", root_group->id())));
    ereport(WARNING, (errmsg("[CASCADES] >>> Pushing ExploreGroupTask")));

    // Start exploration from the root group
    scheduler->push_task(MakeTask<ExploreGroupTask>(root_task_context_, root_group));
    ereport(WARNING, (errmsg("[CASCADES] >>> Executing tasks")));
    scheduler->execute_tasks(root_task_context_);
    ereport(WARNING, (errmsg("[CASCADES] >>> Explore() completed")));
}

OptExpression* CascadesOptimizer::extract_best_plan(const PhysicalPropertySet& required_property) {
    OptExpression* result = context_.get_memo()->extract_best_plan(required_property);

    // Print debug string for the optimized plan
    if (result != nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] extract_best_plan: optimized plan tree:\n%s",
                             result->debug_string().c_str())));
    } else {
        ereport(WARNING, (errmsg("[CASCADES] extract_best_plan: failed to extract plan, memo state:")));
        ereport(WARNING, (errmsg("[CASCADES] %s", context_.get_memo()->to_string().c_str())));
    }

    return result;
}


}  // namespace cascades