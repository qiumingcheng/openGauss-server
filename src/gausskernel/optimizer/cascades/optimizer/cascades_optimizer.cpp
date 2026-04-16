#include "cascades_optimizer.h"
#include "../base/memory_utils.h"
#include "../task/explore_group_task.h"
#include "../task/optimize_expression_task.h"
#include "../task/enforce_and_cost_task.h"
#include "../memo/group.h"
#include "../rule/rule_set.h"
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
    logical_rewrite(expression);

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

void CascadesOptimizer::logical_rewrite(OptExpression* expression) {
    // Apply logical transformation rules to normalize the expression tree
    // This includes:
    // - Predicate pushdown
    // - Column pruning
    // - Join reordering hints (if supported)

    // Logical rewrite would apply transformation rules here
    // For now, we rely on the explore phase to do transformations
}

void CascadesOptimizer::init_memo(OptExpression* expression) {
    GroupExpression* root_expr = context_.get_memo()->init(expression);
    root_expr->derive_logical_property_recursively();
}

void CascadesOptimizer::explore() {
    ereport(WARNING, (errmsg("[CASCADES] >>> Entering explore()")));

    // Initialize root task context if not already done
    // Note: Memory context already switched in optimize(), no need to switch here
    if (root_task_context_ == nullptr) {
        root_task_context_ = NEW_EX(TaskContext, &context_,
                                              PhysicalPropertySet(),
                                              nullptr,
                                              std::numeric_limits<double>::infinity());
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