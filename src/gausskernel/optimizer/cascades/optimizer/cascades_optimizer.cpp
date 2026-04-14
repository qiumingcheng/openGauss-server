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
    : root_(root), root_task_context_(nullptr) {}

CascadesOptimizer::~CascadesOptimizer() {
    delete root_task_context_;
}

OptExpression* CascadesOptimizer::optimize(OptExpression* expression,
                                          const PhysicalPropertySet& required_property,
                                          ColumnRefSet* required_columns) {
    // Phase 1: Rewrite - apply logical transformation rules to normalize the tree
    logical_rewrite(expression);

    // Phase 2: Initialize memo
    init_memo(expression);

    // Phase 3: Explore and cost
    explore();

    // Phase 4: Extract best plan
    return extract_best_plan(required_property);
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
    // Initialize root task context if not already done
    if (root_task_context_ == nullptr) {
        root_task_context_ = new TaskContext(&context_,
                                              PhysicalPropertySet(),
                                              nullptr,
                                              std::numeric_limits<double>::infinity());
    }

    TaskScheduler* scheduler = context_.get_task_scheduler();
    Group* root_group = context_.get_memo()->get_root_group();

    // Start exploration from the root group
    scheduler->push_task(MakeTask<ExploreGroupTask>(root_task_context_, root_group));
    scheduler->execute_tasks(root_task_context_);
}

OptExpression* CascadesOptimizer::extract_best_plan(const PhysicalPropertySet& required_property) {
    return context_.get_memo()->extract_best_plan(required_property);
}


}  // namespace cascades