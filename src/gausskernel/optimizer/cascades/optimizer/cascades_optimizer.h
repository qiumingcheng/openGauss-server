#pragma once
#include "optimizer_context.h"
#include "../memo/memo.h"
#include "../memo/opt_expression.h"
#include "../property/physical_property_set.h"
#include "../task/task_context.h"
#include "nodes/relation.h"
#include <memory>

namespace cascades {

class CascadesOptimizer {
public:
    CascadesOptimizer(PlannerInfo* root = nullptr);
    ~CascadesOptimizer() = default;

    OptExpression* optimize(OptExpression* expression,
                            const PhysicalPropertySet& required_property,
                            ColumnRefSet* required_columns);

    OptimizerContext* get_context() { return &context_; }

private:
    void logical_rewrite(OptExpression* expression);
    void init_memo(OptExpression* expression);
    void explore();
    OptExpression* extract_best_plan(const PhysicalPropertySet& required_property);

    PlannerInfo* root_;
    OptimizerContext context_;
    TaskContext* root_task_context_;
};

}