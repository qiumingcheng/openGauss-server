#pragma once
#include "../memo/memo.h"
#include "../task/task_scheduler.h"
#include "../rule/rule_set.h"
#include "nodes/relation.h"
#include "nodes/memnodes.h"

namespace cascades {

class OptimizerContext {
public:
    explicit OptimizerContext(PlannerInfo* root, MemoryContext context);
    ~OptimizerContext() = default;

    PlannerInfo* get_root() const { return root_; }
    void set_root(PlannerInfo* root) { root_ = root; }

    Memo* get_memo() { return &memo_; }
    TaskScheduler* get_task_scheduler() { return &scheduler_; }
    RuleSet* get_rule_set() { return &rule_set_; }

    MemoryContext get_memory_context() const { return memory_context_; }

private:
    PlannerInfo* root_ = nullptr;
    MemoryContext memory_context_;
    Memo memo_;
    TaskScheduler scheduler_;
    RuleSet rule_set_;
};

}