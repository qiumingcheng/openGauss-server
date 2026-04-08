#pragma once
#include "../memo/memo.h"
#include "../task/task_scheduler.h"
#include "../rule/rule_set.h"
#include "nodes/relation.h"

namespace cascades {

class OptimizerContext {
public:
    OptimizerContext(PlannerInfo* root = nullptr);
    ~OptimizerContext() = default;

    PlannerInfo* get_root() const { return root_; }
    void set_root(PlannerInfo* root) { root_ = root; }

    Memo* get_memo() { return &memo_; }
    TaskScheduler* get_task_scheduler() { return &scheduler_; }
    RuleSet* get_rule_set() { return &rule_set_; }

private:
    PlannerInfo* root_ = nullptr;
    Memo memo_;
    TaskScheduler scheduler_;
    RuleSet rule_set_;
};

}