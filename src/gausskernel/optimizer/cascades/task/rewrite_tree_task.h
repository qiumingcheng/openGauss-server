#pragma once
#include "optimizer_task.h"
#include "../rule/rule.h"
#include "../memo/opt_expression.h"
#include <vector>

namespace cascades {

class RewriteTreeTask : public OptimizerTask {
public:
    RewriteTreeTask(TaskContext* context, OptExpression* root,
                    const std::vector<Rule*>& rules, bool only_once = false);
    ~RewriteTreeTask() = default;

    void execute() override;
    std::string to_string() const override;
    OptExpression* get_result() { return root_; }
    bool has_change() const { return change_ > 0; }

protected:
    void rewrite(OptExpression* parent, int child_index, OptExpression* root);
    OptExpression* apply_rules(OptExpression* parent, int child_index,
                              OptExpression* root, const std::vector<Rule*>& rules);

private:
    OptExpression* root_;
    bool only_once_;
    std::vector<Rule*> rules_;
    long change_ = 0;
};

}  // namespace cascades