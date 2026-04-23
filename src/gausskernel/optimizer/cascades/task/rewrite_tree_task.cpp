#include "rewrite_tree_task.h"
#include "task_context.h"
#include "../memo/expression_context.h"
#include "../rule/rule.h"
#include "../optimizer/optimizer_context.h"
#include "../base/memory_utils.h"

namespace cascades {

RewriteTreeTask::RewriteTreeTask(TaskContext* context, OptExpression* root,
                                 const std::vector<Rule*>& rules, bool only_once)
    : OptimizerTask(context), root_(root), only_once_(only_once), rules_(rules), change_(0) {}

void RewriteTreeTask::execute() {
    ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask: starting, only_once=%d, rules=%d",
                         only_once_ ? 1 : 0, (int)rules_.size())));

    // Start recursive rewrite from root (parent=0 means root has no parent)
    rewrite(nullptr, 0, root_);

    ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask: finished, change_=%ld", change_)));

    // If changes occurred and not only_once, push another RewriteTreeTask for continued optimization
    if (change_ > 0 && !only_once_) {
        ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask: pushing follow-up RewriteTreeTask")));
        push_task(MakeTask<RewriteTreeTask>(context_, root_, rules_, false));
    }
}

void RewriteTreeTask::rewrite(OptExpression* parent, int child_index, OptExpression* root) {
    ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask::rewrite: processing node=%s",
                         root->to_string().c_str())));

    // Apply rules to this node
    OptExpression* new_root = apply_rules(parent, child_index, root, rules_);

    // Check if transformation occurred
    if (new_root != root) {
        change_++;
        ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask::rewrite: transformation occurred, change_=%ld",
                             change_)));

        // Update parent's child pointer if parent exists
        if (parent != nullptr) {
            parent->set_input(child_index, new_root);
        } else {
            // New root is the result of transformation at root level
            root_ = new_root;
        }
        root = new_root;
    }

    // Recursively process children in LIFO order (from last to first)
    for (int i = root->arity() - 1; i >= 0; --i) {
        ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask::rewrite: recursing into child %d", i)));
        rewrite(root, i, root->input_at(i));
    }
}

OptExpression* RewriteTreeTask::apply_rules(OptExpression* parent, int child_index,
                                          OptExpression* root, const std::vector<Rule*>& rules) {
    OptExpression* current = root;

    for (Rule* rule : rules) {
        ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask::apply_rules: trying rule=%s on node=%s",
                             rule->to_string().c_str(), current->to_string().c_str())));

        // Check if rule pattern matches using OptExpression version
        if (!rule->get_pattern()->match_without_child(current)) {
            ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask::apply_rules: pattern mismatch")));
            continue;
        }

        // Check rule's precondition using OptExpression version
        OptimizerContext* optimizer_context = context_->get_optimizer_context();
        if (!rule->check(current, optimizer_context)) {
            ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask::apply_rules: check failed")));
            continue;
        }

        // Apply the transformation
        std::vector<OptExpression*> results = rule->transform(current, optimizer_context);

        if (!results.empty()) {
            OptExpression* transformed = results[0];
            ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask::apply_rules: transformed to=%s",
                                 transformed->to_string().c_str())));

            // Return first transformation result
            // Note: results are allocated in MemoryContext, no need to delete individually
            return transformed;
        }
    }

    ereport(WARNING, (errmsg("[CASCADES] RewriteTreeTask::apply_rules: no transformation, returning original")));
    return current;
}

std::string RewriteTreeTask::to_string() const {
    return "RewriteTreeTask with " + std::to_string(rules_.size()) + " rules, change=" + std::to_string(change_);
}

}  // namespace cascades