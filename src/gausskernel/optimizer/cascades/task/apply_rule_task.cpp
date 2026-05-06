#include "apply_rule_task.h"
#include "task_context.h"
#include "optimize_expression_task.h"
#include "enforce_and_cost_task.h"
#include "../memo/group_expression.h"
#include "../memo/memo.h"
#include "../memo/group.h"
#include "../rule/rule.h"
#include "../rule/rule_type.h"
#include "../rule/pattern.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include "../property/physical_property_set.h"
#include "../property/sort_property.h"
#include "nodes/relation.h"

namespace cascades {

// Helper function to build PhysicalPropertySet from sort_clause (List of SortGroupClause)
static PhysicalPropertySet build_physical_property_set_from_sort_clause(void* sort_clause)
{
    if (sort_clause == nullptr) {
        return PhysicalPropertySet();
    }

    List* clauses = static_cast<List*>(sort_clause);
    OrderSpec spec;
    ListCell* lc = nullptr;
    foreach (lc, clauses) {
        SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
        int column_id = sgcl->tleSortGroupRef;
        // Note: sgcl->sortop is the ordering operator Oid.
        // BTLessStrategyNumber (1) and BTGreaterStrategyNumber (5) are STRATEGY NUMBERS, not Oids.
        // We cannot directly compare sortop (Oid) with strategy numbers.
        // For most types, < operator Oid < > operator Oid numerically.
        // So we use a heuristic: if sortop Oid is "small", assume ASC.
        bool ascending = (sgcl->sortop < 500);  // heuristic: < operators have smaller Oids
        ereport(WARNING, (errmsg("[CASCADES] build_physical_property_set_from_sort_clause: column_id=%d, sortop=%u, ascending=%d",
                             column_id, sgcl->sortop, ascending)));
        spec.add_ordering(column_id, ascending);
    }
    SortProperty sort_prop(spec);
    return PhysicalPropertySet(DistributionProperty(DistributionType::ANY), sort_prop);
}

// Helper function to build OptExpression tree from GroupExpression for rule transform
static OptExpression* build_opt_expression(GroupExpression* group_expr) {
    OptExprOp* op = group_expr->get_op();
    std::vector<OptExpression*> children;
    children.reserve(group_expr->arity());

    // Recursively build children from input groups
    for (Group* input_group : group_expr->get_inputs()) {
        GroupExpression* child_expr = input_group->get_first_logical_expression();
        Assert(child_expr != nullptr);  // Input group must have at least one logical expression
        OptExpression* child_opt = build_opt_expression(child_expr);
        Assert(child_opt != nullptr);  // Recursive build must succeed if child_expr exists
        children.push_back(child_opt);
    }

    OptExpression* result = OptExpression::create_with_childrens(op, children);

    // Copy logical property from group
    Group* group = group_expr->get_group();
    if (group != nullptr && group->get_logical_property() != nullptr) {
        result->set_logical_property(group->get_logical_property());
    }

    return result;
}

ApplyRuleTask::ApplyRuleTask(TaskContext* context, GroupExpression* expr, Rule* rule, bool is_explore)
    : OptimizerTask(context), expression_(expr), rule_(rule), is_explore_(is_explore) {}

void ApplyRuleTask::execute() {
    ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: expr=%s, rule=%s, is_explore=%d",
                         expression_->to_string().c_str(), rule_->to_string().c_str(),
                         is_explore_ ? 1 : 0)));

    // Skip if rule has been explored (for transformation rules)
    if (is_transformation_rule(rule_->type())) {
        if (expression_->has_rule_explored(rule_->type())) {
            ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: skipped, rule already explored")));
            return;
        }
    }

    // Skip if rule has been applied (for implementation rules)
    if (expression_->has_rule_applied(rule_->type())) {
        ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: skipped, rule already applied")));
        return;
    }

    // Skip if expression is unused (has empty child groups)
    if (expression_->is_unused()) {
        ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: skipped, expression unused")));
        return;
    }

    // Skip if rule is exhausted (timeout protection)
    if (rule_->exhausted(context_->get_optimizer_context())) {
        ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: skipped, rule exhausted")));
        return;
    }

    // Check if rule's check condition passes using GroupExpression directly
    OptimizerContext* optimizer_context = context_->get_optimizer_context();
    if (!rule_->check(expression_, optimizer_context)) {
        ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: skipped, rule check failed")));
        return;
    }

    ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: rule check passed, building opt expression")));

    // Build OptExpression tree from GroupExpression for rule transform
    OptExpression* input = build_opt_expression(expression_);

    // Transform using the rule
    std::vector<OptExpression*> results = rule_->transform(input, optimizer_context);

    ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: transform produced %d results",
                         (int)results.size())));

    // Insert transformed expressions into memo
    Memo* memo = optimizer_context->get_memo();
    for (OptExpression* result : results) {
        // Use insert_transform_result which handles group expression creation
        auto insert_result = memo->insert_transform_result(expression_, result);
        GroupExpression* new_group_expr = insert_result.second;

        // Skip if memo was merged during insertion
        if (expression_->get_group()->is_empty()) {
            ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: skipped, group is empty after merge")));
            return;
        }

        // Merge applied rules from parent expression
        new_group_expr->merge_applied_rules(expression_->get_applied_rules());
        new_group_expr->add_applied_rule(rule_->type());

        // If result has sort_requirement (set by SortImplRule for PassThrough),
        // convert it to PhysicalPropertySet and set on GroupExpression
        if (result->get_sort_requirement() != nullptr) {
            PhysicalPropertySet sort_prop = build_physical_property_set_from_sort_clause(result->get_sort_requirement());
            new_group_expr->set_children_required_property(
                NEW_EX(PhysicalPropertySet, DistributionProperty(DistributionType::ANY), sort_prop.get_sort()));
            ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: set children_required_property from sort_requirement")));
        }

        ereport(WARNING, (errmsg("[CASCADES] ApplyRuleTask: inserted new expr, is_logical=%d, group id=%d",
                             new_group_expr->get_op()->is_logical() ? 1 : 0, new_group_expr->get_group()->id())));

        // Push follow-up tasks based on expression type
        if (new_group_expr->get_op()->is_logical()) {
            // For logical expressions, push OptimizeExpressionTask
            push_task(MakeTask<OptimizeExpressionTask>(context_, new_group_expr, is_explore_));
        } else {
            push_task(MakeTask<EnforceAndCostTask>(context_, new_group_expr->get_group(),
                                                       context_->get_required_property()));
        }
    }

    // Mark rule as explored/applied
    if (is_transformation_rule(rule_->type())) {
        expression_->set_rule_explored(rule_->type());
    } else {
        expression_->set_rule_applied(rule_->type());
    }
}

std::string ApplyRuleTask::to_string() const {
    return "ApplyRuleTask" + std::string(is_explore_ ? "[explore]" : "") +
           " for expression " + expression_->to_string() +
           " rule " + rule_->to_string();
}


}  // namespace cascades
