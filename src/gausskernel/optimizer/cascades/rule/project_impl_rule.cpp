#include "project_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"
#include "nodes/relation.h"

namespace cascades {

ProjectImplRule* ProjectImplRule::instance() {
    static ProjectImplRule inst;
    return &inst;
}

ProjectImplRule::ProjectImplRule()
    : Rule(RuleType::IMP_PROJECT, Pattern::create(OptExprOpType::LOGICAL_PROJECT,
                                                  Pattern::any())) {}

std::vector<OptExpression*> ProjectImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    ereport(WARNING, (errmsg("[CASCADES] ProjectImplRule:transform, input=%s",
                         input->to_string().c_str())));

    Assert(input->arity() > 0);  // Pattern must match expression with child

    LogicalProject* project_op = static_cast<LogicalProject*>(input->get_op());

    // Get child expression
    OptExpression* child = input->input_at(0);
    Assert(child != nullptr);  // Guaranteed by arity check above

    // Get child GroupExpression
    GroupExpression* child_group_expr = child->get_group_expression();
    if (child_group_expr == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] ProjectImplRule:transform, child_group_expr is null")));
        return {};
    }

    // Get child group
    Group* child_group = child_group_expr->get_group();
    if (child_group == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] ProjectImplRule:transform, child_group is null")));
        return {};
    }

    PhysicalPropertySet required_prop;

    GroupExpression* child_best = child_group->get_best_expression(required_prop);
    if (child_best == nullptr || !child_best->get_op()->is_physical()) {
        ereport(WARNING, (errmsg("[CASCADES] ProjectImplRule:transform, no valid child expression found")));
        return {};
    }

    Path* child_path = static_cast<PhysicalOptExprOp*>(child_best->get_op())->get_path();
    if (child_path == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] ProjectImplRule:transform, child_path is null")));
        return {};
    }

    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] ProjectImplRule:transform, PlannerInfo is null")));
        return {};
    }

    // Create a simple path for project (just passes through child path)
    // In PostgreSQL/openGauss, Project doesn't have a separate path type
    // The projection is handled by the parent node
    Path* project_path = child_path;  // Project inherits child's path

    // Create physical project operator
    PhysicalProject* physical_project = NEW_EX(PhysicalProject, project_path, child_path);

    OptExpression* new_expr = OptExpression::create(physical_project, child);
    ereport(WARNING, (errmsg("[CASCADES] ProjectImplRule:transform, created physical project: %s",
                         new_expr->to_string().c_str())));
    return {new_expr};
}

}  // namespace cascades
