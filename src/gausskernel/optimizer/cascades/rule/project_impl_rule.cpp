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

    // Create physical project operator
    PhysicalProject* physical_project = NEW_EX(PhysicalProject);

    OptExpression* new_expr = OptExpression::create(physical_project);
    ereport(WARNING, (errmsg("[CASCADES] ProjectImplRule:transform, created physical project: %s",
                         new_expr->to_string().c_str())));
    return {new_expr};
}

}  // namespace cascades
