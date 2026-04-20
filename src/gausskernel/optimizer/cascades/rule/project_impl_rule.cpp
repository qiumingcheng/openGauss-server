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
                         input->debug_string().c_str())));

    Assert(input->arity() > 0);  // Pattern must match expression with child

    // Create physical project operator
    // Must preserve the child expression so Memo::insert_transform_result can
    // recursively insert it and get the correct input group
    PhysicalProject* physical_project = NEW_EX(PhysicalProject);

    // Get the child input expression and preserve it
    OptExpression* child = input->input_at(0);

    OptExpression* new_expr = OptExpression::create(physical_project, child);
    ereport(WARNING, (errmsg("[CASCADES] ProjectImplRule:transform, created PhysicalProject with child")));
    return {new_expr};
}

}  // namespace cascades
