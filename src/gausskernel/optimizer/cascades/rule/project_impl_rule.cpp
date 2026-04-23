#include "project_impl_rule.h"
#include "../operator/operators.h"
#include "../operator/operators/physical_project.h"
#include "../memo/opt_expression.h"

namespace cascades {

ProjectImplRule* ProjectImplRule::instance() {
    static ProjectImplRule inst;
    return &inst;
}

ProjectImplRule::ProjectImplRule()
    : Rule(RuleType::IMP_PROJECT,
           Pattern::create(OptExprOpType::LOGICAL_PROJECT,
                          Pattern::any())) {}

std::vector<OptExpression*> ProjectImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    // Convert LogicalProject to PhysicalProject
    LogicalProject* logical_proj = static_cast<LogicalProject*>(input->get_op());

    PhysicalProject* physical_proj = NEW_EX(PhysicalProject);
    physical_proj->set_target_list(logical_proj->get_target_list());

    std::vector<OptExpression*> result;
    OptExpression* physical_expr = OptExpression::create(physical_proj, input->input_at(0));
    result.push_back(physical_expr);
    return result;
}

}  // namespace cascades
