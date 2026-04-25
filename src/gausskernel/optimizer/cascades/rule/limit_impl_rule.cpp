#include "../base/memory_utils.h"
#include "limit_impl_rule.h"
#include "../operator/operators.h"
#include "../operator/operators/logical_limit.h"
#include "../operator/operators/physical_limit.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"

namespace cascades {

LimitImplRule* LimitImplRule::instance() {
    static LimitImplRule inst;
    return &inst;
}

LimitImplRule::LimitImplRule()
    : Rule(RuleType::IMP_LIMIT,
           Pattern::create(OptExprOpType::LOGICAL_LIMIT,
                          Pattern::any())) {}

std::vector<OptExpression*> LimitImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    ereport(WARNING, (errmsg("[CASCADES] LimitImplRule:transform, input=%s",
                         input->to_string().c_str())));

    LogicalLimit* logical_limit = static_cast<LogicalLimit*>(input->get_op());

    PhysicalLimit* physical_limit = new PhysicalLimit();
    physical_limit->set_limit_count(logical_limit->get_limit_count());
    physical_limit->set_limit_offset(logical_limit->get_limit_offset());

    OptExpression* new_expr = OptExpression::create_with_childrens(physical_limit, input->get_inputs());

    ereport(WARNING, (errmsg("[CASCADES] LimitImplRule:transform, created physical limit: %s",
                         new_expr->to_string().c_str())));
    return {new_expr};
}

}  // namespace cascades
