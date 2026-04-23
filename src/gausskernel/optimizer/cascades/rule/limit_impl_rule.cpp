#include "../base/memory_utils.h"
#include "limit_impl_rule.h"
#include "../operator/operators.h"
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
    // LimitImplRule: Transform LogicalLimit into PhysicalLimit
    // For now, return input as-is
    ereport(WARNING, (errmsg("[CASCADES] LimitImplRule:transform, input=%s",
                         input->to_string().c_str())));
    return {input};
}

}  // namespace cascades
