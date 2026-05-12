#include "../base/memory_utils.h"
#include "nest_loop_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include "postgres.h"

namespace cascades {

// Static singleton instance
NestLoopImplRule* NestLoopImplRule::instance() {
    static NestLoopImplRule inst;
    return &inst;
}

NestLoopImplRule::NestLoopImplRule()
    : Rule(RuleType::IMP_NESTLOOP_JOIN, Pattern::create(OptExprOpType::LOGICAL_JOIN, Pattern::any(), Pattern::any())) {}

std::vector<OptExpression*> NestLoopImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    OptExpression* left_child = input->input_at(0);
    OptExpression* right_child = input->input_at(1);

    ereport(WARNING, (errmsg("[CASCADES] NestLoopImplRule:transform, creating PhysicalNestLoop")));

    // Note: Parameterized IndexScan paths for the right child should already exist
    // in rel->pathlist, created by create_index_paths -> consider_index_join_clauses
    // during IndexScanImplRule::transform. The parameterized_scan_req property will
    // ensure EnforceAndCostTask selects the parameterized path for the inner table.

    // Create PhysicalNestLoop(left_child, right_child)
    PhysicalNestLoop* nestloop_join = NEW_EX(PhysicalNestLoop, nullptr, nullptr, nullptr);
    OptExpression* new_expr = OptExpression::create(nestloop_join, left_child, right_child);

    return {new_expr};
}

}  // namespace cascades
