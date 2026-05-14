#include "../base/memory_utils.h"
#include "merge_join_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include "postgres.h"

namespace cascades {

// Static singleton instance
MergeJoinImplRule* MergeJoinImplRule::instance() {
    static MergeJoinImplRule inst;
    return &inst;
}

MergeJoinImplRule::MergeJoinImplRule()
    : Rule(RuleType::IMP_MERGE_JOIN, Pattern::create(OptExprOpType::LOGICAL_JOIN, Pattern::any(), Pattern::any())) {}

std::vector<OptExpression*> MergeJoinImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    OptExpression* left_child = input->input_at(0);
    OptExpression* right_child = input->input_at(1);

    ereport(WARNING, (errmsg("[CASCADES] MergeJoinImplRule:transform, creating PhysicalMergeJoin")));

    // Create PhysicalMergeJoin(left_child, right_child) - paths set later in translate_merge_join
    PhysicalMergeJoin* merge_join = NEW_EX(PhysicalMergeJoin, nullptr, nullptr, nullptr);
    OptExpression* new_expr = OptExpression::create(merge_join, left_child, right_child);

    return {new_expr};
}

}  // namespace cascades
