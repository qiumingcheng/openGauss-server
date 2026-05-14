#include "../base/memory_utils.h"
#include "hash_join_impl_rule.h"
#include "../operator/operators.h"
#include "../operator/operators/logical_join.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include "postgres.h"

namespace cascades {

// Static singleton instance
HashJoinImplRule* HashJoinImplRule::instance() {
    static HashJoinImplRule inst;
    return &inst;
}

HashJoinImplRule::HashJoinImplRule()
    : Rule(RuleType::IMP_HASH_JOIN, Pattern::create(OptExprOpType::LOGICAL_JOIN, Pattern::any(), Pattern::any())) {}

std::vector<OptExpression*> HashJoinImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    OptExpression* left_child = input->input_at(0);
    OptExpression* right_child = input->input_at(1);

    ereport(WARNING, (errmsg("[CASCADES] HashJoinImplRule:transform, creating PhysicalHashJoin")));

    // Copy join conditions from LogicalJoin to PhysicalHashJoin
    LogicalJoin* logical_join = dynamic_cast<LogicalJoin*>(input->get_op());
    PhysicalHashJoin* hash_join = NEW_EX(PhysicalHashJoin, nullptr, nullptr, nullptr);
    if (logical_join != nullptr) {
        hash_join->set_build_keys(logical_join->get_left_join_keys());
        hash_join->set_probe_keys(logical_join->get_right_join_keys());
        hash_join->set_qual(logical_join->get_qual());
        hash_join->set_join_type(logical_join->get_join_type());
    }
    OptExpression* new_expr = OptExpression::create(hash_join, left_child, right_child);

    return {new_expr};
}

}  // namespace cascades
