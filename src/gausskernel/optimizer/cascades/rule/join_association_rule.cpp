#include "../base/memory_utils.h"
#include "join_association_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"

namespace cascades {

// Static singleton instance
JoinAssociationRule* JoinAssociationRule::instance() {
    static JoinAssociationRule inst;
    return &inst;
}

JoinAssociationRule::JoinAssociationRule()
    : Rule(RuleType::TF_JOIN_ASSOCIATIVITY, Pattern::create(OptExprOpType::LOGICAL_JOIN)) {}

std::vector<OptExpression*> JoinAssociationRule::transform(
    OptExpression* input, OptimizerContext* context) {
    // (A join B) join C -> A join (B join C)
    // Requires binary join with at least 2 children
    if (input->arity() != 2) {
        return {};
    }

    OptExpression* left = input->input_at(0);
    OptExpression* right = input->input_at(1);

    // Check if left child is also a join
    if (left->arity() != 2) {
        return {};
    }

    // (L join R) join C  ->  L join (R join C)
    // left = (L join R), right = C
    // L = left->input_at(0), R = left->input_at(1)
    // New structure: L join (R join C)
    OptExpression* L = left->input_at(0);
    OptExpression* R = left->input_at(1);
    OptExpression* C = right;

    // Create inner join: (R join C)
    LogicalOptExprOp* inner_join_op = static_cast<LogicalOptExprOp*>(input->get_op()->clone());
    OptExpression* inner_join = OptExpression::create(inner_join_op, R, C);

    // Create outer join: L join (R join C)
    OptExprOp* outer_join_op = static_cast<OptExprOp*>(input->get_op()->clone());
    OptExpression* new_outer = OptExpression::create(outer_join_op, L, inner_join);

    return {new_outer};
}


}  // namespace cascades