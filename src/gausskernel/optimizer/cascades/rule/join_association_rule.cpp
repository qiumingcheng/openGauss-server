#include "join_association_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"

namespace cascades {
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
    // Left: L join R, Right: C
    // New structure: L join (R join C)
    OptExprOp* op = input->get_op()->clone();
    LogicalOptExprOp* new_inner_join = static_cast<LogicalOptExprOp*>(right->get_op()->clone());
    OptExpression* new_right = OptExpression::create(new_inner_join, right, left->input_at(1));

    // Create outer join: L join (R join C)
    std::vector<OptExpression*> outer_inputs = {left->input_at(0), new_right};
    OptExpression* new_outer = new OptExpression(op, outer_inputs);

    return {new_outer};
}


}  // namespace cascades