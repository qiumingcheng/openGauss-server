#include "join_commutativity_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"

namespace cascades {

JoinCommutativityRule* JoinCommutativityRule::instance(){
    static JoinCommutativityRule inst;
    return &inst;
}

JoinCommutativityRule::JoinCommutativityRule()
    : Rule(RuleType::TF_JOIN_COMMUTATIVITY, Pattern::create(OptExprOpType::LOGICAL_JOIN, Pattern::any(), Pattern::any())) {}

std::vector<OptExpression*> JoinCommutativityRule::transform(
    OptExpression* input, OptimizerContext* context) {
    ereport(WARNING, (errmsg("[CASCADES] JoinCommutativityRule:transform, input=%s",
                         input->to_string().c_str())));

    // Pattern guarantees: op_type == LOGICAL_JOIN && arity == 2
    // Get the join operator and its children
    LogicalOptExprOp* join_op = static_cast<LogicalOptExprOp*>(input->get_op());
    OptExpression* left = input->input_at(0);
    OptExpression* right = input->input_at(1);

    // Clone the join operator and create new expression with swapped children
    OptExprOp* swapped_join = join_op->clone();
    OptExpression* new_expr = OptExpression::create(swapped_join, right, left);

    ereport(WARNING, (errmsg("[CASCADES] JoinCommutativityRule:transform, created swapped join")));

    return {new_expr};
}


}  // namespace cascades