#include "logical_join.h"
#include "../../memo/opt_expression.h"

namespace cascades {

LogicalJoin::LogicalJoin(OptExprOpType type, const std::vector<OptExpression*>& children)
    : LogicalOptExprOp(type) {
    // Join keys would be set separately
    (void)children;
}

LogicalJoin::LogicalJoin(OptExprOpType type, OptExpression* left, OptExpression* right)
    : LogicalOptExprOp(type) {
    // Join keys would be set separately
    (void)left;
    (void)right;
}

void LogicalJoin::swap_children() {
    // Note: This swaps children at the operator level
    // The actual OptExpression children are swapped when creating new OptExpression
}

}  // namespace cascades
