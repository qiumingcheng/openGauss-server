#include "logical_property.h"
#include "../memo/expression_context.h"
#include "../memo/group_expression.h"
#include "../operator/operator.h"

namespace cascades {
LogicalProperty::LogicalProperty() {}

void LogicalProperty::set_output_columns(const ColumnRefSet& cols) {
    output_columns_ = cols;
}

void LogicalProperty::derive(ExpressionContext* context) {
    if (context == nullptr) {
        return;
    }

    OptExprOp* op = context->get_op();
    if (op == nullptr) {
        return;
    }

    // Derive output columns based on operator type
    switch (op->get_type()) {
        case OptExprOpType::LOGICAL_PROJECT: {
            // Project gets columns from child
            if (context->arity() > 0) {
                output_columns_ = context->get_child_output_columns(0);
            }
            break;
        }
        case OptExprOpType::LOGICAL_FILTER: {
            // Filter passes through columns from child
            if (context->arity() > 0) {
                output_columns_ = context->get_child_output_columns(0);
            }
            break;
        }
        case OptExprOpType::LOGICAL_JOIN: {
            // Join outputs columns from both children
            ColumnRefSet combined;
            if (context->arity() >= 1) {
                for (int col : context->get_child_output_columns(0)) {
                    combined.add(col);
                }
            }
            if (context->arity() >= 2) {
                for (int col : context->get_child_output_columns(1)) {
                    combined.add(col);
                }
            }
            output_columns_ = combined;
            break;
        }
        case OptExprOpType::LOGICAL_SCAN: {
            // Scan outputs all columns of the table
            // In real implementation, this would come from table metadata
            output_columns_ = ColumnRefSet();
            break;
        }
        default:
            // For other operators, union of children outputs
            output_columns_ = ColumnRefSet();
            for (int i = 0; i < context->arity(); ++i) {
                for (int col : context->get_child_output_columns(i)) {
                    output_columns_.add(col);
                }
            }
            break;
    }
}

std::string LogicalProperty::to_string() const {
    return "LogicalProperty(cols=" + std::to_string(output_columns_.size()) + ")";
}


}  // namespace cascades