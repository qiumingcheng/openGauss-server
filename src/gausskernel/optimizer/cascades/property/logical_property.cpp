#include "logical_property.h"
#include "../memo/expression_context.h"
#include "../memo/group_expression.h"
#include "../operator/operator.h"
#include "../operator/operators/logical_scan.h"

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

    // Derive output columns and sort keys based on operator type
    switch (op->get_type()) {
        case OptExprOpType::LOGICAL_PROJECT: {
            // Project passes through columns from child
            if (context->arity() > 0) {
                output_columns_ = context->get_child_output_columns(0);
            }
            // Sort order preserved through project
            break;
        }

        case OptExprOpType::LOGICAL_FILTER: {
            // Filter passes through columns from child
            if (context->arity() > 0) {
                output_columns_ = context->get_child_output_columns(0);
            }
            // Sort order preserved through filter
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
            // Join destroys sort order from inputs
            sort_keys_.clear();
            break;
        }

        case OptExprOpType::LOGICAL_AGGREGATE: {
            // Aggregate outputs group keys + aggregate columns
            if (context->arity() > 0) {
                // For now, inherit from child - actual implementation depends on aggregate definition
                output_columns_ = context->get_child_output_columns(0);
            }
            // Aggregation destroys input sort order
            sort_keys_.clear();
            break;
        }

        case OptExprOpType::LOGICAL_SCAN: {
            // Scan outputs all columns of the table
            // Get column info from RelOptInfo's reltarget
            LogicalScan* scan_op = static_cast<LogicalScan*>(op);
            RelOptInfo* rel = scan_op->get_rel();
            if (rel != nullptr && rel->reltarget != nullptr) {
                // Extract column refs from reltarget's exprs
                for (int i = 0; i < rel->reltarget->numAttrs; ++i) {
                    output_columns_.add(rel->reltarget->attrs[i]);
                }
            }
            // Base table may have a defined sort order (e.g., primary key)
            // This would come from table metadata in a full implementation
            break;
        }

        default: {
            // For other operators, union of children outputs
            output_columns_ = ColumnRefSet();
            for (int i = 0; i < context->arity(); ++i) {
                for (int col : context->get_child_output_columns(i)) {
                    output_columns_.add(col);
                }
            }
            // Default: assume sort order is destroyed
            sort_keys_.clear();
            break;
        }
    }
}

std::string LogicalProperty::to_string() const {
    std::string result = "LogicalProperty(cols=" + std::to_string(output_columns_.size());
    if (!sort_keys_.empty()) {
        result += ", sort_keys=" + std::to_string(sort_keys_.size());
    }
    result += ")";
    return result;
}


}  // namespace cascades