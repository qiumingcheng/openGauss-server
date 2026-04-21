#include "logical_property.h"
#include "../memo/expression_context.h"
#include "../memo/group_expression.h"
#include "../operator/operator.h"
#include "../operator/operators/logical_scan.h"
#include "../operator/operators/logical_project.h"

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
            // Project outputs columns defined by its target_list or projections
            LogicalProject* project_op = static_cast<LogicalProject*>(op);
            List* target_list = project_op->get_target_list();

            if (target_list != nullptr && target_list != NIL) {
                // Extract column numbers from target list expressions
                ListCell* lc;
                foreach(lc, target_list) {
                    Node* expr = (Node*)lfirst(lc);
                    if (IsA(expr, Var)) {
                        Var* var = (Var*)expr;
                        output_columns_.add(var->varattno);
                    }
                    // For other expression types (Const, FuncExpr, etc.), they don't
                    // reference child columns, so no column pruning needed for them
                }
            } else {
                // Fallback: use projections_ if set
                const std::vector<int>& projections = project_op->get_projections();
                if (!projections.empty()) {
                    for (int col : projections) {
                        output_columns_.add(col);
                    }
                } else if (context->arity() > 0) {
                    // No projection info, pass through all child columns
                    output_columns_ = context->get_child_output_columns(0);
                }
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
                // Extract column refs from reltarget->exprs (list of Var expressions)
                ListCell* lc;
                foreach(lc, rel->reltarget->exprs) {
                    Node* expr = (Node*)lfirst(lc);
                    if (IsA(expr, Var)) {
                        Var* var = (Var*)expr;
                        output_columns_.add(var->varattno);
                    }
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