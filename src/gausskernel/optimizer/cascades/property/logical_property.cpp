#include "logical_property.h"
#include "../memo/expression_context.h"
#include "../memo/group_expression.h"
#include "../operator/operator.h"
#include "../operator/operators/logical_scan.h"
#include "../operator/operators/logical_project.h"
#include "../operator/operators/logical_filter.h"
#include "nodes/primnodes.h"
#include "nodes/nodeFuncs.h"

namespace cascades {

/*
 * Helper context for extracting Var references from expressions
 */
typedef struct
{
    ColumnRefSet* columns;
} ExtractVarsContext;

/*
 * Walker function to extract all Var references from an expression tree
 */
static bool
extract_vars_walker(Node* node, ExtractVarsContext* context)
{
    if (node == NULL)
        return false;

    if (IsA(node, Var))
    {
        Var* var = (Var*)node;
        /* Only add base relation Vars (not OUTER_VAR, INNER_VAR, etc.) */
        if (var->varno > 0 && var->varno < 65000)
        {
            context->columns->add(var->varattno);
        }
    }

    return expression_tree_walker(node, (bool (*)())extract_vars_walker, (void*)context);
}

/*
 * Extract all Var references from an expression and add to column set
 */
static void
extract_vars_from_expr(Node* expr, ColumnRefSet* columns)
{
    ExtractVarsContext context;
    context.columns = columns;
    (void)extract_vars_walker(expr, &context);
}

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
                // Note: target_list is List<TargetEntry*>, need to extract TargetEntry->expr
                ListCell* lc;
                foreach(lc, target_list) {
                    TargetEntry* tle = (TargetEntry*)lfirst(lc);
                    if (tle == nullptr || tle->expr == nullptr) {
                        continue;
                    }
                    Node* expr = (Node*)tle->expr;
                    // Extract all Var references from the expression (not just bare Vars)
                    // This handles expressions like "id+id" which reference column 1
                    extract_vars_from_expr(expr, &output_columns_);
                }
            } else if (context->arity() > 0) {
                // No target_list, pass through all child columns
                output_columns_ = context->get_child_output_columns(0);
            }
            // Sort order preserved through project
            break;
        }

        case OptExprOpType::LOGICAL_FILTER: {
            // Filter passes through columns from child unchanged
            // Note: qual columns are NOT part of logical output - they are evaluation
            // conditions that must be computed at runtime, but not returned to parent.
            // The pathtarget (physical property) will include qual columns, but the
            // logical output columns are just the child's output columns.
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
            // Scan outputs columns stored in the operator itself
            // Note: We do NOT read from rel->reltarget because reltarget may be
            // modified by ColumnPruneRule during logical_rewrite phase.
            // The output_columns_ member stores the logical output independently.
            LogicalScan* scan_op = static_cast<LogicalScan*>(op);
            output_columns_ = scan_op->get_output_columns();
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