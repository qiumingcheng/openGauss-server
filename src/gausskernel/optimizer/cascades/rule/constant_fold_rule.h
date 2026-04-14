#pragma once
#include "rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"

namespace cascades {


/**
 * ConstantFoldRule - Performs constant folding optimization
 *
 * Constant folding evaluates constant expressions at optimization time
 * instead of at execution time, reducing runtime computation.
 *
 * Examples:
 *   1 + 2                   -> 3
 *   'Hello' || 'World'      -> 'HelloWorld'
 *   1 = 1                   -> true (always true)
 *   1 = 2                   -> false (always false)
 *   WHERE 1 = 1             -> eliminate filter (always true)
 *   WHERE 1 = 0             -> eliminate entire branch (always false)
 *   x IS NULL (where x=const)-> evaluate based on const value
 *
 * This rule handles:
 * - Arithmetic operations on constants (+, -, *, /, etc.)
 * - String concatenation on constants (||)
 * - Comparison operations on constants (=, <>, <, >, <=, >=)
 * - Boolean operations on constants (AND, OR, NOT)
 * - IS NULL / IS NOT NULL on constant expressions
 * - Elimination of filters with constant true/false conditions
 */
class ConstantFoldRule : public Rule {
public:
    static ConstantFoldRule* instance();
    ConstantFoldRule();

    int promise() const override { return 3; }  // Higher priority than most rules

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

private:
    /**
     * Check if an operator represents a constant expression
     * For now, we identify constants by their predicate_id or special markers
     */
    bool is_constant_expression(OptExpression* expr) const;

    /**
     * Evaluate a constant expression and return the result
     * Returns true if evaluation succeeded and result is in out参数
     */
    bool evaluate_constant(OptExpression* expr, bool* result) const;

    /**
     * Try to fold a project node with constant expressions
     */
    OptExpression* fold_project(LogicalProject* project, OptExpression* child) const;

    /**
     * Check if a filter predicate is constant true or false
     * Returns:  1 = always true, -1 = always false, 0 = not constant
     */
    int get_filter_const_value(LogicalFilter* filter) const;

    /**
     * Eliminate a filter that is always true - push up the child
     */
    OptExpression* eliminate_true_filter(OptExpression* filter_expr) const;

    /**
     * Handle a filter that is always false - return nullptr to indicate empty result
     */
    OptExpression* eliminate_false_filter(OptExpression* filter_expr) const;
};

}