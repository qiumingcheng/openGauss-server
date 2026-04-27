#pragma once
#include "rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "nodes/relation.h"

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
 * - Elimination of filters with constant true/false conditions
 * - Uses RestrictInfo.pseudoconstant to identify constant predicates
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
     * Eliminate a filter that is always true - push up the child
     */
    OptExpression* eliminate_true_filter(OptExpression* filter_expr) const;
};

/**
 * Get constant type from RestrictInfo
 * Analyzes the clause expression to determine if it's a constant predicate
 */
int get_restrictinfo_const_type(RestrictInfo* restrict_info);

}  // namespace cascades