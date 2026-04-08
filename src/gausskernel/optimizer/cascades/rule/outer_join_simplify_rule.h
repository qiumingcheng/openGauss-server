#pragma once
#include "rule.h"
#include "../memo/opt_expression.h"

namespace cascades {


/**
 * OuterJoinSimplifyRule - Reduces outer joins to inner joins when possible
 *
 * Based on OpenGauss's reduce_outer_joins optimization:
 * - LEFT JOIN with strict WHERE condition on inner side -> INNER JOIN
 * - RIGHT JOIN -> flip to LEFT JOIN, then apply same reduction
 * - JOIN_SEMI with non-strict qual -> ANTI JOIN
 *
 * Example:
 *   SELECT * FROM a LEFT JOIN b ON (a.id = b.id) WHERE b.y = 42
 *   can be reduced to:
 *   SELECT * FROM a INNER JOIN b ON (a.id = b.id) WHERE b.y = 42
 *   because the strict equality b.y = 42 would reject null-extended rows
 */
class OuterJoinSimplifyRule : public Rule {
public:
    OuterJoinSimplifyRule();

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}