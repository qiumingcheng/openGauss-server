#pragma once
#include "rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"

namespace cascades {


/**
 * CrossJoinEliminationRule - Eliminates unnecessary cross joins
 *
 * A cross join (cartesian product) can be eliminated when:
 * 1. One side is provably empty (no rows) - the whole join is empty
 * 2. One side has only one row - cross join reduces to a simple broadcast
 * 3. There's an implicit join condition through WHERE clause that
 *    creates a valid join (semi-join reduction)
 * 4. The query references the same table twice (self-join elimination)
 *
 * Examples:
 *   SELECT * FROM a, b WHERE a.id = b.id  -- cross join with filter
 *   can become: SELECT * FROM a JOIN b ON a.id = b.id
 *
 *   SELECT * FROM a, b WHERE a.x = 1 AND b.y = 2  -- no join condition
 *   can become: SELECT * FROM a WHERE a.x = 1  (if b is not used)
 */
class CrossJoinEliminationRule : public Rule {
public:
    static CrossJoinEliminationRule* instance();
    CrossJoinEliminationRule();

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

private:
    /**
     * Check if a join has no join condition (pure cross join)
     */
    bool is_cross_join(const LogicalJoin* join) const;

    /**
     * Check if there's an implicit join condition through filter
     */
    bool has_implicit_join_condition(OptExpression* join) const;

    /**
     * Check if one side is a simple filter that can be pushed down
     */
    bool can_push_down_filter(OptExpression* child) const;
};

}