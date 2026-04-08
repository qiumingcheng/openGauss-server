#pragma once
#include "logical_join.h"

namespace cascades {

class OptExpression;

/**
 * LogicalInnerJoin - Logical inner join operator
 */
class LogicalInnerJoin : public LogicalJoin {
public:
    LogicalInnerJoin();
    LogicalInnerJoin(OptExpression* left, OptExpression* right);
    LogicalInnerJoin(const std::vector<int>& join_keys);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
};

}  // namespace cascades
