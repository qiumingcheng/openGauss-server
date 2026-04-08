#pragma once
#include "rule.h"

namespace cascades {


class HashJoinImplRule : public Rule {
public:
    HashJoinImplRule();

    int promise() const override { return 3; }  // implementation rules higher

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}