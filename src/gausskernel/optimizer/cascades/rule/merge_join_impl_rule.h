#pragma once
#include "rule.h"

namespace cascades {


class MergeJoinImplRule : public Rule {
public:
    MergeJoinImplRule();

    int promise() const override { return 3; }  // implementation rules higher

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}