#pragma once
#include "rule.h"

namespace cascades {


class NestLoopImplRule : public Rule {
public:
    NestLoopImplRule();

    int promise() const override { return 3; }  // implementation rules higher

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}