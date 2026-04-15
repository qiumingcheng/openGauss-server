#pragma once
#include "rule.h"

namespace cascades {


class JoinCommutativityRule : public Rule {
public:
    static JoinCommutativityRule* instance();
    JoinCommutativityRule();

    int promise() const override { return 2; }  // higher promise = earlier

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}