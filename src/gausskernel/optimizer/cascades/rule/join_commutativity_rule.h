#pragma once
#include "rule.h"

namespace cascades {


class JoinCommutativityRule : public Rule {
public:
    static JoinCommutativityRule* instance();
    JoinCommutativityRule();

    int promise() const override { return 4; }  // higher promise = earlier

    bool check(OptExpression* input, OptimizerContext* context) const override;

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}