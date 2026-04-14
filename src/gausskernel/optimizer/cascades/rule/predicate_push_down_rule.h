#pragma once
#include "rule.h"

namespace cascades {


class PredicatePushDownRule : public Rule {
public:
    static PredicatePushDownRule* instance();
    PredicatePushDownRule();

    int promise() const override { return 2; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};
}