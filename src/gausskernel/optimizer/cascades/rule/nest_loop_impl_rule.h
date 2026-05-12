#pragma once
#include "rule.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"

namespace cascades {

class NestLoopImplRule : public Rule {
public:
    static NestLoopImplRule* instance();
    NestLoopImplRule();

    int promise() const override { return 3; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}