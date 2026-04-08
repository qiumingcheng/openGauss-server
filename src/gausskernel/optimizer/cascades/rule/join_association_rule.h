#pragma once
#include "rule.h"

namespace cascades {


class JoinAssociationRule : public Rule {
public:
    JoinAssociationRule();

    int promise() const override { return 2; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}