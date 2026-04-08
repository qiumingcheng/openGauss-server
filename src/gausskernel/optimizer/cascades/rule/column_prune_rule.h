#pragma once
#include "rule.h"

namespace cascades {


class ColumnPruneRule : public Rule {
public:
    ColumnPruneRule();

    int promise() const override { return 1; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}