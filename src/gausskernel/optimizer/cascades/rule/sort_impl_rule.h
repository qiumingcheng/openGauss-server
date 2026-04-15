#pragma once
#include "rule.h"

namespace cascades {


class SortImplRule : public Rule {
public:
    static SortImplRule* instance();
    SortImplRule();

    int promise() const override { return 3; }  // implementation rules higher

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}