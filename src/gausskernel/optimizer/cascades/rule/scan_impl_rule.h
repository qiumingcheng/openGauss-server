#pragma once
#include "rule.h"

namespace cascades {


class ScanImplRule : public Rule {
public:
    static ScanImplRule* instance();
    ScanImplRule();

    int promise() const override { return 3; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}