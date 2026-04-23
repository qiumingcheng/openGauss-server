#pragma once
#include "rule.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include <vector>

namespace cascades {

class SubqueryScanImplRule : public Rule {
public:
    static SubqueryScanImplRule* instance();
    SubqueryScanImplRule();

    int promise() const override { return 1; }
    std::vector<OptExpression*> transform(OptExpression* input, OptimizerContext* context) override;
};

}  // namespace cascades
