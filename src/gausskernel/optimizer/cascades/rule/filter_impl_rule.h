#pragma once
#include "rule.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include <vector>

namespace cascades {

class FilterImplRule : public Rule {
public:
    static FilterImplRule* instance();
    FilterImplRule();

    int promise() const override { return 1; }
    std::vector<OptExpression*> transform(OptExpression* input, OptimizerContext* context) override;
};

}  // namespace cascades
