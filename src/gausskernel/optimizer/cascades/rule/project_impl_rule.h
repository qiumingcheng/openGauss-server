#pragma once
#include "rule.h"

namespace cascades {

class ProjectImplRule : public Rule {
public:
    static ProjectImplRule* instance();
    ProjectImplRule();

    int promise() const override { return 3; }  // implementation rules higher

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}  // namespace cascades
