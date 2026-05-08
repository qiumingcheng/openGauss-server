#pragma once
#include "rule.h"

namespace cascades {


class JoinAssociationRule : public Rule {
public:
    static JoinAssociationRule* instance();
    JoinAssociationRule();

    int promise() const override { return 2; }
    bool check(GroupExpression* input, OptimizerContext* context) const override;

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}