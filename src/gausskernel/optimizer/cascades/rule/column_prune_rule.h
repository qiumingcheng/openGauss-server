#pragma once
#include "rule.h"
#include <set>

namespace cascades {


class ColumnPruneRule : public Rule {
public:
    static ColumnPruneRule* instance();
    ColumnPruneRule();

    int promise() const override { return 1; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

protected:
    List* filter_target_list(List* target_list, const std::set<int>& needed);
};

}