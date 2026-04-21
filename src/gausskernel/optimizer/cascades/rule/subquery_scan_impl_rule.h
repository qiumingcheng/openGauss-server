#pragma once
#include "rule.h"

namespace cascades {

/**
 * SubqueryScanImplRule - Implementation rule for subquery scans
 *
 * Transforms LogicalSubqueryScan into PhysicalSubqueryScan by:
 * 1. Planning the subquery independently
 * 2. Creating SubqueryScanPath
 * 3. Wrapping in PhysicalSubqueryScan operator
 */
class SubqueryScanImplRule : public Rule {
public:
    static SubqueryScanImplRule* instance();
    SubqueryScanImplRule();

    int promise() const override { return 2; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;
};

}  // namespace cascades
