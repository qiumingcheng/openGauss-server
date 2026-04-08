#pragma once
#include "pattern.h"
#include "rule_type.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include <vector>
#include <string>

namespace cascades {


/**
 * Rule - Base class for transformation and implementation rules
 */
class Rule {
public:
    explicit Rule(RuleType type, Pattern* pattern);
    virtual ~Rule() = default;

    RuleType type() const { return type_; }
    Pattern* get_pattern() const { return pattern_; }

    virtual int promise() const { return 1; }
    virtual bool check(OptExpression* input, OptimizerContext* context) const;
    virtual std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) = 0;
    virtual std::vector<Rule*> predecessor_rules() const;
    virtual std::vector<Rule*> successor_rules() const;
    virtual bool exhausted(OptimizerContext* context) const;

    std::string to_string() const;

protected:
    RuleType type_;
    Pattern* pattern_;
};

}