#include "rule.h"

namespace cascades {
Rule::Rule(RuleType type, Pattern* pattern) : type_(type), pattern_(pattern) {}

bool Rule::check(OptExpression* input, OptimizerContext* context) const {
    return pattern_->match(input);
}

std::vector<Rule*> Rule::predecessor_rules() const {
    return {};
}

std::vector<Rule*> Rule::successor_rules() const {
    return {};
}

bool Rule::exhausted(OptimizerContext* context) const {
    return false;
}

std::string Rule::to_string() const {
    return "Rule " + std::to_string(static_cast<int>(type_));
}


}  // namespace cascades