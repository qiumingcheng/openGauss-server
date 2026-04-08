#include "rule_set.h"
#include "rule.h"

namespace cascades {
RuleSet::RuleSet() {}

void RuleSet::add_rule(Rule* rule) {
    rules_.push_back(rule);
    if (rule->type() < RuleType::IMP_HASH_JOIN) {
        transformation_rules_.push_back(rule);
    } else {
        implementation_rules_.push_back(rule);
    }

    // Index rule by its pattern's operator type for O(1) lookup
    // Rules that match OptExprOpType::ANY are stored separately and applied to all types
    OptExprOpType op_type = rule->get_pattern()->get_operator_type();
    if (op_type != OptExprOpType::ANY) {
        rules_by_operator_type_[op_type].push_back(rule);
    }
}

const std::vector<Rule*>& RuleSet::get_rules_for_operator(OptExprOpType op_type) const {
    static const std::vector<Rule*> empty_result;
    auto it = rules_by_operator_type_.find(op_type);
    if (it != rules_by_operator_type_.end()) {
        return it->second;
    }
    return empty_result;
}


}  // namespace cascades