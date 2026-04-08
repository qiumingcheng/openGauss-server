#pragma once
#include <vector>
#include <map>
#include "../operator/operator.h"

namespace cascades {


// Forward declaration to break circular dependency
class Rule;

class RuleSet {
public:
    RuleSet();
    ~RuleSet() = default;

    void add_rule(Rule* rule);
    const std::vector<Rule*>& get_rules() const { return rules_; }
    const std::vector<Rule*>& get_transformation_rules() const { return transformation_rules_; }
    const std::vector<Rule*>& get_implementation_rules() const { return implementation_rules_; }

    // Get rules that apply to a specific operator type - O(log n) lookup
    const std::vector<Rule*>& get_rules_for_operator(OptExprOpType op_type) const;

private:
    std::vector<Rule*> rules_;
    std::vector<Rule*> transformation_rules_;
    std::vector<Rule*> implementation_rules_;

    // Map from OptExprOpType to applicable rules for fast lookup
    std::map<OptExprOpType, std::vector<Rule*>> rules_by_operator_type_;
};

}