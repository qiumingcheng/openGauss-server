#include "group_expression.h"
#include "group.h"
#include "expression_context.h"

namespace cascades {

GroupExpression::GroupExpression(OptExprOp* op, const std::vector<Group*>& inputs)
    : group_(nullptr), op_(op), inputs_(inputs) {}

GroupExpression::~GroupExpression() {}

bool GroupExpression::is_logical() const {
    return op_->is_logical();
}

bool GroupExpression::is_physical() const {
    return op_->is_physical();
}

void GroupExpression::derive_logical_property() {
    ExpressionContext context(this);
    context.derive_logical_property();
    group_->set_logical_property(context.get_root_property());
}

void GroupExpression::derive_logical_property_recursively() {
    // First, derive logical property for all input groups
    for (Group* input_group : inputs_) {
        GroupExpression* first_logical = input_group->get_first_logical_expression();
        if (first_logical != nullptr) {
            first_logical->derive_logical_property_recursively();
        }
    }
    // Then, derive logical property for this expression
    derive_logical_property();
}

void GroupExpression::compute_property(const PhysicalPropertySet& required_property) {
    // Compute output property and propagate requirements to children
    // Based on the operator type, we determine what properties are needed from inputs

    std::vector<PhysicalPropertySet> input_properties;

    // For each input, derive required property based on operator semantics
    for (size_t i = 0; i < inputs_.size(); ++i) {
        Group* input_group = inputs_[i];

        // Create required property for child based on operator semantics
        PhysicalPropertySet child_required;

        // Derive child requirements based on operator type
        switch (op_->get_type()) {
            case OptExprOpType::PHYSICAL_MERGE_JOIN: {
                // MergeJoin requires both children to be sorted on join keys
                // The left child's sort order must match the join's probe keys
                // The right child's sort order must match the join's build keys
                if (i == 0) {
                    // Left (probe) side: needs sort on probe keys
                    // For now, inherit parent's sort requirement
                    child_required = required_property;
                } else if (i == 1) {
                    // Right (build) side: needs sort on build keys
                    child_required = required_property;
                }
                break;
            }
            case OptExprOpType::PHYSICAL_HASH_JOIN: {
                // HashJoin requires children to be hash-partitioned on join keys
                // The inner (build) side must be hash-partitioned on build keys
                // The outer (probe) side doesn't have distribution requirements
                if (i == 1) {
                    // Right (build) side: needs HASH distribution on build keys
                    // For now, keep as ANY
                    child_required = required_property;
                }
                break;
            }
            case OptExprOpType::PHYSICAL_HASH_AGG: {
                // HashAgg requires input to be hash-partitioned on group keys
                if (i == 0) {
                    // Input: needs HASH distribution on group keys
                    child_required = required_property;
                }
                break;
            }
            case OptExprOpType::PHYSICAL_SORT: {
                // Sort requires input to be ANY (will enforce after getting data)
                child_required = PhysicalPropertySet();
                break;
            }
            case OptExprOpType::PHYSICAL_SCAN: {
                // Scan doesn't have input requirements
                child_required = PhysicalPropertySet();
                break;
            }
            case OptExprOpType::LOGICAL_JOIN:
            case OptExprOpType::LOGICAL_FILTER:
            case OptExprOpType::LOGICAL_PROJECT: {
                // Logical operators pass through parent's requirements
                child_required = required_property;
                break;
            }
            default:
                // For other operators, no specific input requirements
                child_required = PhysicalPropertySet();
                break;
        }

        input_properties.push_back(child_required);
    }

    // Store the computed property with its cost
    // Cost will be computed separately by EnforceAndCostTask
    update_property_with_cost(required_property, input_properties, 0.0);
}

double GroupExpression::get_cost(const PhysicalPropertySet& required_property) const {
    auto it = cost_table_.find(required_property.hash());
    if (it != cost_table_.end()) {
        return it->second.first;
    }
    return -1.0;  // indicates not computed
}

std::vector<PhysicalPropertySet> GroupExpression::get_input_properties(
    const PhysicalPropertySet& required_property) const {
    auto it = cost_table_.find(required_property.hash());
    if (it != cost_table_.end()) {
        return it->second.second;
    }
    return {};  // empty vector if not computed
}

void GroupExpression::update_property_with_cost(
    const PhysicalPropertySet& output_property,
    const std::vector<PhysicalPropertySet>& input_properties,
    double cost) {
    cost_table_[output_property.hash()] = {cost, input_properties};
}

void GroupExpression::set_rule_explored(RuleType rule_type) {
    explored_rules_.insert(rule_type);
}

bool GroupExpression::has_rule_explored(RuleType rule_type) const {
    return explored_rules_.find(rule_type) != explored_rules_.end();
}

size_t GroupExpression::hash() const {
    size_t h = op_->hash();
    for (Group* g : inputs_) {
        h = h * 31 + g->id();
    }
    return h;
}

bool GroupExpression::operator==(const GroupExpression& other) const {
    if (op_ != other.op_) return false;
    if (inputs_.size() != other.inputs_.size()) return false;
    for (size_t i = 0; i < inputs_.size(); ++i) {
        if (inputs_[i]->id() != other.inputs_[i]->id()) return false;
    }
    return true;
}

std::string GroupExpression::to_string() const {
    return op_->to_string();
}

}  // namespace cascades
