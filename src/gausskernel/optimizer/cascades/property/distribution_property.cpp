#include "distribution_property.h"
#include "../base/memory_utils.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../operator/operators.h"

namespace cascades {
DistributionProperty::DistributionProperty() : type_(DistributionType::ANY) {}

DistributionProperty::DistributionProperty(DistributionType type) : type_(type) {}

DistributionProperty::DistributionProperty(DistributionType type, const std::vector<int>& hash_columns)
    : type_(type), hash_columns_(hash_columns) {}

bool DistributionProperty::is_satisfy(const DistributionProperty& required) const {
    // ANY requirement is always satisfied
    if (required.type_ == DistributionType::ANY) {
        return true;
    }

    // If our type is ANY, we cannot satisfy any specific requirement
    if (type_ == DistributionType::ANY) {
        return false;
    }

    // Type must match
    if (type_ != required.type_) {
        return false;
    }

    // For HASH distribution, columns must match (or be a prefix)
    if (type_ == DistributionType::HASH) {
        // Required hash columns must be satisfied
        const size_t required_cols = required.hash_columns_.size();
        const size_t our_cols = hash_columns_.size();

        // Our hash columns must be at least as many as required
        if (our_cols < required_cols) {
            return false;
        }

        // Check that our columns start with the required columns
        for (size_t i = 0; i < required_cols; ++i) {
            if (hash_columns_[i] != required.hash_columns_[i]) {
                return false;
            }
        }
        return true;
    }

    // For other types (BROADCAST, GATHER, RANDOM, REPLICATED), type match is sufficient
    return true;
}

size_t DistributionProperty::hash() const {
    size_t h = static_cast<size_t>(type_);
    for (int col : hash_columns_) {
        h = h * 31 + col;
    }
    return h;
}

bool DistributionProperty::operator==(const DistributionProperty& other) const {
    if (type_ != other.type_) {
        return false;
    }
    // For HASH type, columns must match exactly
    if (type_ == DistributionType::HASH) {
        return hash_columns_ == other.hash_columns_;
    }
    return true;
}

std::string DistributionProperty::to_string() const {
    std::string type_str;
    switch (type_) {
        case DistributionType::ANY: type_str = "ANY"; break;
        case DistributionType::BROADCAST: type_str = "BROADCAST"; break;
        case DistributionType::GATHER: type_str = "GATHER"; break;
        case DistributionType::HASH: type_str = "HASH"; break;
        case DistributionType::RANDOM: type_str = "RANDOM"; break;
        case DistributionType::REPLICATED: type_str = "REPLICATED"; break;
    }

    std::string result = "DistributionProperty(" + type_str;
    if (type_ == DistributionType::HASH && !hash_columns_.empty()) {
        result += ", cols=[";
        for (size_t i = 0; i < hash_columns_.size(); ++i) {
            if (i > 0) result += ", ";
            result += std::to_string(hash_columns_[i]);
        }
        result += "]";
    }
    result += ")";
    return result;
}

GroupExpression* DistributionProperty::append_enforcers(Group* child) const {
    // Create PhysicalExchange enforcer for distribution property
    PhysicalExchange* exchange = NEW_EX(PhysicalExchange, nullptr, nullptr, type_);
    std::vector<Group*> inputs = {child};
    GroupExpression* enforcer = NEW_EX(GroupExpression, exchange, inputs);
    enforcer->set_group(child);
    return enforcer;
}


}  // namespace cascades