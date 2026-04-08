#include "sort_property.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../operator/operators.h"

namespace cascades {
bool OrderSpec::is_satisfy(const OrderSpec& required) const {
    // A output order satisfies required order if it starts with the required order
    // e.g., output [a, b, c] satisfies required [a, b] but not required [b, c]
    if (required.orderings_.empty()) {
        return true;  // Any order satisfies "no order required"
    }
    if (orderings_.size() < required.orderings_.size()) {
        return false;
    }
    for (size_t i = 0; i < required.orderings_.size(); ++i) {
        if (orderings_[i].column_id != required.orderings_[i].column_id ||
            orderings_[i].ascending != required.orderings_[i].ascending) {
            return false;
        }
    }
    return true;
}

size_t OrderSpec::hash() const {
    size_t h = 0;
    for (const auto& ord : orderings_) {
        h = h * 31 + ord.column_id;
        h = h * 37 + (ord.ascending ? 1 : 0);
    }
    return h;
}

SortProperty::SortProperty() : is_empty_(true) {}

SortProperty::SortProperty(const OrderSpec& spec) : is_empty_(false), spec_(spec) {}

bool SortProperty::is_satisfy(const SortProperty& required) const {
    if (required.is_empty_) {
        return true;  // Any output satisfies "no sort required"
    }
    if (is_empty_) {
        return false;  // No output cannot satisfy "sort required"
    }
    return spec_.is_satisfy(required.spec_);
}

size_t SortProperty::hash() const {
    if (is_empty_) {
        return 0;
    }
    return spec_.hash();
}

bool SortProperty::operator==(const SortProperty& other) const {
    if (is_empty_ && other.is_empty_) return true;
    if (is_empty_ || other.is_empty_) return false;
    return spec_ == other.spec_;
}

std::string SortProperty::to_string() const {
    if (is_empty_) {
        return "SortProperty(empty)";
    }
    std::string result = "SortProperty(";
    const auto& orderings = spec_.get_orderings();
    for (size_t i = 0; i < orderings.size(); ++i) {
        if (i > 0) result += ", ";
        result += "col" + std::to_string(orderings[i].column_id);
        result += orderings[i].ascending ? " ASC" : " DESC";
    }
    result += ")";
    return result;
}

GroupExpression* SortProperty::append_enforcers(Group* child) const {
    // Create PhysicalSort enforcer for sort property
    PhysicalSort* sort = new PhysicalSort(nullptr, nullptr);
    std::vector<Group*> inputs = {child};
    GroupExpression* enforcer = new GroupExpression(sort, inputs);
    enforcer->set_group(child);
    return enforcer;
}


}  // namespace cascades