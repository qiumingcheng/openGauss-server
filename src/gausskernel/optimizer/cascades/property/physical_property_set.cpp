#include "physical_property_set.h"

namespace cascades {
PhysicalPropertySet::PhysicalPropertySet() {}

PhysicalPropertySet::PhysicalPropertySet(const DistributionProperty& dist) : distribution_(dist) {}

PhysicalPropertySet::PhysicalPropertySet(const DistributionProperty& dist, const SortProperty& sort)
    : distribution_(dist), sort_(sort) {}

void PhysicalPropertySet::set_distribution(const DistributionProperty& dist) {
    distribution_ = dist;
}

void PhysicalPropertySet::set_sort(const SortProperty& sort) {
    sort_ = sort;
}

bool PhysicalPropertySet::is_satisfy(const PhysicalPropertySet& required) const {
    return distribution_.is_satisfy(required.distribution_) &&
           sort_.is_satisfy(required.sort_);
}

bool PhysicalPropertySet::operator==(const PhysicalPropertySet& other) const {
    return distribution_ == other.distribution_ && sort_ == other.sort_;
}

size_t PhysicalPropertySet::hash() const {
    size_t h = distribution_.hash();
    if (!sort_.is_empty()) {
        h = h * 37 + sort_.hash();
    }
    return h;
}

std::string PhysicalPropertySet::to_string() const {
    return "PhysicalPropertySet(" + distribution_.to_string() + ", " + sort_.to_string() + ")";
}


}  // namespace cascades