#pragma once
#include "distribution_property.h"
#include "sort_property.h"
#include <string>

namespace cascades {


class PhysicalPropertySet {
public:
    PhysicalPropertySet();
    explicit PhysicalPropertySet(const DistributionProperty& dist);
    PhysicalPropertySet(const DistributionProperty& dist, const SortProperty& sort);
    ~PhysicalPropertySet() = default;

    const DistributionProperty& get_distribution() const { return distribution_; }
    void set_distribution(const DistributionProperty& dist);

    const SortProperty& get_sort() const { return sort_; }
    void set_sort(const SortProperty& sort);
    bool has_sort() const { return !sort_.is_empty(); }

    bool is_satisfy(const PhysicalPropertySet& required) const;
    bool operator==(const PhysicalPropertySet& other) const;
    bool operator!=(const PhysicalPropertySet& other) const { return !(*this == other); }

    size_t hash() const;
    std::string to_string() const;

private:
    DistributionProperty distribution_;
    SortProperty sort_;
};

struct PhysicalPropertySetHash {
    size_t operator()(const PhysicalPropertySet& prop) const {
        return prop.hash();
    }
};

}