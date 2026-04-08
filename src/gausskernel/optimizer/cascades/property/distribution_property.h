#pragma once
#include <vector>
#include <string>

namespace cascades {


class Group;
class GroupExpression;

enum class DistributionType {
    ANY,
    BROADCAST,
    GATHER,
    HASH,
    RANDOM,
    REPLICATED
};

class DistributionProperty {
public:
    DistributionProperty();
    explicit DistributionProperty(DistributionType type);
    DistributionProperty(DistributionType type, const std::vector<int>& hash_columns);
    ~DistributionProperty() = default;

    DistributionType get_type() const { return type_; }
    const std::vector<int>& get_hash_columns() const { return hash_columns_; }

    bool is_satisfy(const DistributionProperty& required) const;
    size_t hash() const;
    bool operator==(const DistributionProperty& other) const;
    std::string to_string() const;

    /**
     * Append enforcer (Exchange) operator to child group
     * Returns a new GroupExpression with Exchange operator on top
     */
    GroupExpression* append_enforcers(Group* child) const;

private:
    DistributionType type_;
    std::vector<int> hash_columns_;
};

}