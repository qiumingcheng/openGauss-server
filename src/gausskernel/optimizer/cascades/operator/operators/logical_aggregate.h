#pragma once
#include "../operator.h"
#include <vector>

namespace cascades {

class LogicalAggregate : public LogicalOptExprOp {
public:
    LogicalAggregate();
    LogicalAggregate(const std::vector<int>& group_keys);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    const std::vector<int>& get_group_keys() const { return group_keys_; }
    void set_group_keys(const std::vector<int>& keys) { group_keys_ = keys; }

private:
    std::vector<int> group_keys_;
};

}  // namespace cascades
