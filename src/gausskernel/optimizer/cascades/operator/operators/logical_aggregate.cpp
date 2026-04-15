#include "logical_aggregate.h"

namespace cascades {

LogicalAggregate::LogicalAggregate()
    : LogicalOptExprOp(OptExprOpType::LOGICAL_AGGREGATE) {}

LogicalAggregate::LogicalAggregate(const std::vector<int>& group_keys)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_AGGREGATE),
      group_keys_(group_keys) {}

OptExprOp* LogicalAggregate::clone() const {
    return NEW_EX(LogicalAggregate, group_keys_);
}

size_t LogicalAggregate::hash() const {
    size_t h = static_cast<size_t>(get_type());
    for (int key : group_keys_) {
        h = h * 31 + static_cast<size_t>(key);
    }
    return h;
}

std::string LogicalAggregate::to_string() const {
    std::string result = "LogicalAggregate(group_keys=[";
    for (size_t i = 0; i < group_keys_.size(); ++i) {
        if (i > 0) result += ", ";
        result += std::to_string(group_keys_[i]);
    }
    result += "])";
    return result;
}

}  // namespace cascades
