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

bool LogicalAggregate::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const LogicalAggregate* other_aggr = static_cast<const LogicalAggregate*>(other);
    if (group_keys_ != other_aggr->group_keys_) {
        return false;
    }
    if (!equal(having_qual_, other_aggr->having_qual_)) {
        return false;
    }
    if (!equal(target_list_, other_aggr->target_list_)) {
        return false;
    }
    return true;
}

}  // namespace cascades
