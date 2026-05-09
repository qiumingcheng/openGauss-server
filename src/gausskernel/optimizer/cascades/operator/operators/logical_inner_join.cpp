#include "logical_inner_join.h"
#include "../../memo/opt_expression.h"

namespace cascades {

LogicalInnerJoin::LogicalInnerJoin()
    : LogicalJoin(OptExprOpType::LOGICAL_JOIN) {}

LogicalInnerJoin::LogicalInnerJoin(OptExpression* left, OptExpression* right)
    : LogicalJoin(OptExprOpType::LOGICAL_JOIN, left, right) {}

LogicalInnerJoin::LogicalInnerJoin(const std::vector<int>& join_keys)
    : LogicalJoin(OptExprOpType::LOGICAL_JOIN) {
    left_join_keys_ = join_keys;
}

OptExprOp* LogicalInnerJoin::clone() const {
    LogicalInnerJoin* cloned = NEW_EX(LogicalInnerJoin, left_join_keys_);
    cloned->set_join_type(join_type_);
    cloned->set_join_keys(left_join_keys_, right_join_keys_);
    cloned->set_qual(qual_);
    return cloned;
}

size_t LogicalInnerJoin::hash() const {
    size_t h = static_cast<size_t>(get_type());
    for (int key : left_join_keys_) {
        h = h * 31 + key;
    }
    return h;
}

std::string LogicalInnerJoin::to_string() const {
    return "LogicalInnerJoin(keys=" + std::to_string(left_join_keys_.size()) + ")";
}

bool LogicalInnerJoin::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const LogicalInnerJoin* other_join = static_cast<const LogicalInnerJoin*>(other);
    if (left_join_keys_ != other_join->left_join_keys_) {
        return false;
    }
    if (right_join_keys_ != other_join->right_join_keys_) {
        return false;
    }
    if (join_type_ != other_join->join_type_) {
        return false;
    }
    if (!equal(qual_, other_join->qual_)) {
        return false;
    }
    return true;
}

}  // namespace cascades
