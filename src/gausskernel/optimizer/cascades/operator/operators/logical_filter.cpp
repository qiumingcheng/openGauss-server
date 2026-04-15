#include "logical_filter.h"
#include "../../memo/opt_expression.h"

namespace cascades {

LogicalFilter::LogicalFilter()
    : LogicalOptExprOp(OptExprOpType::LOGICAL_FILTER),
      predicate_id_(-1),
      qual_(nullptr),
      qual_list_(nullptr) {}

LogicalFilter::LogicalFilter(OptExpression* child)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_FILTER),
      predicate_id_(-1),
      qual_(nullptr),
      qual_list_(nullptr) {}

LogicalFilter::LogicalFilter(Node* qual)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_FILTER),
      predicate_id_(-1),
      qual_(qual),
      qual_list_(nullptr) {}

LogicalFilter::LogicalFilter(Node* qual, List* qual_list)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_FILTER),
      predicate_id_(-1),
      qual_(qual),
      qual_list_(qual_list) {}

OptExprOp* LogicalFilter::clone() const {
    LogicalFilter* cloned = NEW_EX(LogicalFilter);
    cloned->set_predicate_id(predicate_id_);
    cloned->set_qual(qual_);
    cloned->set_qual_list(qual_list_);
    return cloned;
}

size_t LogicalFilter::hash() const {
    size_t h = static_cast<size_t>(get_type());
    h = h * 31 + static_cast<size_t>(predicate_id_);
    if (qual_ != nullptr) {
        h = h * 31 + reinterpret_cast<size_t>(qual_);
    }
    return h;
}

std::string LogicalFilter::to_string() const {
    if (qual_list_ != nullptr) {
        return "LogicalFilter(qual_list)";
    } else if (qual_ != nullptr) {
        return "LogicalFilter(qual)";
    } else if (predicate_id_ >= 0) {
        return "LogicalFilter(predicate_id=" + std::to_string(predicate_id_) + ")";
    }
    return "LogicalFilter";
}

}  // namespace cascades
