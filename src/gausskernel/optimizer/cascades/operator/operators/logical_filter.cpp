#include "logical_filter.h"
#include "../../memo/opt_expression.h"

namespace cascades {

LogicalFilter::LogicalFilter()
    : LogicalOptExprOp(OptExprOpType::LOGICAL_FILTER), predicate_id_(-1) {}

LogicalFilter::LogicalFilter(OptExpression* child)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_FILTER), predicate_id_(-1) {}

LogicalFilter::LogicalFilter(Node* qual)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_FILTER), predicate_id_(-1), qual_(qual) {}

OptExprOp* LogicalFilter::clone() const {
    LogicalFilter* cloned = new LogicalFilter();
    cloned->set_predicate_id(predicate_id_);
    cloned->set_qual(qual_);
    return cloned;
}

size_t LogicalFilter::hash() const {
    return static_cast<size_t>(get_type()) * 31 + predicate_id_;
}

std::string LogicalFilter::to_string() const {
    return "LogicalFilter(predicate=" + std::to_string(predicate_id_) + ")";
}

}  // namespace cascades
