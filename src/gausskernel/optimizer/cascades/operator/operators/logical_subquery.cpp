#include "logical_subquery.h"
#include "../../base/memory_utils.h"

namespace cascades {

LogicalSubquery::LogicalSubquery()
    : LogicalOptExprOp(OptExprOpType::LOGICAL_SUBQUERY),
      subquery_id_(-1),
      is_correlated_(false) {}

LogicalSubquery::LogicalSubquery(int subquery_id)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_SUBQUERY),
      subquery_id_(subquery_id),
      is_correlated_(false) {}

OptExprOp* LogicalSubquery::clone() const {
    LogicalSubquery* cloned = NEW_EX(LogicalSubquery, subquery_id_);
    cloned->set_correlated(is_correlated_);
    cloned->set_output_columns(output_columns_);
    return cloned;
}

size_t LogicalSubquery::hash() const {
    size_t h = static_cast<size_t>(get_type());
    h = h * 31 + static_cast<size_t>(subquery_id_);
    h = h * 37 + (is_correlated_ ? 1 : 0);
    for (int col : output_columns_) {
        h = h * 31 + static_cast<size_t>(col);
    }
    return h;
}

std::string LogicalSubquery::to_string() const {
    std::string result = "LogicalSubquery(id=" + std::to_string(subquery_id_);
    result += ", correlated=" + std::string(is_correlated_ ? "true" : "false");
    result += ", outputs=" + std::to_string(output_columns_.size());
    result += ")";
    return result;
}

}  // namespace cascades
