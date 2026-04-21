#include "logical_subquery_scan.h"
#include "../../base/memory_utils.h"

namespace cascades {

LogicalSubqueryScan::LogicalSubqueryScan(int rtindex)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_SUBQUERY_SCAN),
      rtindex_(rtindex),
      subquery_(nullptr) {}

LogicalSubqueryScan::LogicalSubqueryScan(int rtindex, Query* subquery)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_SUBQUERY_SCAN),
      rtindex_(rtindex),
      subquery_(subquery) {}

OptExprOp* LogicalSubqueryScan::clone() const {
    LogicalSubqueryScan* cloned = NEW_EX(LogicalSubqueryScan, rtindex_, subquery_);
    cloned->set_rel(rel_);
    return cloned;
}

size_t LogicalSubqueryScan::hash() const {
    size_t h = static_cast<size_t>(get_type());
    h = h * 31 + static_cast<size_t>(rtindex_);
    if (subquery_ != nullptr) {
        h = h * 31 + reinterpret_cast<size_t>(subquery_);
    }
    return h;
}

std::string LogicalSubqueryScan::to_string() const {
    return "LogicalSubqueryScan(rtindex=" + std::to_string(rtindex_) + ")";
}

}  // namespace cascades
