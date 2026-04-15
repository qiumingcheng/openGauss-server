#include "logical_scan.h"

namespace cascades {

LogicalScan::LogicalScan(int table_id)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_SCAN), table_id_(table_id) {}

LogicalScan::LogicalScan(int table_id, int predicate_id)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_SCAN), table_id_(table_id), predicate_id_(predicate_id) {}

OptExprOp* LogicalScan::clone() const {
    LogicalScan* cloned = NEW_EX(LogicalScan, table_id_, predicate_id_);
    cloned->set_rel(rel_);
    return cloned;
}

size_t LogicalScan::hash() const {
    size_t h = static_cast<size_t>(get_type()) * 31 + table_id_;
    if (predicate_id_ >= 0) {
        h = h * 31 + static_cast<size_t>(predicate_id_);
    }
    return h;
}

std::string LogicalScan::to_string() const {
    std::string result = "LogicalScan(table=" + std::to_string(table_id_);
    if (predicate_id_ >= 0) {
        result += ", predicate=" + std::to_string(predicate_id_);
    }
    result += ")";
    return result;
}

}  // namespace cascades
