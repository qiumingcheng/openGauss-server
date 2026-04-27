#include "logical_scan.h"

namespace cascades {

LogicalScan::LogicalScan(int table_id)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_SCAN), table_id_(table_id) {}

OptExprOp* LogicalScan::clone() const {
    LogicalScan* cloned = NEW_EX(LogicalScan, table_id_);
    cloned->set_rel(rel_);
    cloned->set_qual_list(qual_list_);
    cloned->set_output_columns(output_columns_);
    return cloned;
}

size_t LogicalScan::hash() const {
    size_t h = static_cast<size_t>(get_type()) * 31 + table_id_;
    if (qual_list_ != nullptr) {
        h = h * 31 + list_length(qual_list_);
    }
    return h;
}

std::string LogicalScan::to_string() const {
    std::string result = "LogicalScan(table=" + std::to_string(table_id_);
    if (qual_list_ != nullptr) {
        result += ", qual_list=" + std::to_string(list_length(qual_list_));
    }
    result += ")";
    return result;
}

}  // namespace cascades
