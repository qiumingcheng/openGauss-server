#include "logical_scan.h"
#include "nodes/nodes.h"

namespace cascades {

LogicalScan::LogicalScan(int table_id)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_SCAN), table_id_(table_id) {}

OptExprOp* LogicalScan::clone() const {
    LogicalScan* cloned = NEW_EX(LogicalScan, table_id_);
    cloned->set_rel(rel_);
    cloned->set_qual_list(qual_list_);
    cloned->set_output_columns(output_columns_);
    cloned->set_sort_clause(sort_clause_);
    return cloned;
}

size_t LogicalScan::hash() const {
    size_t h = static_cast<size_t>(get_type()) * 31 + table_id_;
    if (qual_list_ != nullptr) {
        h = h * 31 + list_length(qual_list_);
    }
    if (sort_clause_ != nullptr) {
        h = h * 31 + list_length((List*)sort_clause_);
    }
    return h;
}

std::string LogicalScan::to_string() const {
    std::string result = "LogicalScan(table=" + std::to_string(table_id_);
    if (qual_list_ != nullptr) {
        result += ", qual_list=" + std::to_string(list_length(qual_list_));
    }
    if (sort_clause_ != nullptr) {
        result += ", sort_required";
    }
    result += ")";
    return result;
}

bool LogicalScan::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const LogicalScan* other_scan = static_cast<const LogicalScan*>(other);
    if (table_id_ != other_scan->table_id_) {
        return false;
    }
    if (!equal(qual_list_, other_scan->qual_list_)) {
        return false;
    }
    if (!equal(sort_clause_, other_scan->sort_clause_)) {
        return false;
    }
    return true;
}

}  // namespace cascades