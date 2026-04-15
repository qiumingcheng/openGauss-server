#include "physical_scan.h"
#include "postgres.h"

namespace cascades {

PhysicalScan::PhysicalScan()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SCAN),
      relid_(InvalidOid),
      table_id_(-1) {}

PhysicalScan::PhysicalScan(Path* scan_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SCAN, scan_path),
      relid_(InvalidOid),
      table_id_(-1) {}

PhysicalScan::PhysicalScan(Path* scan_path, Oid relid)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SCAN, scan_path),
      relid_(relid),
      table_id_(-1) {}

OptExprOp* PhysicalScan::clone() const {
    PhysicalScan* cloned = NEW_EX(PhysicalScan);
    cloned->set_relid(relid_);
    cloned->set_table_id(table_id_);
    return cloned;
}

size_t PhysicalScan::hash() const {
    size_t h = static_cast<size_t>(get_type());
    if (relid_ != InvalidOid) {
        h = h * 31 + static_cast<size_t>(relid_);
    } else {
        h = h * 31 + static_cast<size_t>(table_id_);
    }
    return h;
}

std::string PhysicalScan::to_string() const {
    if (relid_ != InvalidOid) {
        return "PhysicalScan(relid=" + std::to_string(relid_) + ")";
    } else if (table_id_ >= 0) {
        return "PhysicalScan(table_id=" + std::to_string(table_id_) + ")";
    }
    return "PhysicalScan";
}

}  // namespace cascades
