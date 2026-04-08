#include "physical_scan.h"

namespace cascades {

PhysicalScan::PhysicalScan()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SCAN) {}

PhysicalScan::PhysicalScan(Path* scan_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SCAN, scan_path) {}

OptExprOp* PhysicalScan::clone() const {
    return new PhysicalScan();
}

size_t PhysicalScan::hash() const {
    return static_cast<size_t>(get_type());
}

std::string PhysicalScan::to_string() const {
    return "PhysicalScan";
}

}  // namespace cascades
