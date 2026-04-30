#include "physical_subquery_scan.h"

namespace cascades {

PhysicalSubqueryScan::PhysicalSubqueryScan(Path* subquery_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SUBQUERY_SCAN),
      subquery_path_(subquery_path) {}

PhysicalSubqueryScan::PhysicalSubqueryScan(Path* subquery_path, PlannerInfo* subroot)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SUBQUERY_SCAN),
      subquery_path_(subquery_path),
      subroot_(subroot) {}

OptExprOp* PhysicalSubqueryScan::clone() const {
    PhysicalSubqueryScan* cloned = NEW_EX(PhysicalSubqueryScan, subquery_path_, subroot_);
    cloned->set_subplan_params(subplan_params_);
    return cloned;
}

size_t PhysicalSubqueryScan::hash() const {
    size_t h = static_cast<size_t>(get_type());
    if (subquery_path_ != nullptr) {
        h = h * 31 + reinterpret_cast<size_t>(subquery_path_);
    }
    if (subroot_ != nullptr) {
        h = h * 31 + reinterpret_cast<size_t>(subroot_);
    }
    return h;
}

std::string PhysicalSubqueryScan::to_string() const {
    return "PhysicalSubqueryScan";
}

bool PhysicalSubqueryScan::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const PhysicalSubqueryScan* other_scan = static_cast<const PhysicalSubqueryScan*>(other);
    if (!equal(subplan_params_, other_scan->subplan_params_)) {
        return false;
    }
    return true;
}

}  // namespace cascades
