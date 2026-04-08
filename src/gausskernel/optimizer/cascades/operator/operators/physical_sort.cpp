#include "physical_sort.h"

namespace cascades {

PhysicalSort::PhysicalSort()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SORT) {}

PhysicalSort::PhysicalSort(Path* sort_path, Path* child_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SORT, sort_path),
      child_path_(child_path) {}

OptExprOp* PhysicalSort::clone() const {
    PhysicalSort* cloned = new PhysicalSort(path_, child_path_);
    return cloned;
}

size_t PhysicalSort::hash() const {
    return static_cast<size_t>(get_type());
}

std::string PhysicalSort::to_string() const {
    return "PhysicalSort";
}

}  // namespace cascades
