#include "physical_merge_join.h"

namespace cascades {

PhysicalMergeJoin::PhysicalMergeJoin()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_MERGE_JOIN) {}

PhysicalMergeJoin::PhysicalMergeJoin(Path* join_path, Path* left_path, Path* right_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_MERGE_JOIN, join_path),
      left_path_(left_path), right_path_(right_path) {}

OptExprOp* PhysicalMergeJoin::clone() const {
    PhysicalMergeJoin* cloned = NEW_EX(PhysicalMergeJoin, path_, left_path_, right_path_);
    cloned->set_build_keys(build_keys_);
    cloned->set_probe_keys(probe_keys_);
    return cloned;
}

size_t PhysicalMergeJoin::hash() const {
    size_t h = static_cast<size_t>(get_type());
    for (int key : build_keys_) {
        h = h * 31 + key;
    }
    for (int key : probe_keys_) {
        h = h * 37 + key;
    }
    return h;
}

std::string PhysicalMergeJoin::to_string() const {
    return "PhysicalMergeJoin";
}

bool PhysicalMergeJoin::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const PhysicalMergeJoin* other_join = static_cast<const PhysicalMergeJoin*>(other);
    if (build_keys_ != other_join->build_keys_) {
        return false;
    }
    if (probe_keys_ != other_join->probe_keys_) {
        return false;
    }
    return true;
}

}  // namespace cascades
