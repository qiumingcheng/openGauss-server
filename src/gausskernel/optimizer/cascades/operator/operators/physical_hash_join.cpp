#include "physical_hash_join.h"

namespace cascades {

PhysicalHashJoin::PhysicalHashJoin()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_HASH_JOIN) {}

PhysicalHashJoin::PhysicalHashJoin(Path* join_path, Path* left_path, Path* right_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_HASH_JOIN, join_path),
      left_path_(left_path), right_path_(right_path) {}

OptExprOp* PhysicalHashJoin::clone() const {
    PhysicalHashJoin* cloned = NEW_EX(PhysicalHashJoin, path_, left_path_, right_path_);
    cloned->set_build_keys(build_keys_);
    cloned->set_probe_keys(probe_keys_);
    return cloned;
}

size_t PhysicalHashJoin::hash() const {
    size_t h = static_cast<size_t>(get_type());
    for (int key : build_keys_) {
        h = h * 31 + key;
    }
    for (int key : probe_keys_) {
        h = h * 37 + key;
    }
    return h;
}

std::string PhysicalHashJoin::to_string() const {
    return "PhysicalHashJoin";
}

}  // namespace cascades
