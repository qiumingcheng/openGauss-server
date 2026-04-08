#include "physical_hash_agg.h"

namespace cascades {

PhysicalHashAgg::PhysicalHashAgg()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_HASH_AGG) {}

PhysicalHashAgg::PhysicalHashAgg(Path* agg_path, Path* child_path, const std::vector<int>& group_keys)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_HASH_AGG, agg_path),
      child_path_(child_path),
      group_keys_(group_keys) {}

OptExprOp* PhysicalHashAgg::clone() const {
    return new PhysicalHashAgg(path_, child_path_, group_keys_);
}

size_t PhysicalHashAgg::hash() const {
    size_t h = static_cast<size_t>(get_type());
    for (int key : group_keys_) {
        h = h * 31 + static_cast<size_t>(key);
    }
    return h;
}

std::string PhysicalHashAgg::to_string() const {
    std::string result = "PhysicalHashAgg(group_keys=[";
    for (size_t i = 0; i < group_keys_.size(); ++i) {
        if (i > 0) result += ", ";
        result += std::to_string(group_keys_[i]);
    }
    result += "])";
    return result;
}

}  // namespace cascades
