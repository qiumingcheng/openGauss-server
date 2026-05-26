#include "physical_hash_agg.h"

namespace cascades {

PhysicalHashAgg::PhysicalHashAgg()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_HASH_AGG) {}

PhysicalHashAgg::PhysicalHashAgg(Path* agg_path, const std::vector<int>& group_keys)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_HASH_AGG, agg_path),
      group_keys_(group_keys) {}

OptExprOp* PhysicalHashAgg::clone() const {
    PhysicalHashAgg* cloned = NEW_EX(PhysicalHashAgg, path_, group_keys_);
    cloned->having_qual_ = having_qual_;
    cloned->grp_operators_ = grp_operators_;
    cloned->grp_collations_ = grp_collations_;
    cloned->target_list_ = target_list_;
    return cloned;
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

bool PhysicalHashAgg::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const PhysicalHashAgg* other_agg = static_cast<const PhysicalHashAgg*>(other);
    if (group_keys_ != other_agg->group_keys_) {
        return false;
    }
    if (!equal(having_qual_, other_agg->having_qual_)) {
        return false;
    }
    return true;
}

}  // namespace cascades
