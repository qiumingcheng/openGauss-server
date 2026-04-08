#pragma once
#include "../operator.h"
#include "nodes/relation.h"

namespace cascades {

class PhysicalHashAgg : public PhysicalOptExprOp {
public:
    PhysicalHashAgg();
    PhysicalHashAgg(Path* agg_path, Path* child_path, const std::vector<int>& group_keys);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    Path* get_child_path() const { return child_path_; }
    const std::vector<int>& get_group_keys() const { return group_keys_; }

private:
    Path* child_path_ = nullptr;
    std::vector<int> group_keys_;
};

}  // namespace cascades
