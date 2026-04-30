#pragma once
#include "../operator.h"
#include "nodes/relation.h"

namespace cascades {

class PhysicalMergeJoin : public PhysicalOptExprOp {
public:
    PhysicalMergeJoin();
    PhysicalMergeJoin(Path* join_path, Path* left_path, Path* right_path);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
    bool equals(const OptExprOp* other) const override;

    Path* get_left_path() const { return left_path_; }
    Path* get_right_path() const { return right_path_; }

    const std::vector<int>& get_build_keys() const { return build_keys_; }
    const std::vector<int>& get_probe_keys() const { return probe_keys_; }
    void set_build_keys(const std::vector<int>& keys) { build_keys_ = keys; }
    void set_probe_keys(const std::vector<int>& keys) { probe_keys_ = keys; }

private:
    Path* left_path_ = nullptr;
    Path* right_path_ = nullptr;
    std::vector<int> build_keys_;
    std::vector<int> probe_keys_;
};

}  // namespace cascades
