#pragma once
#include "../operator.h"
#include "nodes/relation.h"
#include <vector>

namespace cascades {

class OptExpression;

/**
 * PhysicalHashJoin - Physical hash join implementation
 */
class PhysicalHashJoin : public PhysicalOptExprOp {
public:
    PhysicalHashJoin();
    PhysicalHashJoin(Path* join_path, Path* left_path, Path* right_path);

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

    Node* get_qual() const { return qual_; }
    void set_qual(Node* q) { qual_ = q; }
    JoinType get_join_type() const { return join_type_; }
    void set_join_type(JoinType t) { join_type_ = t; }

private:
    Path* left_path_ = nullptr;
    Path* right_path_ = nullptr;
    std::vector<int> build_keys_;
    std::vector<int> probe_keys_;
    Node* qual_ = nullptr;
    JoinType join_type_ = JOIN_INNER;
};

}  // namespace cascades
