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
    void set_child_path(Path* p) { child_path_ = p; }
    const std::vector<int>& get_group_keys() const { return group_keys_; }

    Node* get_having_qual() const { return having_qual_; }
    void set_having_qual(Node* qual) { having_qual_ = qual; }
    bool has_having_qual() const { return having_qual_ != nullptr; }

    Oid* get_grp_operators() const { return grp_operators_; }
    void set_grp_operators(Oid* ops) { grp_operators_ = ops; }

    List* get_target_list() const { return target_list_; }
    void set_target_list(List* tl) { target_list_ = tl; }

private:
    Path* child_path_ = nullptr;
    std::vector<int> group_keys_;
    Node* having_qual_ = nullptr;
    Oid* grp_operators_ = nullptr;
    List* target_list_ = nullptr;
};

}  // namespace cascades
