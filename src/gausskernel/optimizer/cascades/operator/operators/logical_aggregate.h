#pragma once
#include "../operator.h"
#include <vector>

namespace cascades {

class LogicalAggregate : public LogicalOptExprOp {
public:
    LogicalAggregate();
    LogicalAggregate(const std::vector<int>& group_keys);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    const std::vector<int>& get_group_keys() const { return group_keys_; }
    void set_group_keys(const std::vector<int>& keys) { group_keys_ = keys; }

    Node* get_having_qual() const { return having_qual_; }
    void set_having_qual(Node* qual) { having_qual_ = qual; }
    bool has_having_qual() const { return having_qual_ != nullptr; }

    List* get_target_list() const { return target_list_; }
    void set_target_list(List* tl) { target_list_ = tl; }

private:
    std::vector<int> group_keys_;
    Node* having_qual_ = nullptr;
    List* target_list_ = nullptr;
};

}  // namespace cascades
