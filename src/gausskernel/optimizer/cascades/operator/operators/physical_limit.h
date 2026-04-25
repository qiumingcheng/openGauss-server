#pragma once
#include "../operator.h"

namespace cascades {

class PhysicalLimit : public PhysicalOptExprOp {
public:
    PhysicalLimit() : PhysicalOptExprOp(OptExprOpType::PHYSICAL_LIMIT), limit_count_(nullptr), limit_offset_(nullptr) {}
    explicit PhysicalLimit(Path* path) : PhysicalOptExprOp(OptExprOpType::PHYSICAL_LIMIT, path), limit_count_(nullptr), limit_offset_(nullptr) {}

    OptExprOp* clone() const override { return new PhysicalLimit(*this); }
    size_t hash() const override { return 0; }
    std::string to_string() const override { return "PhysicalLimit"; }

    Node* get_limit_count() const { return limit_count_; }
    Node* get_limit_offset() const { return limit_offset_; }
    void set_limit_count(Node* c) { limit_count_ = c; }
    void set_limit_offset(Node* o) { limit_offset_ = o; }

private:
    Node* limit_count_;
    Node* limit_offset_;
};

}  // namespace cascades
