#pragma once
#include "../operator.h"
#include "nodes/nodes.h"

namespace cascades {

class LogicalLimit : public LogicalOptExprOp {
public:
    LogicalLimit() : LogicalOptExprOp(OptExprOpType::LOGICAL_LIMIT), limit_count_(nullptr), limit_offset_(nullptr) {}

    OptExprOp* clone() const override { return new LogicalLimit(*this); }
    size_t hash() const override { return 0; }
    std::string to_string() const override { return "LogicalLimit"; }
    bool equals(const OptExprOp* other) const override;

    Node* get_limit_count() const { return limit_count_; }
    Node* get_limit_offset() const { return limit_offset_; }
    void set_limit_count(Node* c) { limit_count_ = c; }
    void set_limit_offset(Node* o) { limit_offset_ = o; }

private:
    Node* limit_count_;
    Node* limit_offset_;
};

}  // namespace cascades
