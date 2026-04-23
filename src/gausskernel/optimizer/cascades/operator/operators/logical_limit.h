#pragma once
#include "../operator.h"
#include "nodes/nodes.h"

namespace cascades {

class LogicalLimit : public LogicalOptExprOp {
public:
    LogicalLimit() : LogicalOptExprOp(OptExprOpType::LOGICAL_LIMIT), limit_count_(-1), limit_offset_(0) {}

    OptExprOp* clone() const override { return new LogicalLimit(*this); }
    size_t hash() const override { return 0; }
    std::string to_string() const override { return "LogicalLimit"; }

    Node* get_limit_count() const { return nullptr; }
    Node* get_limit_offset() const { return nullptr; }
    void set_limit_count(Node* c) { }
    void set_limit_offset(Node* o) { }

private:
    long limit_count_;
    long limit_offset_;
};

}  // namespace cascades
