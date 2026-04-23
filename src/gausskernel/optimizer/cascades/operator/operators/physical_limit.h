#pragma once
#include "../operator.h"

namespace cascades {

class PhysicalLimit : public PhysicalOptExprOp {
public:
    PhysicalLimit() : PhysicalOptExprOp(OptExprOpType::PHYSICAL_LIMIT), limit_count_(-1), limit_offset_(0) {}
    explicit PhysicalLimit(Path* path) : PhysicalOptExprOp(OptExprOpType::PHYSICAL_LIMIT, path), limit_count_(-1), limit_offset_(0) {}

    OptExprOp* clone() const override { return new PhysicalLimit(*this); }
    size_t hash() const override { return 0; }
    std::string to_string() const override { return "PhysicalLimit"; }

    long get_limit_count() const { return limit_count_; }
    long get_limit_offset() const { return limit_offset_; }

private:
    long limit_count_;
    long limit_offset_;
};

}  // namespace cascades
