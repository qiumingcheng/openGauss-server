#pragma once
#include "../operator.h"

namespace cascades {

class PhysicalFilter : public PhysicalOptExprOp {
public:
    PhysicalFilter() : PhysicalOptExprOp(OptExprOpType::PHYSICAL_FILTER), predicate_id_(-1) {}
    explicit PhysicalFilter(Path* path) : PhysicalOptExprOp(OptExprOpType::PHYSICAL_FILTER, path), predicate_id_(-1) {}

    OptExprOp* clone() const override { return new PhysicalFilter(*this); }
    size_t hash() const override { return 0; }
    std::string to_string() const override { return "PhysicalFilter"; }

    int get_predicate_id() const { return predicate_id_; }
    void set_predicate_id(int id) { predicate_id_ = id; }

    void* get_predicate_ids() const { return nullptr; }

private:
    int predicate_id_;
};

}  // namespace cascades
