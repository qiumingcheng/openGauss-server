#pragma once
#include "../operator.h"
#include "nodes/nodes.h"

namespace cascades {

class LogicalSort : public LogicalOptExprOp {
public:
    LogicalSort() : LogicalOptExprOp(OptExprOpType::LOGICAL_SORT), sort_clause_(nullptr) {}

    OptExprOp* clone() const override { return new LogicalSort(*this); }
    size_t hash() const override { return 0; }
    std::string to_string() const override { return "LogicalSort"; }

    void* get_sort_clause() const { return sort_clause_; }
    void set_sort_clause(void* clause) { sort_clause_ = clause; }

private:
    void* sort_clause_;
};

}  // namespace cascades
