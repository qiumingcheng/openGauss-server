#pragma once
#include "../operator.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"

namespace cascades {

/**
 * LogicalFilter - Logical filter operator
 *
 * Uses qual_list (List* of RestrictInfo) for predicate expressions
 */
class LogicalFilter : public LogicalOptExprOp {
public:
    LogicalFilter() : LogicalOptExprOp(OptExprOpType::LOGICAL_FILTER), qual_list_(nullptr) {}

    OptExprOp* clone() const override { return new LogicalFilter(*this); }
    size_t hash() const override { return 0; }
    std::string to_string() const override { return "LogicalFilter"; }

    // Qual list for expressions (List* of RestrictInfo)
    List* get_qual_list() const { return qual_list_; }
    void set_qual_list(List* list) { qual_list_ = list; }

private:
    List* qual_list_ = nullptr;      // List* of qualifier clauses (RestrictInfo*)
};

}  // namespace cascades
