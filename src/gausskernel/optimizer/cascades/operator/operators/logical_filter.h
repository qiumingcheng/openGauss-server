#pragma once
#include "../operator.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"

namespace cascades {

/**
 * LogicalFilter - Logical filter operator supporting multiple predicates
 *
 * Uses qual_list (List* of RestrictInfo) for complex predicate expressions
 * and predicate_ids for tracking individual predicates during optimization
 */
class LogicalFilter : public LogicalOptExprOp {
public:
    LogicalFilter() : LogicalOptExprOp(OptExprOpType::LOGICAL_FILTER), predicate_ids_(nullptr), qual_list_(nullptr) {}

    OptExprOp* clone() const override { return new LogicalFilter(*this); }
    size_t hash() const override { return 0; }
    std::string to_string() const override { return "LogicalFilter"; }

    // Multiple predicate support (primary API)
    List* get_predicate_ids() const { return predicate_ids_; }
    void set_predicate_ids(List* ids) { predicate_ids_ = ids; }

    // Qual list for complex expressions (List* of RestrictInfo)
    void* get_qual_list() const { return qual_list_; }
    void set_qual_list(void* list) { qual_list_ = list; }

    // Single predicate compatibility API (wrappers for multi-predicate API)
    // These are convenience methods for simple single-predicate cases
    int get_predicate_id() const {
        if (predicate_ids_ == nullptr || list_length(predicate_ids_) == 0) {
            return -1;  // No predicate
        }
        return lfirst_int(list_head(predicate_ids_));  // Return first predicate ID
    }
    void set_predicate_id(int id) {
        predicate_ids_ = list_make1_int(id);  // Single element list
    }

    // Check if filter has predicates
    bool has_predicates() const { return predicate_ids_ != nullptr || qual_list_ != nullptr; }
    bool has_predicate() const { return has_predicates(); }  // Compatibility alias

private:
    List* predicate_ids_ = nullptr;  // Support for multiple predicate IDs
    void* qual_list_ = nullptr;      // List* of qualifier clauses for complex expressions
};

}  // namespace cascades
