#pragma once
#include "../operator.h"
#include "nodes/pg_list.h"

namespace cascades {

/**
 * PhysicalFilter - Physical filter operator for applying runtime predicates
 *
 * Supports both single predicate (predicate_id_) and multi-predicate (predicate_ids_, qual_list_)
 */
class PhysicalFilter : public PhysicalOptExprOp {
public:
    PhysicalFilter() : PhysicalOptExprOp(OptExprOpType::PHYSICAL_FILTER),
                       predicate_id_(-1), predicate_ids_(nullptr), qual_list_(nullptr) {}
    explicit PhysicalFilter(Path* path) : PhysicalOptExprOp(OptExprOpType::PHYSICAL_FILTER, path),
                                          predicate_id_(-1), predicate_ids_(nullptr), qual_list_(nullptr) {}

    OptExprOp* clone() const override {
        PhysicalFilter* cloned = new PhysicalFilter(*this);
        cloned->predicate_ids_ = predicate_ids_;  // Shallow copy
        cloned->qual_list_ = qual_list_;
        return cloned;
    }
    size_t hash() const override { return 0; }
    std::string to_string() const override { return "PhysicalFilter"; }

    // Single predicate API (for simple cases)
    int get_predicate_id() const { return predicate_id_; }
    void set_predicate_id(int id) { predicate_id_ = id; }

    // Multi-predicate API (for complex cases with multiple predicates)
    List* get_predicate_ids() const { return predicate_ids_; }
    void set_predicate_ids(List* ids) { predicate_ids_ = ids; }

    // Qual list for complex expressions (List* of RestrictInfo)
    void* get_qual_list() const { return qual_list_; }
    void set_qual_list(void* list) { qual_list_ = list; }

    // Check if filter has predicates
    bool has_predicates() const { return predicate_id_ >= 0 || predicate_ids_ != nullptr || qual_list_ != nullptr; }

private:
    int predicate_id_;           // Single predicate ID (for simple cases)
    List* predicate_ids_;        // Multiple predicate IDs (for complex cases)
    void* qual_list_;            // List* of RestrictInfo for complex expressions
};

}  // namespace cascades
