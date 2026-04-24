#pragma once
#include "../operator.h"
#include "nodes/pg_list.h"

namespace cascades {

/**
 * PhysicalFilter - Physical filter operator for applying runtime predicates
 *
 * Uses a single qual_list_ field to store predicates:
 * - Single predicate: qual_list_ contains one RestrictInfo
 * - Multiple predicates: qual_list_ contains multiple RestrictInfo entries
 */
class PhysicalFilter : public PhysicalOptExprOp {
public:
    PhysicalFilter() : PhysicalOptExprOp(OptExprOpType::PHYSICAL_FILTER),
                       qual_list_(nullptr) {}
    explicit PhysicalFilter(Path* path) : PhysicalOptExprOp(OptExprOpType::PHYSICAL_FILTER, path),
                                          qual_list_(nullptr) {}

    OptExprOp* clone() const override {
        PhysicalFilter* cloned = new PhysicalFilter(*this);
        cloned->qual_list_ = qual_list_;  // Shallow copy - qual_list_ points to same RestrictInfo list
        return cloned;
    }

    /**
     * Hash function - uses qual_list_ pointer for uniqueness
     * Note: Returns 0 if qual_list_ is nullptr or not set
     */
    size_t hash() const override {
        if (qual_list_ == nullptr) {
            return 0;
        }
        return reinterpret_cast<size_t>(qual_list_);
    }

    std::string to_string() const override { return "PhysicalFilter"; }

    /**
     * Get the qual list (List* of RestrictInfo)
     * @return The qual list, or nullptr if not set
     */
    void* get_qual_list() const { return qual_list_; }

    /**
     * Set the qual list
     * @param list List* of RestrictInfo for predicates
     */
    void set_qual_list(void* list) { qual_list_ = list; }

    /**
     * Check if filter has predicates
     * @return true if qual_list_ is not nullptr and not empty
     */
    bool has_predicates() const {
        if (qual_list_ == nullptr) {
            return false;
        }
        return list_length((List*)qual_list_) > 0;
    }

private:
    void* qual_list_;            // List* of RestrictInfo for predicates (single or multiple)
};

}  // namespace cascades
