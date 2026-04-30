#pragma once
#include "../operator.h"
#include "nodes/relation.h"
#include "nodes/parsenodes.h"

namespace cascades {

/**
 * LogicalSubqueryScan - Logical subquery scan operator
 *
 * Represents scanning a subquery in FROM clause.
 * The subquery is planned independently and its output
 * becomes the input to this scan operator.
 */
class LogicalSubqueryScan : public LogicalOptExprOp {
public:
    explicit LogicalSubqueryScan(int rtindex);
    LogicalSubqueryScan(int rtindex, Query* subquery);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
    bool equals(const OptExprOp* other) const override;

    // Subquery access
    Query* get_subquery() const { return subquery_; }
    void set_subquery(Query* subquery) { subquery_ = subquery; }

    // Range table index
    int get_rtindex() const { return rtindex_; }

    // RelOptInfo access (set during planning)
    RelOptInfo* get_rel() const { return rel_; }
    void set_rel(RelOptInfo* rel) { rel_ = rel; }

private:
    int rtindex_;                    // Range table index
    Query* subquery_;                // The subquery to scan
    RelOptInfo* rel_ = nullptr;      // RelOptInfo for the subquery RTE
};

}  // namespace cascades
