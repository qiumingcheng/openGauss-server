#pragma once
#include "rule.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/relation.h"
#include "nodes/nodes.h"

namespace cascades {

/**
 * IndexScanImplRule - Creates index scan implementations for table scans
 *
 * This rule transforms a LOGICAL_SCAN into multiple PHYSICAL_SCAN alternatives:
 * 1. Sequential scan (if no suitable index or index scan is more expensive)
 * 2. Index scan (B-tree, hash, gist, gin, etc.)
 * 3. Bitmap scan (for low selectivity predicates)
 *
 * The optimizer will cost each alternative and choose the best one.
 *
 * Reference: OpenGauss's create_index_paths() in indxpath.cpp
 */
class IndexScanImplRule : public Rule {
public:
    IndexScanImplRule();

    int promise() const override { return 3; }  // High priority, like ScanImplRule

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

private:
    /**
     * Check if there are any indexes on this relation
     */
    bool has_indexes(RelOptInfo* rel) const;

    /**
     * Check if a predicate can use an index
     */
    bool can_use_index(RelOptInfo* rel, int predicate_id) const;

    /**
     * Get the list of available indexes for a relation
     */
    List* get_available_indexes(RelOptInfo* rel) const;
};

}
