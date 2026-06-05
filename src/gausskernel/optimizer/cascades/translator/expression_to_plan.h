#pragma once
#include <stddef.h>
#include "nodes/plannodes.h"
#include "nodes/relation.h"

namespace cascades {

class OptExpression;

class ExprToPlanConverter {
public:
    ExprToPlanConverter();
    explicit ExprToPlanConverter(PlannerInfo* root);
    ~ExprToPlanConverter() = default;

    PlannedStmt* convert(OptExpression* expr);

private:
    PlannerInfo* root_;

    Plan* translate_expression(OptExpression* expr);
    Plan* translate_scan(OptExpression* expr);
    Plan* translate_project(OptExpression* expr);
    Plan* translate_filter(OptExpression* expr);
    Plan* translate_join(OptExpression* expr);
    Plan* translate_hash_join(OptExpression* expr);
    Plan* translate_merge_join(OptExpression* expr);
    Plan* translate_nestloop_join(OptExpression* expr);
    Plan* translate_hash_agg(OptExpression* expr);
    Plan* translate_sort(OptExpression* expr);
    Plan* translate_material(OptExpression* expr);
    Plan* translate_exchange(OptExpression* expr);
    Plan* translate_default(OptExpression* expr);
    Plan* translate_limit(OptExpression* expr);
    Plan* translate_subquery_scan(OptExpression* expr);

    Path* get_or_create_path(OptExpression* expr);

    // Helper functions for scan translation
    void init_scan_plan(Scan* scan_plan, RelOptInfo* rel, List* tlist);
    Plan* translate_index_scan(IndexPath* index_path, RelOptInfo* rel, List* tlist);
    Plan* translate_bitmap_scan(BitmapHeapPath* bitmap_path, RelOptInfo* rel, List* tlist);
    Plan* translate_seq_scan(RelOptInfo* rel, List* tlist);
};

}