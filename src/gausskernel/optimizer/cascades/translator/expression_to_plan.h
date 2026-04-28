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

    // Helper functions for scan translation
    void init_scan_plan(Scan* scan_plan, RelOptInfo* rel, List* tlist);
    Plan* translate_index_scan(IndexPath* index_path, RelOptInfo* rel, List* tlist);
    Plan* translate_bitmap_scan(BitmapHeapPath* bitmap_path, RelOptInfo* rel, List* tlist);
    Plan* translate_seq_scan(RelOptInfo* rel, List* tlist);

    // Var reference fixing (统一转换)
    void fix_var_references(Plan* plan, PlannerInfo* root);
    void fix_agg_var_references(Agg* agg, PlannerInfo* root);
    void fix_sort_var_references(Sort* sort, PlannerInfo* root);
    void fix_result_var_references(Plan* plan, PlannerInfo* root);
    void fix_generic_var_references(Plan* plan, PlannerInfo* root);
    AttrNumber map_varattno_to_output_pos(AttrNumber varattno, List* child_tlist);
};

}