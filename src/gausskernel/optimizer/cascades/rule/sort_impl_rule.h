#pragma once
#include "rule.h"
#include "nodes/relation.h"

namespace cascades {

class OptExpression;
class OptimizerContext;
class LogicalScan;
class Group;

class SortImplRule : public Rule {
public:
    static SortImplRule* instance();
    SortImplRule();

    int promise() const override { return 4; }
    std::vector<OptExpression*> transform(OptExpression* input, OptimizerContext* context) override;

private:
    LogicalScan* find_base_scan_with_sort_check(OptExpression* expr, void* sort_clause) const;
    LogicalScan* find_logical_scan(OptExpression* expr) const;
    Group* find_group_for_scan(LogicalScan* scan, OptimizerContext* context) const;
    bool sort_matches_group_keys(void* sort_clause, const std::vector<int>& group_keys) const;
    bool can_use_index_for_sort(OptExpression* scan_expr, void* sort_clause, OptimizerContext* context) const;
    bool find_index_for_sort(OptExpression* scan_expr, void* sort_clause, OptimizerContext* context,
                             IndexOptInfo** out_index, List** out_pathkeys) const;
    bool find_index_for_sort_on_join_outer(OptExpression* join_expr, void* sort_clause,
                                           OptimizerContext* context, IndexOptInfo** out_index,
                                           List** out_pathkeys) const;
    void set_sort_requirement_on_scan(OptExpression* expr, void* sort_clause) const;
    OptExpression* clone_expression_tree(OptExpression* expr, void* sort_clause) const;
};

}  // namespace cascades