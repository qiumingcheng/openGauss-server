#pragma once
#include "rule.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/relation.h"
#include "nodes/nodes.h"

namespace cascades {

class IndexScanImplRule : public Rule {
public:
    static IndexScanImplRule* instance();
    IndexScanImplRule();

    int promise() const override { return 3; }

    std::vector<OptExpression*> transform(
        OptExpression* input, OptimizerContext* context) override;

private:
    bool has_indexes(RelOptInfo* rel) const;
    bool can_use_index(RelOptInfo* rel, int predicate_id) const;
    List* get_available_indexes(RelOptInfo* rel) const;
    bool expr_in_ec(EquivalenceClass* ec, Node* expr) const;
    bool pathkeys_match_sort_clause(List* pathkeys, void* sort_clause, PlannerInfo* root) const;
};

}  // namespace cascades