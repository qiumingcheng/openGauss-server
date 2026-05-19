#include "nodes/parsenodes.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "../base/memory_utils.h"
#include "hash_agg_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../optimizer/optimizer_context.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "catalog/pg_operator.h"
#include "nodes/makefuncs.h"

namespace cascades {

// Static singleton instance
HashAggImplRule* HashAggImplRule::instance() {
    static HashAggImplRule inst;
    return &inst;
}

HashAggImplRule::HashAggImplRule()
    : Rule(RuleType::IMP_HASH_AGG, Pattern::create(OptExprOpType::LOGICAL_AGGREGATE, Pattern::any())) {}

std::vector<OptExpression*> HashAggImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, input=%s",
                         input->to_string().c_str())));

    LogicalAggregate* agg_op = static_cast<LogicalAggregate*>(input->get_op());
    const std::vector<int>& group_keys = agg_op->get_group_keys();

    // Create physical operator with group keys
    PhysicalHashAgg* hash_agg = NEW_EX(PhysicalHashAgg, (Path*)nullptr, group_keys);

    // Copy HAVING clause if present
    if (agg_op->has_having_qual()) {
        hash_agg->set_having_qual(agg_op->get_having_qual());
        ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, copying having_qual")));
    }

    // Extract grpOperators from parse->groupClause
    PlannerInfo* root = context->get_root();
    if (root != nullptr && root->parse != nullptr && root->parse->groupClause != nullptr) {
        Oid* grp_ops = extract_grouping_ops(root->parse->groupClause);
        hash_agg->set_grp_operators(grp_ops);
        int num_keys = list_length(root->parse->groupClause);
        ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, extracted grpOperators, num_keys=%d, first_op=%u",
                             num_keys, grp_ops[0])));
    } else {
        ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, groupClause is null or empty")));
    }

    // Pass target list to physical operator
    hash_agg->set_target_list(agg_op->get_target_list());

    OptExpression* new_expr = OptExpression::create_with_childrens(hash_agg, input->get_inputs());

    ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, created PhysicalHashAgg")));
    return {new_expr};
}

}  // namespace cascades
