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

    // Pattern guarantees: op_type == LOGICAL_AGGREGATE && arity == 1
    LogicalAggregate* agg_op = static_cast<LogicalAggregate*>(input->get_op());
    const std::vector<int>& group_keys = agg_op->get_group_keys();

    // Get child expression
    OptExpression* child = input->input_at(0);

    // Get child GroupExpression
    GroupExpression* child_group_expr = child->get_group_expression();
    if (child_group_expr == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, child_group_expr is null")));
        return {};
    }

    // Get child group
    Group* child_group = child_group_expr->get_group();
    if (child_group == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, child_group is null")));
        return {};
    }

    PhysicalPropertySet required_prop;

    Path* child_path = nullptr;

    GroupExpression* child_best = child_group->get_best_expression(required_prop);
    if (child_best != nullptr && child_best->get_op()->is_physical()) {
        child_path = static_cast<PhysicalOptExprOp*>(child_best->get_op())->get_path();
    }

    if (child_path == nullptr) {
        auto& phys_exprs = child_group->get_physical_expressions();
        if (!phys_exprs.empty()) {
            GroupExpression* first_phys = phys_exprs.front();
            if (first_phys->get_op()->is_physical()) {
                child_path = static_cast<PhysicalOptExprOp*>(first_phys->get_op())->get_path();
            }
        }
    }

    if (child_path == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, child_path is null")));
        return {};
    }

    ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, child_path found, startup_cost=%.2f",
                         child_path->startup_cost)));

    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, PlannerInfo is null")));
        return {};
    }

    RelOptInfo* child_rel = child_path->parent;

    List* groupClause = NIL;

    if (!group_keys.empty()) {
        int child_relid = -1;
        (void)bms_get_singleton_member(child_rel->relids, &child_relid);

        if (child_relid > 0) {
            ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, creating groupClause with %d keys",
                                 (int)group_keys.size())));
            for (int key_col : group_keys) {
                SortGroupClause* grpcl = makeNode(SortGroupClause);
                grpcl->tleSortGroupRef = key_col;
                grpcl->eqop = INT4EQOID;
                grpcl->sortop = INT4LTOID;
                grpcl->nulls_first = false;
                grpcl->hashable = true;

                groupClause = lappend(groupClause, grpcl);
            }
        }
    }

    // Create HashPath for hash aggregate
    HashPath* hash_path = makeNode(HashPath);
    hash_path->jpath.path.parent = child_rel;
    hash_path->jpath.path.pathtarget = child_path->pathtarget;
    hash_path->jpath.path.param_info = child_path->param_info;
    hash_path->jpath.path.rows = child_rel->rows;
    hash_path->jpath.path.pathkeys = NIL;
    hash_path->path_hashclauses = NIL;
    hash_path->num_batches = 1;

    PhysicalHashAgg* hash_agg = NEW_EX(PhysicalHashAgg, (Path*)hash_path, child_path, group_keys);

    OptExpression* new_expr = OptExpression::create(hash_agg);
    ereport(WARNING, (errmsg("[CASCADES] HashAggImplRule:transform, created hash agg expr")));
    return {new_expr};
}

}  // namespace cascades
