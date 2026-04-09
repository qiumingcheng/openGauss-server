#include "nodes/parsenodes.h"
#include "hash_join_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../optimizer/optimizer_context.h"
#include "optimizer/pathnode.h"
#include "nodes/relation.h"
#include "nodes/makefuncs.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "catalog/pg_operator.h"

namespace cascades {

HashJoinImplRule::HashJoinImplRule()
    : Rule(RuleType::IMP_HASH_JOIN, Pattern::create(OptExprOpType::LOGICAL_JOIN)) {}

std::vector<OptExpression*> HashJoinImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    LogicalJoin* join_op = static_cast<LogicalJoin*>(input->get_op());

    // Get child expressions
    OptExpression* left_child = input->input_at(0);
    OptExpression* right_child = input->input_at(1);

    // Get child GroupExpressions
    GroupExpression* left_group_expr = left_child->get_group_expression();
    GroupExpression* right_group_expr = right_child->get_group_expression();
    if (left_group_expr == nullptr || right_group_expr == nullptr) {
        return {};  // Cannot transform without group expressions
    }

    // Get child groups
    Group* left_group = left_group_expr->get_group();
    Group* right_group = right_group_expr->get_group();
    if (left_group == nullptr || right_group == nullptr) {
        return {};
    }

    // Get required property - use default empty property since Rule doesn't have TaskContext
    PhysicalPropertySet required_prop;

    // Get best physical paths from child groups
    Path* left_path = nullptr;
    Path* right_path = nullptr;

    GroupExpression* left_best = left_group->get_best_expression(required_prop);
    GroupExpression* right_best = right_group->get_best_expression(required_prop);

    if (left_best != nullptr && left_best->get_op()->is_physical()) {
        left_path = static_cast<PhysicalOptExprOp*>(left_best->get_op())->get_path();
    }
    if (right_best != nullptr && right_best->get_op()->is_physical()) {
        right_path = static_cast<PhysicalOptExprOp*>(right_best->get_op())->get_path();
    }

    // If no best path yet, get the first available physical expression's path
    if (left_path == nullptr) {
        auto& phys_exprs = left_group->get_physical_expressions();
        if (!phys_exprs.empty()) {
            GroupExpression* first_phys = phys_exprs.front();
            if (first_phys->get_op()->is_physical()) {
                left_path = static_cast<PhysicalOptExprOp*>(first_phys->get_op())->get_path();
            }
        }
    }
    if (right_path == nullptr) {
        auto& phys_exprs = right_group->get_physical_expressions();
        if (!phys_exprs.empty()) {
            GroupExpression* first_phys = phys_exprs.front();
            if (first_phys->get_op()->is_physical()) {
                right_path = static_cast<PhysicalOptExprOp*>(first_phys->get_op())->get_path();
            }
        }
    }

    if (left_path == nullptr || right_path == nullptr) {
        return {};  // Cannot create join without child paths
    }

    // Get PlannerInfo and create joinrel
    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        return {};  // Cannot create paths without PlannerInfo
    }

    RelOptInfo* left_rel = left_path->parent;
    RelOptInfo* right_rel = right_path->parent;

    // Build joinrelids from left and right relids
    Relids joinrelids = bms_union(left_rel->relids, right_rel->relids);

    // Build joinrel from left and right using OpenGauss function
    RelOptInfo* joinrel = build_join_rel(root, joinrelids, left_rel, right_rel, nullptr, nullptr);

    // Create hashclauses from join keys
    List* hashclauses = NIL;

    const std::vector<int>& join_keys = join_op->get_join_keys();
    if (!join_keys.empty()) {
        int left_relid = -1;
        int right_relid = -1;
        (void)bms_get_singleton_member(left_rel->relids, &left_relid);
        (void)bms_get_singleton_member(right_rel->relids, &right_relid);

        if (left_relid > 0 && right_relid > 0) {
            for (int key_col : join_keys) {
                Var* left_var = makeVar((Index)left_relid, (AttrNumber)key_col, InvalidOid, -1, InvalidOid, 0);
                Var* right_var = makeVar((Index)right_relid, (AttrNumber)key_col, InvalidOid, -1, InvalidOid, 0);

                OpExpr* eq_clause = (OpExpr*)make_opclause(
                    INT4EQOID, BOOLOID, false,
                    (Expr*)left_var, (Expr*)right_var, InvalidOid, InvalidOid);

                set_opfuncid(eq_clause);

                RestrictInfo* restrictinfo = make_restrictinfo(
                    (Expr*)eq_clause, false, false, false, 0,
                    joinrelids, NULL, NULL, false);

                restrictinfo->can_join = true;
                restrictinfo->left_relids = bms_make_singleton(left_relid);
                restrictinfo->right_relids = bms_make_singleton(right_relid);
                restrictinfo->clause_relids = bms_union(restrictinfo->left_relids, restrictinfo->right_relids);
                restrictinfo->hashjoinoperator = eq_clause->opno;

                hashclauses = lappend(hashclauses, restrictinfo);
            }
        }
    }

    HashPath* hash_path = create_hashjoin_path(
        root, joinrel, join_op->get_join_type(),
        nullptr, nullptr, left_path, right_path,
        NIL, nullptr, hashclauses, 1);

    PhysicalHashJoin* hash_join = new PhysicalHashJoin(
        (Path*)hash_path, left_path, right_path);
    hash_join->set_build_keys(join_op->get_join_keys());
    hash_join->set_probe_keys(join_op->get_join_keys());

    OptExpression* new_expr = OptExpression::create(hash_join, left_child, right_child);
    return {new_expr};
}

}  // namespace cascades
