#include "../base/memory_utils.h"
#include "merge_join_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"
#include "nodes/relation.h"

namespace cascades {

// Static singleton instance
MergeJoinImplRule* MergeJoinImplRule::instance() {
    static MergeJoinImplRule inst;
    return &inst;
}

MergeJoinImplRule::MergeJoinImplRule()
    : Rule(RuleType::IMP_MERGE_JOIN, Pattern::create(OptExprOpType::LOGICAL_JOIN, Pattern::any(), Pattern::any())) {}

std::vector<OptExpression*> MergeJoinImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    // Pattern guarantees: op_type == LOGICAL_JOIN && arity == 2
    LogicalJoin* join_op = static_cast<LogicalJoin*>(input->get_op());

    // Get child expressions
    OptExpression* left_child = input->input_at(0);
    OptExpression* right_child = input->input_at(1);

    // Get child GroupExpressions
    GroupExpression* left_group_expr = left_child->get_group_expression();
    GroupExpression* right_group_expr = right_child->get_group_expression();
    if (left_group_expr == nullptr || right_group_expr == nullptr) {
        return {};
    }

    // Get child groups
    Group* left_group = left_group_expr->get_group();
    Group* right_group = right_group_expr->get_group();
    if (left_group == nullptr || right_group == nullptr) {
        return {};
    }

    PhysicalPropertySet required_prop;

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

    if (left_path == nullptr || right_path == nullptr) {
        return {};
    }

    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        return {};
    }

    RelOptInfo* left_rel = left_path->parent;
    RelOptInfo* right_rel = right_path->parent;

    Relids joinrelids = bms_union(left_rel->relids, right_rel->relids);
    RelOptInfo* joinrel = build_join_rel(root, joinrelids, left_rel, right_rel, nullptr, nullptr);

    MergePath* merge_path = create_mergejoin_path(
        root, joinrel, join_op->get_join_type(),
        nullptr, nullptr, left_path, right_path,
        NIL, NIL, nullptr, NIL, NIL, NIL);

    PhysicalMergeJoin* merge_join = NEW_EX(PhysicalMergeJoin,
        (Path*)merge_path, left_path, right_path);
    merge_join->set_build_keys(join_op->get_join_keys());
    merge_join->set_probe_keys(join_op->get_join_keys());

    OptExpression* new_expr = OptExpression::create(merge_join, left_child, right_child);
    return {new_expr};
}

}  // namespace cascades
