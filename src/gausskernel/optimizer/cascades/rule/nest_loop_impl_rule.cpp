#include "../base/memory_utils.h"
#include "join_helper.h"
#include "nest_loop_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/cost.h"
#include "nodes/relation.h"
#include <limits>

namespace cascades {

// Static singleton instance
NestLoopImplRule* NestLoopImplRule::instance() {
    static NestLoopImplRule inst;
    return &inst;
}

NestLoopImplRule::NestLoopImplRule()
    : Rule(RuleType::IMP_NESTLOOP_JOIN, Pattern::create(OptExprOpType::LOGICAL_JOIN, Pattern::any(), Pattern::any())) {}

/*
 * select_outer_scan_path: 为 nestloop outer table 选择非参数化扫描路径。
 *
 * Outer table 不能依赖外部关联条件，必须使用独立的扫描路径（SeqScan）。
 * 如果 group 中没有 SeqScan，选择成本最低的非参数化扫描。
 */
static Path* select_outer_scan_path(Group* group)
{
    const auto& phys_exprs = group->get_physical_expressions();
    if (phys_exprs.empty()) {
        return nullptr;
    }

    PhysicalPropertySet required_prop;
    Path* best_path = nullptr;
    double best_cost = std::numeric_limits<double>::max();

    for (GroupExpression* expr : phys_exprs) {
        if (expr->is_unused()) {
            continue;
        }

        OptExprOp* op = expr->get_op();
        if (!op->is_physical()) {
            continue;
        }

        PhysicalOptExprOp* phys_op = static_cast<PhysicalOptExprOp*>(op);
        Path* path = phys_op->get_path();
        if (path == nullptr) {
            continue;
        }

        // 跳过参数化路径（param_info != NULL 表示依赖外部条件）
        if (path->param_info != nullptr) {
            continue;
        }

        // 优先选择 SeqScan（T_SeqScan）
        if (path->pathtype == T_SeqScan) {
            return path;
        }

        // 如果没有 SeqScan，选择成本最低的非参数化路径
        double cost = path->total_cost;
        if (cost < best_cost) {
            best_cost = cost;
            best_path = path;
        }
    }

    // Fallback: 如果没有找到非参数化路径，使用 best expression
    if (best_path == nullptr) {
        GroupExpression* best_expr = group->get_best_expression(required_prop);
        if (best_expr != nullptr && best_expr->get_op()->is_physical()) {
            best_path = static_cast<PhysicalOptExprOp*>(best_expr->get_op())->get_path();
        }
    }

    return best_path;
}

/*
 * select_inner_path: 为 nestloop inner table 选择路径。
 *
 * Inner table 可以使用参数化 index scan（依赖 outer 表的 join key）。
 * 直接从 RelOptInfo::pathlist 获取路径，包括参数化路径。
 */
static Path* select_inner_path(RelOptInfo* rel, Relids outer_relids)
{
    if (rel == nullptr || rel->pathlist == NIL) {
        return nullptr;
    }

    Path* best_path = nullptr;
    double best_cost = std::numeric_limits<double>::max();

    ListCell* lc = nullptr;
    foreach (lc, rel->pathlist) {
        Path* path = (Path*)lfirst(lc);
        if (path == nullptr) {
            continue;
        }

        // 参数化路径必须依赖 outer relids 才是有效的
        if (path->param_info != nullptr) {
            Relids req_outer = path->param_info->ppi_req_outer;
            if (!bms_overlap(req_outer, outer_relids)) {
                continue;
            }
        }

        double cost = path->total_cost;
        if (cost < best_cost) {
            best_cost = cost;
            best_path = path;
        }
    }

    return best_path;
}

std::vector<OptExpression*> NestLoopImplRule::transform(
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

    // Outer path: 选择非参数化扫描路径（SeqScan 优先）
    Path* left_path = select_outer_scan_path(left_group);
    Path* right_path = nullptr;

    if (left_path != nullptr) {
        // Get left_rel before inner path selection
        RelOptInfo* left_rel_for_inner = left_path->parent;
        RelOptInfo* right_rel = nullptr;

        if (!right_group->get_physical_expressions().empty()) {
            GroupExpression* first_phys = right_group->get_physical_expressions().front();
            if (first_phys->get_op()->is_physical()) {
                Path* sample_path = static_cast<PhysicalOptExprOp*>(first_phys->get_op())->get_path();
                if (sample_path != nullptr && sample_path->parent != nullptr) {
                    right_rel = sample_path->parent;
                }
            }
        }

        if (right_rel != nullptr) {
            right_path = select_inner_path(right_rel, left_rel_for_inner->relids);
        }
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

    SpecialJoinInfo sjinfo;
    init_sjinfo_for_join(&sjinfo, left_rel, right_rel, join_op->get_join_type());

    JoinPathExtraData extra;
    init_extra_for_join(&extra, &sjinfo);

    JoinCostWorkspace cost_workspace;
    init_cost_workspace(&cost_workspace);
    initial_cost_nestloop(root, &cost_workspace, join_op->get_join_type(),
        left_path, right_path, &extra, 1);

    RelOptInfo* joinrel = build_join_rel(root, joinrelids, left_rel, right_rel, &sjinfo, nullptr);

    NestPath* nestloop_path = create_nestloop_path(
        root, joinrel, join_op->get_join_type(),
        &cost_workspace, &extra, left_path, right_path,
        NIL, nullptr, nullptr, 1);

    PhysicalNestLoop* nestloop_join = NEW_EX(PhysicalNestLoop,
        (Path*)nestloop_path, left_path, right_path);

    OptExpression* new_expr = OptExpression::create(nestloop_join, left_child, right_child);
    return {new_expr};
}

}  // namespace cascades
