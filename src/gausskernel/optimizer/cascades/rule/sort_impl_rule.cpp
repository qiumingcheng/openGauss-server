#include "sort_impl_rule.h"
#include "../operator/operators.h"
#include "../operator/operators/physical_sort.h"
#include "../operator/operators/physical_passthrough.h"
#include "../operator/operators/physical_scan.h"
#include "../memo/opt_expression.h"
#include "../memo/group_expression.h"
#include "../memo/group.h"
#include "../memo/memo.h"
#include "../optimizer/optimizer_context.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/tlist.h"
#include "nodes/relation.h"
#include "postgres.h"
#include "nodes/makefuncs.h"

namespace cascades {

SortImplRule* SortImplRule::instance() {
    static SortImplRule inst;
    return &inst;
}

SortImplRule::SortImplRule()
    : Rule(RuleType::IMP_SORT,
           Pattern::create(OptExprOpType::LOGICAL_SORT,
                          Pattern::any())) {}

bool SortImplRule::sort_matches_group_keys(
    void* sort_clause, const std::vector<int>& group_keys) const
{
    if (sort_clause == nullptr) {
        return false;
    }

    List* clauses = static_cast<List*>(sort_clause);
    int sort_key_count = list_length(clauses);

    // Must have same number of keys
    if (sort_key_count != (int)group_keys.size()) {
        return false;
    }

    return true;
}

LogicalScan* SortImplRule::find_base_scan_with_sort_check(
    OptExpression* expr, void* sort_clause) const
{
    if (expr == nullptr) {
        return nullptr;
    }

    OptExprOp* op = expr->get_op();
    if (op == nullptr || op->is_physical()) {
        return nullptr;
    }

    OptExprOpType op_type = op->get_type();

    if (op_type == OptExprOpType::LOGICAL_SCAN) {
        return static_cast<LogicalScan*>(op);
    }

    if (op_type == OptExprOpType::LOGICAL_AGGREGATE) {
        LogicalAggregate* aggr = static_cast<LogicalAggregate*>(op);
        const std::vector<int>& group_keys = aggr->get_group_keys();

        if (sort_matches_group_keys(sort_clause, group_keys)) {
            if (expr->arity() > 0) {
                return find_base_scan_with_sort_check(expr->input_at(0), sort_clause);
            }
        }
    } else {
        if (expr->arity() > 0) {
          return find_base_scan_with_sort_check(expr->input_at(0), sort_clause);
        }
    }
    return nullptr;
}

LogicalScan* SortImplRule::find_logical_scan(OptExpression* expr) const
{
    return find_base_scan_with_sort_check(expr, nullptr);
}

Group* SortImplRule::find_group_for_scan(LogicalScan* scan, OptimizerContext* context) const
{
    Memo* memo = context->get_memo();
    const auto& groups = memo->get_groups();

    for (Group* group : groups) {
        GroupExpression* first_logical = group->get_first_logical_expression();
        if (first_logical != nullptr) {
            OptExprOp* op = first_logical->get_op();
            if (op != nullptr && op->get_type() == OptExprOpType::LOGICAL_SCAN) {
                LogicalScan* group_scan = static_cast<LogicalScan*>(op);
                if (group_scan == scan) {
                    return group;
                }
            }
        }
    }
    return nullptr;
}

bool SortImplRule::can_use_index_for_sort(OptExpression* scan_expr, void* sort_clause, OptimizerContext* context) const
{
    if (sort_clause == nullptr) {
        return false;
    }

    LogicalScan* scan = find_base_scan_with_sort_check(scan_expr, sort_clause);
    if (scan == nullptr) {
        return false;
    }

    int table_id = scan->get_table_id();

    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        return false;
    }

    RelOptInfo* rel = find_base_rel(root, table_id);
    if (rel == nullptr || rel->indexlist == NIL) {
        return false;
    }

    IndexOptInfo* index = static_cast<IndexOptInfo*>(lfirst(list_head(rel->indexlist)));
    if (index == nullptr || index->sortopfamily == NULL) {
        return false;
    }

    SortGroupClause* first_sort = static_cast<SortGroupClause*>(lfirst(list_head((List*)sort_clause)));
    if (first_sort == nullptr) {
        return false;
    }

    return true;
}

bool SortImplRule::find_index_for_sort(OptExpression* scan_expr, void* sort_clause,
                                       OptimizerContext* context, IndexOptInfo** out_index,
                                       List** out_pathkeys) const
{
    *out_index = nullptr;
    *out_pathkeys = nullptr;

    if (sort_clause == nullptr) {
        return false;
    }

    LogicalScan* scan = find_base_scan_with_sort_check(scan_expr, sort_clause);
    if (scan == nullptr) {
        return false;
    }

    int table_id = scan->get_table_id();

    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        return false;
    }

    RelOptInfo* rel = find_base_rel(root, table_id);
    if (rel == nullptr || rel->indexlist == NIL) {
        return false;
    }

    List* clauses = static_cast<List*>(sort_clause);
    List* targetlist = root->parse->targetList;

    // Find an index that matches the sort clause
    ListCell* lc = nullptr;
    foreach (lc, rel->indexlist) {
        IndexOptInfo* index = static_cast<IndexOptInfo*>(lfirst(lc));
        if (index == nullptr || index->sortopfamily == NULL) {
            continue;
        }

        // Check if this index can satisfy the sort clause
        List* pathkeys = NIL;
        bool match = true;

        ListCell* lc_clause = nullptr;
        int key_idx = 0;
        foreach (lc_clause, clauses) {
            SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc_clause));
            if (key_idx >= index->nkeycolumns) {
                match = false;
                break;
            }

            // Get the sort expression
            TargetEntry* tle = get_sortgroupref_tle(sgcl->tleSortGroupRef, targetlist, false);
            if (tle == nullptr) {
                match = false;
                break;
            }

            Node* sort_expr = (Node*)tle->expr;
            if (sort_expr == nullptr || !IsA(sort_expr, Var)) {
                match = false;
                break;
            }

            Var* var = (Var*)sort_expr;
            int key_attr = index->indexkeys[key_idx];

            // Check if the sort column matches the index key
            if (var->varattno != key_attr) {
                match = false;
                break;
            }

            // Build PathKey for index path creation
            PathKey* pk = makeNode(PathKey);
            pk->pk_eclass = nullptr;
            pk->pk_strategy = (sgcl->sortop < 500) ? BTLessStrategyNumber : BTGreaterStrategyNumber;
            pk->pk_nulls_first = sgcl->nulls_first;
            pk->pk_opfamily = InvalidOid;

            pathkeys = lappend(pathkeys, pk);
            key_idx++;
        }

        if (match && key_idx == index->nkeycolumns) {
            *out_index = index;
            *out_pathkeys = pathkeys;
            return true;
        }
    }

    return false;
}

bool SortImplRule::find_index_for_sort_on_join_outer(OptExpression* join_expr, void* sort_clause,
                                                    OptimizerContext* context, IndexOptInfo** out_index,
                                                    List** out_pathkeys) const
{
    *out_index = nullptr;
    *out_pathkeys = nullptr;

    if (join_expr == nullptr || sort_clause == nullptr) {
        return false;
    }

    OptExprOp* op = join_expr->get_op();
    if (op == nullptr || op->get_type() != OptExprOpType::LOGICAL_JOIN) {
        return false;
    }

    if (join_expr->arity() < 1) {
        return false;
    }

    // Check join type - only non-FULL/RIGHT joins can propagate sort order
    LogicalJoin* join = static_cast<LogicalJoin*>(op);
    JoinType jtype = join->get_join_type();
    if (jtype == JOIN_FULL || jtype == JOIN_RIGHT || jtype == JOIN_RIGHT_ANTI_FULL) {
        ereport(WARNING, (errmsg("[CASCADES] find_index_for_sort_on_join_outer: join type=%d, skipping FULL/RIGHT join", (int)jtype)));
        return false;
    }

    OptExpression* outer_expr = join_expr->input_at(0);
    return find_index_for_sort(outer_expr, sort_clause, context, out_index, out_pathkeys);
}

void SortImplRule::set_sort_requirement_on_scan(OptExpression* expr, void* sort_clause) const
{
    if (expr == nullptr || sort_clause == nullptr) {
        return;
    }

    OptExprOp* op = expr->get_op();
    if (op == nullptr) {
        return;
    }

    OptExprOpType op_type = op->get_type();

    if (op_type == OptExprOpType::LOGICAL_SCAN) {
        LogicalScan* scan = static_cast<LogicalScan*>(op);
        scan->set_sort_clause(sort_clause);
        return;
    }

    // Recursively set on child for filter/project/aggregate/join
    if (op_type == OptExprOpType::LOGICAL_FILTER ||
        op_type == OptExprOpType::LOGICAL_PROJECT ||
        op_type == OptExprOpType::LOGICAL_AGGREGATE) {
        if (expr->arity() > 0) {
            set_sort_requirement_on_scan(expr->input_at(0), sort_clause);
        }
        return;
    }

    if (op_type == OptExprOpType::LOGICAL_JOIN) {
        // Pass sort requirement to outer (left) child
        LogicalJoin* join = static_cast<LogicalJoin*>(op);
        JoinType jtype = join->get_join_type();
        if (jtype != JOIN_FULL && jtype != JOIN_RIGHT && jtype != JOIN_RIGHT_ANTI_FULL) {
            if (expr->arity() > 0) {
                set_sort_requirement_on_scan(expr->input_at(0), sort_clause);
            }
        }
        return;
    }
}

OptExpression* SortImplRule::clone_expression_tree(OptExpression* expr, void* sort_clause) const
{
    if (expr == nullptr) {
        return nullptr;
    }

    // Clone children first
    std::vector<OptExpression*> cloned_children;
    for (int i = 0; i < expr->arity(); ++i) {
        cloned_children.push_back(clone_expression_tree(expr->input_at(i), sort_clause));
    }

    // Clone the operator
    OptExprOp* cloned_op = expr->get_op()->clone();

    // Create new OptExpression with cloned operator and children
    OptExpression* result = OptExpression::create_with_childrens(cloned_op, cloned_children);

    // Set sort requirement on the LogicalScan in the cloned tree
    if (sort_clause != nullptr) {
        set_sort_requirement_on_scan(result, sort_clause);
    }

    return result;
}

PhysicalIndexScan* SortImplRule::create_index_scan_to_group(
    OptExpression* scan_expr,
    void* sort_clause,
    IndexOptInfo* index,
    List* pathkeys,
    OptimizerContext* context) const
{
    // Find the LogicalScan in the expression tree
    LogicalScan* scan = find_base_scan_with_sort_check(scan_expr, sort_clause);
    if (scan == nullptr) {
        return nullptr;
    }

    Group* scan_group = find_group_for_scan(scan, context);
    if (scan_group == nullptr) {
        return nullptr;
    }

    // Determine scan direction from pathkeys
    // BTLessStrategyNumber = ASC, BTGreaterStrategyNumber = DESC
    ScanDirection scan_dir = ForwardScanDirection;
    if (pathkeys != NIL) {
        PathKey* first_pk = (PathKey*)linitial(pathkeys);
        if (first_pk->pk_strategy == BTGreaterStrategyNumber) {
            scan_dir = BackwardScanDirection;
        }
    }

    // Create IndexPath with pathkeys
    PlannerInfo* root = context->get_root();
    bool can_index_only = check_index_only(root, scan->get_rel(), index);
    List* indexclauses = index->rel->baserestrictinfo;
    IndexPath* index_path = create_index_path(root, index,
        indexclauses, NIL, NIL, NIL, pathkeys,
        scan_dir, can_index_only, nullptr, nullptr, 1.0, 1);

    if (index_path == nullptr) {
        return nullptr;
    }

    // Create PhysicalIndexScan with the IndexPath
    PhysicalIndexScan* index_scan = NEW_EX(PhysicalIndexScan, (Path*)index_path);
    index_scan->set_relid(scan->get_relid());
    index_scan->set_table_id(scan->get_table_id());

    // Set multi-column sort info from sort_clause
    List* clauses = static_cast<List*>(sort_clause);
    List* targetlist = root->parse->targetList;
    ListCell* lc = nullptr;
    int scan_table_id = scan->get_table_id();
    foreach (lc, clauses) {
        SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
        bool ascending = (sgcl->sortop < 500);  // heuristic
        index_scan->add_sort_column(sgcl->tleSortGroupRef, ascending, scan_table_id);
    }

    // Add the PhysicalIndexScan to the scan_group
    std::vector<Group*> inputs;
    GroupExpression* index_scan_ge = NEW_EX(GroupExpression, (OptExprOp*)index_scan, inputs);
    scan_group->add_expression(index_scan_ge);

    return index_scan;
}

std::vector<OptExpression*> SortImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    LogicalSort* logical_sort = static_cast<LogicalSort*>(input->get_op());
    void* sort_clause = logical_sort->get_sort_clause();
    OptExpression* child = input->input_at(0);

    std::vector<OptExpression*> results;

    // Path 1: PhysicalSort(child) - explicit sort
    PhysicalSort* physical_sort = NEW_EX(PhysicalSort);
    physical_sort->set_sort_clause(sort_clause);
    OptExpression* sort_expr = OptExpression::create(physical_sort, child);
    results.push_back(sort_expr);

    // ========== 判断是否有 Join 节点 ==========
    // Find the actual LogicalJoin by traversing through Project/Filter
    OptExpression* actual_child = child;
    bool has_join = false;
    while (actual_child->get_op()->get_type() == OptExprOpType::LOGICAL_PROJECT ||
           actual_child->get_op()->get_type() == OptExprOpType::LOGICAL_FILTER) {
        if (actual_child->arity() > 0) {
            actual_child = actual_child->input_at(0);
        } else {
            break;
        }
    }
    if (actual_child->get_op()->get_type() == OptExprOpType::LOGICAL_JOIN) {
        has_join = true;
    }

    IndexOptInfo* index = nullptr;
    List* pathkeys = nullptr;
    bool found_index = false;

    if (has_join) {
        // Join path: 尝试在外表上找索引
        found_index = find_index_for_sort_on_join_outer(actual_child, sort_clause, context, &index, &pathkeys);
    } else {
        // Non-Join path: 单表场景
        found_index = find_index_for_sort(child, sort_clause, context, &index, &pathkeys);
    }

    // 使用公共函数创建 IndexScan
    if (found_index) {
        // 确定要传递的表达式（Join场景传outer_expr，单表传child）
        OptExpression* scan_expr = has_join ? actual_child->input_at(0) : child;
        PhysicalIndexScan* index_scan = create_index_scan_to_group(
            scan_expr, sort_clause, index, pathkeys, context);
        if (index_scan != nullptr) {
            PassThroughOperator* pass = NEW_EX(PassThroughOperator);
            OptExpression* pass_expr = OptExpression::create(pass, child);
            pass_expr->set_sort_requirement(sort_clause);
            results.push_back(pass_expr);
        }
    }

    return results;
}

}  // namespace cascades