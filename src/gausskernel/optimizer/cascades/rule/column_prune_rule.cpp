#include "../base/memory_utils.h"
#include "column_prune_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/expression_context.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/tlist.h"

namespace cascades {

// Helper: Extract column indices from a target list
static std::set<int> collect_columns_from_target_list(List* target_list) {
    std::set<int> columns;
    if (target_list == NIL) {
        return columns;
    }
    ListCell* lc;
    foreach(lc, target_list) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (tle && tle->expr && IsA(tle->expr, Var)) {
            Var* var = (Var*)tle->expr;
            columns.insert(var->varattno);
        }
    }
    return columns;
}

// Helper: Extract column indices from RelOptInfo's reltarget
static std::set<int> collect_columns_from_reltarget(RelOptInfo* rel) {
    std::set<int> columns;
    if (rel == nullptr || rel->reltarget == nullptr) {
        return columns;
    }
    ListCell* lc;
    foreach(lc, rel->reltarget->exprs) {
        Var* var = (Var*)lfirst(lc);
        columns.insert(var->varattno);
    }
    return columns;
}

// Helper: Create a pruned scan with only needed columns
static OptExpression* prune_scan(LogicalScan* scan_op, const std::set<int>& needed_columns,
                                 int orig_col_count) {
    RelOptInfo* rel = scan_op->get_rel();
    if (rel == nullptr || rel->reltarget == nullptr) {
        return nullptr;
    }

    // Build pruned target list
    List* new_target_list = NIL;
    int new_width = 0;
    ListCell* lc;
    foreach(lc, rel->reltarget->exprs) {
        Var* var = (Var*)lfirst(lc);
        if (needed_columns.find(var->varattno) != needed_columns.end()) {
            new_target_list = lappend(new_target_list, var);
            new_width += 32;
        }
    }

    // Only create pruned scan if we actually removed columns
    if (list_length(new_target_list) >= orig_col_count) {
        return nullptr;
    }

    LogicalScan* pruned_scan = NEW_EX(LogicalScan, scan_op->get_table_id());
    pruned_scan->set_rel(rel);

    PathTarget* pruned_target = create_empty_pathtarget();
    pruned_target->exprs = new_target_list;
    pruned_target->width = new_width;
    rel->reltarget = pruned_target;

    return OptExpression::create(pruned_scan);
}

// Helper: Check if set A is a proper subset of set B
static bool is_proper_subset(const std::set<int>& subset, const std::set<int>& superset) {
    if (subset.size() >= superset.size()) {
        return false;
    }
    for (int col : subset) {
        if (superset.find(col) == superset.end()) {
            return false;
        }
    }
    return true;
}

// Static singleton instance
ColumnPruneRule* ColumnPruneRule::instance() {
    static ColumnPruneRule inst;
    return &inst;
}

ColumnPruneRule::ColumnPruneRule()
    : Rule(RuleType::TF_COLUMN_PRUNE,
           Pattern::create(OptExprOpType::LOGICAL_PROJECT,
                          Pattern::any())) {}

std::vector<OptExpression*> ColumnPruneRule::transform(
    OptExpression* input, OptimizerContext* context) {
    Assert(input->arity() > 0);

    LogicalProject* project_op = static_cast<LogicalProject*>(input->get_op());
    OptExpression* child = input->input_at(0);
    OptExprOp* child_op = child->get_op();

    std::set<int> project_columns = collect_columns_from_target_list(project_op->get_target_list());

    std::set<int> child_columns;
    int orig_col_count = 0;
    bool child_is_scan = child_op->get_type() == OptExprOpType::LOGICAL_SCAN;
    if (child_is_scan) {
        LogicalScan* scan_op = static_cast<LogicalScan*>(child_op);
        RelOptInfo* rel = scan_op->get_rel();
        child_columns = collect_columns_from_reltarget(rel);
        orig_col_count = (int)child_columns.size();
    }

    bool can_prune = false;
    if (child_is_scan) {
        can_prune = is_proper_subset(project_columns, child_columns);
    } else if (child_op->get_type() == OptExprOpType::LOGICAL_PROJECT) {
        can_prune = !project_columns.empty();
    }

    ereport(WARNING, (errmsg("[CASCADES][COLUMN_PRUNE] project_columns=%d, child_columns=%d, can_prune=%d",
                         (int)project_columns.size(), (int)child_columns.size(), can_prune ? 1 : 0)));

    if (!can_prune) {
        return {};
    }

    OptExpression* new_child = nullptr;

    if (child_is_scan) {
        LogicalScan* scan_op = static_cast<LogicalScan*>(child_op);
        new_child = prune_scan(scan_op, project_columns, orig_col_count);
    } else if (child_op->get_type() == OptExprOpType::LOGICAL_PROJECT) {
        new_child = transform_nested_project(child, project_columns);
    }

    if (new_child == nullptr) {
        return {};
    }

    LogicalProject* parent_project = NEW_EX(LogicalProject);
    parent_project->set_target_list(filter_target_list(project_op->get_target_list(), project_columns));

    OptExpression* result_expr = OptExpression::create(parent_project, new_child);
    return {result_expr};
}

OptExpression* ColumnPruneRule::transform_nested_project(OptExpression* child,
                                                          const std::set<int>& needed_columns) {
    LogicalProject* child_project = static_cast<LogicalProject*>(child->get_op());
    OptExpression* grandchild = child->input_at(0);
    OptExprOp* grandchild_op = grandchild->get_op();

    std::set<int> inner_output = collect_columns_from_target_list(child_project->get_target_list());

    std::set<int> grandchild_columns;
    bool is_grandchild_scan = grandchild_op->get_type() == OptExprOpType::LOGICAL_SCAN;
    if (is_grandchild_scan) {
        LogicalScan* scan_op = static_cast<LogicalScan*>(grandchild_op);
        grandchild_columns = collect_columns_from_reltarget(scan_op->get_rel());
    }

    std::set<int> needed_grandchild;
    if (is_grandchild_scan && !grandchild_columns.empty()) {
        for (int col : needed_columns) {
            if (grandchild_columns.find(col) != grandchild_columns.end()) {
                needed_grandchild.insert(col);
            }
        }
    } else {
        needed_grandchild = needed_columns;
    }

    OptExpression* pruned_grandchild = nullptr;
    if (is_grandchild_scan) {
        LogicalScan* scan_op = static_cast<LogicalScan*>(grandchild_op);
        RelOptInfo* rel = scan_op->get_rel();
        int orig_count = list_length(rel->reltarget->exprs);
        if (needed_grandchild.size() < (size_t)orig_count) {
            pruned_grandchild = prune_scan(scan_op, needed_grandchild, orig_count);
        }
    }
    if (pruned_grandchild == nullptr) {
        pruned_grandchild = grandchild;
    }

    std::vector<int> pushed;
    for (int col : needed_columns) {
        if (inner_output.find(col) != inner_output.end()) {
            pushed.push_back(col);
        }
    }

    List* new_inner_tl = NIL;
    ListCell* lc;
    foreach(lc, child_project->get_target_list()) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (tle && tle->expr && IsA(tle->expr, Var)) {
            Var* var = (Var*)tle->expr;
            if (needed_columns.find(var->varattno) != needed_columns.end()) {
                new_inner_tl = lappend(new_inner_tl, tle);
            }
        } else if (tle && tle->expr) {
            new_inner_tl = lappend(new_inner_tl, tle);
        }
    }

    LogicalProject* pruned_project = NEW_EX(LogicalProject);
    pruned_project->set_target_list(new_inner_tl);

    return OptExpression::create(pruned_project, pruned_grandchild);
}

List* ColumnPruneRule::filter_target_list(List* target_list, const std::set<int>& needed) {
    List* result = NIL;
    ListCell* lc;
    foreach(lc, target_list) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (tle && tle->expr && IsA(tle->expr, Var)) {
            Var* var = (Var*)tle->expr;
            if (needed.find(var->varattno) != needed.end()) {
                result = lappend(result, tle);
            }
        } else if (tle && tle->expr) {
            result = lappend(result, tle);
        }
    }
    return result;
}


}  // namespace cascades
