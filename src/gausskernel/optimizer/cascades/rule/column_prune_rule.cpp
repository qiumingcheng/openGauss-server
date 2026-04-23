#include "../base/memory_utils.h"
#include "column_prune_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../operator/operators/logical_scan.h"
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

// Helper: Create a pruned scan with only needed columns
static OptExpression* prune_scan(LogicalScan* scan_op, const std::set<int>& needed_columns) {
    RelOptInfo* rel = scan_op->get_rel();
    if (rel == nullptr || rel->reltarget == nullptr) {
        return nullptr;
    }

    // Get original columns from scan's output_columns member
    ColumnRefSet scan_output = scan_op->get_output_columns();
    std::set<int> orig_columns_set(scan_output.begin(), scan_output.end());
    int orig_col_count = (int)orig_columns_set.size();

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
    if ((int)needed_columns.size() >= orig_col_count) {
        return nullptr;
    }

    LogicalScan* pruned_scan = NEW_EX(LogicalScan, scan_op->get_table_id());
    pruned_scan->set_rel(rel);

    // Set pruned output_columns
    ColumnRefSet pruned_columns;
    for (int col : needed_columns) {
        if (scan_output.contains(col)) {
            pruned_columns.add(col);
        }
    }
    pruned_scan->set_output_columns(pruned_columns);

    // Update reltarget
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
                          Pattern::create(OptExprOpType::LOGICAL_SCAN))) {}

std::vector<OptExpression*> ColumnPruneRule::transform(
    OptExpression* input, OptimizerContext* context) {
    Assert(input->arity() > 0);

    LogicalProject* project_op = static_cast<LogicalProject*>(input->get_op());
    OptExpression* child = input->input_at(0);
    LogicalScan* scan_op = static_cast<LogicalScan*>(child->get_op());

    std::set<int> project_columns = collect_columns_from_target_list(project_op->get_target_list());

    // Get child columns from Scan's output_columns member
    ColumnRefSet scan_output = scan_op->get_output_columns();
    std::set<int> child_columns(scan_output.begin(), scan_output.end());

    if (project_columns.empty()) {
        return {};
    }

    // Check if we can prune (project_columns is proper subset of child_columns)
    if (!is_proper_subset(project_columns, child_columns)) {
        return {};
    }

    ereport(WARNING, (errmsg("[CASCADES][COLUMN_PRUNE] project_columns=%d, child_columns=%d, can_prune=1",
                         (int)project_columns.size(), (int)child_columns.size())));

    // Build pruned scan
    OptExpression* new_child = prune_scan(scan_op, project_columns);
    if (new_child == nullptr) {
        return {};
    }

    // Create new project with filtered target list
    LogicalProject* parent_project = NEW_EX(LogicalProject);
    parent_project->set_target_list(filter_target_list(project_op->get_target_list(), project_columns));

    OptExpression* result_expr = OptExpression::create(parent_project, new_child);
    return {result_expr};
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
