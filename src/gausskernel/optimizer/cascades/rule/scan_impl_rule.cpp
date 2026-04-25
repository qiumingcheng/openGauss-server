#include "../base/memory_utils.h"
#include "scan_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"
#include "nodes/relation.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "optimizer/pathnode.h"

namespace cascades {

// Static singleton instance
ScanImplRule* ScanImplRule::instance() {
    static ScanImplRule inst;
    return &inst;
}

ScanImplRule::ScanImplRule()
    : Rule(RuleType::IMP_SCAN, Pattern::create(OptExprOpType::LOGICAL_SCAN)) {}

std::vector<OptExpression*> ScanImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    ereport(WARNING, (errmsg("[CASCADES] ScanImplRule:transform, input=%s",
                         input->to_string().c_str())));

    // Pattern guarantees: op_type == LOGICAL_SCAN && arity == 0
    LogicalScan* scan_op = static_cast<LogicalScan*>(input->get_op());
    int table_id = scan_op->get_table_id();

    // Note: predicate_id is no longer used directly from LogicalScan
    // Predicates are now handled by LogicalFilter on top of LogicalScan
    // and transferred by FilterImplRule to PhysicalScan

    // Get PlannerInfo
    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] ScanImplRule:transform, PlannerRoot is null")));
        return {};
    }

    // Find the base RelOptInfo for this table
    RelOptInfo* rel = find_base_rel(root, table_id);
    if (rel == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] ScanImplRule:transform, RelOptInfo not found for table_id=%d", table_id)));
        return {};
    }

    // Check if this is a base relation (not a subquery or other RTE type)
    // Subquery RTEs have rtekind == RTE_SUBQUERY and cannot use SeqScan
    if (rel->rtekind != RTE_RELATION) {
        ereport(WARNING, (errmsg("[CASCADES] ScanImplRule:transform, skipping non-relation rtekind=%d for table_id=%d",
                             (int)rel->rtekind, table_id)));
        return {};
    }

    ereport(WARNING, (errmsg("[CASCADES] ScanImplRule:transform, creating seqscan path for table_id=%d",
                         table_id)));

    // Get output columns from LogicalScan operator (not from logical property)
    // The output_columns_ member stores the logical output after column pruning
    ColumnRefSet output_cols = scan_op->get_output_columns();
    ereport(WARNING, (errmsg("[CASCADES] ScanImplRule: LogicalScan output_columns.size=%d", output_cols.size())));

    // Log reltarget before creating path
    int reltarget_cols = 0;
    if (rel->reltarget != nullptr && rel->reltarget->exprs != NIL) {
        reltarget_cols = list_length(rel->reltarget->exprs);
    }
    ereport(WARNING, (errmsg("[CASCADES] ScanImplRule: reltarget has %d columns before create_seqscan_path", reltarget_cols)));

    // Set reltarget from output_columns if available (for column pruning)
    // This ensures the path's pathtarget reflects the actual columns needed
    if (output_cols.size() > 0) {
        if (rel->reltarget == nullptr) {
            rel->reltarget = (PathTarget*)palloc0(sizeof(PathTarget));
            rel->reltarget->exprs = NIL;
            rel->reltarget->width = 0;
        } else {
            // Clear existing reltarget and rebuild from output_columns
            rel->reltarget->exprs = NIL;
            rel->reltarget->width = 0;
        }

        RangeTblEntry* rte = (root->parse && table_id <= list_length(root->parse->rtable))
                             ? (RangeTblEntry*)list_nth(root->parse->rtable, table_id - 1)
                             : nullptr;
        for (int col : output_cols) {
            if (col > 0 && col <= rel->max_attr && rte != nullptr) {
                Oid atttype = get_atttype(rte->relid, col);
                Var* var = makeVar(table_id, col, atttype, -1, InvalidOid, 0);
                rel->reltarget->exprs = lappend(rel->reltarget->exprs, var);
                rel->reltarget->width += get_typlen(atttype);
                ereport(WARNING, (errmsg("[CASCADES] ScanImplRule: adding column %d to reltarget", col)));
            }
        }
    }

    // Create SeqScan path using OpenGauss function
    // Note: predicates are attached via baserestrictinfo in RelOptInfo
    // and will be picked up by create_seqscan_path through get_baserel_parampathinfo

    // If LogicalScan has qual_list from predicate pushdown, restore it to rel->baserestrictinfo
    // This handles the case where Filter was eliminated by PredicatePushDownRule
    List* pushed_qual_list = (List*)scan_op->get_qual_list();
    if (pushed_qual_list != NIL) {
        ereport(WARNING, (errmsg("[CASCADES] ScanImplRule: restoring %d predicates from LogicalScan.qual_list",
                             list_length(pushed_qual_list))));
        // Merge with existing baserestrictinfo if present
        if (rel->baserestrictinfo != NIL) {
            // Already has baserestrictinfo - predicates were not eliminated
            ereport(WARNING, (errmsg("[CASCADES] ScanImplRule: baserestrictinfo already has %d clauses",
                                 list_length(rel->baserestrictinfo))));
        } else {
            // Restore baserestrictinfo from qual_list
            rel->baserestrictinfo = pushed_qual_list;
        }
    }

    Path* scan_path = create_seqscan_path(root, rel, nullptr, 1);

    ereport(WARNING, (errmsg("[CASCADES] ScanImplRule: scan_path=%p, type=%d, total_cost=%.2f, startup_cost=%.2f",
                             (void*)scan_path, (int)scan_path->type, scan_path->total_cost, scan_path->startup_cost)));

    // Log pathtarget after creating path
    int pathtarget_cols = 0;
    if (scan_path->pathtarget != nullptr && scan_path->pathtarget->exprs != NIL) {
        pathtarget_cols = list_length(scan_path->pathtarget->exprs);
    }
    ereport(WARNING, (errmsg("[CASCADES] ScanImplRule: created Path with pathtarget=%d columns, path=%p", pathtarget_cols, (void*)scan_path)));

    // Create physical scan operator using NEW_EX macro
    PhysicalScan* physical_scan = NEW_EX(PhysicalScan, scan_path);
    physical_scan->set_relid(rel->relid);

    // Note: predicate transfer is now handled by FilterImplRule
    // which adds predicate_ids from LogicalFilter to PhysicalScan

    OptExpression* new_expr = OptExpression::create(physical_scan);
    ereport(WARNING, (errmsg("[CASCADES] ScanImplRule:transform, created physical scan: %s",
                         new_expr->to_string().c_str())));
    return {new_expr};
}

}  // namespace cascades
