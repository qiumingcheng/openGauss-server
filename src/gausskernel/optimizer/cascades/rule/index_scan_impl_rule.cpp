#include "nodes/parsenodes.h"
#include "../base/memory_utils.h"
#include "index_scan_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../optimizer/optimizer_context.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "nodes/relation.h"

namespace cascades {

bool IndexScanImplRule::has_indexes(RelOptInfo* rel) const {
    // Check if the relation has any indexes
    // In OpenGauss, RelOptInfo has an 'indexlist' field that contains IndexOptInfo
    return rel->indexlist != NIL && rel->indexlist->length > 0;
}

bool IndexScanImplRule::can_use_index(RelOptInfo* rel, int predicate_id) const {
    // Check if a predicate can use an index
    // This would analyze the predicate to determine if an index can be used
    // For now, return true if there's a predicate and indexes exist
    if (predicate_id < 0) {
        return false;  // No predicate
    }
    return has_indexes(rel);
}

List* IndexScanImplRule::get_available_indexes(RelOptInfo* rel) const {
    // Return the list of available index options
    return rel->indexlist;
}

IndexScanImplRule* IndexScanImplRule::instance() {
    static IndexScanImplRule inst;
    return &inst;
}

IndexScanImplRule::IndexScanImplRule()
    : Rule(RuleType::IMP_INDEX_SCAN, Pattern::create(OptExprOpType::LOGICAL_SCAN)) {}

std::vector<OptExpression*> IndexScanImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, input=%s",
                         input->to_string().c_str())));

    std::vector<OptExpression*> result;

    LogicalScan* scan_op = static_cast<LogicalScan*>(input->get_op());
    int table_id = scan_op->get_table_id();

    // Get PlannerInfo
    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, PlannerInfo is null")));
        return {};  // Cannot create paths without PlannerInfo
    }

    // Find the base RelOptInfo for this table
    RelOptInfo* rel = find_base_rel(root, table_id);
    if (rel == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, RelOptInfo not found for table_id=%d", table_id)));
        return {};
    }

    // Check if there are any indexes on this relation
    if (!has_indexes(rel)) {
        ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, no indexes on relation")));
        return {};  // No indexes, cannot create index scan
    }

    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, found indexes, creating index paths")));

    // Get predicate if any
    int predicate_id = scan_op->get_predicate_id();
    (void)predicate_id;  // unused but kept for future expansion

    // Generate all possible index paths using OpenGauss's create_index_paths
    // This function creates multiple index scan paths with different access methods
    create_index_paths(root, rel);

    // After create_index_paths, rel->pathlist contains all possible paths
    // including index scans
    List* pathlist = rel->pathlist;
    ListCell* lc = nullptr;

    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, pathlist length=%d",
                         list_length(pathlist))));

    foreach (lc, pathlist) {
        Path* path = (Path*)lfirst(lc);

        // Check if this is an index path (IndexPath or BitmapPath)
        if (IsA(path, IndexPath)) {
            IndexPath* index_path = (IndexPath*)path;
            ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, found IndexPath, startup_cost=%.2f",
                                 index_path->path.startup_cost)));

            // Create physical scan operator with the index path
            PhysicalScan* physical_scan = NEW_EX(PhysicalScan, (Path*)index_path);

            // Create the OptExpression
            OptExpression* new_expr = OptExpression::create(physical_scan);
            result.push_back(new_expr);
        } else if (IsA(path, BitmapHeapPath)) {
            // Bitmap scan path
            BitmapHeapPath* bitmap_path = (BitmapHeapPath*)path;
            ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, found BitmapPath")));

            // Create physical scan for bitmap scan
            PhysicalScan* physical_scan = NEW_EX(PhysicalScan, (Path*)bitmap_path);

            OptExpression* new_expr = OptExpression::create(physical_scan);
            result.push_back(new_expr);
        }
        // Skip other path types (SeqScan, etc.)
    }

    // If no index paths were generated, return empty
    if (result.empty()) {
        ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, no index paths generated")));
        return {};
    }

    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, generated %d index paths",
                         (int)result.size())));

    return result;
}

}  // namespace cascades
