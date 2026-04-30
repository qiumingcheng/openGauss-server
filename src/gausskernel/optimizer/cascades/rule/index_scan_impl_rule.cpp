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
#include "optimizer/plancat.h"
#include "optimizer/tlist.h"
#include "nodes/relation.h"

namespace cascades {

bool IndexScanImplRule::has_indexes(RelOptInfo* rel) const {
    return rel->indexlist != NIL && list_length(rel->indexlist) > 0;
}

bool IndexScanImplRule::can_use_index(RelOptInfo* rel, int predicate_id) const {
    if (predicate_id < 0) {
        return false;
    }
    return has_indexes(rel);
}

List* IndexScanImplRule::get_available_indexes(RelOptInfo* rel) const {
    return rel->indexlist;
}

bool IndexScanImplRule::expr_in_ec(EquivalenceClass* ec, Node* expr) const
{
    if (ec == nullptr || expr == nullptr) {
        return false;
    }

    // Check if expr is a member of the equivalence class
    ListCell* lc = nullptr;
    foreach (lc, ec->ec_members) {
        EquivalenceMember* em = static_cast<EquivalenceMember*>(lfirst(lc));
        if (em != nullptr && em->em_expr != nullptr) {
            // Check if expressions are equal
            if (equal(em->em_expr, expr)) {
                return true;
            }
        }
    }
    return false;
}

bool IndexScanImplRule::pathkeys_match_sort_clause(
    List* pathkeys, void* sort_clause, PlannerInfo* root) const
{
    if (pathkeys == NIL || sort_clause == nullptr) {
        return false;
    }

    List* clauses = static_cast<List*>(sort_clause);

    // Must have same number of keys
    if (list_length(pathkeys) != list_length(clauses)) {
        return false;
    }

    // Get the target list from PlannerInfo for looking up expressions
    List* targetlist = root->parse->targetList;
    if (targetlist == NIL) {
        return false;
    }

    // Compare each pathkey with corresponding sort clause
    ListCell* lc_pathkey = nullptr;
    ListCell* lc_clause = nullptr;
    forboth(lc_pathkey, pathkeys, lc_clause, clauses) {
        PathKey* pk = static_cast<PathKey*>(lfirst(lc_pathkey));
        SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc_clause));

        // Compare strategy (ASC/DESC) and nulls_first
        if (pk->pk_strategy != sgcl->sortop) {
            return false;
        }
        if (pk->pk_nulls_first != sgcl->nulls_first) {
            return false;
        }

        // Get the expression for this sort clause from target list
        TargetEntry* tle = get_sortgroupref_tle(sgcl->tleSortGroupRef, targetlist, false);
        if (tle == nullptr) {
            return false;
        }

        Node* sort_expr = (Node*)tle->expr;

        // Compare EquivalenceClass to ensure same column
        EquivalenceClass* pk_eclass = pk->pk_eclass;
        if (pk_eclass == nullptr) {
            return false;
        }

        // Check if the sort expression belongs to the same equivalence class
        if (!expr_in_ec(pk_eclass, sort_expr)) {
            return false;
        }
    }

    return true;
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
    void* sort_clause = scan_op->get_sort_clause();

    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, PlannerInfo is null")));
        return {};
    }

    RelOptInfo* rel = find_base_rel(root, table_id);
    if (rel == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, RelOptInfo not found for table_id=%d", table_id)));
        return {};
    }

    RangeTblEntry* rte = root->simple_rte_array[table_id];
    RangeTblEntry* rte_by_relid = nullptr;
    for (int i = 1; i < root->simple_rel_array_size; i++) {
        if (root->simple_rte_array[i] != nullptr && root->simple_rte_array[i]->relid == (Oid)rel->relid) {
            rte_by_relid = root->simple_rte_array[i];
            break;
        }
    }

    if (rel->indexlist == NIL) {
        RangeTblEntry* target_rte = rte_by_relid ? rte_by_relid : rte;
        if (target_rte != nullptr && target_rte->rtekind == RTE_RELATION) {
            get_relation_info(root, target_rte, rel);
        }
    }

    if (!has_indexes(rel)) {
        ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, no indexes on relation")));
        return {};
    }

    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, found indexes, creating index paths")));

    List* qual_list = scan_op->get_qual_list();
    (void)qual_list;

    create_index_paths(root, rel);

    List* pathlist = rel->pathlist;
    ListCell* lc = nullptr;

    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, pathlist length=%d, sort_required=%d",
                         list_length(pathlist), sort_clause != nullptr)));

    foreach (lc, pathlist) {
        Path* path = (Path*)lfirst(lc);

        if (IsA(path, IndexPath)) {
            IndexPath* index_path = (IndexPath*)path;

            // If LogicalScan has sort requirement, check if IndexPath's pathkeys match
            if (sort_clause != nullptr) {
                List* pathkeys = index_path->path.pathkeys;
                if (!pathkeys_match_sort_clause(pathkeys, sort_clause, root)) {
                    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, skipping IndexPath, pathkeys don't match sort")));
                    continue;
                }
                ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, found matching IndexPath for sort")));
            }

            PhysicalScan* physical_scan = NEW_EX(PhysicalScan, (Path*)index_path);
            OptExpression* new_expr = OptExpression::create(physical_scan);
            result.push_back(new_expr);
        } else if (IsA(path, BitmapHeapPath)) {
            // Bitmap scan - cannot provide sorted output, skip if sort is required
            if (sort_clause != nullptr) {
                ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, skipping BitmapPath, cannot satisfy sort")));
                continue;
            }

            BitmapHeapPath* bitmap_path = (BitmapHeapPath*)path;
            PhysicalScan* physical_scan = NEW_EX(PhysicalScan, (Path*)bitmap_path);
            OptExpression* new_expr = OptExpression::create(physical_scan);
            result.push_back(new_expr);
        }
    }

    if (result.empty() && sort_clause != nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, no index paths match sort requirement")));
    }

    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, generated %d index paths",
                         (int)result.size())));

    return result;
}

}  // namespace cascades