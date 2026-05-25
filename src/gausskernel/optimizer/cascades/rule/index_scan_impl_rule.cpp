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
    if (pathkeys == NIL) {
        ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: pathkeys is NIL")));
        return false;
    }
    if (sort_clause == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: sort_clause is null")));
        return false;
    }

    List* clauses = static_cast<List*>(sort_clause);

    // Must have same number of keys
    if (list_length(pathkeys) != list_length(clauses)) {
        ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: key count mismatch pathkeys=%d clauses=%d",
                             list_length(pathkeys), list_length(clauses))));
        return false;
    }

    // Get the target list from PlannerInfo for looking up expressions
    List* targetlist = root->parse->targetList;
    if (targetlist == NIL) {
        ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: targetlist is NIL")));
        return false;
    }

    // Compare each pathkey with corresponding sort clause
    ListCell* lc_pathkey = nullptr;
    ListCell* lc_clause = nullptr;
    forboth(lc_pathkey, pathkeys, lc_clause, clauses) {
        PathKey* pk = static_cast<PathKey*>(lfirst(lc_pathkey));
        SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc_clause));

        ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: pk_strategy=%d sgcl->sortop=%d pk_nulls_first=%d sgcl->nulls_first=%d",
                             pk->pk_strategy, sgcl->sortop, pk->pk_nulls_first, sgcl->nulls_first)));

        // Compare strategy (ASC/DESC) and nulls_first
        // pk_strategy is BTLessStrategyNumber (1) for ASC or BTGreaterStrategyNumber (5) for DESC
        // sgcl->sortop is the ordering operator Oid (e.g., 97 for int4lt, 521 for int4gt)
        // We cannot directly compare them since they're different types.
        // For most types, < operator Oid < > operator Oid numerically.
        // So we use a heuristic: if sortop Oid < 500, assume ASC (matches < operator).
        bool sortop_is_ascending = (sgcl->sortop < 500);
        bool pk_is_ascending = (pk->pk_strategy == BTLessStrategyNumber);
        if (sortop_is_ascending != pk_is_ascending) {
            ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: direction mismatch, sortop_asc=%d, pk_asc=%d",
                                 sortop_is_ascending, pk_is_ascending)));
            return false;
        }
        if (pk->pk_nulls_first != sgcl->nulls_first) {
            ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: nulls_first mismatch")));
            return false;
        }

        // Get the expression for this sort clause from target list
        TargetEntry* tle = get_sortgroupref_tle(sgcl->tleSortGroupRef, targetlist, false);
        if (tle == nullptr) {
            ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: tle is null for tleSortGroupRef=%u", sgcl->tleSortGroupRef)));
            return false;
        }

        Node* sort_expr = (Node*)tle->expr;
        ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: sort_expr nodeTag=%d", sort_expr ? sort_expr->type : -1)));

        // Compare EquivalenceClass to ensure same column
        EquivalenceClass* pk_eclass = pk->pk_eclass;
        if (pk_eclass != nullptr) {
            ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: pk_eclass=%p, ec_members count=%d",
                                 pk_eclass, list_length(pk_eclass->ec_members))));

            // Check if the sort expression belongs to the same equivalence class
            if (!expr_in_ec(pk_eclass, sort_expr)) {
                ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: expr not in ec")));
                return false;
            }
        } else {
            // pk_eclass is null when pathkeys are built from sort_clause
            // Skip EC validation, sortop and nulls_first already matched above
            ereport(WARNING, (errmsg("[CASCADES] pathkeys_match_sort_clause: pk_eclass is null, skipping EC validation")));
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
    PhysicalIndexScan* physical_scan = nullptr;

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

    List* qual_list = scan_op->get_qual_list();
    (void)qual_list;

    create_index_paths(root, rel);

    List* pathlist = rel->pathlist;
    ListCell* lc = nullptr;

    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, creating index paths, pathlist length=%d, sort_required=%d",
                         list_length(pathlist), sort_clause != nullptr)));

    foreach (lc, pathlist) {
        physical_scan = nullptr;
        Path* path = (Path*)lfirst(lc);
        
        if (IsA(path, IndexPath)) {
            IndexPath* index_path = (IndexPath*)path;
            List* pathkeys = index_path->path.pathkeys;

            ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, IndexPath path type=%d, indexonly=%d, pathkeys=%p, sort_clause=%p",
                                 index_path->path.pathtype, (index_path->path.pathtype == T_IndexOnlyScan), (void*)pathkeys, sort_clause)));

            // If LogicalScan has sort requirement, check if IndexPath's pathkeys match
            if (sort_clause != nullptr) {
                bool match = false;
                if (pathkeys != NIL) {
                    // Use IndexPath's built-in pathkeys
                    if (pathkeys_match_sort_clause(pathkeys, sort_clause, root)) {
                        match = true;
                    }
                } else {
                    // Try to build pathkeys from sort_clause
                    List* built_pathkeys = build_pathkeys_from_sort_clause(root, index_path->indexinfo, (List*)sort_clause);
                    if (built_pathkeys != NIL && pathkeys_match_sort_clause(built_pathkeys, sort_clause, root)) {
                        match = true;
                    }
                }

                if (!match) {
                    ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, skipping IndexPath, pathkeys don't match sort")));
                    continue;
                }
                ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, found matching IndexPath for sort")));
            }
            physical_scan = NEW_EX(PhysicalIndexScan, (Path*)index_path);

            // 设置排序信息：从 sort_clause 提取
            if (sort_clause != nullptr) {
                List* clauses = static_cast<List*>(sort_clause);
                List* targetlist = root->parse->targetList;
                ListCell* lc_clause = nullptr;
                foreach (lc_clause, clauses) {
                    SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc_clause));
                    bool ascending = (sgcl->sortop < 500);  // heuristic
                    physical_scan->add_sort_column(sgcl->tleSortGroupRef, ascending);    
                }
            }
        } else if (IsA(path, BitmapHeapPath)) {
            BitmapHeapPath* bitmap_path = (BitmapHeapPath*)path;

            // Bitmap scan - cannot provide sorted output, skip if sort is required
            if (sort_clause != nullptr) {
                ereport(WARNING, (errmsg("[CASCADES] IndexScanImplRule:transform, skipping BitmapPath, cannot satisfy sort")));
                continue;
            }

            physical_scan = NEW_EX(PhysicalIndexScan, (Path*)bitmap_path);
        }

        if(physical_scan) {
            physical_scan->set_relid(scan_op->get_relid());
            physical_scan->set_table_id(scan_op->get_table_id());
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

List* IndexScanImplRule::build_pathkeys_from_sort_clause(
    PlannerInfo* root, IndexOptInfo* index, List* sort_clause) const
{
    if (sort_clause == NIL || index == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: sort_clause=%p or index is null", (void*)sort_clause)));
        return NIL;
    }

    List* pathkeys = NIL;
    ListCell* lc = nullptr;
    List* targetlist = root->parse->targetList;

    ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: sort_clause length=%d, index nkeycolumns=%d",
                         list_length(sort_clause), index->nkeycolumns)));

    foreach (lc, sort_clause) {
        SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));

        // Get the expression for this sort clause
        TargetEntry* tle = get_sortgroupref_tle(sgcl->tleSortGroupRef, targetlist, false);
        if (tle == nullptr) {
            ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: tle is null for tleSortGroupRef=%u", sgcl->tleSortGroupRef)));
            return NIL;
        }

        Node* sort_expr = (Node*)tle->expr;
        ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: sort_expr nodeTag=%d", sort_expr ? sort_expr->type : -1)));

        // Find the corresponding index column
        int indexcol = -1;
        for (int i = 0; i < index->nkeycolumns; i++) {
            int key_attr = index->indexkeys[i];
            ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: checking index key %d, key_attr=%d", i, key_attr)));
            if (key_attr != 0) {
                // Check if this matches the sort expression
                if (IsA(sort_expr, Var)) {
                    Var* var = (Var*)sort_expr;
                    ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: Var varattno=%d", var->varattno)));
                    if (var->varattno == key_attr) {
                        indexcol = i;
                        ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: matched indexcol=%d", indexcol)));
                        break;
                    }
                }
            }
        }

        if (indexcol < 0) {
            ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: no matching index column found")));
            return NIL;
        }

        // Build PathKey for this column
        PathKey* pk = makeNode(PathKey);
        pk->pk_eclass = nullptr;  // Will be set by index pathkey building
        pk->pk_strategy = (sgcl->sortop < 500) ? BTLessStrategyNumber : BTGreaterStrategyNumber;
        pk->pk_nulls_first = sgcl->nulls_first;

        pathkeys = lappend(pathkeys, pk);
        ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: added pathkey, total count=%d", list_length(pathkeys))));
    }

    ereport(WARNING, (errmsg("[CASCADES] build_pathkeys_from_sort_clause: returning %d pathkeys", list_length(pathkeys))));
    return pathkeys;
}

}  // namespace cascades