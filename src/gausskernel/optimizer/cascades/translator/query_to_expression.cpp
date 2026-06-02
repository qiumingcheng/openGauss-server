#include "query_to_expression.h"
#include "../base/memory_utils.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../statistics/statistics.h"
#include "optimizer/planmain.h"
#include "optimizer/pathnode.h"
#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "nodes/primnodes.h"
#include "optimizer/tlist.h"
#include "utils/lsyscache.h"
#include "optimizer/var.h"

namespace cascades {

// Forward declaration
bool check_subquery_in_fromlist(List* fromlist);

OptExpression* QueryToExprConverter::convert(PlannerInfo* root, Query* parse) {
    if (parse == nullptr) {
        return nullptr;
    }

    /* Check for SubLink in WHERE clause - subqueries not yet supported */
    if (parse->jointree != nullptr && parse->jointree->quals != nullptr) {
        if (check_sublink_in_node(parse->jointree->quals)) {
            ereport(WARNING, (errmsg("[CASCADES] convert: SubLink found in WHERE, returning nullptr")));
            return nullptr;
        }
    }

    /* Check for subqueries in FROM clause (e.g., SELECT * FROM (SELECT ...) t) */
    if (parse->jointree != nullptr && parse->jointree->fromlist != nullptr) {
        if (check_subquery_in_fromlist(parse->jointree->fromlist)) {
            ereport(WARNING, (errmsg("[CASCADES] convert: subquery found in FROM, returning nullptr")));
            return nullptr;
        }
    }

    /* Check if rtable contains subqueries (RTE_SUBQUERY) */
    if (parse->rtable != nullptr) {
        ListCell* lc = NULL;
        foreach (lc, parse->rtable) {
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
            if (rte->rtekind == RTE_SUBQUERY) {
                ereport(WARNING, (errmsg("[CASCADES] convert: RTE_SUBQUERY found in rtable, returning nullptr")));
                return nullptr;
            }
        }
    }

    root_ = root;
    parse_ = parse;

    // Convert SELECT statement
    OptExpression* result = translate_select(root, parse);

    // Print debug string for the converted expression tree
    if (result != nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] QueryToExprConverter: converted expression tree:\n%s",
                             result->debug_string().c_str())));
    }

    return result;
}

OptExpression* QueryToExprConverter::translate_select(PlannerInfo* root, Query* parse) {
    // 1. Convert FROM clause
    // Note: translate_scan directly attaches baserestrictinfo to LogicalScan.qual_list
    OptExpression* from_expr = nullptr;
    if (parse->jointree != nullptr) {
        from_expr = translate_from(root, parse, (Node*)parse->jointree);
    }

    // If from_expr is nullptr (e.g., SELECT 1; with no FROM clause),
    // return nullptr to fallback to old optimizer
    if (from_expr == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_select: no FROM clause, returning nullptr")));
        return nullptr;
    }

    // 2. WHERE clause handling
    // Predicates from baserestrictinfo are already attached to scans by translate_scan
    // For join quals, they are handled by translate_join
    // So we don't need to create an additional filter here
    OptExpression* filter_expr = from_expr;

    // 2.5. Handle aggregation - insert LogicalAggregate between scan and project
    OptExpression* child_expr = filter_expr;
    if (parse->hasAggs) {
        LogicalAggregate* agg_op = nullptr;
        if (parse->groupClause != nullptr && parse->groupClause != NIL) {
            std::vector<int> group_keys;
            ListCell* lc;
            foreach(lc, parse->groupClause) {
                SortGroupClause* sgc = (SortGroupClause*)lfirst(lc);
                // Use get_sortgroupclause_tle to find the correct TLE by tleSortGroupRef
                // This ensures we get the GROUP BY entry (at end of targetList), not SELECT entry
                TargetEntry* te = get_sortgroupclause_tle(sgc, parse->targetList);
                if (te != nullptr) {
                    group_keys.push_back(te->resno);
                    ereport(WARNING, (errmsg("[CASCADES] translate_select: group key resno=%d", te->resno)));
                } else {
                    // Fallback to tleSortGroupRef
                    group_keys.push_back(sgc->tleSortGroupRef);
                    ereport(WARNING, (errmsg("[CASCADES] translate_select: group key tleSortGroupRef=%d (fallback)",
                                         sgc->tleSortGroupRef)));
                }
            }
            agg_op = NEW_EX(LogicalAggregate, group_keys);
            ereport(WARNING, (errmsg("[CASCADES] translate_select: creating LogicalAggregate with %zu group keys",
                                 group_keys.size())));
        } else {
            agg_op = NEW_EX(LogicalAggregate);
            ereport(WARNING, (errmsg("[CASCADES] translate_select: creating LogicalAggregate (global aggregate)")));
        }
        // Set HAVING clause if present
        if (parse->havingQual != nullptr) {
            agg_op->set_having_qual(parse->havingQual);
            ereport(WARNING, (errmsg("[CASCADES] translate_select: setting havingQual")));
        }
        // Set target list for aggregate
        agg_op->set_target_list(parse->targetList);
        ereport(WARNING, (errmsg("[CASCADES] translate_select: setting targetList length=%d",
                             list_length(parse->targetList))));
        child_expr = OptExpression::create(agg_op, filter_expr);
    }

    // 3. Convert SELECT LIST to Project
    // Note: if hasAggs is true, we don't need to create LogicalProject separately
    // because the aggregation is already handled above
    OptExpression* project_expr = nullptr;
    ereport(WARNING, (errmsg("[CASCADES] translate_select: hasAggs=%d, calling translate_target_list=%d",
                             parse->hasAggs, !parse->hasAggs)));
    if (!parse->hasAggs) {
        project_expr = translate_target_list(root, parse, child_expr);
    }

    // 4. Handle ORDER BY
    OptExpression* result = project_expr ? project_expr : (parse->hasAggs ? child_expr : filter_expr);
    if (parse->sortClause != nullptr) {
        // Create LogicalSort operator
        LogicalSort* sort_op = NEW_EX(LogicalSort);
        sort_op->set_sort_clause(parse->sortClause);

        if (result != nullptr) {
            result = OptExpression::create(sort_op, result);
        } else {
            result = OptExpression::create(sort_op);
        }
    }

    // 5. Handle LIMIT/OFFSET
    if (parse->limitCount != nullptr || parse->limitOffset != nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] translate_select: limitCount=%p, type=%d",
                             (void*)parse->limitCount, parse->limitCount ? (int)nodeTag(parse->limitCount) : -1)));
        LogicalLimit* limit_op = NEW_EX(LogicalLimit);
        limit_op->set_limit_count(parse->limitCount);
        limit_op->set_limit_offset(parse->limitOffset);

        if (result != nullptr) {
            result = OptExpression::create(limit_op, result);
        } else {
            result = OptExpression::create(limit_op);
        }
    }

    return result;
}

OptExpression* QueryToExprConverter::translate_from(PlannerInfo* root, Query* parse, Node* jointree) {
    if (jointree == nullptr) {
        return nullptr;
    }

    if (IsA(jointree, RangeTblRef)) {
        RangeTblRef* rtr = (RangeTblRef*)jointree;
        return translate_scan(root, parse, rtr);
    } else if (IsA(jointree, FromExpr)) {
        FromExpr* from = (FromExpr*)jointree;
        OptExpression* result = nullptr;
        int last_varno = 0;

        if (list_length(from->fromlist) > 0) {
            Node* first_item = (Node*)linitial(from->fromlist);
            result = translate_from_item(root, parse, first_item);
            if (IsA(first_item, RangeTblRef)) {
                last_varno = ((RangeTblRef*)first_item)->rtindex;
            }
        }

        ListCell* lc = NULL;
        for (lc = list_head(from->fromlist); lc != NULL; lc = lnext(lc)) {
            if (lc == list_head(from->fromlist)) {
                continue;
            }
            Node* item = (Node*)lfirst(lc);
            OptExpression* item_expr = translate_from_item(root, parse, item);
            int item_varno = 0;
            if (IsA(item, RangeTblRef)) {
                item_varno = ((RangeTblRef*)item)->rtindex;
            } else {
                Relids item_relids = pull_varnos(item, 0, false);
                if (item_relids != nullptr && bms_num_members(item_relids) == 1) {
                    bms_get_singleton_member(item_relids, &item_varno);
                }
                bms_free(item_relids);
            }
            if (result != nullptr && item_expr != nullptr) {
                LogicalInnerJoin* join_op = NEW_EX(LogicalInnerJoin);
                if (last_varno > 0 && item_varno > 0 && from->quals != nullptr) {
                    JoinKeyPairs keys = extract_join_keys_from_quals_ex(
                        from->quals, last_varno, item_varno);
                    if (!keys.left_keys.empty()) {
                        join_op->set_join_keys(keys.left_keys, keys.right_keys);
                    }
                }
                result = OptExpression::create(join_op, result, item_expr);
            } else {
                result = item_expr;
            }
            last_varno = item_varno;
        }

        return result;
    } else if (IsA(jointree, JoinExpr)) {
        JoinExpr* join = (JoinExpr*)jointree;
        return translate_join(root, parse, join);
    }

    return nullptr;
}

OptExpression* QueryToExprConverter::translate_from_item(PlannerInfo* root, Query* parse, Node* node) {
    if (node == nullptr) {
        return nullptr;
    }

    if (IsA(node, RangeTblRef)) {
        return translate_scan(root, parse, (RangeTblRef*)node);
    } else if (IsA(node, JoinExpr)) {
        return translate_join(root, parse, (JoinExpr*)node);
    } else if (IsA(node, FromExpr)) {
        return translate_from(root, parse, node);
    }

    return nullptr;
}

OptExpression* QueryToExprConverter::translate_join(PlannerInfo* root, Query* parse, JoinExpr* join) {
    if (join == nullptr) {
        return nullptr;
    }

    (void)parse;  // unused for now

    OptExpression* left = translate_from_item(root, parse, join->larg);
    OptExpression* right = translate_from_item(root, parse, join->rarg);

    // Determine join type - use OpenGauss JoinType directly
    JoinType join_type = JOIN_INNER;
    if (join->jointype == JOIN_INNER) {
        join_type = JOIN_INNER;
    } else if (join->jointype == JOIN_LEFT) {
        join_type = JOIN_LEFT;
    } else if (join->jointype == JOIN_RIGHT) {
        join_type = JOIN_RIGHT;
    } else if (join->jointype == JOIN_FULL) {
        join_type = JOIN_FULL;
    }

    LogicalInnerJoin* join_op = NEW_EX(LogicalInnerJoin);
    join_op->set_join_type(join_type);

    // Set join qualifier
    Node* quals = join->quals;
    if (quals != nullptr) {
        join_op->set_qual(quals);
    }

    // Extract join keys
    if (join->usingClause != nullptr) {
        std::vector<int> keys = extract_join_keys_from_using((Node*)join->usingClause);
        if (!keys.empty()) {
            join_op->set_join_keys(keys, keys);
        }
    } else if (quals != nullptr) {
        // Find left and right varno to distinguish Vars by side
        int left_varno = 0;
        int right_varno = 0;
        if (IsA(join->larg, RangeTblRef)) {
            left_varno = ((RangeTblRef*)join->larg)->rtindex;
        } else {
            Relids left_relids = pull_varnos(join->larg, 0, false);
            if (left_relids != nullptr && bms_num_members(left_relids) == 1) {
                bms_get_singleton_member(left_relids, &left_varno);
            }
            bms_free(left_relids);
        }
        if (IsA(join->rarg, RangeTblRef)) {
            right_varno = ((RangeTblRef*)join->rarg)->rtindex;
        } else {
            Relids right_relids = pull_varnos(join->rarg, 0, false);
            if (right_relids != nullptr && bms_num_members(right_relids) == 1) {
                bms_get_singleton_member(right_relids, &right_varno);
            }
            bms_free(right_relids);
        }
        if (left_varno > 0 && right_varno > 0) {
            JoinKeyPairs keys = extract_join_keys_from_quals_ex(quals, left_varno, right_varno);
            if (!keys.left_keys.empty()) {
                join_op->set_join_keys(keys.left_keys, keys.right_keys);
            }
        }
    }

    OptExpression* result = OptExpression::create(join_op, left, right);
    return result;
}

OptExpression* QueryToExprConverter::translate_scan(PlannerInfo* root, Query* parse, RangeTblRef* rtr) {
    if (rtr == nullptr || parse->rtable == nullptr) {
        return nullptr;
    }

    // rtindex is 1-based in PostgreSQL/openGauss, list_nth is 0-based
    int rtindex = rtr->rtindex;
    if (rtindex <= 0 || rtindex > list_length(parse->rtable)) {
        return nullptr;
    }

    RangeTblEntry* rte = (RangeTblEntry*)list_nth(parse->rtable, rtindex - 1);

    // Debug: print rte-inh (inheritance flag)
    ereport(WARNING, (errmsg("[CASCADES] translate_scan: rtindex=%d, rtekind=%d, relid=%u, inh=%d",
                             rtindex, rte->rtekind, rte->relid, rte->inh)));

    if (rte->relid != InvalidOid) {
        LogicalScan* scan_op = NEW_EX(LogicalScan, rtindex);
        scan_op->set_relid(rte->relid);  // Store table OID for statistics derivation
        if (root != nullptr) {
            // Find the existing RelOptInfo (already created by add_base_rels_to_query)
            RelOptInfo* rel = find_base_rel(root, rtindex);
            if (rel != nullptr) {
                scan_op->set_rel(rel);

                // Initialize reltarget with all table columns from rel->max_attr
                // reltarget is used by physical implementation (ScanImplRule) for pathtarget
                if (rel->reltarget == nullptr) {
                    rel->reltarget = create_empty_pathtarget();
                }
                if (rel->max_attr > 0) {
                    for (int att = 1; att <= rel->max_attr; att++) {
                        Oid atttype;
                        int32 atttypmod;
                        Oid attcollid;
                        get_atttypetypmodcoll(rte->relid, att, &atttype, &atttypmod, &attcollid);
                        Var* var = makeVar(rtindex, att, atttype, atttypmod, attcollid, 0);
                        rel->reltarget->exprs = lappend(rel->reltarget->exprs, var);
                        rel->reltarget->width += 32;
                    }
                }

                // Initialize output_columns_ with all table columns
                // This is the LOGICAL output columns, independent of reltarget
                // ColumnPruneRule will modify reltarget but output_columns_ tracks logical output
                ColumnRefSet all_columns;
                for (int att = 1; att <= rel->max_attr; att++) {
                    all_columns.add(att);
                }
                scan_op->set_output_columns(all_columns);

                ereport(WARNING, (errmsg("[CASCADES] translate_scan: table_id=%d, output_columns=%ld, reltarget=%d",
                                     rtindex, (long)scan_op->get_output_columns().size(),
                                     rel->reltarget ? list_length(rel->reltarget->exprs) : 0)));

                // Directly attach restriction clauses to LogicalScan instead of wrapping with LogicalFilter
                // This avoids a Filter->Scan pattern that would be immediately eliminated by PredicatePushDownRule
                if (rel->baserestrictinfo != NIL) {
                    scan_op->set_qual_list(rel->baserestrictinfo);
                }
            }
        }
        OptExpression* result = OptExpression::create(scan_op);
        return result;
    } else if (rte->rtekind == RTE_SUBQUERY) {
        /* Temporarily not supported: subqueries cause fallback to standard planner */
        ereport(WARNING, (errmsg("[CASCADES] translate_scan: subquery not supported, returning nullptr")));
        return nullptr;
    }

    return nullptr;
}

OptExpression* QueryToExprConverter::translate_target_list(PlannerInfo* root, Query* parse, OptExpression* child) {
    List* targetList = parse->targetList;
    if (targetList == nullptr || targetList == NIL) {
        return child;
    }

    ereport(WARNING, (errmsg("[CASCADES] translate_target_list: targetList length=%d", list_length(targetList))));

    // Debug: print first TargetEntry type
    if (list_length(targetList) > 0) {
        TargetEntry* tle = (TargetEntry*)linitial(targetList);
        ereport(WARNING, (errmsg("[CASCADES] translate_target_list: first TargetEntry=%p, expr=%p, expr->type=%d",
                             (void*)tle, (void*)tle->expr, tle->expr ? (int)((Node*)tle->expr)->type : -1)));
    }

    LogicalProject* project_op = NEW_EX(LogicalProject, targetList);

    if (child != nullptr) {
        OptExpression* result = OptExpression::create(project_op, child);
        return result;
    }
    OptExpression* result = OptExpression::create(project_op);
    return result;
}

std::vector<int> QueryToExprConverter::extract_join_keys_from_using(Node* usingClause) {
    std::vector<int> keys;

    if (usingClause == nullptr) {
        return keys;
    }

    List* list = (List*)usingClause;
    ListCell* lc = NULL;
    foreach(lc, list) {
        Node* node = (Node*)lfirst(lc);
        if (IsA(node, String)) {
            // Column name in USING clause - would need column resolution
            // For now, just return empty keys
        }
    }

    return keys;
}

std::vector<int> QueryToExprConverter::extract_join_keys_from_quals(Node* quals) {
    std::vector<int> keys;

    if (quals == nullptr) {
        return keys;
    }

    if (IsA(quals, OpExpr)) {
        OpExpr* op = (OpExpr*)quals;
        if (list_length(op->args) == 2) {
            Node* left = (Node*)linitial(op->args);
            Node* right = (Node*)lsecond(op->args);

            if (IsA(left, Var)) {
                keys.push_back(((Var*)left)->varattno);
            }
            if (IsA(right, Var)) {
                keys.push_back(((Var*)right)->varattno);
            }
        }
    } else if (IsA(quals, BoolExpr)) {
        BoolExpr* bool_expr = (BoolExpr*)quals;
        ListCell* lc = NULL;
        foreach(lc, bool_expr->args) {
            Node* arg = (Node*)lfirst(lc);
            std::vector<int> sub_keys = extract_join_keys_from_quals(arg);
            keys.insert(keys.end(), sub_keys.begin(), sub_keys.end());
        }
    }

    return keys;
}

JoinKeyPairs QueryToExprConverter::extract_join_keys_from_quals_ex(
    Node* quals, int left_varno, int right_varno)
{
    JoinKeyPairs result;
    if (quals == nullptr) {
        return result;
    }

    auto process_eq_expr = [&](Node* expr) {
        if (!IsA(expr, OpExpr)) {
            return;
        }
        OpExpr* op = (OpExpr*)expr;
        if (list_length(op->args) != 2) {
            return;
        }
        Node* arg1 = (Node*)linitial(op->args);
        Node* arg2 = (Node*)lsecond(op->args);

        Var* left_var = nullptr;
        Var* right_var = nullptr;

        // Classify each Var by its varno
        if (IsA(arg1, Var) && ((Var*)arg1)->varno == (Index)left_varno) {
            left_var = (Var*)arg1;
        }
        if (IsA(arg2, Var) && ((Var*)arg2)->varno == (Index)right_varno) {
            right_var = (Var*)arg2;
        }
        // Handle swapped order: right = left
        if (left_var == nullptr && IsA(arg1, Var) && ((Var*)arg1)->varno == (Index)right_varno) {
            right_var = (Var*)arg1;
        }
        if (right_var == nullptr && IsA(arg2, Var) && ((Var*)arg2)->varno == (Index)left_varno) {
            left_var = (Var*)arg2;
        }

        if (left_var != nullptr && right_var != nullptr) {
            result.left_keys.push_back(left_var->varattno);
            result.right_keys.push_back(right_var->varattno);
        }
    };

    // Recurse into List or BoolExpr (AND tree)
    if (IsA(quals, List)) {
        ListCell* lc = NULL;
        foreach(lc, (List*)quals) {
            process_eq_expr((Node*)lfirst(lc));
        }
    } else if (IsA(quals, BoolExpr)) {
        ListCell* lc = NULL;
        foreach(lc, ((BoolExpr*)quals)->args) {
            process_eq_expr((Node*)lfirst(lc));
        }
    } else if (IsA(quals, OpExpr)) {
        process_eq_expr(quals);
    }

    return result;
}

/*
 * check_sublink_in_node - Check if a node tree contains any SubLink
 *
 * Used to detect subqueries in WHERE clause (e.g., WHERE age > (SELECT avg(age)...))
 */
bool check_sublink_in_node(Node* node) {
    if (node == nullptr) {
        return false;
    }

    if (IsA(node, SubLink)) {
        return true;
    }

    if (IsA(node, List)) {
        List* list = (List*)node;
        ListCell* lc = NULL;
        foreach (lc, list) {
            if (check_sublink_in_node((Node*)lfirst(lc))) {
                return true;
            }
        }
        return false;
    }

    /* Recurse into common node types that may contain SubLink */
    if (IsA(node, BoolExpr)) {
        BoolExpr* bool_expr = (BoolExpr*)node;
        return check_sublink_in_node((Node*)bool_expr->args);
    }

    if (IsA(node, FuncExpr)) {
        FuncExpr* func = (FuncExpr*)node;
        return check_sublink_in_node((Node*)func->args);
    }

    if (IsA(node, OpExpr)) {
        OpExpr* op = (OpExpr*)node;
        return check_sublink_in_node((Node*)op->args);
    }

    if (IsA(node, SubPlan)) {
        return true;
    }

    return false;
}

/*
 * check_subquery_in_fromlist - Check if fromlist contains any subqueries
 *
 * Used to detect inline views (e.g., SELECT * FROM (SELECT ...) t)
 */
bool check_subquery_in_fromlist(List* fromlist) {
    if (fromlist == nullptr) {
        return false;
    }

    ListCell* lc = NULL;
    foreach (lc, fromlist) {
        Node* item = (Node*)lfirst(lc);
        if (item == nullptr) {
            continue;
        }

        if (IsA(item, RangeSubselect)) {
            return true;
        }

        if (IsA(item, JoinExpr)) {
            JoinExpr* join = (JoinExpr*)item;
            if (check_subquery_in_fromlist(list_make1(join->larg)) ||
                check_subquery_in_fromlist(list_make1(join->rarg))) {
                return true;
            }
        }

        if (IsA(item, FromExpr)) {
            FromExpr* from = (FromExpr*)item;
            if (check_subquery_in_fromlist(from->fromlist)) {
                return true;
            }
        }
    }

    return false;
}

}  // namespace cascades