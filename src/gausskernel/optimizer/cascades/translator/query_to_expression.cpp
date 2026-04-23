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

namespace cascades {

OptExpression* QueryToExprConverter::convert(PlannerInfo* root, Query* parse) {
    if (parse == nullptr) {
        return nullptr;
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
    // Note: translate_scan now creates LogicalFilter on top of scans with baserestrictinfo
    OptExpression* from_expr = nullptr;
    if (parse->jointree != nullptr) {
        from_expr = translate_from(root, parse, (Node*)parse->jointree);
    }

    // 2. WHERE clause handling
    // Predicates from baserestrictinfo are already attached to scans by translate_scan
    // For join quals, they are handled by translate_join
    // So we don't need to create an additional filter here
    OptExpression* filter_expr = from_expr;

    // 3. Convert SELECT LIST to Project
    OptExpression* project_expr = translate_target_list(root, parse, filter_expr);

    // 4. Handle ORDER BY
    OptExpression* result = project_expr ? project_expr : filter_expr;
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
        // Create LogicalLimit operator
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

        if (list_length(from->fromlist) > 0) {
            Node* first_item = (Node*)linitial(from->fromlist);
            result = translate_from_item(root, parse, first_item);
        }

        ListCell* lc = NULL;
        for (lc = list_head(from->fromlist); lc != NULL; lc = lnext(lc)) {
            if (lc == list_head(from->fromlist)) {
                continue;
            }
            Node* item = (Node*)lfirst(lc);
            OptExpression* item_expr = translate_from_item(root, parse, item);
            if (result != nullptr && item_expr != nullptr) {
                LogicalInnerJoin* join_op = NEW_EX(LogicalInnerJoin);
                result = OptExpression::create(join_op, result, item_expr);
            } else {
                result = item_expr;
            }
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
        join_op->set_join_keys(keys, keys);
    } else if (quals != nullptr) {
        std::vector<int> keys = extract_join_keys_from_quals(quals);
        if (!keys.empty()) {
            join_op->set_join_keys(keys, keys);
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

    if (rte->relid != InvalidOid) {
        LogicalScan* scan_op = NEW_EX(LogicalScan, rtindex);
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
                        Oid atttype = get_atttype(rte->relid, att);
                        Var* var = makeVar(rtindex, att, atttype, -1, InvalidOid, 0);
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

                ereport(WARNING, (errmsg("[CASCADES] translate_scan: table_id=%d, output_columns=%d, reltarget=%d",
                                     rtindex, scan_op->get_output_columns().size(),
                                     rel->reltarget ? list_length(rel->reltarget->exprs) : 0)));

                // If there are restriction clauses, create a LogicalFilter on top of the scan
                // This handles multiple predicates from baserestrictinfo
                if (rel->baserestrictinfo != NIL) {
                    // Create predicate_ids list from baserestrictinfo
                    // Each RestrictInfo contains a clause that needs to be applied
                    // For now, we create a filter with the full baserestrictinfo list
                    // The predicate_id mechanism will be used for tracking during optimization

                    LogicalFilter* filter_op = NEW_EX(LogicalFilter);
                    filter_op->set_qual_list(rel->baserestrictinfo);

                    // Create predicate_ids: for simplicity, use index 0 to indicate "use baserestrictinfo"
                    // In a full implementation, each clause would have its own predicate_id
                    List* predicate_ids = nullptr;
                    int idx = 0;
                    for (ListCell* cell = list_head(rel->baserestrictinfo); cell != nullptr; cell = lnext(cell)) {
                        predicate_ids = lappend_int(predicate_ids, idx++);
                    }
                    filter_op->set_predicate_ids(predicate_ids);

                    OptExpression* scan_expr = OptExpression::create(scan_op);
                    OptExpression* result = OptExpression::create(filter_op, scan_expr);
                    return result;
                }
            }
        }
        OptExpression* result = OptExpression::create(scan_op);
        return result;
    } else if (rte->rtekind == RTE_SUBQUERY) {
        // Handle subquery RTE
        // Note: We do NOT expand the subquery here. The subquery will be planned by
        // SubqueryScanImplRule which calls subquery_planner() to create proper RelOptInfo.
        if (rte->subquery != nullptr) {
            // Create LogicalSubqueryScan operator with the subquery
            // rel_ will be set by SubqueryScanImplRule after subquery_planner() runs
            LogicalSubqueryScan* subquery_scan = NEW_EX(LogicalSubqueryScan, rtindex, rte->subquery);

            OptExpression* scan_expr = OptExpression::create(subquery_scan);
            return scan_expr;
        }
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

void QueryToExprConverter::derive_statistics(OptExpression* expr, RelOptInfo* rel) {
    if (expr == nullptr || rel == nullptr) {
        return;
    }

    Statistics* stats = NEW_EX(Statistics);
    stats->set_row_count(rel->rows);
    stats->set_width(rel->reltarget ? rel->reltarget->width : 0);

    expr->set_statistics(stats);
}

}  // namespace cascades