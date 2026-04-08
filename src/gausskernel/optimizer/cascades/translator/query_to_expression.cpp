#include "query_to_expression.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../statistics/statistics.h"
#include "optimizer/planmain.h"
#include "optimizer/pathnode.h"
#include "nodes/relation.h"
#include "nodes/makefuncs.h"

namespace cascades {

OptExpression* QueryToExprConverter::convert(PlannerInfo* root, Query* parse) {
    if (parse == nullptr) {
        return nullptr;
    }

    root_ = root;
    parse_ = parse;

    // Convert SELECT statement
    OptExpression* result = translate_select(root, parse);

    return result;
}

OptExpression* QueryToExprConverter::translate_select(PlannerInfo* root, Query* parse) {
    // 1. Convert FROM clause
    OptExpression* from_expr = nullptr;
    if (parse->jointree != nullptr) {
        from_expr = translate_from(root, parse, (Node*)parse->jointree);
    }

    // 2. Convert WHERE clause to Filter
    OptExpression* filter_expr = nullptr;
    if (parse->jointree != nullptr && parse->jointree->quals != nullptr) {
        LogicalFilter* filter_op = new LogicalFilter(parse->jointree->quals);
        if (from_expr != nullptr) {
            filter_expr = OptExpression::create(filter_op, from_expr);
        } else {
            filter_expr = OptExpression::create(filter_op);
        }
    } else {
        filter_expr = from_expr;
    }

    // 3. Convert SELECT LIST to Project
    OptExpression* project_expr = translate_target_list(root, parse, filter_expr);

    return project_expr ? project_expr : filter_expr;
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
                LogicalInnerJoin* join_op = new LogicalInnerJoin();
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

    LogicalInnerJoin* join_op = new LogicalInnerJoin();
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

    int rtindex = rtr->rtindex;
    if (rtindex < 0 || rtindex >= list_length(parse->rtable)) {
        return nullptr;
    }

    RangeTblEntry* rte = (RangeTblEntry*)list_nth(parse->rtable, rtindex);

    if (rte->relid != InvalidOid) {
        LogicalScan* scan_op = new LogicalScan(rtindex);
        if (root != nullptr) {
            RelOptInfo* rel = find_base_rel(root, rtindex);
            if (rel != nullptr) {
                scan_op->set_rel(rel);
            }
        }
        return OptExpression::create(scan_op);
    } else if (rte->subquery != nullptr) {
        return translate_select(root, rte->subquery);
    }

    return nullptr;
}

OptExpression* QueryToExprConverter::translate_target_list(PlannerInfo* root, Query* parse, OptExpression* child) {
    List* targetList = parse->targetList;
    if (targetList == nullptr || targetList == NIL) {
        return child;
    }

    LogicalProject* project_op = new LogicalProject(targetList);

    if (child != nullptr) {
        return OptExpression::create(project_op, child);
    }
    return OptExpression::create(project_op);
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

    Statistics* stats = new Statistics();
    stats->set_row_count(rel->rows);
    stats->set_width(rel->reltarget ? rel->reltarget->width : 0);

    expr->set_statistics(stats);
}

}  // namespace cascades