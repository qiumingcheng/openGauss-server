#pragma once

#include <vector>
#include <stddef.h>
#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "nodes/nodes.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"

namespace cascades {

class OptExpression;

/**
 * QueryToExprConverter - Converts OpenGauss Query parse tree to Cascades OptExpression tree
 *
 * This translator handles:
 * - SELECT statement conversion
 * - FROM clause translation (scan, join, fromexpr)
 * - WHERE clause translation to Filter operators
 * - Target list translation to Project operators
 * - Join key extraction from USING clause and qualifiers
 */
class QueryToExprConverter {
public:
    QueryToExprConverter() = default;
    ~QueryToExprConverter() = default;

    // Convert Query to OptExpression tree
    OptExpression* convert(PlannerInfo* root, Query* parse);

private:
    // Translation helpers
    OptExpression* translate_select(PlannerInfo* root, Query* parse);
    OptExpression* translate_from(PlannerInfo* root, Query* parse, Node* jointree);
    OptExpression* translate_from_item(PlannerInfo* root, Query* parse, Node* node);
    OptExpression* translate_join(PlannerInfo* root, Query* parse, JoinExpr* join);
    OptExpression* translate_scan(PlannerInfo* root, Query* parse, RangeTblRef* rtr);
    OptExpression* translate_target_list(PlannerInfo* root, Query* parse, OptExpression* child);
    std::vector<int> extract_join_keys_from_using(Node* usingClause);
    std::vector<int> extract_join_keys_from_quals(Node* quals);
    void derive_statistics(OptExpression* expr, RelOptInfo* rel);

    // Member variables
    PlannerInfo* root_ = nullptr;
    Query* parse_ = nullptr;
};

}
