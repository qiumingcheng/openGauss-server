/*
 * cascades_planner.cpp
 * Cascades optimizer entry point
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/pathnode.h"
#include "parser/parsetree.h"
#include "utils/guc.h"

#include "query_to_expression.h"
#include "expression_to_plan.h"
#include "../optimizer/cascades_optimizer.h"
#include "../property/physical_property_set.h"
#include "../memo/opt_expression.h"

PlannedStmt* cascades_planner(Query* parse, int cursorOptions, ParamListInfo boundParams)
{
    if (parse != nullptr && parse->jointree != nullptr) {
        FromExpr* f = (FromExpr*)parse->jointree;
        ereport(WARNING, (errmsg("[CASCADES] ENTRY: quals type=%d",
                             f->quals ? ((Node*)f->quals)->type : -1)));
    }

    PlannedStmt* result = NULL;
    PlannerGlobal* glob = NULL;
    PlannerInfo* root = NULL;

    /* Build PlannerInfo similar to standard_planner */
    glob = makeNode(PlannerGlobal);
    glob->boundParams = boundParams;
    glob->subplans = NIL;
    glob->subroots = NIL;
    glob->rewindPlanIDs = NULL;
    glob->finalrtable = parse->rtable;
    glob->finalrowmarks = NIL;
    glob->resultRelations = NIL;
    glob->relationOids = NIL;
    glob->invalItems = NIL;
    glob->nParamExec = 0;
    glob->lastPHId = 0;
    glob->lastRowMarkId = 0;
    glob->transientPlan = false;
    glob->dependsOnRole = false;
    glob->insideRecursion = false;
    glob->estiopmem = u_sess->opt_cxt.op_work_mem;

    /* Build PlannerInfo */
    root = makeNode(PlannerInfo);
    root->glob = glob;
    root->parse = parse;
    root->query_level = 1;
    root->tuple_fraction = 0.0;

    /* Initialize subquery_planner data */
    root->planner_cxt = CurrentMemoryContext;

    /* Initialize simple_rel_array and simple_rte_array */
    setup_simple_rel_arrays(root);

    /*
     * Deconstruct the jointree to distribute WHERE quals to baserestrictinfo lists.
     * This is the same process used by the standard planner.
     *
     * Standard planner flow (planmain.cpp):
     * 1. setup_simple_rel_arrays() - initialize arrays
     * 2. add_base_rels_to_query() - create RelOptInfo for all base relations
     * 3. build_base_rel_tlists() - add targetlist entries
     * 4. find_placeholders_in_jointree() - find PlaceHolderVars
     * 5. find_lateral_references() - find lateral references
     * 6. deconstruct_jointree() - distribute quals to baserestrictinfo
     *
     * For Cascades, we only need steps 1, 2, and 6 for basic queries.
     */

    // Step 2: Create RelOptInfo for all base relations in the jointree
    Bitmapset* checkDuplicate = NULL;
    add_base_rels_to_query(root, (Node*)parse->jointree, &checkDuplicate);
    bms_free(checkDuplicate);

    // Preprocess qual conditions (convert single qual node to List)
    preprocess_qual_conditions(root, (Node*)parse->jointree);

    // Debug: check f->quals after preprocess
    if (parse->jointree != nullptr && IsA(parse->jointree, FromExpr)) {
        FromExpr* f = (FromExpr*)parse->jointree;
        if (f->quals == nullptr) {
            ereport(WARNING, (errmsg("[CASCADES] After preprocess: f->quals is NULL")));
        } else if (IsA(f->quals, List)) {
            StringInfoData qual_types;
            initStringInfo(&qual_types);
            int idx = 0;
            ListCell* lc;
            foreach(lc, (List*)f->quals) {
                Node* qual = (Node*)lfirst(lc);
                if (idx > 0)
                    appendStringInfoString(&qual_types, ", ");
                appendStringInfo(&qual_types, "qual[%d]=%d", idx++, (int)qual->type);
            }
            ereport(WARNING, (errmsg("[CASCADES] After preprocess: f->quals is List, length=%d, %s",
                                 list_length((List*)f->quals), qual_types.data)));
            pfree(qual_types.data);
        } else {
            ereport(WARNING, (errmsg("[CASCADES] After preprocess: f->quals is not List, type=%d",
                                 (int)((Node*)f->quals)->type)));
        }
    }

    // Step 6: Distribute WHERE quals to baserestrictinfo lists
    // Safety check: ensure f->quals is a List before deconstruct_jointree
    if (parse->jointree != nullptr && IsA(parse->jointree, FromExpr)) {
        FromExpr* f = (FromExpr*)parse->jointree;
        if (f->quals != nullptr && !IsA(f->quals, List)) {
            ereport(WARNING, (errmsg("[CASCADES] ERROR: f->quals is not a List! type=%d",
                                 (int)((Node*)f->quals)->type)));
            f->quals = (Node*)list_make1(f->quals);
        }
    }

    Relids non_keypreserved = NULL;
    deconstruct_jointree(root, &non_keypreserved);
    bms_free(non_keypreserved);

    /*
     * Generate implied equalities from equivalence classes.
     * This is required because mergejoinable equality clauses (like age=30)
     * are processed by process_equivalence() during deconstruct_jointree,
     * which stores them in eq_classes and returns early without adding to
     * baserestrictinfo. This function generates the actual restriction clauses
     * from the equivalence classes.
     */
    generate_base_implied_equalities(root);
    generate_base_implied_qualities(root);

    /* Step 1: Query → OptExpression */
    cascades::QueryToExprConverter query_converter;
    cascades::OptExpression* opt_expr = query_converter.convert(root, parse);

    if (opt_expr == nullptr) {
        /* Fallback to standard planner */
        ereport(WARNING, (errmsg("Cascades optimizer: Query conversion failed, falling back to standard planner")));
        return standard_planner(parse, cursorOptions, boundParams);
    }

    ereport(WARNING, (errmsg("[CASCADES] Query conversion succeeded, starting optimization")));

    cascades::PhysicalPropertySet required_prop;
    cascades::ColumnRefSet required_cols;

    cascades::CascadesOptimizer optimizer(root);
    cascades::OptExpression* optimized_expr = optimizer.optimize(opt_expr, required_prop, &required_cols);

    if (optimized_expr == nullptr) {
        ereport(WARNING, (errmsg("Cascades optimizer: optimization failed, falling back to standard planner")));
        return standard_planner(parse, cursorOptions, boundParams);
    }

    /* Step 3: OptExpression -> PlannedStmt */
    cascades::ExprToPlanConverter plan_converter(root);
    result = plan_converter.convert(optimized_expr);

    if (result == nullptr) {
        ereport(WARNING, (errmsg("Cascades optimizer: plan conversion failed, falling back to standard planner")));
        return standard_planner(parse, cursorOptions, boundParams);
    }

    result->utilityStmt = parse->utilityStmt;
    result->commandType = parse->commandType;

    return result;
}