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
#include "optimizer/prep.h"
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

    /* Setup optimizer data structures and preprocess query */
    setup_cascades_optimizer(root, parse);

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

/*
 * setup_cascades_optimizer
 *
 * Initialize optimizer data structures and preprocess the query.
 * This is equivalent to the preprocessing done by standard_planner
 * before query optimization.
 *
 * Steps:
 * 1. setup_simple_rel_arrays() - initialize simple_rel_array and simple_rte_array
 * 2. expand_inherited_tables() - expand inherited tables (fixes rte->inh issue)
 * 3. add_base_rels_to_query() - create RelOptInfo for all base relations
 * 4. preprocess_qual_conditions() - preprocess qual conditions
 * 5. deconstruct_jointree() - distribute WHERE quals to baserestrictinfo
 * 6. generate_base_implied_equalities() - generate implied equalities
 */
void setup_cascades_optimizer(PlannerInfo* root, Query* parse)
{
    /* Initialize simple_rel_array and simple_rte_array */
    setup_simple_rel_arrays(root);

    /* Expand inherited tables - must be done before add_base_rels_to_query */
    expand_inherited_tables(root);

    /* Create RelOptInfo for all base relations in the jointree */
    Bitmapset* checkDuplicate = NULL;
    add_base_rels_to_query(root, (Node*)parse->jointree, &checkDuplicate);
    bms_free(checkDuplicate);

    /* Preprocess qual conditions (convert single qual node to List) */
    preprocess_qual_conditions(root, (Node*)parse->jointree);

    /* Safety check: ensure f->quals is a List before deconstruct_jointree */
    if (parse->jointree != nullptr && IsA(parse->jointree, FromExpr)) {
        FromExpr* f = (FromExpr*)parse->jointree;
        if (f->quals != nullptr && !IsA(f->quals, List)) {
            ereport(WARNING, (errmsg("[CASCADES] setup_cascades_optimizer: f->quals is not a List! type=%d",
                                 (int)((Node*)f->quals)->type)));
            f->quals = (Node*)list_make1(f->quals);
        }
    }

    /* Distribute WHERE quals to baserestrictinfo lists */
    Relids non_keypreserved = NULL;
    deconstruct_jointree(root, &non_keypreserved);
    bms_free(non_keypreserved);

    /* Generate implied equalities from equivalence classes */
    generate_base_implied_equalities(root);
    generate_base_implied_qualities(root);
}