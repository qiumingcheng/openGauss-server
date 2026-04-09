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
#include "parser/parsetree.h"
#include "utils/guc.h"

#include "query_to_expression.h"
#include "expression_to_plan.h"
#include "../optimizer/cascades_optimizer.h"
#include "../property/physical_property_set.h"
#include "../memo/opt_expression.h"

/*
 * GUC parameter to enable/disable Cascades optimizer
 * Default is off to maintain compatibility with standard planner
 */
bool enable_cascades_optimizer = false;

PlannedStmt* cascades_planner(Query* parse, int cursorOptions, ParamListInfo boundParams)
{
    ereport(LOG, (errmsg("[CASCADES] >>> Entering cascades_planner, enable_cascades_optimizer=%d",
                         enable_cascades_optimizer)));

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

    /* Step 1: Query → OptExpression */
    cascades::QueryToExprConverter query_converter;
    cascades::OptExpression* opt_expr = query_converter.convert(root, parse);

    if (opt_expr == nullptr) {
        /* Fallback to standard planner */
        ereport(WARNING, (errmsg("Cascades optimizer: Query conversion failed, falling back to standard planner")));
        return standard_planner(parse, cursorOptions, boundParams);
    }

    ereport(LOG, (errmsg("[CASCADES] >>> Query conversion succeeded, starting optimization")));

    /* Step 2: Cascades optimization */
    cascades::PhysicalPropertySet required_prop;
    cascades::ColumnRefSet required_cols;

    cascades::CascadesOptimizer optimizer(root);
    cascades::OptExpression* optimized_expr = optimizer.optimize(opt_expr, required_prop, &required_cols);

    if (optimized_expr == nullptr) {
        ereport(WARNING, (errmsg("Cascades optimizer: optimization failed, falling back to standard planner")));
        return standard_planner(parse, cursorOptions, boundParams);
    }

    ereport(LOG, (errmsg("[CASCADES] >>> Optimization succeeded, converting to plan")));

    /* Step 3: OptExpression → PlannedStmt */
    cascades::ExprToPlanConverter plan_converter(root);
    result = plan_converter.convert(optimized_expr);

    if (result == nullptr) {
        ereport(WARNING, (errmsg("Cascades optimizer: plan conversion failed, falling back to standard planner")));
        return standard_planner(parse, cursorOptions, boundParams);
    }

    ereport(LOG, (errmsg("[CASCADES] >>> Plan conversion succeeded, returning plan")));

    result->utilityStmt = parse->utilityStmt;
    result->commandType = parse->commandType;

    return result;
}