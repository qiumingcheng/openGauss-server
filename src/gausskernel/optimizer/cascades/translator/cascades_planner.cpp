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
    ereport(LOG, (errmsg("[CASCADES] >>> Entering cascades_planner, enable_cascades_optimizer=%d",
                         u_sess->attr.attr_sql.enable_cascades_optimizer)));

    // Debug: check quals at entry
    if (parse != nullptr && parse->jointree != nullptr) {
        FromExpr* f = (FromExpr*)parse->jointree;
        Node* quals = f->quals;
        ereport(WARNING, (errmsg("[CASCADES] ENTRY: quals=%p, nodeType=%d", (void*)quals, quals ? ((Node*)quals)->type : -1)));
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

    // Debug: check baserestrictinfo after preprocess
    if (parse->jointree != nullptr && IsA(parse->jointree, FromExpr)) {
        FromExpr* f = (FromExpr*)parse->jointree;
        ereport(WARNING, (errmsg("[CASCADES] After preprocess: f->quals type=%d, isList=%d",
                             (int)((Node*)f->quals)->type, IsA(f->quals, List))));
        if (IsA(f->quals, List)) {
            ereport(WARNING, (errmsg("[CASCADES] After preprocess: f->quals length=%d",
                                 list_length((List*)f->quals))));
            // Print each qual's type
            int idx = 0;
            ListCell* lc;
            foreach(lc, (List*)f->quals) {
                Node* qual = (Node*)lfirst(lc);
                ereport(WARNING, (errmsg("[CASCADES] After preprocess: qual[%d] type=%d", idx++, (int)qual->type)));
            }
        } else if (f->quals != nullptr) {
            ereport(WARNING, (errmsg("[CASCADES] After preprocess: f->quals is not List, type=%d",
                                 (int)((Node*)f->quals)->type)));
        } else {
            ereport(WARNING, (errmsg("[CASCADES] After preprocess: f->quals is NULL")));
        }
    }

    // Check baserestrictinfo before deconstruct
    int rtable_len = list_length(parse->rtable);
    for (int i = 1; i <= rtable_len && i < root->simple_rel_array_size; i++) {
        RelOptInfo* rel = root->simple_rel_array[i];
        if (rel != nullptr) {
            ereport(WARNING, (errmsg("[CASCADES] Before deconstruct: rtindex=%d, baserestrictinfo len=%d",
                                 i, list_length(rel->baserestrictinfo))));
        }
    }

    // Step 6: Distribute WHERE quals to baserestrictinfo lists
    ereport(WARNING, (errmsg("[CASCADES] Before deconstruct_jointree: join_info_list length=%d",
                         list_length(root->join_info_list))));
    Relids non_keypreserved = NULL;
    deconstruct_jointree(root, &non_keypreserved);
    bms_free(non_keypreserved);
    ereport(WARNING, (errmsg("[CASCADES] After deconstruct_jointree: join_info_list length=%d",
                         list_length(root->join_info_list))));

    /*
     * Generate implied equalities from equivalence classes.
     * This is required because mergejoinable equality clauses (like age=30)
     * are processed by process_equivalence() during deconstruct_jointree,
     * which stores them in eq_classes and returns early without adding to
     * baserestrictinfo. This function generates the actual restriction clauses
     * from the equivalence classes.
     */
    ereport(WARNING, (errmsg("[CASCADES] Before generate_base_implied_equalities: eq_classes length=%d",
                         list_length(root->eq_classes))));
    generate_base_implied_equalities(root);
    ereport(WARNING, (errmsg("[CASCADES] After generate_base_implied_equalities: eq_classes length=%d",
                         list_length(root->eq_classes))));

    generate_base_implied_qualities(root);

    // Debug: check baserestrictinfo after generate_base_implied_equalities
    for (int i = 1; i <= rtable_len && i < root->simple_rel_array_size; i++) {
        RelOptInfo* rel = root->simple_rel_array[i];
        if (rel != nullptr) {
            ereport(WARNING, (errmsg("[CASCADES] After generate_base_implied_equalities: rtindex=%d, baserestrictinfo len=%d",
                                 i, list_length(rel->baserestrictinfo))));
        }
    }

    /* Step 1: Query → OptExpression */
    cascades::QueryToExprConverter query_converter;
    cascades::OptExpression* opt_expr = query_converter.convert(root, parse);

    if (opt_expr == nullptr) {
        /* Fallback to standard planner */
        ereport(WARNING, (errmsg("Cascades optimizer: Query conversion failed, falling back to standard planner")));
        return standard_planner(parse, cursorOptions, boundParams);
    }

    ereport(WARNING, (errmsg("[CASCADES] >>> Query conversion succeeded, starting optimization")));

    /* Step 2: Cascades optimization */
    cascades::PhysicalPropertySet required_prop;
    cascades::ColumnRefSet required_cols;

    ereport(WARNING, (errmsg("[CASCADES] >>> Creating CascadesOptimizer")));
    cascades::CascadesOptimizer optimizer(root);
    ereport(WARNING, (errmsg("[CASCADES] >>> Calling optimizer.optimize()")));
    cascades::OptExpression* optimized_expr = optimizer.optimize(opt_expr, required_prop, &required_cols);
    ereport(WARNING, (errmsg("[CASCADES] >>> optimize() returned, optimized_expr=%p", (void*)optimized_expr)));

    if (optimized_expr == nullptr) {
        ereport(WARNING, (errmsg("Cascades optimizer: optimization failed, falling back to standard planner")));
        return standard_planner(parse, cursorOptions, boundParams);
    }

    ereport(WARNING, (errmsg("[CASCADES] >>> Optimization succeeded, converting to plan")));

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