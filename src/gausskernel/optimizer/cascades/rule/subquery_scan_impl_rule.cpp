#include "../base/memory_utils.h"
#include "subquery_scan_impl_rule.h"
#include "../operator/operators.h"
#include "../operator/operators/logical_subquery_scan.h"
#include "../operator/operators/physical_subquery_scan.h"
#include "../memo/opt_expression.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "nodes/relation.h"
#include "nodes/parsenodes.h"

namespace cascades {

SubqueryScanImplRule* SubqueryScanImplRule::instance() {
    static SubqueryScanImplRule inst;
    return &inst;
}

SubqueryScanImplRule::SubqueryScanImplRule()
    : Rule(RuleType::IMP_SUBQUERY_SCAN,
           Pattern::create(OptExprOpType::LOGICAL_SUBQUERY_SCAN)) {}

std::vector<OptExpression*> SubqueryScanImplRule::transform(
    OptExpression* input, OptimizerContext* context) {

    ereport(WARNING, (errmsg("[CASCADES] SubqueryScanImplRule:transform, input=%s",
                         input->to_string().c_str())));

    LogicalSubqueryScan* scan_op = static_cast<LogicalSubqueryScan*>(input->get_op());
    int rtindex = scan_op->get_rtindex();
    Query* subquery = scan_op->get_subquery();
    RelOptInfo* rel = scan_op->get_rel();

    if (subquery == nullptr || rel == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] SubqueryScanImplRule: subquery or rel is null")));
        return {};
    }

    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] SubqueryScanImplRule: PlannerInfo is null")));
        return {};
    }

    // Plan the subquery
    // Reference: set_subquery_pathlist() in allpaths.cpp:3052
    double tuple_fraction = 0.0;
    PlannerInfo* subroot = nullptr;

    // Recursively plan the subquery
    rel->subplan = subquery_planner(
        root->glob,
        subquery,
        root,
        false,
        tuple_fraction,
        &subroot,
        0  // options
    );
    rel->subroot = subroot;

    if (rel->subplan == nullptr) {
        ereport(WARNING, (errmsg("[CASCADES] SubqueryScanImplRule: subquery planning failed")));
        return {};
    }

    // Convert subquery pathkeys
    List* pathkeys = convert_subquery_pathkeys(root, rel, subroot->query_pathkeys);

    // Create SubqueryScanPath
    Path* subquery_path = create_subqueryscan_path(
        root, rel, pathkeys, rel->lateral_relids, rel->subplan_params
    );

    // Create physical operator
    PhysicalSubqueryScan* physical_scan = NEW_EX(PhysicalSubqueryScan, subquery_path, subroot);
    physical_scan->set_subplan_params(rel->subplan_params);

    OptExpression* new_expr = OptExpression::create(physical_scan);
    ereport(WARNING, (errmsg("[CASCADES] SubqueryScanImplRule: created PhysicalSubqueryScan")));

    return {new_expr};
}

}  // namespace cascades
