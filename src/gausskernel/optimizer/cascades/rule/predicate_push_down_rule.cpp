#include "../base/memory_utils.h"
#include "predicate_push_down_rule.h"
#include "../operator/operators.h"
#include "../operator/operators/logical_filter.h"
#include "../operator/operators/logical_join.h"
#include "../operator/operators/logical_scan.h"
#include "../operator/operators/logical_project.h"
#include "../memo/opt_expression.h"
#include "nodes/relation.h"

namespace cascades {

// Static singleton instance
PredicatePushDownRule* PredicatePushDownRule::instance() {
    static PredicatePushDownRule inst;
    return &inst;
}

PredicatePushDownRule::PredicatePushDownRule()
    : Rule(RuleType::TF_PREDICATE_PUSH_DOWN,
           Pattern::create(OptExprOpType::LOGICAL_FILTER,
                          Pattern::any())) {}

std::vector<OptExpression*> PredicatePushDownRule::transform(
    OptExpression* input, OptimizerContext* context) {
    ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, input=%s",
                         input->to_string().c_str())));

    Assert(input->arity() > 0);

    LogicalFilter* filter = static_cast<LogicalFilter*>(input->get_op());
    OptExpression* child = input->input_at(0);
    OptExprOp* child_op = child->get_op();

    std::vector<OptExpression*> result;

    // Get predicate information from qual_list
    List* qual_list = filter->get_qual_list();

    // Check if we have any predicates to push
    bool has_quals = (qual_list != nullptr && list_length(qual_list) > 0);

    if (!has_quals) {
        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, no qualifiers to push")));
        result.push_back(input);
        return result;
    }

    // Case 1: Push filter down through joins
    if (child_op->get_type() == OptExprOpType::LOGICAL_JOIN) {
        LogicalJoin* join = static_cast<LogicalJoin*>(child_op);
        JoinType join_type = join->get_join_type();

        OptExpression* left_child = child->input_at(0);
        OptExpression* right_child = child->input_at(1);

        if (join_type == JOIN_INNER) {
            // For inner join, push predicates to both sides
            LogicalFilter* left_filter = NEW_EX(LogicalFilter);
            left_filter->set_qual_list(qual_list);

            LogicalFilter* right_filter = NEW_EX(LogicalFilter);
            right_filter->set_qual_list(qual_list);

            OptExpression* left_filtered = OptExpression::create(left_filter, left_child);
            OptExpression* right_filtered = OptExpression::create(right_filter, right_child);

            LogicalJoin* new_join = static_cast<LogicalJoin*>(join->clone());
            OptExpression* new_expr = OptExpression::create(new_join, left_filtered, right_filtered);
            result.push_back(new_expr);

            ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule: pushed to both sides of INNER JOIN")));
        } else if (join_type == JOIN_LEFT) {
            // For left outer join, push to right side only (where match is required)
            LogicalFilter* right_filter = NEW_EX(LogicalFilter);
            right_filter->set_qual_list(qual_list);

            OptExpression* right_filtered = OptExpression::create(right_filter, right_child);

            LogicalJoin* new_join = static_cast<LogicalJoin*>(join->clone());
            OptExpression* new_expr = OptExpression::create(new_join, left_child, right_filtered);
            result.push_back(new_expr);

            ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule: pushed to right side of LEFT JOIN")));
        } else if (join_type == JOIN_RIGHT) {
            // For right outer join, push to left side only
            LogicalFilter* left_filter = NEW_EX(LogicalFilter);
            left_filter->set_qual_list(qual_list);

            OptExpression* left_filtered = OptExpression::create(left_filter, left_child);

            LogicalJoin* new_join = static_cast<LogicalJoin*>(join->clone());
            OptExpression* new_expr = OptExpression::create(new_join, left_filtered, right_child);
            result.push_back(new_expr);

            ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule: pushed to left side of RIGHT JOIN")));
        } else {
            // JOIN_FULL - cannot safely push
            ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, skipping JOIN_FULL")));
            result.push_back(input);
        }
    }
    // Case 2: Push filter down to scan
    else if (child_op->get_type() == OptExprOpType::LOGICAL_SCAN) {
        LogicalScan* scan = static_cast<LogicalScan*>(child_op->clone());
        // Get existing qual_list from scan
        List* existing_quals = scan->get_qual_list();

        // Merge with existing qualifiers
        if (existing_quals != nullptr) {
            // Append new qual list to existing one
            List* merged_quals = existing_quals;
            ListCell* cell;
            foreach(cell, qual_list) {
                merged_quals = lappend(merged_quals, lfirst(cell));
            }
            scan->set_qual_list(merged_quals);
        } else {
            scan->set_qual_list(qual_list);
        }

        // Return scan directly without filter wrapper (filter is now part of scan)
        OptExpression* scan_expr = OptExpression::create(scan);
        result.push_back(scan_expr);

        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, pushing to SCAN"))); 
    }
    // Case 3: Push filter through projection
    else if (child_op->get_type() == OptExprOpType::LOGICAL_PROJECT) {
        LogicalProject* project = static_cast<LogicalProject*>(child_op);
        OptExpression* project_child = child->input_at(0);

        LogicalFilter* pushed_filter = NEW_EX(LogicalFilter);
        pushed_filter->set_qual_list(qual_list);

        OptExpression* pushed_expr = OptExpression::create(pushed_filter, project_child);

        LogicalProject* new_project = NEW_EX(LogicalProject);
        new_project->set_target_list(project->get_target_list());

        OptExpression* result_expr = OptExpression::create(new_project, pushed_expr);
        result.push_back(result_expr);
        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, pushing through PROJECT")));        
    }
    // Case 4: Merge with another filter - combine qual lists
    else if (child_op->get_type() == OptExprOpType::LOGICAL_FILTER) {
        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, merging FILTERs")));
        LogicalFilter* child_filter = static_cast<LogicalFilter*>(child_op);

        // Get child's qual_list
        List* child_quals = child_filter->get_qual_list();

        // Merge qual lists
        List* merged_quals = qual_list;
        if (child_quals != nullptr) {
            ListCell* cell;
            foreach(cell, child_quals) {
                merged_quals = lappend(merged_quals, lfirst(cell));
            }
        }

        // Create merged filter
        LogicalFilter* merged_filter = NEW_EX(LogicalFilter);
        merged_filter->set_qual_list(merged_quals);

        OptExpression* grandchild = child->input_at(0);
        OptExpression* merged_expr = OptExpression::create(merged_filter, grandchild);
        result.push_back(merged_expr);

        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule: merged filters, total quals=%d",
                             list_length(merged_quals))));
    }
    // Case 5: Cannot push through other operators
    else {
        ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, cannot push through operator type %d",
                             (int)child_op->get_type())));
        result.push_back(input);
    }

    ereport(WARNING, (errmsg("[CASCADES] PredicatePushDownRule:transform, returning %d results",
                         (int)result.size())));

    return result;
}

}  // namespace cascades