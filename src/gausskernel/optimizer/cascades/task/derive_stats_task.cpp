#include "derive_stats_task.h"
#include "task_context.h"
#include "../optimizer/optimizer_context.h"
#include "../memo/group_expression.h"
#include "../memo/group.h"
#include "../statistics/statistics.h"
#include "../statistics/statistics_utils.h"
#include "../operator/operators.h"
#include "../operator/operators/logical_scan.h"
#include "../operator/operators/logical_filter.h"
#include "../operator/operators/logical_join.h"
#include "../operator/operators/logical_aggregate.h"
#include "../operator/operators/logical_subquery_scan.h"
#include "../operator/operators/logical_limit.h"
#include "optimizer/clauses.h"
#include "nodes/plannodes.h"
#include <cstdint>

namespace cascades {

const double DEFAULT_ROW_COUNT = 1010.5;
DeriveStatsTask::DeriveStatsTask(TaskContext* context, GroupExpression* expr)
    : OptimizerTask(context), expression_(expr) {}

void DeriveStatsTask::execute() {
    Group* group = expression_->get_group();

    ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: group id=%d, expr=%s",
                         group->id(), expression_->to_string().c_str())));
    if(group->get_statistics()) {
        ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: get group statistics row_count=%.2f",
                         group->get_statistics()->row_count()))); 
        return;
    }

    // Collect statistics from input groups
    std::vector<Statistics*> children_stats;
    for (int i = 0; i < expression_->arity(); ++i) {
        Group* input_group = expression_->input_at(i);
        Statistics* child_stats = input_group->get_statistics();
        children_stats.push_back(child_stats);
        ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: child %d, group id=%d, stats=[%p, %.0f]",
                             i, input_group->id(), (void*)child_stats, child_stats->row_count())));
    }

    // Create result statistics using NEW_EX macro
    Statistics* result_stats = NEW_EX(Statistics);
    OptExprOp* op = expression_->get_op();

    // Derive statistics based on operator type
    derive_statistics_by_type(op, result_stats, children_stats);

    ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: result row_count=%.2f",
                         result_stats->row_count())));

    // Set the computed statistics on the group
    group->set_statistics(result_stats);
}

void DeriveStatsTask::derive_statistics_by_type(
    OptExprOp* op,
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats) {

    switch (op->get_type()) {
        case OptExprOpType::LOGICAL_SCAN:
        case OptExprOpType::LOGICAL_SUBQUERY_SCAN:
            derive_scan_statistics(result_stats, children_stats, op);
            break;

        case OptExprOpType::LOGICAL_FILTER:
            derive_filter_statistics(result_stats, children_stats, op);
            break;

        case OptExprOpType::LOGICAL_PROJECT:
            derive_project_statistics(result_stats, children_stats);
            break;

        case OptExprOpType::LOGICAL_JOIN:
            derive_join_statistics(result_stats, children_stats, op);
            break;

        case OptExprOpType::LOGICAL_AGGREGATE:
            derive_aggregation_statistics(result_stats, children_stats, op);
            break;

        case OptExprOpType::LOGICAL_LIMIT:
            derive_limit_statistics(result_stats, children_stats, op);
            break;

        default:
            derive_default_statistics(result_stats, children_stats);
            break;
    }
}

// ============================================================================
// Scan: Extension Point 4 - Table OID from LogicalScan
// ============================================================================

void DeriveStatsTask::derive_scan_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats,
    OptExprOp* op) {

    // Handle LogicalSubqueryScan - derive stats from subquery plan
    if (op->get_type() == OptExprOpType::LOGICAL_SUBQUERY_SCAN) {
        LogicalSubqueryScan* subquery_scan = static_cast<LogicalSubqueryScan*>(op);
        RelOptInfo* rel = subquery_scan->get_rel();

        if (rel != nullptr && rel->subplan != nullptr) {
            // Use subplan's row count estimate
            result_stats->set_row_count(rel->subplan->plan_rows);
            result_stats->set_width(rel->subplan->plan_width);

            ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: subquery stats, rows=%.0f, width=%d",
                                 rel->subplan->plan_rows, rel->subplan->plan_width)));
            return;
        }
    }

    // Try to get table OID from LogicalScan
    LogicalScan* scan_op = static_cast<LogicalScan*>(op);
    Oid relid = scan_op->get_relid();
    if (OidIsValid(relid)) {
        // Extension Point 4: Use openGauss pg_class/pg_statistic
        Statistics* table_stats = StatisticsUtils::build_table_statistics(relid, nullptr);
        if (table_stats != nullptr) {
            result_stats->copy_from(table_stats);
            // table_stats is allocated in MemoryContext, no need to delete
            return;
        }
    }

    // Fallback: copy from child if available
    if (result_stats->row_count() <= 0 && !children_stats.empty() && children_stats[0] != nullptr) {
        result_stats->copy_from(children_stats[0]);
    }

    // Default estimate
    if (result_stats->row_count() <= 0) {
        result_stats->set_row_count(DEFAULT_ROW_COUNT);
    }
}

// ============================================================================
// Filter: Extension Point 1 - Filter Selectivity with clauselist_selectivity
// ============================================================================

void DeriveStatsTask::derive_filter_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats,
    OptExprOp* op) {

    if (children_stats.empty() || children_stats[0] == nullptr) {
        result_stats->set_row_count(DEFAULT_ROW_COUNT);
        return;
    }

    // Copy base statistics from child
    result_stats->copy_from(children_stats[0]);

    // Extension Point 1: Use qual_list for selectivity estimation
    LogicalFilter* filter_op = static_cast<LogicalFilter*>(op);
    if (filter_op != nullptr) {
        void* qual_list = filter_op->get_qual_list();
        if (qual_list != nullptr) {
            // Use openGauss clauselist_selectivity through StatisticsUtils
            // Note: In a full integration, this would require PlannerInfo
            double selectivity = StatisticsUtils::estimate_filter_selectivity(qual_list, InvalidOid);
            result_stats->apply_selectivity(selectivity);
            return;
        }
    }

    // Default selectivity
    result_stats->apply_selectivity(0.1);
}

// ============================================================================
// Project: Pass through child statistics with column mapping
// ============================================================================

void DeriveStatsTask::derive_project_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats) {

    if (!children_stats.empty() && children_stats[0] != nullptr) {
        result_stats->copy_from(children_stats[0]);
    } else {
        result_stats->set_row_count(DEFAULT_ROW_COUNT);
    }
}

// ============================================================================
// Join: Extension Point 2 & 3 - Multi-key Join and Column Stats
// ============================================================================

void DeriveStatsTask::derive_join_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats,
    OptExprOp* op) {

    if (children_stats.size() < 2 ||
        children_stats[0] == nullptr || children_stats[1] == nullptr) {
        result_stats->set_row_count(DEFAULT_ROW_COUNT);
        return;
    }

    LogicalJoin* join_op = static_cast<LogicalJoin*>(op);

    // Extension Point 2: Multi-key join estimation
    const std::vector<int>& left_keys = join_op->get_left_join_keys();
    const std::vector<int>& right_keys = join_op->get_right_join_keys();

    double join_rows;
    if (!left_keys.empty() && !right_keys.empty() && left_keys.size() == right_keys.size()) {
        // Multi-key join
        join_rows = Statistics::estimate_join_cardinality_advanced(
            children_stats[0], children_stats[1],
            left_keys, right_keys);
    } else {
        // Single key or symmetric join
        const std::vector<int>& join_keys = join_op->get_join_keys();
        int left_key = join_keys.empty() ? 0 : join_keys[0];
        int right_key = join_keys.empty() ? 0 : join_keys[0];
        join_rows = Statistics::estimate_join_cardinality(
            children_stats[0], children_stats[1],
            left_key, right_key);
    }

    result_stats->set_row_count(join_rows);

    // Extension Point 3: Merge column statistics
    if (!left_keys.empty() && !right_keys.empty()) {
        StatisticsUtils::merge_join_column_statistics(
            result_stats, children_stats[0], children_stats[1],
            left_keys, right_keys);
    } else {
        StatisticsUtils::merge_column_statistics(result_stats, children_stats);
    }
}

void DeriveStatsTask::derive_physical_join_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats,
    OptExprOp* op) {

    if (children_stats.size() < 2 ||
        children_stats[0] == nullptr || children_stats[1] == nullptr) {
        result_stats->set_row_count(DEFAULT_ROW_COUNT);
        return;
    }

    // Physical joins: same cardinality estimation as logical
    // Use Statistics utility for estimation
    double join_rows = Statistics::estimate_join_cardinality(
        children_stats[0], children_stats[1], 0, 0);

    result_stats->set_row_count(join_rows);

    // Merge column statistics
    StatisticsUtils::merge_column_statistics(result_stats, children_stats);
}

// ============================================================================
// Aggregation: Extension Point 3 - Column Stats for Group Keys
// ============================================================================

void DeriveStatsTask::derive_aggregation_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats,
    OptExprOp* op) {

    if (children_stats.empty() || children_stats[0] == nullptr) {
        result_stats->set_row_count(1.0);
        return;
    }

    // Copy child statistics as base
    result_stats->copy_from(children_stats[0]);

    // Get group keys and having_qual from LogicalAggregate
    LogicalAggregate* agg_op = dynamic_cast<LogicalAggregate*>(op);
    if (agg_op == nullptr) {
        result_stats->set_row_count(1.0);
        return;
    }

    const std::vector<int>& group_keys = agg_op->get_group_keys();
    Node* having_qual = agg_op->get_having_qual();

    ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: Agg: group_keys=%zu, having_qual=%p",
                         group_keys.size(), (void*)having_qual)));

    // Compute aggregation output cardinality using group key NDV
    result_stats->compute_aggregation(group_keys);

    // Apply having_qual selectivity if present
    if (having_qual != nullptr) {
        Node* estimated_having = estimate_expression_value(
            context_->get_optimizer_context()->get_root(), having_qual);
        if (estimated_having != nullptr && IsA(estimated_having, Const)) {
            Const* having_const = (Const*)estimated_having;
            if (having_const->constisnull == false && having_const->constvalue > 0) {
                double selectivity = 0.1;  // Default selectivity for having
                result_stats->apply_selectivity(selectivity);
                ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: Agg: after having, rows=%.0f",
                                    result_stats->row_count())));
            }
        }
    }

    // Extension Point 3: Update column stats for group keys
    // After aggregation, only group keys and aggregate columns remain
    ColumnRefSet output_cols;
    for (int key : group_keys) {
        output_cols.add(key);
        // Preserve column stats for group keys
        auto col_stats = children_stats[0]->get_column_stats(key);
        if (col_stats != nullptr) {
            result_stats->add_column_stats(key, col_stats);
        }
    }
    result_stats->set_output_columns(output_cols);
}

// ============================================================================
// Sort: Pass through
// ============================================================================

void DeriveStatsTask::derive_sort_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats) {

    if (!children_stats.empty() && children_stats[0] != nullptr) {
        result_stats->copy_from(children_stats[0]);
    } else {
        result_stats->set_row_count(DEFAULT_ROW_COUNT);
    }
}

// ============================================================================
// Exchange: Pass through
// ============================================================================

void DeriveStatsTask::derive_exchange_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats) {

    if (!children_stats.empty() && children_stats[0] != nullptr) {
        result_stats->copy_from(children_stats[0]);
    } else {
        result_stats->set_row_count(DEFAULT_ROW_COUNT);
    }
}

// ============================================================================
// Limit: Apply LIMIT count to child statistics
// ============================================================================

void DeriveStatsTask::derive_limit_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats,
    OptExprOp* op) {

    if (children_stats.empty() || children_stats[0] == nullptr) {
        result_stats->set_row_count(DEFAULT_ROW_COUNT);
        return;
    }

    // Copy base statistics from child
    result_stats->copy_from(children_stats[0]);

    // Get limit count and offset
    Node* limit_count = nullptr;
    Node* limit_offset = nullptr;

    // Try LogicalLimit first
    LogicalLimit* logical_limit = dynamic_cast<LogicalLimit*>(op);
    if (logical_limit != nullptr) {
        limit_count = logical_limit->get_limit_count();
        limit_offset = logical_limit->get_limit_offset();
    } else {
        // Try PhysicalLimit
        PhysicalLimit* physical_limit = dynamic_cast<PhysicalLimit*>(op);
        if (physical_limit != nullptr) {
            limit_count = physical_limit->get_limit_count();
            limit_offset = physical_limit->get_limit_offset();
        }
    }

    ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: Limit: limit_count=%p, type=%d, IsA(Const)=%d",
                         (void*)limit_count, limit_count ? (int)nodeTag(limit_count) : -1,
                         limit_count ? IsA(limit_count, Const) : 0)));

    // Try to estimate the limit value using estimate_expression_value
    // This handles cases where limit_count is a Param or complex expression
    Node* estimated_limit = limit_count;
    if (limit_count != nullptr) {
        estimated_limit = estimate_expression_value(context_->get_optimizer_context()->get_root(), limit_count);
    }

    // Apply LIMIT selectivity
    if (estimated_limit != nullptr && IsA(estimated_limit, Const)) {
        Const* limit_const = (Const*)estimated_limit;
        if (limit_const->constisnull == false && limit_const->constvalue > 0) {
            int64 limit_val = DatumGetInt64(limit_const->constvalue);
            double child_rows = result_stats->row_count();
            ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: Limit: limit_val=%ld, child_rows=%.0f",
                                limit_val, child_rows)));
            if (limit_val < child_rows) {
                double selectivity = (double)limit_val / child_rows;
                result_stats->apply_selectivity(selectivity);
                ereport(WARNING, (errmsg("[CASCADES] DeriveStatsTask: Limit: new_rows=%.0f",
                                    result_stats->row_count())));
            }
        }
    }
}

// ============================================================================
// Default
// ============================================================================

void DeriveStatsTask::derive_default_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats) {

    double max_rows = 0;
    for (const auto* child_stats : children_stats) {
        if (child_stats != nullptr) {
            max_rows = std::max(max_rows, child_stats->row_count());
        }
    }
    result_stats->set_row_count(max_rows > 0 ? max_rows : DEFAULT_ROW_COUNT);
}

std::string DeriveStatsTask::to_string() const {
    return "DeriveStatsTask for " + expression_->to_string();
}

}  // namespace cascades
