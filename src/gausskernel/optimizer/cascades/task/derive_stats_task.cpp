#include "derive_stats_task.h"
#include "task_context.h"
#include "../memo/group_expression.h"
#include "../memo/group.h"
#include "../statistics/statistics.h"
#include "../statistics/statistics_utils.h"
#include "../operator/operators.h"
#include "../operator/operators/physical_scan.h"
#include "../operator/operators/logical_filter.h"
#include "../operator/operators/logical_join.h"
#include "../operator/operators/physical_hash_agg.h"
#include <cstdint>

namespace cascades {

DeriveStatsTask::DeriveStatsTask(TaskContext* context, GroupExpression* expr)
    : OptimizerTask(context), expression_(expr) {}

void DeriveStatsTask::execute() {
    Group* group = expression_->get_group();
    OptExprOp* op = expression_->get_op();

    // Collect statistics from input groups
    std::vector<Statistics*> children_stats;
    for (int i = 0; i < expression_->arity(); ++i) {
        Group* input_group = expression_->input_at(i);
        Statistics* child_stats = input_group->get_statistics();
        children_stats.push_back(child_stats);
    }

    // Create result statistics using NEW_EX macro
    Statistics* result_stats = NEW_EX(Statistics);

    // Derive statistics based on operator type
    derive_statistics_by_type(op, result_stats, children_stats);

    // Set the computed statistics on the group
    group->set_statistics(result_stats);
}

void DeriveStatsTask::derive_statistics_by_type(
    OptExprOp* op,
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats) {

    switch (op->get_type()) {
        case OptExprOpType::LOGICAL_SCAN:
        case OptExprOpType::PHYSICAL_SCAN:
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

        case OptExprOpType::PHYSICAL_HASH_JOIN:
        case OptExprOpType::PHYSICAL_MERGE_JOIN:
        case OptExprOpType::PHYSICAL_NESTLOOP_JOIN:
            derive_physical_join_statistics(result_stats, children_stats, op);
            break;

        case OptExprOpType::PHYSICAL_HASH_AGG:
        case OptExprOpType::PHYSICAL_SORT_AGG:
            derive_aggregation_statistics(result_stats, children_stats, op);
            break;

        case OptExprOpType::PHYSICAL_SORT:
            derive_sort_statistics(result_stats, children_stats);
            break;

        case OptExprOpType::PHYSICAL_EXCHANGE:
            derive_exchange_statistics(result_stats, children_stats);
            break;

        default:
            derive_default_statistics(result_stats, children_stats);
            break;
    }
}

// ============================================================================
// Scan: Extension Point 4 - Table OID from PhysicalScan
// ============================================================================

void DeriveStatsTask::derive_scan_statistics(
    Statistics* result_stats,
    const std::vector<Statistics*>& children_stats,
    OptExprOp* op) {

    // Try to get table OID from PhysicalScan
    PhysicalScan* scan_op = dynamic_cast<PhysicalScan*>(op);
    if (scan_op != nullptr) {
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
    }

    // Fallback: copy from child if available
    if (result_stats->row_count() <= 0 && !children_stats.empty() && children_stats[0] != nullptr) {
        result_stats->copy_from(children_stats[0]);
    }

    // Default estimate
    if (result_stats->row_count() <= 0) {
        result_stats->set_row_count(1000.0);
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
        result_stats->set_row_count(100.0);
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
        result_stats->set_row_count(1000.0);
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
        result_stats->set_row_count(1000.0);
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
        result_stats->set_row_count(1000.0);
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

    // Get group keys from the aggregation operator
    PhysicalHashAgg* agg_op = static_cast<PhysicalHashAgg*>(op);
    const std::vector<int>& group_keys = agg_op->get_group_keys();

    // Compute aggregation output cardinality using group key NDV
    result_stats->compute_aggregation(group_keys);

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
        result_stats->set_row_count(1000.0);
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
        result_stats->set_row_count(1000.0);
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
    result_stats->set_row_count(max_rows > 0 ? max_rows : 1000.0);
}

std::string DeriveStatsTask::to_string() const {
    return "DeriveStatsTask for " + expression_->to_string();
}

}  // namespace cascades
