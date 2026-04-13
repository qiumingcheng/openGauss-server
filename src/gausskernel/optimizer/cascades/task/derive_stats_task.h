#pragma once
#include "optimizer_task.h"
#include "../statistics/statistics.h"

namespace cascades {

class GroupExpression;
class OptExprOp;

/**
 * DeriveStatsTask - Derive statistics for group expressions
 *
 * This task derives statistics (row count, width, column stats) for each
 * expression in the memo. It integrates with openGauss statistics:
 *
 * 1. For base table scans: Uses pg_class.reltuples and pg_statistic
 * 2. For operators: Computes derived statistics using selectivity estimation
 *
 * Statistics derivation follows these rules:
 * - Scan: Use table statistics from pg_class/pg_statistic
 * - Filter: Apply selectivity (currently default 0.1)
 * - Join: Use join cardinality estimation with NDV
 * - Agg: Estimate based on group keys cardinality
 * - Others: Pass through or simple estimation
 *
 * Integration with openGauss:
 * - Uses StatisticsUtils for pg_class/pg_statistic access
 * - Uses Statistics::estimate_join_cardinality for join estimation
 * - Provides unified statistics derivation interface
 */
class DeriveStatsTask : public OptimizerTask {
public:
    DeriveStatsTask(TaskContext* context, GroupExpression* expr);
    void execute() override;
    std::string to_string() const override;

private:
    // Main dispatcher
    void derive_statistics_by_type(
        OptExprOp* op,
        Statistics* result_stats,
        const std::vector<Statistics*>& children_stats);

    // Operator-specific derivation
    void derive_scan_statistics(Statistics* result_stats,
                                const std::vector<Statistics*>& children_stats,
                                OptExprOp* op);
    void derive_filter_statistics(Statistics* result_stats,
                                  const std::vector<Statistics*>& children_stats,
                                  OptExprOp* op);
    void derive_project_statistics(Statistics* result_stats,
                                   const std::vector<Statistics*>& children_stats);
    void derive_join_statistics(Statistics* result_stats,
                                const std::vector<Statistics*>& children_stats,
                                OptExprOp* op);
    void derive_physical_join_statistics(Statistics* result_stats,
                                         const std::vector<Statistics*>& children_stats,
                                         OptExprOp* op);
    void derive_aggregation_statistics(Statistics* result_stats,
                                       const std::vector<Statistics*>& children_stats,
                                       OptExprOp* op);
    void derive_sort_statistics(Statistics* result_stats,
                                const std::vector<Statistics*>& children_stats);
    void derive_exchange_statistics(Statistics* result_stats,
                                    const std::vector<Statistics*>& children_stats);
    void derive_default_statistics(Statistics* result_stats,
                                   const std::vector<Statistics*>& children_stats);

private:
    GroupExpression* expression_;
};

}
