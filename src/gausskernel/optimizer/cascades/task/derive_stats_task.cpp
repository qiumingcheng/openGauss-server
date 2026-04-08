#include "derive_stats_task.h"
#include "task_context.h"
#include "../memo/group_expression.h"
#include "../memo/group.h"
#include "../statistics/statistics.h"
#include "../operator/operators.h"

namespace cascades {
DeriveStatsTask::DeriveStatsTask(TaskContext* context, GroupExpression* expr)
    : OptimizerTask(context), expression_(expr) {}

void DeriveStatsTask::execute() {
    // Derive statistics from children based on operator type
    Group* group = expression_->get_group();
    OptExprOp* op = expression_->get_op();

    // Collect statistics from input groups
    std::vector<Statistics*> children_stats;
    for (int i = 0; i < expression_->arity(); ++i) {
        Group* input_group = expression_->input_at(i);
        Statistics* child_stats = input_group->get_statistics();
        children_stats.push_back(child_stats);
    }

    // Compute statistics based on operator type
    Statistics* result_stats = new Statistics();

    switch (op->get_type()) {
        case OptExprOpType::LOGICAL_SCAN: {
            // For scan, we use the child's statistics directly
            // In a real implementation, this would come from table metadata
            if (!children_stats.empty() && children_stats[0] != nullptr) {
                *result_stats = *children_stats[0];
            }
            break;
        }

        case OptExprOpType::LOGICAL_FILTER: {
            // Filter reduces row count based on selectivity
            if (!children_stats.empty() && children_stats[0] != nullptr) {
                *result_stats = *children_stats[0];
                // Apply filter selectivity (default 0.1 = 10% selectivity)
                double filter_selectivity = 0.1;
                result_stats->set_row_count(result_stats->row_count() * filter_selectivity);
            }
            break;
        }

        case OptExprOpType::LOGICAL_PROJECT: {
            // Project passes through rows but may change columns
            if (!children_stats.empty() && children_stats[0] != nullptr) {
                *result_stats = *children_stats[0];
                // Projection doesn't change row count
            }
            break;
        }

        case OptExprOpType::LOGICAL_JOIN: {
            // Join cardinality estimation
            if (children_stats.size() >= 2) {
                Statistics* left_stats = children_stats[0];
                Statistics* right_stats = children_stats[1];
                LogicalJoin* join_op = static_cast<LogicalJoin*>(op);
                const std::vector<int>& join_keys = join_op->get_join_keys();

                int left_key = join_keys.empty() ? 0 : join_keys[0];
                int right_key = join_keys.empty() ? 0 : join_keys[0];

                double join_rows = Statistics::estimate_join_cardinality(
                    left_stats, right_stats, left_key, right_key);
                result_stats->set_row_count(join_rows);
            }
            break;
        }

        case OptExprOpType::PHYSICAL_HASH_JOIN:
        case OptExprOpType::PHYSICAL_MERGE_JOIN:
        case OptExprOpType::PHYSICAL_NESTLOOP_JOIN: {
            // Physical join has same cardinality estimation as logical
            if (children_stats.size() >= 2) {
                Statistics* left_stats = children_stats[0];
                Statistics* right_stats = children_stats[1];
                double join_rows = Statistics::estimate_join_cardinality(
                    left_stats, right_stats, 0, 0);
                result_stats->set_row_count(join_rows);
            }
            break;
        }

        case OptExprOpType::PHYSICAL_HASH_AGG:
        case OptExprOpType::PHYSICAL_SORT_AGG: {
            // Aggregation reduces rows (assume 0.1 reduction factor)
            if (!children_stats.empty() && children_stats[0] != nullptr) {
                *result_stats = *children_stats[0];
                result_stats->set_row_count(result_stats->row_count() * 0.1);
            }
            break;
        }

        default: {
            // For other operators, use max of children row counts
            double max_rows = 0;
            for (auto* child_stats : children_stats) {
                if (child_stats != nullptr && child_stats->row_count() > max_rows) {
                    max_rows = child_stats->row_count();
                }
            }
            result_stats->set_row_count(max_rows);
            break;
        }
    }

    // Set the computed statistics on the group
    group->set_statistics(result_stats);
}

std::string DeriveStatsTask::to_string() const {
    return "DeriveStatsTask for " + expression_->to_string();
}


}  // namespace cascades