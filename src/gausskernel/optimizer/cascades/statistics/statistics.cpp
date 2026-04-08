#include "statistics.h"
#include <algorithm>

namespace cascades {
Statistics::Statistics() : row_count_(0.0) {}

Statistics::Statistics(double row_count) : row_count_(row_count) {}

void Statistics::add_column_stats(int column_id, ColumnStatsPtr stats) {
    column_stats_[column_id] = stats;
}

ColumnStatsPtr Statistics::get_column_stats(int column_id) const {
    auto it = column_stats_.find(column_id);
    if (it != column_stats_.end()) {
        return it->second;
    }
    return nullptr;
}

bool Statistics::has_column_stats(int column_id) const {
    return column_stats_.find(column_id) != column_stats_.end();
}

void Statistics::set_output_columns(const ColumnRefSet& cols) {
    output_columns_ = cols;
}

void Statistics::compute_children_statistics(const std::vector<Statistics*>& children) {
    if (children.empty()) {
        return;
    }

    // For most operators, output rows = max of children rows
    double max_rows = 0;
    for (auto* child : children) {
        if (child != nullptr && child->row_count() > max_rows) {
            max_rows = child->row_count();
        }
    }
    row_count_ = max_rows;

    // Combine output columns from all children
    for (auto* child : children) {
        if (child != nullptr) {
            for (int col : child->output_columns_) {
                output_columns_.add(col);
            }
        }
    }
}

double Statistics::estimate_join_cardinality(Statistics* left, Statistics* right,
                                            int left_key, int right_key) {
    if (left == nullptr || right == nullptr) {
        return left != nullptr ? left->row_count() :
               (right != nullptr ? right->row_count() : 0.0);
    }

    double left_rows = left->row_count();
    double right_rows = right->row_count();

    // Get NDV (Number of Distinct Values) for join keys
    double left_ndv = 1.0;
    double right_ndv = 1.0;

    auto left_stats = left->get_column_stats(left_key);
    auto right_stats = right->get_column_stats(right_key);

    if (left_stats != nullptr) {
        left_ndv = left_stats->ndv();
    }
    if (right_stats != nullptr) {
        right_ndv = right_stats->ndv();
    }

    // Estimate join cardinality using classic SQL selectivity formula
    // join_rows = left_rows * right_rows / max(left_ndv, right_ndv)
    double max_ndv = std::max(left_ndv, right_ndv);
    if (max_ndv <= 0) max_ndv = 1.0;

    double join_rows = (left_rows * right_rows) / max_ndv;

    // Clamp to reasonable bounds
    return std::max(1.0, std::min(join_rows, left_rows * right_rows));
}

std::string Statistics::to_string() const {
    return "Statistics(rows=" + std::to_string(row_count_) +
           ", cols=" + std::to_string(output_columns_.size()) + ")";
}

}  // namespace cascades