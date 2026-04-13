#include "statistics.h"
#include <algorithm>
#include <cmath>

namespace cascades {

// ============================================================================
// Constructors and assignment
// ============================================================================

Statistics::Statistics() : row_count_(0.0), width_(0.0) {}

Statistics::Statistics(double row_count) : row_count_(row_count), width_(0.0) {}

Statistics::Statistics(const Statistics& other)
    : row_count_(other.row_count_),
      width_(other.width_),
      output_columns_(other.output_columns_),
      column_stats_(other.column_stats_) {}

Statistics& Statistics::operator=(const Statistics& other) {
    if (this != &other) {
        row_count_ = other.row_count_;
        width_ = other.width_;
        output_columns_ = other.output_columns_;
        column_stats_ = other.column_stats_;
    }
    return *this;
}

// ============================================================================
// Column statistics
// ============================================================================

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

void Statistics::add_output_column(int col_id) {
    output_columns_.add(col_id);
}

void Statistics::set_output_columns(const ColumnRefSet& cols) {
    output_columns_ = cols;
}

// ============================================================================
// Statistics derivation operators
// ============================================================================

void Statistics::copy_from(const Statistics* other) {
    if (other != nullptr) {
        *this = *other;
    }
}

void Statistics::apply_selectivity(double selectivity) {
    if (selectivity < 0.0 || selectivity > 1.0) {
        selectivity = std::max(0.0, std::min(1.0, selectivity));
    }
    row_count_ = std::max(1.0, row_count_ * selectivity);
}

void Statistics::compute_aggregation(const std::vector<int>& group_keys) {
    if (group_keys.empty()) {
        // Global aggregation: result is 1 row
        row_count_ = 1.0;
        return;
    }

    // Estimate output rows based on group key cardinality
    // Product of NDV for all group keys
    double output_rows = 1.0;
    bool has_stats = false;

    for (int key : group_keys) {
        auto col_stats = get_column_stats(key);
        if (col_stats != nullptr && col_stats->ndv() > 0) {
            output_rows *= col_stats->ndv();
            has_stats = true;
        }
    }

    if (has_stats) {
        // Clamp to input row count (can't output more rows than input)
        row_count_ = std::min(row_count_, output_rows);
    } else {
        // Default: assume 10% reduction
        row_count_ = std::max(1.0, row_count_ * 0.1);
    }
}

// ============================================================================
// Join cardinality estimation
// ============================================================================

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

    // Classic SQL join cardinality formula:
    // join_rows = left_rows * right_rows / max(left_ndv, right_ndv)
    double max_ndv = std::max(left_ndv, right_ndv);
    if (max_ndv <= 1.0) max_ndv = 1.0;

    double join_rows = (left_rows * right_rows) / max_ndv;

    // Clamp to reasonable bounds
    return std::max(1.0, std::min(join_rows, left_rows * right_rows));
}

double Statistics::estimate_join_cardinality_advanced(
    Statistics* left, Statistics* right,
    const std::vector<int>& left_keys,
    const std::vector<int>& right_keys) {

    if (left == nullptr || right == nullptr) {
        return left != nullptr ? left->row_count() :
               (right != nullptr ? right->row_count() : 0.0);
    }

    if (left_keys.empty() || right_keys.empty()) {
        // No join keys: cartesian product
        return left->row_count() * right->row_count();
    }

    // Assume all join keys have similar cardinality reduction
    // Use the first key pair for estimation (can be extended for multiple keys)
    double selectivity = 1.0;

    for (size_t i = 0; i < std::min(left_keys.size(), right_keys.size()); ++i) {
        int left_key = left_keys[i];
        int right_key = right_keys[i];

        double left_ndv = 1.0;
        double right_ndv = 1.0;

        auto left_stats = left->get_column_stats(left_key);
        auto right_stats = right->get_column_stats(right_key);

        if (left_stats != nullptr && left_stats->ndv() > 0) {
            left_ndv = left_stats->ndv();
        }
        if (right_stats != nullptr && right_stats->ndv() > 0) {
            right_ndv = right_stats->ndv();
        }

        // Selectivity for this join key pair
        double key_selectivity = 1.0 / std::max(left_ndv, right_ndv);
        selectivity *= key_selectivity;
    }

    double join_rows = left->row_count() * right->row_count() * selectivity;

    // Clamp to reasonable bounds
    return std::max(1.0, std::min(join_rows, left->row_count() * right->row_count()));
}

// ============================================================================
// String representation
// ============================================================================

std::string Statistics::to_string() const {
    return "Statistics(rows=" + std::to_string(row_count_) +
           ", width=" + std::to_string(width_) +
           ", cols=" + std::to_string(output_columns_.size()) + ")";
}

}  // namespace cascades
