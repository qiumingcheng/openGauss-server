#pragma once
#include "column_stats.h"
#include <unordered_map>
#include <memory>
#include <set>
#include <vector>
#include <string>

namespace cascades {


class ColumnStats;
using ColumnStatsPtr = std::shared_ptr<ColumnStats>;

class ColumnRefSet {
public:
    ColumnRefSet() = default;
    void add(int col_id) { columns_.insert(col_id); }
    bool contains(int col_id) const { return columns_.find(col_id) != columns_.end(); }
    size_t size() const { return columns_.size(); }

    // Iterator support
    std::set<int>::const_iterator begin() const { return columns_.begin(); }
    std::set<int>::const_iterator end() const { return columns_.end(); }

private:
    std::set<int> columns_;
};

/**
 * Statistics - Row and column statistics for query optimization
 *
 * Integrates with openGauss statistics system:
 * - pg_class.reltuples for table row counts
 * - pg_statistic for column-level statistics (NDV, null fraction, etc.)
 * - RelOptInfo for intermediate relation estimates
 */
class Statistics {
public:
    Statistics();
    explicit Statistics(double row_count);
    Statistics(const Statistics& other);
    Statistics& operator=(const Statistics& other);
    ~Statistics() = default;

    // Basic accessors
    double row_count() const { return row_count_; }
    void set_row_count(double rows) { row_count_ = rows; }

    double width() const { return width_; }
    void set_width(double w) { width_ = w; }

    // Column statistics
    void add_column_stats(int column_id, ColumnStatsPtr stats);
    ColumnStatsPtr get_column_stats(int column_id) const;
    bool has_column_stats(int column_id) const;

    // Output columns
    const ColumnRefSet& get_output_columns() const { return output_columns_; }
    void set_output_columns(const ColumnRefSet& cols);
    void add_output_column(int col_id);

    // Statistics derivation operators
    void copy_from(const Statistics* other);
    void apply_selectivity(double selectivity);
    void compute_aggregation(const std::vector<int>& group_keys);

    // Join cardinality estimation (supports single-key and multi-key joins)
    static double estimate_join_cardinality(
        Statistics* left, Statistics* right,
        const std::vector<int>& left_keys,
        const std::vector<int>& right_keys);

    std::string to_string() const;

private:
    double row_count_ = 0.0;
    double width_ = 0.0;
    ColumnRefSet output_columns_;
    std::unordered_map<int, ColumnStatsPtr> column_stats_;
};

}  // namespace cascades
