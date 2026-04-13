#pragma once
#include "../statistics/statistics.h"
#include <string>
#include <vector>

namespace cascades {

class ExpressionContext;

/**
 * LogicalProperty - Logical properties of a group/expression
 *
 * Contains logical-level information that is independent of physical execution:
 * - Output columns
 * - Sort order (logical sort keys)
 * - Functional dependencies
 * - Partitioning requirements (logical)
 */
class LogicalProperty {
public:
    LogicalProperty();
    ~LogicalProperty() = default;

    // Output columns
    const ColumnRefSet& get_output_columns() const { return output_columns_; }
    void set_output_columns(const ColumnRefSet& cols);

    // Sort keys (logical ordering)
    const std::vector<int>& get_sort_keys() const { return sort_keys_; }
    void set_sort_keys(const std::vector<int>& keys) { sort_keys_ = keys; }
    bool has_sort_keys() const { return !sort_keys_.empty(); }

    // Derive logical property from expression context
    void derive(ExpressionContext* context);

    std::string to_string() const;

private:
    ColumnRefSet output_columns_;
    std::vector<int> sort_keys_;  // Columns that define logical sort order
};

}