#pragma once
#include "../statistics/statistics.h"
#include <string>

namespace cascades {


class ExpressionContext;

class LogicalProperty {
public:
    LogicalProperty();
    ~LogicalProperty() = default;

    const ColumnRefSet& get_output_columns() const { return output_columns_; }
    void set_output_columns(const ColumnRefSet& cols);

    void derive(ExpressionContext* context);

    std::string to_string() const;

private:
    ColumnRefSet output_columns_;
};

}