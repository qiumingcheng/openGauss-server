#pragma once
#include "../operator.h"

namespace cascades {

class LogicalSubquery : public LogicalOptExprOp {
public:
    LogicalSubquery();
    explicit LogicalSubquery(int subquery_id);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    int get_subquery_id() const { return subquery_id_; }
    void set_subquery_id(int id) { subquery_id_ = id; }

    bool is_correlated() const { return is_correlated_; }
    void set_correlated(bool correlated) { is_correlated_ = correlated; }

    const std::vector<int>& get_output_columns() const { return output_columns_; }
    void set_output_columns(const std::vector<int>& cols) { output_columns_ = cols; }

private:
    int subquery_id_ = -1;  // unique identifier for this subquery
    bool is_correlated_ = false;  // true if references outer query vars
    std::vector<int> output_columns_;  // output column indices
};

}  // namespace cascades
