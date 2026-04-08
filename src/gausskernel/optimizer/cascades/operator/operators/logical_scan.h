#pragma once
#include "../operator.h"
#include "nodes/relation.h"

namespace cascades {

class LogicalScan : public LogicalOptExprOp {
public:
    explicit LogicalScan(int table_id);
    LogicalScan(int table_id, int predicate_id);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    int get_table_id() const { return table_id_; }
    int get_predicate_id() const { return predicate_id_; }
    void set_predicate_id(int id) { predicate_id_ = id; }
    bool has_predicate() const { return predicate_id_ >= 0; }

    // RelOptInfo management
    RelOptInfo* get_rel() const { return rel_; }
    void set_rel(RelOptInfo* rel) { rel_ = rel; }

private:
    int table_id_;
    int predicate_id_ = -1;  // -1 means no predicate pushed down
    RelOptInfo* rel_ = nullptr;  // OpenGauss relation optimization info
};

}  // namespace cascades