#pragma once
#include "../operator.h"
#include "nodes/relation.h"
#include "nodes/pg_list.h"
#include "../../statistics/statistics.h"

namespace cascades {

/**
 * LogicalScan - Logical scan operator
 *
 * Uses qual_list (List* of RestrictInfo) for predicate expressions
 */
class LogicalScan : public LogicalOptExprOp {
public:
    explicit LogicalScan(int table_id);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    int get_table_id() const { return table_id_; }
    Oid get_relid() const { return relid_; }
    void set_relid(Oid relid) { relid_ = relid; }

    // Qual list for expressions (List* of RestrictInfo)
    List* get_qual_list() const { return qual_list_; }
    void set_qual_list(List* list) { qual_list_ = list; }

    // RelOptInfo management
    RelOptInfo* get_rel() const { return rel_; }
    void set_rel(RelOptInfo* rel) { rel_ = rel; }

    // Output columns management
    const ColumnRefSet& get_output_columns() const { return output_columns_; }
    void set_output_columns(const ColumnRefSet& cols) { output_columns_ = cols; }

private:
    int table_id_;
    Oid relid_ = InvalidOid;         // Table OID for openGauss integration
    List* qual_list_ = nullptr;      // List* of qualifier clauses (RestrictInfo*)
    RelOptInfo* rel_ = nullptr;      // OpenGauss relation optimization info
    ColumnRefSet output_columns_;    // Logical output columns (independent of reltarget)
};

}  // namespace cascades
