#pragma once
#include "../operator.h"
#include "nodes/relation.h"
#include "nodes/pg_list.h"
#include "../../statistics/statistics.h"

namespace cascades {

/**
 * LogicalScan - Logical scan operator supporting multiple predicates
 *
 * Uses qual_list (List* of RestrictInfo) for complex predicate expressions
 * and predicate_ids for tracking individual predicates during optimization
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

    // Multiple predicates support (primary API)
    List* get_predicate_ids() const { return predicate_ids_; }
    void set_predicate_ids(List* ids) { predicate_ids_ = ids; }

    // Qual list for complex expressions (List* of RestrictInfo)
    void* get_qual_list() const { return qual_list_; }
    void set_qual_list(void* list) { qual_list_ = list; }

    // Single predicate compatibility API (wrappers for multi-predicate API)
    // These are convenience methods for simple single-predicate cases
    int get_predicate_id() const {
        if (predicate_ids_ == nullptr || list_length(predicate_ids_) == 0) {
            return -1;  // No predicate
        }
        return lfirst_int(list_head(predicate_ids_));  // Return first predicate ID
    }
    void set_predicate_id(int id) {
        predicate_ids_ = list_make1_int(id);  // Single element list
    }

    // Check if scan has predicates
    bool has_predicates() const { return predicate_ids_ != nullptr || qual_list_ != nullptr; }
    bool has_predicate() const { return has_predicates(); }  // Compatibility alias

    // RelOptInfo management
    RelOptInfo* get_rel() const { return rel_; }
    void set_rel(RelOptInfo* rel) { rel_ = rel; }

    // Output columns management
    const ColumnRefSet& get_output_columns() const { return output_columns_; }
    void set_output_columns(const ColumnRefSet& cols) { output_columns_ = cols; }

private:
    int table_id_;
    Oid relid_ = InvalidOid;         // Table OID for openGauss integration
    List* predicate_ids_ = nullptr;  // Support for multiple predicate IDs
    void* qual_list_ = nullptr;      // List* of qualifier clauses for complex expressions
    RelOptInfo* rel_ = nullptr;      // OpenGauss relation optimization info
    ColumnRefSet output_columns_;    // Logical output columns (independent of reltarget)
};

}  // namespace cascades
