#pragma once
#include "../operator.h"
#include "nodes/relation.h"

namespace cascades {

/**
 * PhysicalSeqScan - Physical sequential table scan operator
 *
 * Represents a full table scan without any ordering guarantee.
 */
class PhysicalSeqScan : public PhysicalOptExprOp {
public:
    PhysicalSeqScan();
    explicit PhysicalSeqScan(Path* scan_path);

    OptExprOpType get_type() const;
    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
    bool equals(const OptExprOp* other) const override;

    // Table identification
    Oid get_relid() const { return relid_; }
    void set_relid(Oid relid) { relid_ = relid; }

    int get_table_id() const { return table_id_; }
    void set_table_id(int id) { table_id_ = id; }

private:
    Oid relid_ = InvalidOid;
    int table_id_ = -1;
};

/**
 * PhysicalIndexScan - Physical index scan operator
 *
 * Represents an index scan. The output may be sorted depending on the index
 * and the index's pathkeys.
 */
class PhysicalIndexScan : public PhysicalOptExprOp {
public:
    PhysicalIndexScan();
    explicit PhysicalIndexScan(Path* index_path);

    OptExprOpType get_type() const;
    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
    bool equals(const OptExprOp* other) const override;

    // Table identification
    Oid get_relid() const { return relid_; }
    void set_relid(Oid relid) { relid_ = relid; }

    int get_table_id() const { return table_id_; }
    void set_table_id(int id) { table_id_ = id; }

    // Multi-column sort info from pathkeys (set when created with pathkeys)
    void add_sort_column(int column_id, bool ascending) {
        sort_columns_.push_back(column_id);
        sort_ascending_.push_back(ascending);
    }
    bool has_sort_info() const { return !sort_columns_.empty(); }
    int get_sort_column_count() const { return sort_columns_.size(); }
    int get_sort_column_id(int idx) const { return sort_columns_[idx]; }
    bool get_sort_ascending(int idx) const { return sort_ascending_[idx]; }

private:
    Oid relid_ = InvalidOid;
    int table_id_ = -1;
    // Multi-column sort info extracted from pathkeys
    std::vector<int> sort_columns_;
    std::vector<bool> sort_ascending_;
};

}  // namespace cascades
