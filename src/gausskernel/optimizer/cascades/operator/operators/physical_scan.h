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
    struct SortColumnInfo {
        int column_id = -1;
        bool ascending = true;
        int table_id = -1;
    };

    void add_sort_column(int column_id, bool ascending, int table_id = -1) {
        SortColumnInfo info;
        info.column_id = column_id;
        info.ascending = ascending;
        info.table_id = table_id;
        sort_columns_.push_back(info);
    }
    bool has_sort_info() const { return !sort_columns_.empty(); }
    int get_sort_column_count() const { return sort_columns_.size(); }
    int get_sort_column_id(int idx) const { return sort_columns_[idx].column_id; }
    bool get_sort_ascending(int idx) const { return sort_columns_[idx].ascending; }
    int get_sort_table_id(int idx) const { return sort_columns_[idx].table_id; }

private:
    Oid relid_ = InvalidOid;
    int table_id_ = -1;
    // Multi-column sort info extracted from pathkeys
    std::vector<SortColumnInfo> sort_columns_;
};

}  // namespace cascades
