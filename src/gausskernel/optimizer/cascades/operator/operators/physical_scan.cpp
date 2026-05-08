#include "physical_scan.h"
#include "postgres.h"

namespace cascades {

// ============= PhysicalSeqScan =============

PhysicalSeqScan::PhysicalSeqScan()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SEQ_SCAN),
      relid_(InvalidOid),
      table_id_(-1) {}

PhysicalSeqScan::PhysicalSeqScan(Path* scan_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_SEQ_SCAN, scan_path),
      relid_(InvalidOid),
      table_id_(-1) {}

OptExprOpType PhysicalSeqScan::get_type() const {
    return OptExprOpType::PHYSICAL_SEQ_SCAN;
}

OptExprOp* PhysicalSeqScan::clone() const {
    PhysicalSeqScan* cloned = NEW_EX(PhysicalSeqScan);
    cloned->set_relid(relid_);
    cloned->set_table_id(table_id_);
    cloned->set_path(path_);
    return cloned;
}

size_t PhysicalSeqScan::hash() const {
    size_t h = static_cast<size_t>(get_type());
    if (relid_ != InvalidOid) {
        h = h * 31 + static_cast<size_t>(relid_);
    } else {
        h = h * 31 + static_cast<size_t>(table_id_);
    }
    return h;
}

std::string PhysicalSeqScan::to_string() const {
    if (table_id_ >= 0) {
        return "PhysicalSeqScan(table_id=" + std::to_string(table_id_) + ")";
    } else if (relid_ != InvalidOid) {
        return "PhysicalSeqScan(relid=" + std::to_string(relid_) + ")";
    }
    return "PhysicalSeqScan";
}

bool PhysicalSeqScan::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const PhysicalSeqScan* other_scan = static_cast<const PhysicalSeqScan*>(other);
    if (relid_ != InvalidOid && other_scan->relid_ != InvalidOid) {
        return relid_ == other_scan->relid_;
    }
    return table_id_ == other_scan->table_id_;
}

// ============= PhysicalIndexScan =============

PhysicalIndexScan::PhysicalIndexScan()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_INDEX_SCAN),
      relid_(InvalidOid),
      table_id_(-1) {}

PhysicalIndexScan::PhysicalIndexScan(Path* index_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_INDEX_SCAN, index_path),
      relid_(InvalidOid),
      table_id_(-1) {}

OptExprOpType PhysicalIndexScan::get_type() const {
    return OptExprOpType::PHYSICAL_INDEX_SCAN;
}

OptExprOp* PhysicalIndexScan::clone() const {
    PhysicalIndexScan* cloned = NEW_EX(PhysicalIndexScan);
    cloned->set_relid(relid_);
    cloned->set_table_id(table_id_);
    cloned->set_path(path_);
    // Copy multi-column sort info
    for (size_t i = 0; i < sort_columns_.size(); i++) {
        cloned->add_sort_column(sort_columns_[i], sort_ascending_[i]);
    }
    return cloned;
}

size_t PhysicalIndexScan::hash() const {
    size_t h = static_cast<size_t>(get_type());
    if (relid_ != InvalidOid) {
        h = h * 31 + static_cast<size_t>(relid_);
    } else {
        h = h * 31 + static_cast<size_t>(table_id_);
    }
    return h;
}

std::string PhysicalIndexScan::to_string() const {
    if (table_id_ >= 0) {
        return "PhysicalIndexScan(table_id=" + std::to_string(table_id_) + ")";
    } else if (relid_ != InvalidOid) {
        return "PhysicalIndexScan(relid=" + std::to_string(relid_) + ")";
    } 
    return "PhysicalIndexScan";
}

bool PhysicalIndexScan::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const PhysicalIndexScan* other_scan = static_cast<const PhysicalIndexScan*>(other);
    if (relid_ != other_scan->relid_) {
        return false;
    } else if(table_id_ != other_scan->table_id_) {
        return false;
    } else {
        IndexPath* idx1 = (IndexPath*)path_;
        IndexPath* idx2 = (IndexPath*)other_scan->path_;
        return (idx1->indexinfo->indexoid == idx2->indexinfo->indexoid);
    }
}

}  // namespace cascades
