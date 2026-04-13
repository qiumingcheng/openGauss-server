#pragma once
#include "../operator.h"
#include "nodes/relation.h"

namespace cascades {

/**
 * PhysicalScan - Physical table scan operator
 *
 * Supports both table_id (for simple scans) and relid (for openGauss integration)
 */
class PhysicalScan : public PhysicalOptExprOp {
public:
    PhysicalScan();
    explicit PhysicalScan(Path* scan_path);

    // Constructor with table OID for openGauss integration
    PhysicalScan(Path* scan_path, Oid relid);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    // Table identification
    Oid get_relid() const { return relid_; }
    void set_relid(Oid relid) { relid_ = relid; }

    // Legacy table_id support
    int get_table_id() const { return table_id_; }
    void set_table_id(int id) { table_id_ = id; }

private:
    Oid relid_ = InvalidOid;  // Table OID for openGauss integration
    int table_id_ = -1;       // Legacy table identifier
};

}  // namespace cascades
