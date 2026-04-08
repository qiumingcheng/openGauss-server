#pragma once
#include "../operator.h"
#include "nodes/relation.h"

namespace cascades {

class PhysicalScan : public PhysicalOptExprOp {
public:
    PhysicalScan();
    explicit PhysicalScan(Path* scan_path);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
};

}  // namespace cascades
