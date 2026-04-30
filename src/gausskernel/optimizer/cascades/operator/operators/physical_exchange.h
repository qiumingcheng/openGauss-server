#pragma once
#include "../operator.h"
#include "nodes/relation.h"
#include "../../property/distribution_property.h"

namespace cascades {

class PhysicalExchange : public PhysicalOptExprOp {
public:
    PhysicalExchange();
    PhysicalExchange(Path* exchange_path, Path* child_path, DistributionType distribution_type);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
    bool equals(const OptExprOp* other) const override;

    Path* get_child_path() const { return child_path_; }
    DistributionType get_distribution_type() const { return distribution_type_; }

private:
    Path* child_path_ = nullptr;
    DistributionType distribution_type_ = DistributionType::ANY;
};

}  // namespace cascades
