#include "physical_exchange.h"

namespace cascades {

PhysicalExchange::PhysicalExchange()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_EXCHANGE),
      distribution_type_(DistributionType::ANY) {}

PhysicalExchange::PhysicalExchange(Path* exchange_path, Path* child_path, DistributionType distribution_type)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_EXCHANGE, exchange_path),
      child_path_(child_path),
      distribution_type_(distribution_type) {}

OptExprOp* PhysicalExchange::clone() const {
    return new PhysicalExchange(path_, child_path_, distribution_type_);
}

size_t PhysicalExchange::hash() const {
    return static_cast<size_t>(get_type()) * 31 + static_cast<size_t>(distribution_type_);
}

std::string PhysicalExchange::to_string() const {
    return "PhysicalExchange(type=" + std::to_string(static_cast<int>(distribution_type_)) + ")";
}

}  // namespace cascades
