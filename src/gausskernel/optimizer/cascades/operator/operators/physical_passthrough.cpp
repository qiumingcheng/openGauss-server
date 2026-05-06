#include "physical_passthrough.h"
#include "postgres.h"

namespace cascades {

PassThroughOperator::PassThroughOperator()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_PASSTHROUGH) {}

PassThroughOperator::PassThroughOperator(Path* path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_PASSTHROUGH, path) {}

OptExprOpType PassThroughOperator::get_type() const {
    return OptExprOpType::PHYSICAL_PASSTHROUGH;
}

OptExprOp* PassThroughOperator::clone() const {
    PassThroughOperator* cloned = NEW_EX(PassThroughOperator);
    cloned->set_path(path_);
    return cloned;
}

size_t PassThroughOperator::hash() const {
    return static_cast<size_t>(get_type());
}

std::string PassThroughOperator::to_string() const {
    return "PassThrough";
}

bool PassThroughOperator::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    return true;
}

}  // namespace cascades
