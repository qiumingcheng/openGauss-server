#include "logical_limit.h"
#include "nodes/nodes.h"

namespace cascades {

bool LogicalLimit::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const LogicalLimit* other_limit = static_cast<const LogicalLimit*>(other);
    if (!equal(limit_count_, other_limit->limit_count_)) {
        return false;
    }
    if (!equal(limit_offset_, other_limit->limit_offset_)) {
        return false;
    }
    return true;
}

}  // namespace cascades
