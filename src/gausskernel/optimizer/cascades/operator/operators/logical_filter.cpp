#include "logical_filter.h"

namespace cascades {

bool LogicalFilter::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const LogicalFilter* other_filter = static_cast<const LogicalFilter*>(other);
    if (!equal(qual_list_, other_filter->qual_list_)) {
        return false;
    }
    return true;
}

}  // namespace cascades
