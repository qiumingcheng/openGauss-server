#include "physical_filter.h"
#include "nodes/nodes.h"

namespace cascades {

bool PhysicalFilter::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const PhysicalFilter* other_filter = static_cast<const PhysicalFilter*>(other);
    if (!equal(qual_list_, other_filter->qual_list_)) {
        return false;
    }
    return true;
}

}  // namespace cascades
