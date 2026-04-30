#include "logical_sort.h"
#include "nodes/nodes.h"

namespace cascades {

bool LogicalSort::equals(const OptExprOp* other) const {
    if (other == nullptr || get_type() != other->get_type()) {
        return false;
    }
    const LogicalSort* other_sort = static_cast<const LogicalSort*>(other);
    if (!equal(sort_clause_, other_sort->sort_clause_)) {
        return false;
    }
    return true;
}

}  // namespace cascades
