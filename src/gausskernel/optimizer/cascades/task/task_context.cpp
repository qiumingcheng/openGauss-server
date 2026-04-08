#include "task_context.h"

namespace cascades {
TaskContext::TaskContext(OptimizerContext* context,
                         const PhysicalPropertySet& required_property,
                         ColumnRefSet* required_columns,
                         double upper_bound_cost)
    : context_(context),
      required_property_(required_property),
      required_columns_(required_columns),
      upper_bound_cost_(upper_bound_cost) {}


}  // namespace cascades