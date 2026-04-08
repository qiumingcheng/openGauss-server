#pragma once
#include "../property/physical_property_set.h"
#include "../statistics/statistics.h"

namespace cascades {


// Forward declaration to break circular dependency
class OptimizerContext;

class TaskContext {
public:
    TaskContext(OptimizerContext* context,
                const PhysicalPropertySet& required_property,
                ColumnRefSet* required_columns,
                double upper_bound_cost);
    ~TaskContext() = default;

    OptimizerContext* get_optimizer_context() const { return context_; }
    const PhysicalPropertySet& get_required_property() const { return required_property_; }
    ColumnRefSet* get_required_columns() { return required_columns_; }
    double get_upper_bound_cost() const { return upper_bound_cost_; }
    void set_upper_bound_cost(double cost) { upper_bound_cost_ = cost; }

private:
    OptimizerContext* context_;
    PhysicalPropertySet required_property_;
    ColumnRefSet* required_columns_;
    double upper_bound_cost_;
};

}