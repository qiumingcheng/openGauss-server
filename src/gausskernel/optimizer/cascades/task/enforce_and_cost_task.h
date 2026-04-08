#pragma once
#include "optimizer_task.h"
#include "../property/physical_property_set.h"

namespace cascades {


class Group;
class GroupExpression;

class EnforceAndCostTask : public OptimizerTask {
public:
    EnforceAndCostTask(TaskContext* context, Group* group,
                        const PhysicalPropertySet& required_property);
    void execute() override;
    std::string to_string() const override;

private:
    /**
     * Compute the cost of enforcing property requirements
     */
    double compute_enforcement_cost(const PhysicalPropertySet& output_property,
                                    const PhysicalPropertySet& required_property) const;

    /**
     * Add enforcer operators (Sort/Exchange) when output property doesn't satisfy required property
     */
    GroupExpression* add_enforcers(GroupExpression* expr,
                                     const PhysicalPropertySet& output_property,
                                     const PhysicalPropertySet& required_property) const;

private:
    Group* group_;
    PhysicalPropertySet required_property_;
};

}