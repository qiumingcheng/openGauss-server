#pragma once
#include "../operator.h"
#include "nodes/relation.h"
#include "nodes/pg_list.h"

namespace cascades {

/**
 * PhysicalSubqueryScan - Physical subquery scan operator
 *
 * Executes a pre-planned subquery and returns its results.
 * The subplan is generated during optimization and executed
 * at runtime.
 */
class PhysicalSubqueryScan : public PhysicalOptExprOp {
public:
    explicit PhysicalSubqueryScan(Path* subquery_path);
    PhysicalSubqueryScan(Path* subquery_path, PlannerInfo* subroot);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
    bool equals(const OptExprOp* other) const override;

    // Path and root for subquery execution
    Path* get_subquery_path() const { return subquery_path_; }
    PlannerInfo* get_subroot() const { return subroot_; }
    void set_subroot(PlannerInfo* subroot) { subroot_ = subroot; }

    // Subplan parameters (for parameterized execution)
    List* get_subplan_params() const { return subplan_params_; }
    void set_subplan_params(List* params) { subplan_params_ = params; }

private:
    Path* subquery_path_;              // Path for subquery execution
    PlannerInfo* subroot_ = nullptr;   // PlannerInfo from subquery planning
    List* subplan_params_ = nullptr;   // Parameters needed by subplan
};

}  // namespace cascades
