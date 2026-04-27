#include "../base/memory_utils.h"
#include "filter_impl_rule.h"
#include "../operator/operators.h"
#include "../operator/operators/physical_filter.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include "optimizer/pathnode.h"
#include "nodes/relation.h"

namespace cascades {

FilterImplRule* FilterImplRule::instance() {
    static FilterImplRule inst;
    return &inst;
}

FilterImplRule::FilterImplRule()
    : Rule(RuleType::IMP_FILTER,
           Pattern::create(OptExprOpType::LOGICAL_FILTER,
                          Pattern::any())) {}

std::vector<OptExpression*> FilterImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    // FilterImplRule: Transform LogicalFilter + child into PhysicalFilter
    // This rule creates a physical filter operator that applies predicates

    ereport(WARNING, (errmsg("[CASCADES] FilterImplRule:transform, input=%s",
                         input->to_string().c_str())));

    Assert(input->arity() > 0);

    LogicalFilter* filter = static_cast<LogicalFilter*>(input->get_op());
    OptExpression* child = input->input_at(0);

    // Get qual_list (List* of RestrictInfo)
    List* qual_list = filter->get_qual_list();

    // Check if we have qualifiers
    bool has_quals = (qual_list != nullptr && list_length(qual_list) > 0);

    if (!has_quals) {
        // No qualifiers - eliminate filter, return child directly
        ereport(WARNING, (errmsg("[CASCADES] FilterImplRule: no qualifiers, eliminate filter")));
        return {child};
    }

    // Create PhysicalFilter operator
    // PhysicalFilter applies runtime predicates on top of child plan
    ereport(WARNING, (errmsg("[CASCADES] FilterImplRule: creating PhysicalFilter")));

    PhysicalFilter* physical_filter = NEW_EX(PhysicalFilter);

    // Transfer qual_list to PhysicalFilter
    // qual_list contains List* of RestrictInfo (single or multiple predicates)
    physical_filter->set_qual_list(qual_list);

    // Build new expression with PhysicalFilter on top of child
    OptExpression* new_expr = OptExpression::create(physical_filter, child);

    ereport(WARNING, (errmsg("[CASCADES] FilterImplRule: created PhysicalFilter on child %s",
                         child->to_string().c_str())));

    return {new_expr};
}

}  // namespace cascades
