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

    // Get predicate information
    List* predicate_ids = filter->get_predicate_ids();
    void* qual_list = filter->get_qual_list();

    // Check if we have predicates
    bool has_predicates = (predicate_ids != nullptr && list_length(predicate_ids) > 0) ||
                          (qual_list != nullptr);

    if (!has_predicates) {
        // No predicates - eliminate filter, return child directly
        ereport(WARNING, (errmsg("[CASCADES] FilterImplRule: no predicates, eliminate filter")));
        return {child};
    }

    // Create PhysicalFilter operator
    // PhysicalFilter applies runtime predicates on top of child plan
    ereport(WARNING, (errmsg("[CASCADES] FilterImplRule: creating PhysicalFilter")));

    PhysicalFilter* physical_filter = NEW_EX(PhysicalFilter);

    // Transfer predicate information to PhysicalFilter
    // For multi-predicate support, use qual_list
    if (qual_list != nullptr) {
        physical_filter->set_qual_list(qual_list);
    }
    if (predicate_ids != nullptr && list_length(predicate_ids) > 0) {
        physical_filter->set_predicate_ids(predicate_ids);
    } else if (filter->has_predicate()) {
        // Single predicate fallback
        physical_filter->set_predicate_id(filter->get_predicate_id());
    }

    // Build new expression with PhysicalFilter on top of child
    OptExpression* new_expr = OptExpression::create(physical_filter, child);

    ereport(WARNING, (errmsg("[CASCADES] FilterImplRule: created PhysicalFilter on child %s",
                         child->to_string().c_str())));

    return {new_expr};
}

}  // namespace cascades
