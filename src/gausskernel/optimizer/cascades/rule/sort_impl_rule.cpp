#include "sort_impl_rule.h"
#include "../operator/operators.h"
#include "../operator/operators/physical_sort.h"
#include "../memo/opt_expression.h"

namespace cascades {

SortImplRule* SortImplRule::instance() {
    static SortImplRule inst;
    return &inst;
}

SortImplRule::SortImplRule()
    : Rule(RuleType::IMP_SORT,
           Pattern::create(OptExprOpType::LOGICAL_SORT,
                          Pattern::any())) {}

std::vector<OptExpression*> SortImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    // Convert LogicalSort to PhysicalSort
    LogicalSort* logical_sort = static_cast<LogicalSort*>(input->get_op());
    void* sort_clause = logical_sort->get_sort_clause();

    PhysicalSort* physical_sort = NEW_EX(PhysicalSort);
    physical_sort->set_sort_clause(sort_clause);

    std::vector<OptExpression*> result;
    OptExpression* physical_expr = OptExpression::create(physical_sort, input->input_at(0));
    result.push_back(physical_expr);
    return result;
}

}  // namespace cascades
