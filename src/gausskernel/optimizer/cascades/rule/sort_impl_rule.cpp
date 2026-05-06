#include "sort_impl_rule.h"
#include "../operator/operators.h"
#include "../operator/operators/physical_sort.h"
#include "../operator/operators/physical_passthrough.h"
#include "../memo/opt_expression.h"
#include "../optimizer/optimizer_context.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "nodes/relation.h"
#include "postgres.h"

namespace cascades {

SortImplRule* SortImplRule::instance() {
    static SortImplRule inst;
    return &inst;
}

SortImplRule::SortImplRule()
    : Rule(RuleType::IMP_SORT,
           Pattern::create(OptExprOpType::LOGICAL_SORT,
                          Pattern::any())) {}

bool SortImplRule::sort_matches_group_keys(
    void* sort_clause, const std::vector<int>& group_keys) const
{
    if (sort_clause == nullptr) {
        return false;
    }

    List* clauses = static_cast<List*>(sort_clause);
    int sort_key_count = list_length(clauses);

    // Must have same number of keys
    if (sort_key_count != (int)group_keys.size()) {
        return false;
    }

    return true;
}

LogicalScan* SortImplRule::find_base_scan_with_sort_check(
    OptExpression* expr, void* sort_clause) const
{
    if (expr == nullptr) {
        return nullptr;
    }

    OptExprOp* op = expr->get_op();
    if (op == nullptr) {
        return nullptr;
    }

    OptExprOpType op_type = op->get_type();

    if (op_type == OptExprOpType::LOGICAL_SCAN) {
        return static_cast<LogicalScan*>(op);
    }

    if (op_type == OptExprOpType::LOGICAL_FILTER) {
        if (expr->arity() > 0) {
            return find_base_scan_with_sort_check(expr->input_at(0), sort_clause);
        }
        return nullptr;
    }

    if (op_type == OptExprOpType::LOGICAL_PROJECT) {
        if (expr->arity() > 0) {
            return find_base_scan_with_sort_check(expr->input_at(0), sort_clause);
        }
        return nullptr;
    }

    if (op_type == OptExprOpType::LOGICAL_AGGREGATE) {
        LogicalAggregate* aggr = static_cast<LogicalAggregate*>(op);
        const std::vector<int>& group_keys = aggr->get_group_keys();

        if (sort_matches_group_keys(sort_clause, group_keys)) {
            if (expr->arity() > 0) {
                return find_base_scan_with_sort_check(expr->input_at(0), nullptr);
            }
        }
        return nullptr;
    }

    return nullptr;
}

LogicalScan* SortImplRule::find_logical_scan(OptExpression* expr) const
{
    return find_base_scan_with_sort_check(expr, nullptr);
}

bool SortImplRule::can_use_index_for_sort(OptExpression* scan_expr, void* sort_clause, OptimizerContext* context) const
{
    if (sort_clause == nullptr) {
        return false;
    }

    LogicalScan* scan = find_base_scan_with_sort_check(scan_expr, sort_clause);
    if (scan == nullptr) {
        return false;
    }

    int table_id = scan->get_table_id();

    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        return false;
    }

    RelOptInfo* rel = find_base_rel(root, table_id);
    if (rel == nullptr || rel->indexlist == NIL) {
        return false;
    }

    IndexOptInfo* index = static_cast<IndexOptInfo*>(lfirst(list_head(rel->indexlist)));
    if (index == nullptr || index->sortopfamily == NULL) {
        return false;
    }

    SortGroupClause* first_sort = static_cast<SortGroupClause*>(lfirst(list_head((List*)sort_clause)));
    if (first_sort == nullptr) {
        return false;
    }

    return true;
}

void SortImplRule::set_sort_requirement_on_scan(OptExpression* expr, void* sort_clause) const
{
    if (expr == nullptr || sort_clause == nullptr) {
        return;
    }

    OptExprOp* op = expr->get_op();
    if (op == nullptr) {
        return;
    }

    OptExprOpType op_type = op->get_type();

    if (op_type == OptExprOpType::LOGICAL_SCAN) {
        LogicalScan* scan = static_cast<LogicalScan*>(op);
        scan->set_sort_clause(sort_clause);
        return;
    }

    // Recursively set on child for filter/project/aggregate
    if (op_type == OptExprOpType::LOGICAL_FILTER ||
        op_type == OptExprOpType::LOGICAL_PROJECT ||
        op_type == OptExprOpType::LOGICAL_AGGREGATE) {
        if (expr->arity() > 0) {
            set_sort_requirement_on_scan(expr->input_at(0), sort_clause);
        }
    }
}

OptExpression* SortImplRule::clone_expression_tree(OptExpression* expr, void* sort_clause) const
{
    if (expr == nullptr) {
        return nullptr;
    }

    // Clone children first
    std::vector<OptExpression*> cloned_children;
    for (int i = 0; i < expr->arity(); ++i) {
        cloned_children.push_back(clone_expression_tree(expr->input_at(i), sort_clause));
    }

    // Clone the operator
    OptExprOp* cloned_op = expr->get_op()->clone();

    // Create new OptExpression with cloned operator and children
    OptExpression* result = OptExpression::create_with_childrens(cloned_op, cloned_children);

    // Set sort requirement on the LogicalScan in the cloned tree
    if (sort_clause != nullptr) {
        set_sort_requirement_on_scan(result, sort_clause);
    }

    return result;
}

std::vector<OptExpression*> SortImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    LogicalSort* logical_sort = static_cast<LogicalSort*>(input->get_op());
    void* sort_clause = logical_sort->get_sort_clause();
    OptExpression* child = input->input_at(0);

    std::vector<OptExpression*> results;

    // Path 1: PhysicalSort(child) - explicit sort
    PhysicalSort* physical_sort = NEW_EX(PhysicalSort);
    physical_sort->set_sort_clause(sort_clause);
    OptExpression* sort_expr = OptExpression::create(physical_sort, child);
    results.push_back(sort_expr);

    ereport(WARNING, (errmsg("[CASCADES] SortImplRule: generated PhysicalSort path")));

    // Path 2: PassThrough(child) - eliminate sort by using index
    // PassThrough is a physical operator that passes through the required property
    // to its child. If child can produce sorted output (via index), Sort is eliminated.
    // The children_required_property_ will be set by ApplyRuleTask when PassThrough
    // is inserted into memo.
    if (can_use_index_for_sort(child, sort_clause, context)) {
        PassThroughOperator* pass = NEW_EX(PassThroughOperator);
        OptExpression* pass_expr = OptExpression::create(pass, child);
        pass_expr->set_sort_requirement(sort_clause);
        results.push_back(pass_expr);

        ereport(WARNING, (errmsg("[CASCADES] SortImplRule: generated PassThrough path (sort elimination)")));
    }

    return results;
}

}  // namespace cascades