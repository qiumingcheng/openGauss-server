#include "expression_context.h"
#include "opt_expression.h"
#include "group_expression.h"
#include "group.h"
#include "../operator/operators/logical_join.h"
#include "../operator/operators/logical_aggregate.h"
#include "../operator/operators/logical_filter.h"
#include "../operator/operators/physical_scan.h"
#include "../operator/operators/physical_hash_agg.h"

namespace cascades {

ExpressionContext::ExpressionContext(OptExpression* expression) : expression_(expression) {
    init_from_expression(expression);
}

ExpressionContext::ExpressionContext(GroupExpression* group_expression)
    : expression_(nullptr), group_expression_(group_expression) {
    init_from_group_expression(group_expression);
}

ExpressionContext::~ExpressionContext() {
    if (owns_root_property_) {
        delete root_property_;
    }
}

void ExpressionContext::init_from_expression(OptExpression* expression) {
    root_property_ = expression->get_logical_property();
    owns_root_property_ = false;
    statistics_ = expression->get_statistics();

    for (auto* child : expression->get_inputs()) {
        children_properties_.push_back(child->get_logical_property());
        children_statistics_.push_back(child->get_statistics());
    }
}

void ExpressionContext::init_from_group_expression(GroupExpression* group_expr) {
    root_property_ = group_expr->get_group()->get_logical_property();
    owns_root_property_ = false;
    statistics_ = group_expr->get_group()->get_statistics();

    for (auto* child_group : group_expr->get_inputs()) {
        children_properties_.push_back(child_group->get_logical_property());
        children_statistics_.push_back(child_group->get_statistics());
    }
}

OptExprOp* ExpressionContext::get_op() const {
    if (expression_ != nullptr) return expression_->get_op();
    return group_expression_->get_op();
}

LogicalProperty* ExpressionContext::get_child_property(int index) const {
    return children_properties_[index];
}

void ExpressionContext::derive_logical_property() {
    // Delete existing property if we own it to prevent memory leak
    if (owns_root_property_ && root_property_ != nullptr) {
        delete root_property_;
    }
    root_property_ = new LogicalProperty();
    owns_root_property_ = true;
    root_property_->derive(this);
}

Statistics* ExpressionContext::get_statistics() const {
    return statistics_;
}

Statistics* ExpressionContext::get_child_statistics(int index) const {
    return children_statistics_[index];
}

ColumnRefSet ExpressionContext::get_child_output_columns(int index) const {
    return children_properties_[index]->get_output_columns();
}

int ExpressionContext::arity() const {
    if (expression_ != nullptr) return expression_->arity();
    return group_expression_->arity();
}

// ============================================================================
// Operator-specific property access helpers
// ============================================================================

bool ExpressionContext::get_join_keys(std::vector<int>& left_keys, std::vector<int>& right_keys) const {
    OptExprOp* op = get_op();
    if (op == nullptr) {
        return false;
    }

    // Check if this is a join operator
    if (op->get_type() == OptExprOpType::LOGICAL_JOIN ||
        op->get_type() == OptExprOpType::PHYSICAL_HASH_JOIN ||
        op->get_type() == OptExprOpType::PHYSICAL_MERGE_JOIN ||
        op->get_type() == OptExprOpType::PHYSICAL_NESTLOOP_JOIN) {

        LogicalJoin* join_op = dynamic_cast<LogicalJoin*>(op);
        if (join_op != nullptr) {
            left_keys = join_op->get_left_join_keys();
            right_keys = join_op->get_right_join_keys();

            // Fallback to symmetric join keys if left/right not set
            if (left_keys.empty() && right_keys.empty()) {
                left_keys = join_op->get_join_keys();
                right_keys = join_op->get_join_keys();
            }
            return !left_keys.empty() && !right_keys.empty();
        }
    }
    return false;
}

std::vector<int> ExpressionContext::get_group_keys() const {
    OptExprOp* op = get_op();
    if (op == nullptr) {
        return {};
    }

    // Check if this is an aggregate operator
    if (op->get_type() == OptExprOpType::LOGICAL_AGGREGATE ||
        op->get_type() == OptExprOpType::PHYSICAL_HASH_AGG ||
        op->get_type() == OptExprOpType::PHYSICAL_SORT_AGG) {

        // Try LogicalAggregate first
        LogicalAggregate* agg_op = dynamic_cast<LogicalAggregate*>(const_cast<OptExprOp*>(op));
        if (agg_op != nullptr) {
            return agg_op->get_group_keys();
        }

        // Try PhysicalHashAgg
        PhysicalHashAgg* hash_agg_op = dynamic_cast<PhysicalHashAgg*>(const_cast<OptExprOp*>(op));
        if (hash_agg_op != nullptr) {
            return hash_agg_op->get_group_keys();
        }
    }
    return {};
}

void* ExpressionContext::get_filter_qual_list() const {
    OptExprOp* op = get_op();
    if (op == nullptr) {
        return nullptr;
    }

    if (op->get_type() == OptExprOpType::LOGICAL_FILTER) {
        LogicalFilter* filter_op = dynamic_cast<LogicalFilter*>(const_cast<OptExprOp*>(op));
        if (filter_op != nullptr) {
            return filter_op->get_qual_list();
        }
    }
    return nullptr;
}

Oid ExpressionContext::get_scan_table_oid() const {
    OptExprOp* op = get_op();
    if (op == nullptr) {
        return InvalidOid;
    }

    if (op->get_type() == OptExprOpType::LOGICAL_SCAN ||
        op->get_type() == OptExprOpType::PHYSICAL_SCAN) {

        PhysicalScan* scan_op = dynamic_cast<PhysicalScan*>(const_cast<OptExprOp*>(op));
        if (scan_op != nullptr) {
            return scan_op->get_relid();
        }
    }
    return InvalidOid;
}

}  // namespace cascades