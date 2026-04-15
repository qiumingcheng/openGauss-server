#include "../base/memory_utils.h"
#include "scan_impl_rule.h"
#include "../operator/operators.h"
#include "../memo/opt_expression.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../optimizer/optimizer_context.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"
#include "nodes/relation.h"

namespace cascades {

// Static singleton instance
ScanImplRule* ScanImplRule::instance() {
    static ScanImplRule inst;
    return &inst;
}

ScanImplRule::ScanImplRule()
    : Rule(RuleType::IMP_SCAN, Pattern::create(OptExprOpType::LOGICAL_SCAN)) {}

std::vector<OptExpression*> ScanImplRule::transform(
    OptExpression* input, OptimizerContext* context) {
    // Pattern guarantees: op_type == LOGICAL_SCAN && arity == 0
    LogicalScan* scan_op = static_cast<LogicalScan*>(input->get_op());
    int table_id = scan_op->get_table_id();

    // Get PlannerInfo
    PlannerInfo* root = context->get_root();
    if (root == nullptr) {
        return {};
    }

    // Find the base RelOptInfo for this table
    RelOptInfo* rel = find_base_rel(root, table_id);
    if (rel == nullptr) {
        return {};
    }

    // Create SeqScan path using OpenGauss function
    Path* scan_path = create_seqscan_path(root, rel, nullptr, 1);

    // Create physical scan operator using NEW_EX macro
    PhysicalScan* physical_scan = NEW_EX(PhysicalScan, scan_path);

    OptExpression* new_expr = OptExpression::create(physical_scan);
    return {new_expr};
}

}  // namespace cascades
