#include "optimizer_context.h"
#include "../rule/join_commutativity_rule.h"
#include "../rule/join_association_rule.h"
#include "../rule/predicate_push_down_rule.h"
#include "../rule/filter_merge_rule.h"
#include "../rule/filter_impl_rule.h"
#include "../rule/column_prune_rule.h"
#include "../rule/outer_join_simplify_rule.h"
#include "../rule/cross_join_elimination_rule.h"
#include "../rule/duplicate_join_elimination_rule.h"
#include "../rule/aggregation_simplify_rule.h"
#include "../rule/subquery_unnest_rule.h"
#include "../rule/constant_fold_rule.h"
#include "../rule/hash_join_impl_rule.h"
#include "../rule/merge_join_impl_rule.h"
#include "../rule/nest_loop_impl_rule.h"
#include "../rule/hash_agg_impl_rule.h"
#include "../rule/project_impl_rule.h"
#include "../rule/sort_impl_rule.h"
#include "../rule/scan_impl_rule.h"
#include "../rule/index_scan_impl_rule.h"
#include "../rule/limit_impl_rule.h"
#include "../rule/subquery_scan_impl_rule.h"

namespace cascades {
OptimizerContext::OptimizerContext(PlannerInfo* root, MemoryContext context)
    : root_(root), memory_context_(context), memo_(context) {
    // Initialize transformation rules
    rule_set_.add_rule(JoinCommutativityRule::instance());
    rule_set_.add_rule(JoinAssociationRule::instance());
    // rule_set_.add_rule(OuterJoinSimplifyRule::instance());
    // rule_set_.add_rule(CrossJoinEliminationRule::instance());
    // rule_set_.add_rule(DuplicateJoinEliminationRule::instance());
    rule_set_.add_rule(PredicatePushDownRule::instance()); 
    // rule_set_.add_rule(AggregationSimplifyRule::instance());
    rule_set_.add_rule(SubqueryUnnestRule::instance());

    // Initialize implementation rules
    rule_set_.add_rule(FilterImplRule::instance());
    rule_set_.add_rule(HashAggImplRule::instance());
    rule_set_.add_rule(ProjectImplRule::instance());
    rule_set_.add_rule(SortImplRule::instance());
    rule_set_.add_rule(ScanImplRule::instance());
    rule_set_.add_rule(IndexScanImplRule::instance());
    rule_set_.add_rule(LimitImplRule::instance());
    rule_set_.add_rule(SubqueryScanImplRule::instance());

    rule_set_.add_rule(NestLoopImplRule::instance());
    rule_set_.add_rule(HashJoinImplRule::instance());
    // rule_set_.add_rule(MergeJoinImplRule::instance());
}


}  // namespace cascades