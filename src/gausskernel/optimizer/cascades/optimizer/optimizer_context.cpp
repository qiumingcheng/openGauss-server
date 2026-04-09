#include "optimizer_context.h"
#include "../rule/join_commutativity_rule.h"
#include "../rule/join_association_rule.h"
#include "../rule/predicate_push_down_rule.h"
#include "../rule/filter_merge_rule.h"
#include "../rule/column_prune_rule.h"
#include "../rule/outer_join_simplify_rule.h"
#include "../rule/cross_join_elimination_rule.h"
#include "../rule/duplicate_join_elimination_rule.h"
#include "../rule/sort_elimination_rule.h"
#include "../rule/aggregation_simplify_rule.h"
#include "../rule/subquery_unnest_rule.h"
#include "../rule/constant_fold_rule.h"
#include "../rule/hash_join_impl_rule.h"
#include "../rule/merge_join_impl_rule.h"
#include "../rule/nest_loop_impl_rule.h"
#include "../rule/hash_agg_impl_rule.h"
#include "../rule/sort_impl_rule.h"
#include "../rule/scan_impl_rule.h"
#include "../rule/index_scan_impl_rule.h"

namespace cascades {
OptimizerContext::OptimizerContext(PlannerInfo* root)
    : root_(root) {
    // Initialize transformation rules
    rule_set_.add_rule(new JoinCommutativityRule());
    rule_set_.add_rule(new JoinAssociationRule());
    rule_set_.add_rule(new PredicatePushDownRule());
    rule_set_.add_rule(new FilterMergeRule());
    rule_set_.add_rule(new ColumnPruneRule());
    rule_set_.add_rule(new OuterJoinSimplifyRule());
    rule_set_.add_rule(new CrossJoinEliminationRule());
    rule_set_.add_rule(new DuplicateJoinEliminationRule());
    rule_set_.add_rule(new SortEliminationRule());
    rule_set_.add_rule(new AggregationSimplifyRule());
    rule_set_.add_rule(new SubqueryUnnestRule());
    rule_set_.add_rule(new ConstantFoldRule());

    // Initialize implementation rules
    rule_set_.add_rule(new HashJoinImplRule());
    rule_set_.add_rule(new MergeJoinImplRule());
    rule_set_.add_rule(new NestLoopImplRule());
    rule_set_.add_rule(new HashAggImplRule());
    rule_set_.add_rule(new SortImplRule());
    rule_set_.add_rule(new ScanImplRule());
    rule_set_.add_rule(new IndexScanImplRule());
}


}  // namespace cascades