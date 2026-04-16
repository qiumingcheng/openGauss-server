#pragma once

namespace cascades {

enum class RuleType {
    // Transformation rules (TF_*)
    TF_JOIN_COMMUTATIVITY,
    TF_JOIN_ASSOCIATIVITY,
    TF_PREDICATE_PUSH_DOWN,
    TF_FILTER_MERGE,
    TF_COLUMN_PRUNE,
    TF_OUTER_JOIN_SIMPLIFY,
    TF_CROSS_JOIN_ELIMINATE,
    TF_DUPLICATE_JOIN_ELIMINATION,
    TF_SORT_ELIMINATION,
    TF_AGGREGATION_SIMPLIFY,
    TF_SUBQUERY_UNNEST,
    TF_CONSTANT_FOLD,

    // Implementation rules (IMP_*)
    IMP_HASH_JOIN,
    IMP_MERGE_JOIN,
    IMP_NESTLOOP_JOIN,
    IMP_HASH_AGG,
    IMP_PROJECT,
    IMP_SORT,
    IMP_SCAN,
    IMP_INDEX_SCAN,

    NUM_RULES
};

/**
 * Check if a rule type is a transformation rule
 */
inline bool is_transformation_rule(RuleType type) {
    return type >= RuleType::TF_JOIN_COMMUTATIVITY &&
           type <= RuleType::TF_CONSTANT_FOLD;
}

/**
 * Check if a rule type is an implementation rule
 */
inline bool is_implementation_rule(RuleType type) {
    return type >= RuleType::IMP_HASH_JOIN &&
           type <= RuleType::IMP_INDEX_SCAN;
}

}