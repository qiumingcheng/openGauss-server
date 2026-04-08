# cascades/rule/CMakeLists.txt
# Rules are now flat in this directory (transformation + implementation merged)

set(CASCADES_RULES_SOURCES
    ${CASCADES_RULES_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/pattern.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule_set.cpp
    # Transformation rules
    ${CMAKE_CURRENT_SOURCE_DIR}/aggregation_simplify_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/column_prune_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/constant_fold_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cross_join_elimination_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duplicate_join_elimination_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/filter_merge_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/join_association_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/join_commutativity_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/outer_join_simplify_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/predicate_push_down_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sort_elimination_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/subquery_unnest_rule.cpp
    # Implementation rules
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_agg_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/hash_join_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index_scan_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/merge_join_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/nest_loop_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/scan_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sort_impl_rule.cpp
    PARENT_SCOPE)
