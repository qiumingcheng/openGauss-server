# cascades/CMakeLists.txt
cmake_minimum_required(VERSION 3.10)
project(CascadesOptimizer)

add_subdirectory(base)
add_subdirectory(memo)
add_subdirectory(task)
add_subdirectory(operator)
add_subdirectory(rule)
add_subdirectory(property)
add_subdirectory(cost)
add_subdirectory(statistics)
add_subdirectory(optimizer)
add_subdirectory(translator)

# Memo sources
set(CASCADES_MEMO_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/memo/memo.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/memo/group.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/memo/group_expression.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/memo/opt_expression.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/memo/expression_context.cpp
)

# Task sources
set(CASCADES_TASK_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/task/optimizer_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task/task_context.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task/task_scheduler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task/explore_group_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task/optimize_expression_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task/apply_rule_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task/derive_stats_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task/enforce_and_cost_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task/rewrite_tree_task.cpp
)

# Operator sources
set(CASCADES_OPERATOR_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/logical_aggregate.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/logical_filter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/logical_inner_join.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/logical_join.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/logical_project.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/logical_scan.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/logical_subquery.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/logical_sort.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/logical_limit.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_exchange.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_filter.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_hash_agg.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_hash_join.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_merge_join.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_nest_loop.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_project.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_scan.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_sort.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/operator/operators/physical_limit.cpp
)

# Rule sources (transformation + implementation merged in rule/)
set(CASCADES_RULES_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/pattern.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/rule_set.cpp
    # Transformation rules
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/aggregation_simplify_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/column_prune_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/constant_fold_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/cross_join_elimination_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/duplicate_join_elimination_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/filter_merge_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/join_association_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/join_commutativity_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/outer_join_simplify_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/predicate_push_down_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/subquery_unnest_rule.cpp
    # Implementation rules
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/filter_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/hash_agg_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/hash_join_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/index_scan_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/limit_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/merge_join_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/nest_loop_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/project_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/scan_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/sort_impl_rule.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/rule/subquery_scan_impl_rule.cpp
)

# Property sources
set(CASCADES_PROPERTY_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/property/sort_property.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/property/distribution_property.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/property/logical_property.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/property/physical_property_set.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/property/required_property_deriver.cpp
)

# Cost sources
set(CASCADES_COST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/cost/cost_estimate.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cost/cost_model.cpp
)

# Statistics sources
set(CASCADES_STATISTICS_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/statistics/statistics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/statistics/column_stats.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/statistics/statistics_utils.cpp
)

# Optimizer sources
set(CASCADES_OPTIMIZER_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer/cascades_optimizer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer/optimizer_context.cpp
)

# Translator sources
set(CASCADES_TRANSLATOR_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/translator/query_to_expression.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/translator/expression_to_plan.cpp
)

set(CASCADES_SOURCES
    ${CASCADES_MEMO_SOURCES}
    ${CASCADES_TASK_SOURCES}
    ${CASCADES_OPERATOR_SOURCES}
    ${CASCADES_RULES_SOURCES}
    ${CASCADES_PROPERTY_SOURCES}
    ${CASCADES_COST_SOURCES}
    ${CASCADES_STATISTICS_SOURCES}
    ${CASCADES_OPTIMIZER_SOURCES}
    ${CASCADES_TRANSLATOR_SOURCES}
)

add_library(cascadesoptimizer OBJECT ${CASCADES_SOURCES})
target_compile_definitions(cascadesoptimizer PRIVATE PGXC)
target_compile_options(cascadesoptimizer PRIVATE
    -fPIC
    ${OPTIMIZE_OPTIONS}
    ${OS_OPTIONS}
    ${PROTECT_OPTIONS}
    ${WARNING_OPTIONS}
    ${BIN_SECURE_OPTIONS}
    ${CHECK_OPTIONS}
)
target_include_directories(cascadesoptimizer PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../src/include
    /usr/include
    /usr/include/python3.6m
)

# Cascades planner entry point
set(CASCADES_PLANNER_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/translator/cascades_planner.cpp
)

add_library(cascades_planner OBJECT ${CASCADES_PLANNER_SOURCES})
target_compile_definitions(cascades_planner PRIVATE PGXC)
target_compile_options(cascades_planner PRIVATE
    -fPIC
    ${OPTIMIZE_OPTIONS}
    ${OS_OPTIONS}
    ${PROTECT_OPTIONS}
    ${WARNING_OPTIONS}
    ${BIN_SECURE_OPTIONS}
    ${CHECK_OPTIONS}
)
target_include_directories(cascades_planner PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../src/include
)
