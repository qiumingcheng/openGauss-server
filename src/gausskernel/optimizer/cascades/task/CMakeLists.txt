# cascades/task/CMakeLists.txt
set(CASCADES_TASK_SOURCES
    ${CASCADES_TASK_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/optimizer_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task_context.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task_scheduler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/explore_group_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/optimize_expression_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/apply_rule_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/derive_stats_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/enforce_and_cost_task.cpp
    PARENT_SCOPE)