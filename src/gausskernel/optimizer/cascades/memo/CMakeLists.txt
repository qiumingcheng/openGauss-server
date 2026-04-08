# cascades/memo/CMakeLists.txt
set(CASCADES_MEMO_SOURCES
    ${CASCADES_MEMO_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/memo.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/group.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/group_expression.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/opt_expression.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/expression_context.cpp
    PARENT_SCOPE)