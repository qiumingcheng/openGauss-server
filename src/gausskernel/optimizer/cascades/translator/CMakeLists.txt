# cascades/translator/CMakeLists.txt
set(CASCADES_TRANSLATOR_SOURCES
    ${CASCADES_TRANSLATOR_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/query_to_expression.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/expression_to_plan.cpp
    PARENT_SCOPE)
