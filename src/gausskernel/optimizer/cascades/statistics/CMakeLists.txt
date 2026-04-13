# cascades/statistics/CMakeLists.txt
set(CASCADES_STATISTICS_SOURCES
    ${CASCADES_STATISTICS_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/statistics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/column_stats.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/statistics_utils.cpp
    PARENT_SCOPE)