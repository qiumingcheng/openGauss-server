# CMakeLists.txt for cascades unit tests
set(TGT_ut_cascades_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/physical_hash_join_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/physical_sort_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/physical_hash_agg_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/physical_merge_join_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/physical_nest_loop_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/physical_scan_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/physical_exchange_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/logical_join_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/logical_inner_join_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/logical_filter_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/logical_project_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/logical_scan_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/logical_aggregate_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/logical_subquery_test.cpp
        )

INCLUDE_DIRECTORIES(
        ${PROJECT_SRC_DIR}/include
        ${PROJECT_SRC_DIR}/src/gausskernel/optimizer/cascades
        )

add_executable(ut_cascades_opengauss ${TGT_ut_cascades_SRC})
TARGET_LINK_LIBRARIES(ut_cascades_opengauss ${UNIT_TEST_BASE_LIB_LIST})

target_compile_options(ut_cascades_opengauss PRIVATE ${OPTIMIZE_LEVEL})
target_link_options(ut_cascades_opengauss PRIVATE ${UNIT_TEST_LINK_OPTIONS_LIB_LIST})
add_custom_command(TARGET ut_cascades_opengauss
        POST_BUILD
        COMMAND mkdir -p ${CMAKE_BINARY_DIR}/ut_bin
        COMMAND rm -rf ${CMAKE_BINARY_DIR}/ut_bin/ut_cascades_opengauss
        COMMAND cp ${CMAKE_BINARY_DIR}/${openGauss}/src/test/ut/cascades/ut_cascades_opengauss ${CMAKE_BINARY_DIR}/ut_bin/ut_cascades_opengauss
        COMMAND chmod +x ${CMAKE_BINARY_DIR}/ut_bin/ut_cascades_opengauss
        )

add_custom_target(ut_cascades_test
        DEPENDS ut_cascades_opengauss
        COMMAND ${CMAKE_BINARY_DIR}/ut_bin/ut_cascades_opengauss || sleep 0
        COMMENT "begin cascades unit test..."
        )