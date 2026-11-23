# Standalone static PQ component library for CMake builds (mirrors contrib/og_lake/pq/Makefile).
if(DEFINED OG_LAKE_PQ_IN_TREE_ENABLED AND OG_LAKE_PQ_IN_TREE_ENABLED)
    set(OG_LAKE_PQ_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
    set(OG_LAKE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")
    set(OG_LAKE_PQ_SOURCES
        "${OG_LAKE_PQ_DIR}/pq_cpu.cpp"
        "${OG_LAKE_DIR}/vendor/diskann_pq/pq_cpu_ms.cpp"
        "${OG_LAKE_PQ_DIR}/pq_cpu_locking_blas_meta.cpp"
        "${OG_LAKE_PQ_DIR}/pq_parquet_format.cpp"
        "${OG_LAKE_PQ_DIR}/pq_parquet_frontier_draft.cpp"
    )
    add_library(og_lake_pq_cpu_raw STATIC ${OG_LAKE_PQ_SOURCES})
    target_include_directories(og_lake_pq_cpu_raw PRIVATE
        "${OG_LAKE_DIR}"
        "${OG_LAKE_PQ_DIR}"
        "${OG_LAKE_PQ_BLAS_INCLUDE}"
        "${LIBOPENSSL_INCLUDE_PATH}"
    )
    target_compile_options(og_lake_pq_cpu_raw PRIVATE
        -std=c++14
        -fPIC
        -fvisibility=hidden
        -fvisibility-inlines-hidden
        -include "${OG_LAKE_PQ_DIR}/pq_cpu_locking_blas_meta.h"
    )
    target_compile_definitions(og_lake_pq_cpu_raw PRIVATE
        cblas_sgemm=oglpq_cblas_sgemm
        cblas_snrm2=oglpq_cblas_snrm2
        openblas_get_config=pq_cpu_blas_immutable_config
        openblas_get_parallel=pq_cpu_blas_parallel_checked
        openblas_get_num_threads=pq_cpu_blas_num_threads_checked
    )
    set(OG_LAKE_PQ_ISOLATED_ARCHIVE "${CMAKE_CURRENT_BINARY_DIR}/libog_lake_pq_cpu.a")
    set(OG_LAKE_PQ_SYMBOL_MAP "${CMAKE_CURRENT_BINARY_DIR}/pq-symbol-map.log")
    if(DEFINED CMAKE_OBJCOPY AND CMAKE_OBJCOPY)
        set(PQ_OBJCOPY_BIN "${CMAKE_OBJCOPY}")
    else()
        find_program(PQ_OBJCOPY_BIN objcopy REQUIRED)
    endif()
    add_custom_command(
        OUTPUT "${OG_LAKE_PQ_ISOLATED_ARCHIVE}"
        COMMAND ${CMAKE_COMMAND} -E copy "$<TARGET_FILE:og_lake_pq_cpu_raw>" "${OG_LAKE_PQ_ISOLATED_ARCHIVE}.staging"
        COMMAND bash "${OG_LAKE_PQ_DIR}/isolate_pq_symbols.sh"
                "${OG_LAKE_PQ_ISOLATED_ARCHIVE}.staging"
                "${OG_LAKE_PQ_SYMBOL_MAP}"
                "${PQ_OBJCOPY_BIN}"
                "${CMAKE_RANLIB}"
        COMMAND ${CMAKE_COMMAND} -E rename "${OG_LAKE_PQ_ISOLATED_ARCHIVE}.staging" "${OG_LAKE_PQ_ISOLATED_ARCHIVE}"
        DEPENDS og_lake_pq_cpu_raw "${OG_LAKE_PQ_DIR}/isolate_pq_symbols.sh"
        COMMENT "Isolating private symbols in libog_lake_pq_cpu.a"
    )
    add_custom_target(og_lake_pq_cpu_isolated DEPENDS "${OG_LAKE_PQ_ISOLATED_ARCHIVE}")
    set_property(GLOBAL PROPERTY OG_LAKE_PQ_IN_TREE_TARGET og_lake_pq_cpu_isolated)
    set_property(GLOBAL PROPERTY OG_LAKE_PQ_ARCHIVE "${OG_LAKE_PQ_ISOLATED_ARCHIVE}")
endif()
