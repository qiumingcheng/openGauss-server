# Native lake runtime is part of gaussdb, not a dynamically loaded module.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_og_lake_SRC)
# Protocol DDL is a compile-time include, not an extension installation file.
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/lake_catalog.cpp PROPERTIES
    OBJECT_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/lake_catalog_ddl.inc")
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/lake_snapshot_info.cpp PROPERTIES
    OBJECT_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/lake_snapshot_keywords.inc")
# AUX_SOURCE_DIRECTORY includes lake_index_maintenance.cpp unconditionally;
# optional method adapters remain guarded inside their existing source files.
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/lake_index_maintenance.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_maintenance.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_file_ann_catalog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_file_scalar.cpp PROPERTIES
    OBJECT_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/lake_index_maintenance.h")
set(TGT_og_lake_INC ${PROJECT_OPENGS_DIR}/contrib/og_lake ${PROJECT_OPENGS_DIR}/contrib)
set(og_lake_DEF_OPTIONS ${MACRO_OPTIONS})
# Environment paths are intentionally scoped to this module, never to the SDK.
set(OG_LAKE_ROARING_INCLUDE "$ENV{OG_LAKE_ROARING_INCLUDE}")
set(OG_LAKE_ROARING_ARCHIVE "$ENV{OG_LAKE_ROARING_ARCHIVE}")
if(NOT "${OG_LAKE_ROARING_INCLUDE}${OG_LAKE_ROARING_ARCHIVE}" STREQUAL "")
    if(NOT EXISTS "${OG_LAKE_ROARING_INCLUDE}/roaring/roaring.h" OR
       NOT EXISTS "${OG_LAKE_ROARING_ARCHIVE}" OR
       NOT "${OG_LAKE_ROARING_ARCHIVE}" MATCHES "/libroaring\\.a$")
        message(FATAL_ERROR "Native bitmap requires CRoaring headers and a private libroaring.a")
    endif()
    foreach(bitmap_source lake_bitmap_codec.cpp lake_bitmap_format.cpp)
        if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${bitmap_source}")
            message(FATAL_ERROR "Native bitmap implementation is incomplete: ${bitmap_source}")
        endif()
    endforeach()
    list(PREPEND TGT_og_lake_INC "${OG_LAKE_ROARING_INCLUDE}")
    list(APPEND og_lake_DEF_OPTIONS USE_LAKE_BITMAP)
    set_property(GLOBAL PROPERTY OG_LAKE_ROARING_ARCHIVE "${OG_LAKE_ROARING_ARCHIVE}")
    install(FILES "${OG_LAKE_ROARING_INCLUDE}/../licenses/CRoaring-LICENSE"
            DESTINATION share/postgresql/extension/)
else()
    # AUX_SOURCE_DIRECTORY otherwise includes optional sources unconditionally.
    list(REMOVE_ITEM TGT_og_lake_SRC
         "${CMAKE_CURRENT_SOURCE_DIR}/lake_bitmap_codec.cpp"
         "${CMAKE_CURRENT_SOURCE_DIR}/lake_bitmap_format.cpp")
endif()
# lake_file_pq.cpp remains in AUX_SOURCE_DIRECTORY with a no-PQ raw stub.
set(OG_LAKE_PQ_IN_TREE "$ENV{OG_LAKE_PQ_IN_TREE}")
set(OG_LAKE_PQ_INCLUDE "$ENV{OG_LAKE_PQ_INCLUDE}")
set(OG_LAKE_PQ_ARCHIVE "$ENV{OG_LAKE_PQ_ARCHIVE}")
set(OG_LAKE_PQ_BLAS_INCLUDE "$ENV{OG_LAKE_PQ_BLAS_INCLUDE}")
set(OG_LAKE_PQ_BLAS_ARCHIVE "$ENV{OG_LAKE_PQ_BLAS_ARCHIVE}")
set(OG_LAKE_PQ_BLAS_LICENSES "$ENV{OG_LAKE_PQ_BLAS_LICENSES}")
set(OG_LAKE_PQ_MANIFEST "$ENV{OG_LAKE_PQ_MANIFEST}")
set(OG_LAKE_PQ_IN_TREE_ENABLED FALSE)
set(OG_LAKE_PQ_ACTIVE_INCLUDE "")
if("${OG_LAKE_PQ_ARCHIVE}" STREQUAL "" AND "${OG_LAKE_PQ_IN_TREE}" MATCHES "^(1|yes|true)$")
    if(NOT EXISTS "${OG_LAKE_PQ_BLAS_INCLUDE}/cblas.h" OR
       NOT EXISTS "${OG_LAKE_PQ_BLAS_INCLUDE}/openblas_config.h" OR
       NOT EXISTS "${OG_LAKE_PQ_BLAS_ARCHIVE}" OR
       NOT "${OG_LAKE_PQ_BLAS_ARCHIVE}" MATCHES "/libog_lake_pq_openblas\\.a$")
        message(FATAL_ERROR "Native in-tree PQ requires admitted OpenBLAS headers and libog_lake_pq_openblas.a")
    endif()
    set(OG_LAKE_PQ_IN_TREE_ENABLED TRUE)
    set(OG_LAKE_PQ_ACTIVE_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/pq")
    add_subdirectory(pq)
    get_property(OG_LAKE_PQ_ARCHIVE GLOBAL PROPERTY OG_LAKE_PQ_ARCHIVE)
    list(PREPEND TGT_og_lake_INC "${OG_LAKE_PQ_ACTIVE_INCLUDE}")
    list(APPEND og_lake_DEF_OPTIONS USE_LAKE_PQ_CPU)
    set_property(GLOBAL PROPERTY OG_LAKE_PQ_BLAS_ARCHIVE "${OG_LAKE_PQ_BLAS_ARCHIVE}")
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/vendor/diskann_pq/LICENSE"
            DESTINATION share/postgresql/extension/og_lake_pq_licenses/
            RENAME Microsoft-DiskANN-LICENSE)
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/vendor/diskann_pq/NOTICE.txt"
            DESTINATION share/postgresql/extension/og_lake_pq_licenses/
            RENAME Microsoft-DiskANN-NOTICE.txt)
    if("${OG_LAKE_PQ_BLAS_LICENSES}" STREQUAL "")
        set(OG_LAKE_PQ_BLAS_LICENSES "${OG_LAKE_PQ_BLAS_INCLUDE}/../licenses")
    endif()
    if(EXISTS "${OG_LAKE_PQ_BLAS_LICENSES}")
        install(DIRECTORY "${OG_LAKE_PQ_BLAS_LICENSES}/"
                DESTINATION share/postgresql/extension/og_lake_pq_licenses/OpenBLAS/)
    endif()
elseif(NOT "${OG_LAKE_PQ_INCLUDE}${OG_LAKE_PQ_ARCHIVE}${OG_LAKE_PQ_BLAS_ARCHIVE}${OG_LAKE_PQ_MANIFEST}" STREQUAL "")
    if(NOT EXISTS "${OG_LAKE_PQ_MANIFEST}" OR
       NOT EXISTS "${OG_LAKE_PQ_INCLUDE}/pq_cpu.h" OR
       NOT EXISTS "${OG_LAKE_PQ_INCLUDE}/pq_parquet_format.h" OR
       NOT EXISTS "${OG_LAKE_PQ_INCLUDE}/pq_parquet_frontier_draft.h" OR
       NOT EXISTS "${OG_LAKE_PQ_INCLUDE}/sdk/lake_parquet_sdk.h" OR
       NOT EXISTS "${OG_LAKE_PQ_ARCHIVE}" OR
       NOT EXISTS "${OG_LAKE_PQ_BLAS_ARCHIVE}" OR
       NOT "${OG_LAKE_PQ_ARCHIVE}" MATCHES "/libog_lake_pq_cpu\\.a$" OR
       NOT "${OG_LAKE_PQ_BLAS_ARCHIVE}" MATCHES "/libog_lake_pq_openblas\\.a$")
        message(FATAL_ERROR "Native PQ requires admitted headers, two private static archives and build receipt")
    endif()
    set(OG_LAKE_PQ_ACTIVE_INCLUDE "${OG_LAKE_PQ_INCLUDE}")
    list(PREPEND TGT_og_lake_INC "${OG_LAKE_PQ_INCLUDE}")
    list(APPEND og_lake_DEF_OPTIONS USE_LAKE_PQ_CPU)
    set_property(GLOBAL PROPERTY OG_LAKE_PQ_ARCHIVE "${OG_LAKE_PQ_ARCHIVE}")
    set_property(GLOBAL PROPERTY OG_LAKE_PQ_BLAS_ARCHIVE "${OG_LAKE_PQ_BLAS_ARCHIVE}")
    install(FILES "${OG_LAKE_PQ_INCLUDE}/../licenses/Microsoft-DiskANN-LICENSE"
            DESTINATION share/postgresql/extension/og_lake_pq_licenses/)
    install(DIRECTORY "${OG_LAKE_PQ_INCLUDE}/../licenses/OpenBLAS"
            DESTINATION share/postgresql/extension/og_lake_pq_licenses/)
endif()
set(og_lake_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${BIN_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(og_lake_LINK_OPTIONS ${BIN_LINK_OPTIONS})
add_static_objtarget(contrib_og_lake TGT_og_lake_SRC TGT_og_lake_INC "${og_lake_DEF_OPTIONS}" "${og_lake_COMPILE_OPTIONS}" "${og_lake_LINK_OPTIONS}")
if(OG_LAKE_ROARING_ARCHIVE)
    target_include_directories(contrib_og_lake BEFORE PRIVATE "${OG_LAKE_ROARING_INCLUDE}")
endif()
if(NOT "${OG_LAKE_PQ_ACTIVE_INCLUDE}" STREQUAL "")
    target_include_directories(contrib_og_lake BEFORE PRIVATE "${OG_LAKE_PQ_ACTIVE_INCLUDE}")
endif()
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.1.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.2.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.1--0.2.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.3.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.2--0.3.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.4.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.3--0.4.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.5.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.4--0.5.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.6.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.5--0.6.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.7.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.6--0.7.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.8.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.7--0.8.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.9.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.8--0.9.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.10.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.9--0.10.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.11.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.10--0.11.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.12.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.11--0.12.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.13.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.12--0.13.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.14.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.13--0.14.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.15.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.14--0.15.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.16.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.15--0.16.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.17.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.16--0.17.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.18.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.17--0.18.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.20.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.19--0.20.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.21.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.20--0.21.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.22.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.21--0.22.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.23.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.22--0.23.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.24.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.23--0.24.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.26.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.25--0.26.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.25.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.24--0.25.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.19.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.18--0.19.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/licenses/duckdb_radix_LICENSE
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.29.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.26--0.29.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.30.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.29--0.30.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.31.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.30--0.31.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.32.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.31--0.32.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.33.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.32--0.33.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.34.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.33--0.34.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.35.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.34--0.35.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.74.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.73--0.74.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.73.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.72--0.73.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.72.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.71--0.72.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.71.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.70--0.71.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.70.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.69--0.70.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.69.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.68--0.69.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.68.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.67--0.68.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.67.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.66--0.67.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.66.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.65--0.66.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.65.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.64--0.65.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.64.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.63--0.64.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.63.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.62--0.63.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.62.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.61--0.62.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.61.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.60--0.61.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.60.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.59--0.60.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.59.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.58--0.59.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.58.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.57--0.58.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.57.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.56--0.57.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.56.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.55--0.56.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.55.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.54--0.55.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.54.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.53--0.54.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.53.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.52--0.53.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.52.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.51--0.52.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.51.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.50--0.51.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.50.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.49--0.50.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.49.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.48--0.49.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.48.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.47--0.48.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.47.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.46--0.47.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.46.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.45--0.46.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.45.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.44--0.45.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.44.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.43--0.44.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.43.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.42--0.43.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.42.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.41--0.42.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.41.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.40--0.41.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.40.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.39--0.40.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.39.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.38--0.39.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.38.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.37--0.38.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.37.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.36--0.37.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.36.sql ${CMAKE_CURRENT_SOURCE_DIR}/og_lake--0.35--0.36.sql
        ${CMAKE_CURRENT_SOURCE_DIR}/og_lake.control
        DESTINATION share/postgresql/extension/)

set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/lake_autoanalyze.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_analyze.cpp ${CMAKE_CURRENT_SOURCE_DIR}/lake_maintenance.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_publish.cpp ${CMAKE_CURRENT_SOURCE_DIR}/lake_acl.cpp PROPERTIES
    OBJECT_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/lake_autoanalyze.h")

foreach(LAKE_RESCAN_SOURCE lake_ann_rescan.cpp lake_file_reader.cpp lake_file_ann.cpp
        lake_file_ann_catalog.cpp lake_inline_reader.cpp lake_inline_scan.cpp)
    set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/${LAKE_RESCAN_SOURCE} APPEND PROPERTY
        OBJECT_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/lake_ann_rescan.h")
endforeach()

set_property(SOURCE ${TGT_og_lake_SRC} APPEND PROPERTY
    OBJECT_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/lake_inline_private.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_partition.h")

# SQL24/25 pure contract declarations and shared parent/ANALYZE interfaces.
# AUX_SOURCE_DIRECTORY includes both real newCPP implementations in this tree.
set_property(SOURCE ${TGT_og_lake_SRC} APPEND PROPERTY OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/lake_native_partition.h")
set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/lake_native_partition_analyze.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_sample.cpp ${CMAKE_CURRENT_SOURCE_DIR}/lake_analyze.cpp
    APPEND PROPERTY OBJECT_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/lake_sample.h")

set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/lake_partition_prune.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_inline_scan.cpp APPEND PROPERTY
    OBJECT_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/lake_partition_prune.h")

# Evolving carrier descriptors and the source-bound flush view are internal ABI.
set_property(SOURCE ${TGT_og_lake_SRC} APPEND PROPERTY OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/lake_inline_sources.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_inline_reader.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_flush.h")

# DROP changes the internal transaction journal layout: qualified by a full build.
set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/lake_catalog.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_txn.cpp ${CMAKE_CURRENT_SOURCE_DIR}/lake_publish.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_drop.cpp APPEND PROPERTY
    OBJECT_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/lake_drop.h")

# ADD changes native descriptors, so integration requires a complete rebuild.
set_property(SOURCE ${TGT_og_lake_SRC} APPEND PROPERTY OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/lake_inline_private.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_inline_sources.h")

# SQL38 changes the transaction journal layout: integration requires a full build.
set_property(SOURCE ${TGT_og_lake_SRC} APPEND PROPERTY OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/lake_txn_private.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_version.h")
set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/lake_maintenance.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_rewrite.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_publish.cpp APPEND PROPERTY OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/lake_rewrite.h")
set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/lake_file_sdk.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_external_fdw.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_copy.cpp APPEND PROPERTY OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/lake_parquet_fd_sdk.h")
set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/lake_external_path.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_external_fdw.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_copy.cpp APPEND PROPERTY OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/lake_external_path.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_external_fdw.h")
set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/lake_partition.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lake_partition_prune.cpp APPEND PROPERTY OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/lake_numeric.h")
set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/lake_copy.cpp APPEND PROPERTY OBJECT_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/lake_copy.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_arrow_builder.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_file_reader.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_file_sdk.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_numeric.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_types.h;${CMAKE_CURRENT_SOURCE_DIR}/lake_vector_codec.h")
foreach(LAKE_DEFAULT_SOURCE lake_business_create.cpp lake_column_ddl.cpp lake_column_default.cpp lake_create_publish.cpp lake_default_ddl.cpp lake_publish.cpp lake_catalog.cpp)
    set_property(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/${LAKE_DEFAULT_SOURCE} APPEND PROPERTY OBJECT_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/lake_column_default.h")
endforeach()
