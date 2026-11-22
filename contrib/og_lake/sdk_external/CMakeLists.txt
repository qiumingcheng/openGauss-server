cmake_minimum_required(VERSION 3.25)
project(og_lake_sdk_arrow LANGUAGES C CXX)
find_package(Arrow 20.0.0 EXACT CONFIG REQUIRED)
find_package(Parquet 20.0.0 EXACT CONFIG REQUIRED)
add_library(og_lake_sdk_arrow SHARED ../sdk/arrow_parquet_provider.cpp)
target_compile_definitions(og_lake_sdk_arrow PRIVATE OG_LAKE_SDK_FD)
target_include_directories(og_lake_sdk_arrow PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(og_lake_sdk_arrow PRIVATE cxx_std_17)
target_compile_options(og_lake_sdk_arrow PRIVATE -Wall -Wextra -Werror)
target_link_libraries(og_lake_sdk_arrow PRIVATE Parquet::parquet_static Arrow::arrow_static)
# Keep private C++ dependencies local even if another backend extension uses a
# different Arrow ABI. Only the stable C entrypoint is dynamically exported.
target_link_options(og_lake_sdk_arrow PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/exports.map"
    "-Wl,--exclude-libs,ALL" "-Wl,-z,defs")
set_property(TARGET og_lake_sdk_arrow APPEND PROPERTY LINK_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/exports.map")
set_target_properties(og_lake_sdk_arrow PROPERTIES
    CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN YES
    INSTALL_RPATH "$ORIGIN" BUILD_RPATH_USE_ORIGIN YES)
install(TARGETS og_lake_sdk_arrow LIBRARY DESTINATION lib)
install(FILES ../sdk/lake_parquet_sdk.h lake_parquet_fd_sdk.h DESTINATION include/og_lake)
