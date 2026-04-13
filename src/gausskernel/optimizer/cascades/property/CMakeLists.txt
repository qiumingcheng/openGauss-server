# cascades/property/CMakeLists.txt
set(CASCADES_PROPERTY_SOURCES
    ${CASCADES_PROPERTY_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/distribution_property.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logical_property.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/physical_property_set.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/required_property_deriver.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sort_property.cpp
    PARENT_SCOPE)