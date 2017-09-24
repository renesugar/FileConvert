set(catalog_source_files
    Catalog.cpp
    Catalog.h
)

get_target_property(StringDictionary_BINARY_DIR StringDictionary BINARY_DIR)
include_directories(${StringDictionary_BINARY_DIR})

add_library(Catalog ${catalog_source_files})

target_link_libraries(Catalog StringDictionary)
