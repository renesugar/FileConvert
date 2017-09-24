# Parquet
find_package(Parquet REQUIRED)
include_directories(SYSTEM ${PARQUET_INCLUDE_DIR})

add_library(ParquetImporter ParquetImporter.cpp ParquetImporter.h)
target_link_libraries(ParquetImporter mapd_thrift Shared Catalog DataMgr StringDictionary ${Glog_LIBRARIES} ${PARQUET_LIBRARIES} ${CMAKE_DL_LIBS})
