cmake_minimum_required(VERSION 2.8)
set(RUNTIME_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})

# Boost
find_package(Boost COMPONENTS system filesystem program_options regex REQUIRED QUIET)

# Thrift
find_package(Thrift REQUIRED)

# Arrow
find_package(Arrow REQUIRED)
include_directories(${Arrow_INCLUDE_DIRS})

# Parquet
find_package(Parquet REQUIRED)
include_directories(SYSTEM ${PARQUET_INCLUDE_DIR})

add_executable(FileConvert FileConvert.cpp)

target_link_libraries(FileConvert mapd_thrift Shared Catalog DataMgr StringDictionary QueryEngine CsvImport DelimitedImporter DelimitedWriter GDALImporter ParquetImporter ParquetWriter poly2tri ${GDAL_LIBRARIES} ${Boost_LIBRARIES} ${Glog_LIBRARIES} ${Arrow_LIBRARIES} ${PARQUET_LIBRARIES} ${CMAKE_DL_LIBS})

install(TARGETS FileConvert DESTINATION bin)
