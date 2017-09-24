set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result")

set(datamgr_source_files
    DataMgr.cpp
)

add_library(DataMgr ${datamgr_source_files})

target_link_libraries(DataMgr ${Boost_THREAD_LIBRARY} ${Glog_LIBRARIES})
