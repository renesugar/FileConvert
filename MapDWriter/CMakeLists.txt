add_library(MapDWriter MapDWriter.cpp MapDWriter.h)
target_link_libraries(MapDWriter mapd_thrift Shared Catalog DataMgr StringDictionary ${Glog_LIBRARIES} ${CMAKE_DL_LIBS})
