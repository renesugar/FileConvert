add_library(MapDImporter MapDImporter.cpp MapDImporter.h)
target_link_libraries(MapDImporter mapd_thrift Shared Catalog DataMgr StringDictionary ${Glog_LIBRARIES} ${CMAKE_DL_LIBS})
