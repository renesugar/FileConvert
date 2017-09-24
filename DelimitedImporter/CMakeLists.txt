add_library(DelimitedImporter DelimitedImporter.cpp DelimitedImporter.h)
target_link_libraries(DelimitedImporter mapd_thrift Shared Catalog DataMgr StringDictionary ${Glog_LIBRARIES} ${CMAKE_DL_LIBS})
