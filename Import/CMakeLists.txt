add_library(CsvImport ArrowSupport.cpp Importer.cpp DataGen.cpp DelimitedSupport.cpp Detector.cpp TypedImportBuffer.cpp)
target_link_libraries(CsvImport mapd_thrift Shared Catalog DataMgr StringDictionary ${Glog_LIBRARIES} ${CMAKE_DL_LIBS})
