add_library(DelimitedWriter DelimitedWriter.cpp DelimitedWriter.h)
target_link_libraries(DelimitedWriter mapd_thrift Shared ${Glog_LIBRARIES} ${CMAKE_DL_LIBS})
