set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -Wall -Wno-attributes")
set(query_engine_source_files
    DateTruncate.cpp
    ExtractFromTime.cpp)

execute_process(COMMAND ${llvm_config_cmd} "--includedir"
                OUTPUT_VARIABLE LLVM_INC_FLAGS)

if(ENABLE_LLVM_DBG)
set(CLANG_SDK_INC "-I/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk/usr/include/")
set(CLANG_CRT_INC "-I/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/include/c++/v1/")
endif()

if(ENABLE_LLVM_DBG)
  set(llvm_clangpp_cmd "${LLVM_BIN_DIR}/clang++")
else()
  set(llvm_clangpp_cmd clang++)
endif()

list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_CURRENT_BINARY_DIR}/gen-cpp/)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_library(QueryEngine ${query_engine_source_files})

target_link_libraries(QueryEngine CsvImport StringDictionary ${Arrow_LIBRARIES})