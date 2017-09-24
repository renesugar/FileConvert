cmake_minimum_required(VERSION 2.8)

find_program(CCACHE_EXE ccache)
if(CCACHE_EXE)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_EXE}")
endif()

option(ENABLE_IWYU "Enable include-what-you-use" OFF)
if(ENABLE_IWYU)
  find_program(IWYU_EXE include-what-you-use)
  if(IWYU_EXE)
    set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE "${IWYU_EXE}")
  endif()
endif()

project(mapd)

if(NOT EXISTS "${CMAKE_SOURCE_DIR}/Rendering")
  set(MAPD_EDITION "OS")
elseif(NOT DEFINED MAPD_EDITION)
  set(MAPD_EDITION "EE")
endif()
set(MAPD_EDITION "${MAPD_EDITION}" CACHE STRING "MapD edition" FORCE)
set_property(CACHE MAPD_EDITION PROPERTY STRINGS "EE" "CE" "OS")
add_definitions("-DMAPD_EDITION_${MAPD_EDITION}")
string(TOLOWER "${MAPD_EDITION}" MAPD_EDITION_LOWER)

set(MAPD_VERSION_MAJOR "3")
set(MAPD_VERSION_MINOR "2")
set(MAPD_VERSION_PATCH "2")
set(MAPD_VERSION_EXTRA "dev")
set(MAPD_VERSION_RAW "${MAPD_VERSION_MAJOR}.${MAPD_VERSION_MINOR}.${MAPD_VERSION_PATCH}${MAPD_VERSION_EXTRA}")
set(MAPD_IMMERSE_URL "http://builds.mapd.com/frontend/immerse-v2-latest-master-prod.zip")
set(MAPD_DOCS_URL "http://docs.builds.mapd.com/master/docs-master.zip")
string(TIMESTAMP MAPD_BUILD_DATE "%Y%m%d")

if($ENV{BUILD_NUMBER})
  set(MAPD_BUILD_NUMBER "$ENV{BUILD_NUMBER}")
else()
  set(MAPD_BUILD_NUMBER "dev")
endif()
set(MAPD_VERSION "${MAPD_VERSION_RAW}-${MAPD_BUILD_NUMBER}")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# where to find cmake modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules")
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

find_package(ClangTools)
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

if(APPLE)
  set(CMAKE_MACOSX_RPATH 1)
endif()

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

# Determine compiler version
include(CompilerInfo)

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  # Using Clang with ccache causes a bunch of spurious warnings that are
  # purportedly fixed in the next version of ccache. See the following for details:
  #
  #   http://petereisentraut.blogspot.com/2011/05/ccache-and-clang.html
  #   http://petereisentraut.blogspot.com/2011/09/ccache-and-clang-part-2.html
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CLANG_OPTIONS}")
endif()


############################################################
# "make lint" target
############################################################
if (UNIX)
  find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
  message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")
  # Full lint
  add_custom_target(lint ${CPPLINT_BIN}
  --verbose=2
  --linelength=90
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-runtime/references,-readability/check,-build/c++11,-build/include_order
    `find ${CMAKE_CURRENT_SOURCE_DIR}/ -name \\*.cpp -or -name \\*.h | sed -e '/pph\\/pph_/g'`)
endif (UNIX)


############################################################
# "make format" and "make check-format" targets
############################################################

if (${CLANG_FORMAT_FOUND})
  # runs clang format and updates files in place.
  add_custom_target(format ${BUILD_SUPPORT_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1
  `find ${CMAKE_CURRENT_SOURCE_DIR}/ -name \\*.cpp -or -name \\*.h | sed -e '/_generated/g'`)

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0
  `find ${CMAKE_CURRENT_SOURCE_DIR}/ -name \\*.cpp -or -name \\*.h | sed -e '/_generated/g'`)
endif()


############################################################
# "make clang-tidy" and "make check-clang-tidy" targets
############################################################

if (${CLANG_TIDY_FOUND})
  # runs clang-tidy and attempts to fix any warning automatically
  add_custom_target(clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json 1
  `find ${CMAKE_CURRENT_SOURCE_DIR}/ -name \\*.cpp | sed -e '/_types/g' | sed -e '/_constants/g'`)
  # runs clang-tidy and exits with a non-zero exit code if any errors are found.
  add_custom_target(check-clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json
  0 `find ${CMAKE_CURRENT_SOURCE_DIR}/ -name \\*.cpp |grep -v -F -f ${CMAKE_CURRENT_SOURCE_DIR}/.clang-tidy-ignore`)
endif()


option(PREFER_STATIC_LIBS "Prefer linking against static libraries" OFF)
if(PREFER_STATIC_LIBS)
  set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
  set(Boost_USE_STATIC_LIBS ON)

  if(${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static-libgcc -static-libstdc++")
  endif()

  set(CUDA_USE_STATIC_CUDA_RUNTIME ON)
endif()

option(ENABLE_CUDA "Enable CUDA support" ON)
if(ENABLE_CUDA)
  find_package(CUDA REQUIRED)
  include_directories(${CUDA_INCLUDE_DIRS})
  list(APPEND CUDA_LIBRARIES ${CUDA_CUDA_LIBRARY})
  add_definitions("-DHAVE_CUDA")

  set(MAPD_HOST_COMPILER "" CACHE STRING "Host compiler to use with nvcc")
  if(NOT "${MAPD_HOST_COMPILER}" STREQUAL "")
      set(MAPD_HOST_COMPILER_FLAG "-ccbin=${MAPD_HOST_COMPILER}")
  endif()
else()
  set(CUDA_LIBRARIES "")
  set(MAPD_PACKAGE_FLAGS "${MAPD_PACKAGE_FLAGS}-cpu")
endif()

if(XCODE)
  if(ENABLE_CUDA)
    set(CMAKE_EXE_LINKER_FLAGS "-F/Library/Frameworks -framework CUDA")
  endif()
  add_definitions("-DXCODE")
endif()

find_package(Git)
find_package(Glog REQUIRED)
find_package(Thrift REQUIRED)
include_directories(${Thrift_INCLUDE_DIRS})
find_package(PNG REQUIRED)
find_package(ZLIB REQUIRED)
find_package(GDAL REQUIRED)
find_package(GDALExtra REQUIRED)
list(APPEND GDAL_LIBRARIES ${PNG_LIBRARIES} ${GDALExtra_LIBRARIES})
include_directories(${GDAL_INCLUDE_DIRS})

option(ENABLE_FOLLY "Use Folly" ON)
if(ENABLE_FOLLY)
  find_package(Folly REQUIRED)
  include_directories(${Folly_INCLUDE_DIRS})
  add_definitions("-DHAVE_FOLLY")
endif()

find_package(Curses REQUIRED)
include_directories(${CURSES_INCLUDE_DIRS})
if (CURSES_HAVE_NCURSES_CURSES_H AND NOT CURSES_HAVE_CURSES_H)
  include_directories(${CURSES_INCLUDE_DIRS}/ncurses/)
endif()

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux" AND (("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang"
    AND (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 3.6 OR CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL 3.6))
    OR ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 4.9)))
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -DHAS_SHARED_MUTEX")
  find_package( Boost COMPONENTS filesystem program_options regex system REQUIRED QUIET )
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
  find_package( Boost COMPONENTS filesystem program_options regex thread system REQUIRED QUIET )
  if(PREFER_STATIC_LIBS AND ${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    list(APPEND Boost_LIBRARIES rt)
    list(INSERT Boost_LIBRARIES 0 pthread)
  endif()
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-unused-local-typedefs -D__GCC_HAVE_SYNC_COMPARE_AND_SWAP_1=1 -D__GCC_HAVE_SYNC_COMPARE_AND_SWAP_2=1 -D__GCC_HAVE_SYNC_COMPARE_AND_SWAP_4=1 -D__GCC_HAVE_SYNC_COMPARE_AND_SWAP_8=1 -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS")
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-register")
endif()
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-deprecated-declarations")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
endif()

option(ENABLE_LLVM_DBG "switch to local debug version of llvm" OFF)
if(ENABLE_LLVM_DBG)
  # NOTE: specify LLVM_BIN_DIR to use your local llvm
  if(XCODE)
    set(LLVM_BIN_DIR ${CMAKE_SOURCE_DIR}/../llvm/build/xcode/Release/bin)
  elseif(APPLE)
    set(LLVM_BIN_DIR ${CMAKE_SOURCE_DIR}/../llvm/build/unix/bin)
  endif()
  set(llvm_config_cmd "${LLVM_BIN_DIR}/llvm-config")
else()
  set(llvm_config_cmd llvm-config)
endif()

# address and thread sanitizer
option(ENABLE_STANDALONE_CALCITE "Require standalone Calcite server" OFF)
option(ENABLE_ASAN "Enable address sanitizer" OFF)
option(ENABLE_TSAN "Enable thread sanitizer" OFF)
if(ENABLE_ASAN)
  set(SAN_FLAGS "-fsanitize=address -O1 -fno-omit-frame-pointer")
  add_definitions("-DWITH_DECODERS_BOUNDS_CHECKING")
elseif(ENABLE_TSAN)
  set(SAN_FLAGS "-fsanitize=thread -fPIC -O1 -fno-omit-frame-pointer")
endif()
if(ENABLE_ASAN OR ENABLE_TSAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SAN_FLAGS}")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SAN_FLAGS}")
  set(ENABLE_STANDALONE_CALCITE ON)
endif()

option(ENABLE_DECODERS_BOUNDS_CHECKING "Enable bounds checking for column decoding" OFF)

if(ENABLE_STANDALONE_CALCITE)
  add_definitions("-DSTANDALONE_CALCITE")
endif()

set(llvm_config_inc_arg "--includedir")
set(llvm_config_ld_arg "--ldflags")
set(llvm_config_lib_arg "--libs")
execute_process(COMMAND ${llvm_config_cmd} ${llvm_config_inc_arg}
    OUTPUT_VARIABLE LLVM_INC_FLAGS)
execute_process(COMMAND ${llvm_config_cmd} ${llvm_config_lib_arg}
    OUTPUT_VARIABLE LLVM_LIB_FLAGS)
execute_process(COMMAND ${llvm_config_cmd} ${llvm_config_ld_arg}
    OUTPUT_VARIABLE LLVM_LD_FLAGS)
if(ENABLE_LLVM_DBG)
  set(llvm_config_obj_arg "--obj-root")
  execute_process(COMMAND ${llvm_config_cmd} ${llvm_config_obj_arg}
    OUTPUT_VARIABLE LLVM_BUILD_DIR)
  string(REPLACE "\n" "" LLVM_BUILD_DIR "${LLVM_BUILD_DIR}")
  list(APPEND LLVM_INC_FLAGS "${LLVM_BUILD_DIR}/include")
endif()

string(REPLACE "\n" " " LLVM_LINKER_FLAGS "${LLVM_LIB_FLAGS} ${LLVM_LD_FLAGS}")
string(STRIP "${LLVM_LINKER_FLAGS}" LLVM_LINKER_FLAGS)
include_directories(${Boost_INCLUDE_DIR}
                    "/usr/local/include"
                    ${CMAKE_SOURCE_DIR}
                    ${CMAKE_SOURCE_DIR}/Parser
                    ${CMAKE_CURRENT_BINARY_DIR}
                    ${LLVM_INC_FLAGS}
                    "/usr/local/cuda/include/")

# EGL
include_directories(ThirdParty/egl)

# Google Test and Google Mock
if(NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  add_definitions("-DGTEST_USE_OWN_TR1_TUPLE=0")
endif()
include_directories(ThirdParty/googletest)
add_subdirectory(ThirdParty/googletest)

# Arrow
find_package(Arrow REQUIRED)
include_directories(${Arrow_INCLUDE_DIRS})

# RapidJSON
include_directories(ThirdParty/rapidjson)

# Poly2Tri
include_directories(ThirdParty/poly2tri)
add_subdirectory(ThirdParty/poly2tri)

# Linenoise
include_directories(ThirdParty/linenoise)
add_subdirectory(ThirdParty/linenoise)

# SQLite
include_directories(ThirdParty/sqlite3)
add_subdirectory(ThirdParty/sqlite3)

add_custom_command(
    DEPENDS ${CMAKE_SOURCE_DIR}/mapd.thrift
    OUTPUT
        ${CMAKE_BINARY_DIR}/gen-cpp/MapD.cpp
        ${CMAKE_BINARY_DIR}/gen-cpp/MapD.h
        ${CMAKE_BINARY_DIR}/gen-cpp/mapd_constants.cpp
        ${CMAKE_BINARY_DIR}/gen-cpp/mapd_types.cpp
    COMMAND ${Thrift_EXECUTABLE}
    ARGS -gen cpp -o ${CMAKE_BINARY_DIR} ${CMAKE_SOURCE_DIR}/mapd.thrift)
list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_BINARY_DIR}/gen-cpp/)

add_library(mapd_thrift
    ${CMAKE_BINARY_DIR}/gen-cpp/MapD.cpp
    ${CMAKE_BINARY_DIR}/gen-cpp/MapD.h
    ${CMAKE_BINARY_DIR}/gen-cpp/mapd_constants.cpp
    ${CMAKE_BINARY_DIR}/gen-cpp/mapd_types.cpp)
target_link_libraries(mapd_thrift ${Thrift_LIBRARIES})

option(ENABLE_PROFILER "Enable google perftools" OFF)
if(ENABLE_PROFILER)
  find_package(Gperftools REQUIRED COMPONENTS TCMALLOC PROFILER)
  set(PROFILER_LIBS ${Gperftools_TCMALLOC} ${Gperftools_PROFILER})
  add_definitions("-DHAVE_PROFILER")
else()
  set(PROFILER_LIBS "")
endif()

add_subdirectory(StringDictionary)
add_subdirectory(Catalog)
add_subdirectory(Import)
add_subdirectory(QueryEngine)
add_subdirectory(DataMgr)
add_subdirectory(Shared)
add_subdirectory(DelimitedImporter)
add_subdirectory(DelimitedWriter)
add_subdirectory(GDALImporter)
add_subdirectory(ParquetImporter)
add_subdirectory(ParquetWriter)
# TODO:(renesugar) Add MapDImporter and MapDWriter when reintegrating with MapD
# add_subdirectory(MapDImporter)
# add_subdirectory(MapDWriter)
add_subdirectory(FileConvert)

set(MAPD_LIBRARIES Shared Catalog QueryEngine DataMgr)

if(ENABLE_FOLLY)
  list(APPEND MAPD_LIBRARIES ${Folly_LIBRARIES})
endif()

list(APPEND MAPD_LIBRARIES ${Arrow_LIBRARIES})

list(APPEND MAPD_LIBRARIES ${PARQUET_LIBRARIES})

message(STATUS "Arrow home dir:    " ${ARROW_HOME})
message(STATUS "Arrow include dir: " ${Arrow_INCLUDE_DIRS})
message(STATUS "Arrow library dir: " ${Arrow_LIBRARY_DIRS})

#enable_testing()
#add_subdirectory(Tests)

execute_process(
  # NOTE: Uncomment following line after checking into GIT
  COMMAND ${GIT_EXECUTABLE} log -1 --format=%h
  # TODO: Delete following line after checking into GIT
  #COMMAND echo "ccccccc"
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE MAPD_GIT_HASH
  OUTPUT_STRIP_TRAILING_WHITESPACE
  )
file(WRITE ${CMAKE_BINARY_DIR}/MAPD_GIT_HASH.txt "${MAPD_GIT_HASH}\n")
file(STRINGS ${CMAKE_BINARY_DIR}/MAPD_GIT_HASH.txt MAPD_GIT_HASH)
set(CPACK_PACKAGE_VERSION "${MAPD_VERSION_RAW}-${MAPD_BUILD_DATE}-${MAPD_GIT_HASH}")

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/Shared/release.h"
  "${CMAKE_BINARY_DIR}/MapDRelease.h"
  @ONLY
  )
list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_BINARY_DIR}/MAPD_GIT_HASH.txt)
list(APPEND ADDITIONAL_MAKE_CLEAN_FILES ${CMAKE_BINARY_DIR}/MapDRelease.h)
# required to force regen of MAPD_GIT_HASH.txt, MapDRelease.h
add_custom_target(rerun_cmake ALL
  COMMAND cmake .
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  )
add_dependencies(FileConvert rerun_cmake)

add_custom_target(clean-all
  COMMAND ${CMAKE_BUILD_TOOL} clean
  )

set_directory_properties(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES "${ADDITIONAL_MAKE_CLEAN_FILES}")

install(FILES ${CMAKE_BINARY_DIR}/MAPD_GIT_HASH.txt DESTINATION ".")
install(FILES mapd.thrift DESTINATION ".")

if(NOT PREFER_STATIC_LIBS)
  install(FILES ${Boost_LIBRARIES} DESTINATION ThirdParty/lib)
endif()
