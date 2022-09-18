cmake_minimum_required(VERSION 3.18)
project(hcache VERSION 0.1)
set(CMAKE_CXX_STANDARD 20)

include(ExternalProject)
include(ProcessorCount)
ProcessorCount(N)

ExternalProject_Add(rocksdb_src
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/rocksdb
    URL ${CMAKE_CURRENT_LIST_DIR}/third_party/rocksdb
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND ""
    BUILD_COMMAND make -j${N} static_lib
    INSTALL_COMMAND ""
    BUILD_BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/rocksdb/src/rocksdb_src/librocksdb.a"
)

ExternalProject_Add_Step(rocksdb_src forcebuild DEPENDEES configure DEPENDERS build ALWAYS 1)
ExternalProject_Get_Property(rocksdb_src source_dir)

add_library(rocksdb STATIC IMPORTED GLOBAL)
set_property(TARGET rocksdb
    PROPERTY IMPORTED_LOCATION
    "${source_dir}/librocksdb.a"
)
include_directories("${source_dir}/include")

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()
CHECK_CXX_COMPILER_FLAG("-flto" COMPILER_SUPPORTS_FLTO)
if(COMPILER_SUPPORTS_FLTO)
    if (CMAKE_BUILD_TYPE STREQUAL Release)    
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto")
    endif()
endif()

find_package(ZLIB)
find_package(Boost)
find_package(Threads)
add_subdirectory(third_party/libcds)
add_subdirectory(third_party/uringpp)
add_subdirectory(third_party/fmt)
add_subdirectory(third_party/mimalloc)
add_subdirectory(third_party/simdjson)
add_subdirectory(third_party/f-stack-co)
set(BUILD_EXAMPLES OFF)
set(BUILD_TESTING OFF)
include_directories(third_party/rapidjson/include)
include_directories(third_party/uringpp/include)
include_directories(third_party/f-stack-co/include)
include_directories(include)

add_library(hcache-common src/storage.cc src/threading.cc src/picohttpparser.c)
target_link_libraries(hcache-common 
  PUBLIC        
    rocksdb
    cds-s
    ZLIB::ZLIB
    zstd
    Boost::boost
    Threads::Threads
    snappy
    lz4
    fmt::fmt
    simdjson
    uringpp
    xxhash
)

add_executable(hcache src/hcache.cc)
add_dependencies(hcache rocksdb_src)
target_link_libraries(hcache 
  PRIVATE        
    rocksdb
    hcache-common
    cds-s
    ZLIB::ZLIB
    zstd
    Boost::boost
    Threads::Threads
    snappy
    lz4
    uringpp
    fmt::fmt
    simdjson
    xxhash
)

add_executable(pingpong src/pingpong.cc src/threading.cc)
target_link_libraries(pingpong 
  PRIVATE        
    uringpp
    fmt::fmt
)