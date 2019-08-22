cmake_minimum_required(VERSION 3.9)
project(lambdastream C CXX)

enable_language(CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(PROJECT_NAME "lambdastream")
set(PROJECT_VERSION_MAJOR "0")
set(PROJECT_VERSION_MINOR "1")
set(PROJECT_VERSION_PATCH "0")
set(PROJECT_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake-modules")

set(CMAKE_CXX_FLAGS "-fPIC ${CMAKE_CXX_FLAGS}")
set(CMAKE_C_FLAGS "-fPIC ${CMAKE_C_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -Wall -pedantic -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -pedantic -Wextra")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O3 -g -Wall -pedantic -Wextra")

set(CMAKE_BUILD_TYPE RelWithDebInfo)

include(Dependencies)
add_subdirectory(external/libjiffy)

include_directories(src)
include_directories(external/libjiffy/src)

add_executable(stream_operator
        src/rate_limiter.cpp
        src/rate_limiter.h
        src/main.cpp
        src/functors.h
        src/record.h
        src/stream_operator.h
        src/channel.h
        src/output_buffers.h
        src/utils.h
        src/constants.h
        src/partitioner.h)

set_target_properties(stream_operator PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(stream_operator
        jiffy
        ${JEMALLOC_LIBRARIES}
        ${Boost_LIBRARIES}
        ${CURL_LIBRARY}
        ${OPENSSL_LIBRARIES}
        ${ZLIB_LIBRARY}
        ${CPP_REDIS_LIBRARIES})

if (NOT USE_SYSTEM_BOOST)
  add_dependencies(stream_operator boost)
endif ()

if (NOT USE_SYSTEM_JEMALLOC)
  add_dependencies(stream_operator jemalloc)
endif ()

if (NOT USE_SYSTEM_CPP_REDIS)
  add_dependencies(stream_operator cpp_redis)
endif ()
