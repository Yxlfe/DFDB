cmake_minimum_required(VERSION 2.8)
project(dfdb)

set(CMAKE_CXX_STANDARD 17)
# Compile options for Debug variant
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

# Compile options for Release variant
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O2")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2")

# set(CMAKE_CXX_FLAGS "-std=c++17 -O3 -pthread")

# 使用到的 boost 相关库需要在这里指明
find_package(Boost 1.85.0 REQUIRED COMPONENTS system filesystem thread)

MESSAGE( STATUS "Boost_INCLUDE_DIRS = ${Boost_INCLUDE_DIRS}.")
MESSAGE( STATUS "Boost_LIBRARY_DIRS = ${Boost_LIBRARY_DIRS}.")
MESSAGE( STATUS "Boost_LIBRARIES = ${Boost_LIBRARIES}.")
MESSAGE( STATUS "Boost_LIB_VERSION = ${Boost_LIB_VERSION}.")


file(GLOB SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
message(STATUS "Sources: ${SOURCES}")
add_library(dfdb STATIC ${SOURCES})

# 头文件目录
set(INC_DIR ${Boost_INCLUDE_DIRS} 
            ${CMAKE_CURRENT_SOURCE_DIR}/lib/threadpool 
            ${CMAKE_CURRENT_SOURCE_DIR}/lib/HdrHistogram_c-0.9.4/src
            ${CMAKE_CURRENT_SOURCE_DIR}/inc)

# 指定头文件路径
target_include_directories(dfdb PUBLIC
                                ${INC_DIR}
)

# 将库链接到项目中
# target_link_libraries(dynamic_fencekv ${Boost_LIBRARIES} ${CMAKE_CURRENT_SOURCE_DIR}/lib/leveldb/out-shared/libleveldb.so ${CMAKE_CURRENT_SOURCE_DIR}/lib/HdrHistogram_c-0.9.4/build/src/libhdr_histogram_static.a)
target_link_libraries(dfdb ${Boost_LIBRARIES} leveldb ${CMAKE_CURRENT_SOURCE_DIR}/lib/HdrHistogram_c-0.9.4/build/src/libhdr_histogram_static.a)