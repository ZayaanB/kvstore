cmake_minimum_required(VERSION 3.20)
project(kvstore CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow
    -Wconversion
    -Wsign-conversion
    -Wnon-virtual-dtor
    -Woverloaded-virtual
    -Wnull-dereference
    -Wdouble-promotion
    -Wformat=2
)

find_package(Threads REQUIRED)

add_library(kvstore STATIC
    kvstore.cpp
)
target_include_directories(kvstore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(kvstore PUBLIC Threads::Threads)

add_executable(kvstore_test main.cpp)
target_link_libraries(kvstore_test PRIVATE kvstore Threads::Threads)