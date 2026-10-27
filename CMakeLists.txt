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

include(CTest)
find_package(Threads REQUIRED)

add_library(kvstore STATIC
    kvstore.cpp
)
target_include_directories(kvstore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(kvstore PUBLIC Threads::Threads)

add_executable(kvstore_cli cli.cpp)
target_link_libraries(kvstore_cli PRIVATE kvstore Threads::Threads)

add_executable(kvstore_bench bench.cpp)
target_link_libraries(kvstore_bench PRIVATE kvstore Threads::Threads)

if(BUILD_TESTING)
    add_executable(kvstore_test main.cpp)
    target_link_libraries(kvstore_test PRIVATE kvstore Threads::Threads)
    # The existing suite uses assert; keep its checks active in Release builds too.
    target_compile_options(kvstore_test PRIVATE -UNDEBUG)
    add_test(NAME kvstore_suite COMMAND kvstore_test)

    # Linux linker wrapping injects failures into real POSIX I/O without production hooks.
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        add_executable(kvstore_failure_test tests/failure_tests.cpp)
        target_link_libraries(kvstore_failure_test PRIVATE kvstore Threads::Threads)
        target_link_options(kvstore_failure_test PRIVATE
            -Wl,--wrap=open -Wl,--wrap=write -Wl,--wrap=read
            -Wl,--wrap=ftruncate -Wl,--wrap=fdatasync -Wl,--wrap=fsync
            -Wl,--wrap=rename)
        add_test(NAME kvstore_failures COMMAND kvstore_failure_test)
        set_tests_properties(kvstore_failures PROPERTIES TIMEOUT 120)
    endif()
    set_tests_properties(kvstore_suite PROPERTIES TIMEOUT 180)
endif()
