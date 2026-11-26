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
    metrics.cpp
)
target_include_directories(kvstore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(kvstore PUBLIC Threads::Threads)

add_executable(kvstore_cli cli.cpp metrics_http.cpp)
target_link_libraries(kvstore_cli PRIVATE kvstore Threads::Threads)

add_library(kvstore_mapreduce STATIC mapreduce.cpp word_count.cpp)
target_include_directories(kvstore_mapreduce PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(kvstore_mapreduce PUBLIC Threads::Threads)
add_executable(kvstore_word_count examples/word_count_main.cpp)
target_link_libraries(kvstore_word_count PRIVATE kvstore_mapreduce kvstore)

set(BENCH_REVISION "")
set(BENCH_SOURCE_STATE "")
find_package(Git QUIET)
if(Git_FOUND)
    execute_process(COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} OUTPUT_VARIABLE BENCH_REVISION
        OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
    if(BENCH_REVISION)
        execute_process(COMMAND ${GIT_EXECUTABLE} status --porcelain
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} OUTPUT_VARIABLE BENCH_CHANGES
            RESULT_VARIABLE BENCH_GIT_RESULT ERROR_QUIET)
        if(BENCH_GIT_RESULT EQUAL 0)
            if(BENCH_CHANGES)
                set(BENCH_SOURCE_STATE dirty)
            else()
                set(BENCH_SOURCE_STATE clean)
            endif()
        endif()
    endif()
endif()
configure_file(benchmarks/benchmark_build.hpp.in benchmark_build.hpp @ONLY)
add_executable(kvstore_bench bench.cpp)
target_include_directories(kvstore_bench PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(kvstore_bench PRIVATE kvstore Threads::Threads)

if(BUILD_TESTING)
    add_executable(kvstore_mapreduce_test tests/mapreduce_tests.cpp)
    target_link_libraries(kvstore_mapreduce_test PRIVATE kvstore_mapreduce)
    add_test(NAME kvstore_mapreduce_suite COMMAND kvstore_mapreduce_test)
    set_tests_properties(kvstore_mapreduce_suite PROPERTIES TIMEOUT 120)
    add_test(NAME kvstore_benchmark_math COMMAND kvstore_bench --self-test)
    add_executable(kvstore_test main.cpp)
    target_link_libraries(kvstore_test PRIVATE kvstore Threads::Threads)
    # The existing suite uses assert; keep its checks active in Release builds too.
    target_compile_options(kvstore_test PRIVATE -UNDEBUG)
    add_test(NAME kvstore_suite COMMAND kvstore_test)

    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        target_compile_definitions(kvstore_mapreduce_test PRIVATE MAPREDUCE_WRAP_FSYNC)
        target_link_options(kvstore_mapreduce_test PRIVATE -Wl,--wrap=fsync)
        find_package(Python3 QUIET COMPONENTS Interpreter)
        if(Python3_Interpreter_FOUND)
            add_test(NAME kvstore_benchmark_smoke COMMAND ${Python3_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_smoke.py $<TARGET_FILE:kvstore_bench>
                ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/compare.py)
            set_tests_properties(kvstore_benchmark_smoke PROPERTIES TIMEOUT 120)
        endif()
        # Linker wrapping injects failures into real POSIX I/O without production hooks.
        add_executable(kvstore_failure_test tests/failure_tests.cpp)
        target_link_libraries(kvstore_failure_test PRIVATE kvstore Threads::Threads)
        target_link_options(kvstore_failure_test PRIVATE
            -Wl,--wrap=open -Wl,--wrap=write -Wl,--wrap=read
            -Wl,--wrap=ftruncate -Wl,--wrap=fdatasync -Wl,--wrap=fsync
            -Wl,--wrap=rename -Wl,--wrap=unlink)
        add_test(NAME kvstore_failures COMMAND kvstore_failure_test)
        set_tests_properties(kvstore_failures PROPERTIES TIMEOUT 120)
    endif()
    set_tests_properties(kvstore_suite PROPERTIES TIMEOUT 180)
endif()
