cmake_minimum_required(VERSION 3.20)
project(CivicCore_HyperIngest VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(EXISTS "${CMAKE_BINARY_DIR}/generators/conan_toolchain.cmake")
    include("${CMAKE_BINARY_DIR}/generators/conan_toolchain.cmake")
endif()

find_package(Boost REQUIRED)
find_package(simdjson REQUIRED)
find_package(duckdb REQUIRED)
find_package(Qt6 6.6 REQUIRED COMPONENTS Core Qml)
find_package(GTest REQUIRED)

set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
set(CMAKE_AUTOUIC ON)

include_directories(include)

file(GLOB_RECURSE LIB_SOURCES "src/data/*.cpp" "src/Network/*.cpp")
file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(${PROJECT_NAME} ${SOURCES})

target_link_libraries(CivicCore_HyperIngest
    PRIVATE
    boost::boost
    simdjson::simdjson
    duckdb::duckdb
    Qt6::Core
    Qt6::Qml
)

# ============== TESTS ==============
enable_testing()

# Test sources
file(GLOB_RECURSE TEST_SOURCES "tests/*.cpp")

add_executable(CivicCore_Tests 
    ${LIB_SOURCES}
    ${TEST_SOURCES}
)

target_include_directories(CivicCore_Tests PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(CivicCore_Tests
    PRIVATE
    GTest::gtest
    GTest::gtest_main
    boost::boost
    simdjson::simdjson
    duckdb::duckdb
    Qt6::Core
)

include(GoogleTest)
gtest_discover_tests(CivicCore_Tests)