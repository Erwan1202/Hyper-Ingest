cmake_minimum_required(VERSION 3.20)
project(CivicCore_HyperIngest VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(EXISTS "${CMAKE_BINARY_DIR}/generators/conan_toolchain.cmake")
    include("${CMAKE_BINARY_DIR}/generators/conan_toolchain.cmake")
endif()

find_package(Boost COMPONENTS system thread QUIET)
find_package(simdjson QUIET)
find_package(Qt6 COMPONENTS Core Quick QUIET)
find_package(duckdb QUIET)

include_directories(include)
file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(${PROJECT_NAME} ${SOURCES})

if(Boost_FOUND)
    target_link_libraries(${PROJECT_NAME} PRIVATE Boost::system Boost::thread)
endif()
if(simdjson_FOUND)
    target_link_libraries(${PROJECT_NAME} PRIVATE simdjson::simdjson)
endif()
if(Qt6_FOUND)
    target_link_libraries(${PROJECT_NAME} PRIVATE Qt6::Core Qt6::Quick)
    set_property(TARGET ${PROJECT_NAME} PROPERTY QT_STANDARD_PROJECT_SETUP ON)
endif()
if(duckdb_FOUND)
    target_link_libraries(${PROJECT_NAME} PRIVATE duckdb::duckdb)
endif()