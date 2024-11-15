cmake_minimum_required(VERSION 3.10)
project(BuzzDB)

# Specify the C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Set default build type to Release if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build (Debug or Release)." FORCE)
endif()

# Add the executable
add_executable(buzzdb buzzdb.cpp)

# Add any required libraries
target_link_libraries(buzzdb PRIVATE)

# Compiler flags for all build types
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(buzzdb PRIVATE -Wall -Wextra)
endif()

# Debug and Release-specific compiler flags
set(DEBUG_FLAGS "-g")
set(RELEASE_FLAGS "")

# Apply compiler flags based on build type
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(buzzdb PRIVATE ${DEBUG_FLAGS})
    message(STATUS "Debug build with flags: ${DEBUG_FLAGS}")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(buzzdb PRIVATE ${RELEASE_FLAGS})
    message(STATUS "Release build with flags: ${RELEASE_FLAGS}")
endif()
