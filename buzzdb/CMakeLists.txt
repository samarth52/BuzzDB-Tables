cmake_minimum_required(VERSION 3.10)
project(BuzzDB VERSION 1.0)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/include)

# Find required packages
find_package(Threads REQUIRED)

# Define source files
file(GLOB_RECURSE SOURCES 
    "src/*.cpp"
)

# Define header files
file(GLOB_RECURSE HEADERS 
    "include/*.h"
)

# Create executable
add_executable(buzzdb ${SOURCES} ${HEADERS})

# Link libraries
target_link_libraries(buzzdb PRIVATE Threads::Threads)

# Set compile options
target_compile_options(buzzdb PRIVATE -Wall -Wextra -pedantic)

# Set include directories for the target
target_include_directories(buzzdb PRIVATE 
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/include/core
    ${CMAKE_SOURCE_DIR}/include/operators
    ${CMAKE_SOURCE_DIR}/include/predicates
    ${CMAKE_SOURCE_DIR}/include/index
    ${CMAKE_SOURCE_DIR}/include/query
)

# Install
install(TARGETS buzzdb DESTINATION bin)
