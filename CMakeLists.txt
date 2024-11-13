cmake_minimum_required(VERSION 3.10)
project(BuzzDB)

# Specify the C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Add the executable
add_executable(buzzdb buzzdb.cpp)

# Add any required libraries
target_link_libraries(buzzdb PRIVATE)

# Optional: Set compiler flags
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(buzzdb PRIVATE -Wall -Wextra)
endif() 