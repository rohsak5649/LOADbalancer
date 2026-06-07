cmake_minimum_required(VERSION 3.15)
project(LOADbalancer CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find threads library (needed for std::thread and thread pools)
find_package(Threads REQUIRED)

# Include the headers from our local include folder
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(LOADbalancer Load.cpp)

# Link threads
target_link_libraries(LOADbalancer PRIVATE Threads::Threads)
