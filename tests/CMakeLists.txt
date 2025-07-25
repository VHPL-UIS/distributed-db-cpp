# Find Google Test
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(GTEST gtest_main)
endif()

# If Google Test is not found via pkg-config, provide instructions
if(NOT GTEST_FOUND)
    message(STATUS "Google Test not found. To install on Ubuntu/Debian:")
    message(STATUS "  sudo apt-get install libgtest-dev")
    message(STATUS "To build tests manually, install Google Test and update this CMakeLists.txt")
    return()
endif()

# Create test executable
add_executable(test_distributed_db
    unit/test_storage_engine.cpp
    unit/test_config.cpp
    unit/test_wal.cpp
    unit/test_persistent_storage.cpp
    unit/test_message.cpp
    unit/test_cluster_membership.cpp
    ../src/common/config.cpp
    ../src/storage/storage_engine.cpp
    ../src/storage/wal.cpp
    ../src/storage/persistent_storage_engine.cpp
    ../src/network/message.cpp
    ../src/network/tcp_server.cpp
    ../src/network/tcp_client.cpp
    ../src/cluster/node_info.cpp
    ../src/cluster/cluster_membership.cpp
)

# Link libraries
target_link_libraries(test_distributed_db
    PRIVATE
    ${GTEST_LIBRARIES}
    Threads::Threads
)

# Include directories
target_include_directories(test_distributed_db
    PRIVATE
    ${GTEST_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
)

# Compiler flags for tests
target_compile_options(test_distributed_db
    PRIVATE
    ${GTEST_CFLAGS_OTHER}
)

# Set output directory
set_target_properties(test_distributed_db PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# Add test
add_test(NAME unit_tests COMMAND test_distributed_db)