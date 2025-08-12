# Find GoogleTest (recommended way)
find_package(GTest REQUIRED)

add_executable(test_distributed_db
    unit/test_storage_engine.cpp
    unit/test_config.cpp
    unit/test_wal.cpp
    unit/test_persistent_storage.cpp
    unit/test_message.cpp
    unit/test_cluster_membership.cpp
    unit/test_consistent_hash.cpp
    ../src/common/config.cpp
    ../src/storage/storage_engine.cpp
    ../src/storage/wal.cpp
    ../src/storage/persistent_storage_engine.cpp
    ../src/network/message.cpp
    ../src/network/tcp_server.cpp
    ../src/network/tcp_client.cpp
    ../src/cluster/node_info.cpp
    ../src/cluster/cluster_membership.cpp
    ../src/cluster/consistent_hash.cpp
)

target_link_libraries(test_distributed_db
    PRIVATE
    GTest::gtest_main
    Threads::Threads
)

target_include_directories(test_distributed_db
    PRIVATE
    ${GTEST_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
)

set_target_properties(test_distributed_db PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

add_test(NAME unit_tests COMMAND test_distributed_db)
