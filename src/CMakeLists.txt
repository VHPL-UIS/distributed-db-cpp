# Create the main executable
add_executable(distributed_db
    main.cpp
    common/config.cpp
    storage/storage_engine.cpp
    storage/wal.cpp
    storage/persistent_storage_engine.cpp
)

# Link libraries
target_link_libraries(distributed_db
    PRIVATE
    Threads::Threads
)

# Include directories
target_include_directories(distributed_db
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Set output directory
set_target_properties(distributed_db PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)