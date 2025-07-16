#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <filesystem>

#include "common/logger.hpp"
#include "common/config.hpp"
#include "common/types.hpp"
#include "storage/storage_engine.hpp"
#include "storage/persistent_storage_engine.hpp"

using namespace distributed_db;

void printBanner()
{
    std::cout << R"(
╔══════════════════════════════════════════════════════════════╗
║                    Distributed Database                     ║
║                         Episode 2                           ║
║              Persistent Storage with WAL                    ║
╚══════════════════════════════════════════════════════════════╝
)" << std::endl;
}

void demonstratePersistentStorage()
{
    LOG_INFO("=== Persistent Storage Engine Demonstration ===");

    // Create data directory path
    const auto data_directory = std::filesystem::current_path() / "demo_data";

    // Clean up any existing demo data
    std::filesystem::remove_all(data_directory);

    // Create persistent storage engine
    auto storage = std::make_unique<PersistentStorageEngine>(data_directory);

    LOG_INFO("Persistent storage initialized at: %s", data_directory.string().c_str());

    // Demonstrate basic operations
    LOG_INFO("Testing basic persistent operations...");

    // Store some data
    storage->put("user:alice", "Alice Johnson");
    storage->put("user:bob", "Bob Smith");
    storage->put("user:charlie", "Charlie Brown");
    storage->put("config:version", "1.0.0");
    storage->put("config:environment", "development");

    LOG_INFO("✓ Stored 5 key-value pairs");

    // Read back data
    const auto alice_result = storage->get("user:alice");
    if (alice_result.ok())
    {
        LOG_INFO("✓ Retrieved user:alice -> %s", alice_result.value().c_str());
    }

    // Show current state
    LOG_INFO("Current storage size: %zu", storage->size());
    LOG_INFO("Current WAL sequence: %lu", storage->getCurrentWalSequence());
    LOG_INFO("WAL entries since last checkpoint: %zu", storage->getWalEntryCount());
}

void demonstrateWalRecovery()
{
    LOG_INFO("=== WAL Recovery Demonstration ===");

    const auto data_directory = std::filesystem::current_path() / "demo_data";

    {
        // First session: store initial data and checkpoint
        LOG_INFO("Session 1: Storing initial data...");
        auto storage = std::make_unique<PersistentStorageEngine>(data_directory);

        storage->put("session1:key1", "Initial value 1");
        storage->put("session1:key2", "Initial value 2");

        // Create checkpoint to save snapshot
        const auto checkpoint_status = storage->checkpoint();
        if (checkpoint_status == Status::OK)
        {
            LOG_INFO("✓ Checkpoint created successfully");
        }

        // Add more data that will only be in WAL
        storage->put("session1:key3", "WAL-only value 3");
        storage->put("session1:key4", "WAL-only value 4");
        storage->remove("session1:key1"); // This removal will be in WAL

        // Force flush WAL
        const auto flush_status = storage->flush();
        if (flush_status != Status::OK)
        {
            LOG_WARN("Failed to flush WAL, but continuing demonstration");
        }

        LOG_INFO("Session 1 complete. Storage size: %zu", storage->size());

        // storage goes out of scope here, simulating application shutdown
    }

    {
        // Second session: demonstrate recovery
        LOG_INFO("Session 2: Demonstrating recovery...");
        auto storage = std::make_unique<PersistentStorageEngine>(data_directory);

        LOG_INFO("✓ Storage recovered. Size: %zu", storage->size());

        // Verify snapshot data recovery
        const auto key2_result = storage->get("session1:key2");
        if (key2_result.ok())
        {
            LOG_INFO("✓ Recovered from snapshot: session1:key2 -> %s", key2_result.value().c_str());
        }

        // Verify WAL replay
        const auto key3_result = storage->get("session1:key3");
        if (key3_result.ok())
        {
            LOG_INFO("✓ Recovered from WAL: session1:key3 -> %s", key3_result.value().c_str());
        }

        const auto key4_result = storage->get("session1:key4");
        if (key4_result.ok())
        {
            LOG_INFO("✓ Recovered from WAL: session1:key4 -> %s", key4_result.value().c_str());
        }

        // Verify WAL replay of deletion
        const auto key1_result = storage->get("session1:key1");
        if (!key1_result.ok() && key1_result.status() == Status::NOT_FOUND)
        {
            LOG_INFO("✓ WAL deletion replayed: session1:key1 was correctly removed");
        }
    }
}

void demonstrateBatchOperations()
{
    LOG_INFO("=== Batch Operations Demonstration ===");

    const auto data_directory = std::filesystem::current_path() / "demo_data";
    auto storage = std::make_unique<PersistentStorageEngine>(data_directory);

    // Prepare batch data
    const std::unordered_map<Key, Value> user_batch = {
        {"user:1001", "John Doe"},
        {"user:1002", "Jane Smith"},
        {"user:1003", "Mike Johnson"},
        {"user:1004", "Sarah Wilson"},
        {"user:1005", "David Brown"}};

    // Batch PUT operation
    const auto put_status = storage->putBatch(user_batch);
    if (put_status == Status::OK)
    {
        LOG_INFO("✓ Batch PUT of %zu users successful", user_batch.size());
    }

    // Batch GET operation
    const std::vector<Key> keys_to_fetch = {
        "user:1001", "user:1003", "user:1005", "user:9999" // 9999 doesn't exist
    };

    const auto get_result = storage->getBatch(keys_to_fetch);
    if (get_result.ok())
    {
        const auto &results = get_result.value();
        LOG_INFO("✓ Batch GET retrieved %zu out of %zu requested keys",
                 results.size(), keys_to_fetch.size());

        for (const auto &[key, value] : results)
        {
            LOG_INFO("  %s -> %s", key.c_str(), value.c_str());
        }
    }
}

void demonstratePerformanceAndConcurrency()
{
    LOG_INFO("=== Performance and Concurrency Demonstration ===");

    const auto data_directory = std::filesystem::current_path() / "demo_data";
    auto storage = std::make_unique<PersistentStorageEngine>(data_directory);

    // Performance test: measure write throughput
    const auto start_time = std::chrono::high_resolution_clock::now();
    const int num_operations = 1000;

    for (int i = 0; i < num_operations; ++i)
    {
        const auto key = "perf_test_key_" + std::to_string(i);
        const auto value = "performance_test_value_" + std::to_string(i);
        storage->put(key, value);
    }

    const auto end_time = std::chrono::high_resolution_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    LOG_INFO("✓ Performance test: %d operations in %ldms", num_operations, duration.count());
    LOG_INFO("  Throughput: %.2f operations/second",
             static_cast<double>(num_operations) / (duration.count() / 1000.0));

    // Concurrency test
    LOG_INFO("Testing concurrent operations...");

    constexpr int num_threads = 4;
    constexpr int ops_per_thread = 100;
    std::vector<std::thread> threads;

    const auto concurrent_start = std::chrono::high_resolution_clock::now();

    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&storage, t, ops_per_thread]()
                             {
            for (int i = 0; i < ops_per_thread; ++i) {
                const auto key = "concurrent_" + std::to_string(t) + "_" + std::to_string(i);
                const auto value = "thread_" + std::to_string(t) + "_value_" + std::to_string(i);
                
                storage->put(key, value);
                
                // Verify immediately
                const auto result = storage->get(key);
                if (!result.ok() || result.value() != value) {
                    LOG_ERROR("Concurrent operation failed for key: %s", key.c_str());
                }
            } });
    }

    for (auto &thread : threads)
    {
        thread.join();
    }

    const auto concurrent_end = std::chrono::high_resolution_clock::now();
    const auto concurrent_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        concurrent_end - concurrent_start);

    LOG_INFO("✓ Concurrent test: %d threads × %d operations in %ldms",
             num_threads, ops_per_thread, concurrent_duration.count());

    LOG_INFO("Final storage size: %zu", storage->size());
}

void demonstrateCheckpointing()
{
    LOG_INFO("=== Checkpointing Demonstration ===");

    const auto data_directory = std::filesystem::current_path() / "demo_data";
    auto storage = std::make_unique<PersistentStorageEngine>(data_directory);

    LOG_INFO("Initial WAL entries: %zu", storage->getWalEntryCount());

    // Add enough data to potentially trigger auto-checkpointing
    for (int i = 0; i < 150; ++i)
    {
        const auto key = "checkpoint_test_" + std::to_string(i);
        const auto value = "checkpoint_value_" + std::to_string(i);
        storage->put(key, value);
    }

    LOG_INFO("After 150 operations - WAL entries: %zu", storage->getWalEntryCount());

    // Manual checkpoint
    const auto checkpoint_status = storage->checkpoint();
    if (checkpoint_status == Status::OK)
    {
        LOG_INFO("✓ Manual checkpoint completed");
        LOG_INFO("WAL entries after checkpoint: %zu", storage->getWalEntryCount());
    }

    // Verify all data is still accessible
    bool all_accessible = true;
    for (int i = 0; i < 150; ++i)
    {
        const auto key = "checkpoint_test_" + std::to_string(i);
        if (!storage->exists(key))
        {
            all_accessible = false;
            break;
        }
    }

    if (all_accessible)
    {
        LOG_INFO("✓ All data accessible after checkpoint");
    }
    else
    {
        LOG_ERROR("✗ Some data lost after checkpoint");
    }
}

void showPersistenceStatistics()
{
    LOG_INFO("=== Persistence Statistics ===");

    const auto data_directory = std::filesystem::current_path() / "demo_data";
    auto storage = std::make_unique<PersistentStorageEngine>(data_directory);

    LOG_INFO("Storage Statistics:");
    LOG_INFO("  Data Directory: %s", storage->getDataDirectory().string().c_str());
    LOG_INFO("  Total Keys: %zu", storage->size());
    LOG_INFO("  Current WAL Sequence: %lu", storage->getCurrentWalSequence());
    LOG_INFO("  WAL Entries Since Checkpoint: %zu", storage->getWalEntryCount());

    // Show file sizes
    const auto snapshot_path = data_directory / "snapshot.dat";
    const auto wal_path = data_directory / "wal" / "wal.log";

    if (std::filesystem::exists(snapshot_path))
    {
        const auto snapshot_size = std::filesystem::file_size(snapshot_path);
        LOG_INFO("  Snapshot File Size: %zu bytes", snapshot_size);
    }

    if (std::filesystem::exists(wal_path))
    {
        const auto wal_size = std::filesystem::file_size(wal_path);
        LOG_INFO("  WAL File Size: %zu bytes", wal_size);
    }

    // Show sample data
    const auto all_keys = storage->getAllKeys();
    LOG_INFO("  Sample Keys (first 5):");
    for (std::size_t i = 0; i < std::min(static_cast<std::size_t>(5), all_keys.size()); ++i)
    {
        const auto result = storage->get(all_keys[i]);
        if (result.ok())
        {
            LOG_INFO("    %s -> %s", all_keys[i].c_str(), result.value().c_str());
        }
    }
}

int main(int, char *[])
{
    try
    {
        // printBanner();

        LOG_INFO("Starting Distributed Database");
        LOG_INFO("Persistent Storage with Write-Ahead Logging demonstration");

        // Basic persistent storage demonstration
        demonstratePersistentStorage();

        // WAL recovery demonstration
        demonstrateWalRecovery();

        // Batch operations
        demonstrateBatchOperations();

        // Performance and concurrency
        demonstratePerformanceAndConcurrency();

        // Checkpointing
        demonstrateCheckpointing();

        // Show final statistics
        showPersistenceStatistics();

        // Keep demo data for inspection
        LOG_INFO("Demo data preserved at: %s",
                 (std::filesystem::current_path() / "demo_data").string().c_str());
    }
    catch (const std::exception &e)
    {
        LOG_ERROR("Application error: %s", e.what());
        return 1;
    }

    return 0;
}