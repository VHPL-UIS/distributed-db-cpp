#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>

#include "common/logger.hpp"
#include "common/config.hpp"
#include "common/types.hpp"
#include "storage/storage_engine.hpp"

using namespace distributed_db;

void demonstrateStorageEngine()
{
    LOG_INFO("=== Storage Engine Demonstration ===");

    // Create storage engine using smart pointer
    auto storage = std::make_unique<InMemoryStorageEngine>();

    // Demonstrate basic operations
    LOG_INFO("Testing basic operations...");

    // Put operation
    auto status = storage->put("user:1", "Alice");
    if (status == Status::OK)
    {
        LOG_INFO("✓ Put operation successful");
    }

    status = storage->put("user:2", "Bob");
    if (status == Status::OK)
    {
        LOG_INFO("✓ Put operation successful");
    }

    // Get operation
    auto result = storage->get("user:1");
    if (result.ok())
    {
        LOG_INFO("✓ Get operation successful: %s", result.value().c_str());
    }
    else
    {
        LOG_ERROR("✗ Get operation failed");
    }

    // Batch operations
    const std::unordered_map<Key, Value> batch = {
        {"user:3", "Charlie"},
        {"user:4", "David"},
        {"user:5", "Eve"}};

    status = storage->putBatch(batch);
    if (status == Status::OK)
    {
        LOG_INFO("✓ Batch put operation successful");
    }

    // Get all keys
    const auto keys = storage->getAllKeys();
    LOG_INFO("✓ Total keys in storage: %d", keys.size());

    // Demonstrate thread safety
    LOG_INFO("Testing thread safety...");

    std::vector<std::thread> threads;

    // Create multiple threads to test concurrent access
    for (int i = 0; i < 3; ++i)
    {
        threads.emplace_back([&storage, i]()
                             {
            for (int j = 0; j < 10; ++j) {
                const auto key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
                const auto value = "value_" + std::to_string(j);
                
                storage->put(key, value);
                const auto result = storage->get(key);
                
                if (result.ok()) {
                    LOG_DEBUG("Thread %d successfully stored and retrieved %s", i, key.c_str());
                }
                
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            } });
    }

    // Wait for all threads to complete
    for (auto &thread : threads)
    {
        thread.join();
    }

    LOG_INFO("✓ Thread safety test completed");
    LOG_INFO("Final storage size: %d", storage->size());
}

void demonstrateConfiguration()
{
    LOG_INFO("=== Configuration Demonstration ===");

    auto &config = Config::getInstance();

    // Display current configuration
    LOG_INFO("Current configuration:");
    LOG_INFO("  Node ID: %s", config.getNodeId().c_str());
    LOG_INFO("  Host: %s", config.getHost().c_str());
    LOG_INFO("  Port: %d", config.getPort());
    LOG_INFO("  Replication Factor: %d", config.getReplicationFactor());
    LOG_INFO("  Election Timeout: %dms", config.getElectionTimeout().count());
    LOG_INFO("  Heartbeat Interval: %dms", config.getHeartbeatInterval().count());
    LOG_INFO("  Data Directory: %s", config.getDataDirectory().c_str());

    // Demonstrate configuration changes
    config.setNodeId("distributed_db_node_1");
    config.setPort(9090);
    config.setReplicationFactor(5);

    LOG_INFO("✓ Configuration updated successfully");

    // Test saving and loading configuration
    const std::string config_file = "test_config.conf";
    if (config.saveToFile(config_file))
    {
        LOG_INFO("✓ Configuration saved to file");

        // Reset some values
        config.setPort(8080);
        config.setNodeId("temp_node");

        // Load from file
        if (config.loadFromFile(config_file))
        {
            LOG_INFO("✓ Configuration loaded from file");
            LOG_INFO("  Restored Node ID: %s", config.getNodeId().c_str());
            LOG_INFO("  Restored Port: %d", config.getPort());
        }
        else
        {
            LOG_ERROR("✗ Failed to load configuration from file");
        }
    }
    else
    {
        LOG_ERROR("✗ Failed to save configuration to file");
    }
}

void showArchitectureOverview()
{
    LOG_INFO("=== Architecture Overview ===");

    std::cout << R"(
Distributed Database Architecture:

┌─────────────────────────────────────────────────────────────┐
│                        Client Layer                         │
│                    (SQL-like Interface)                     │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                       Query Engine                          │
│              (Parser, Planner, Executor)                    │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                    Transaction Layer                        │
│                  (ACID, Concurrency Control)                │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                      Consensus Layer                        │
│                    (Raft Algorithm)                         │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                     Replication Layer                       │
│                 (Consistent Hashing)                        │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                      Storage Engine                         │
│                  (WAL, Persistence)                         │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                      Network Layer                          │
│                    (TCP, Messaging)                         │
└─────────────────────────────────────────────────────────────┘

Key Components:
• Storage Engine: In-memory + persistent storage with WAL
• Network Layer: TCP-based communication between nodes
• Consensus Layer: Raft algorithm for distributed coordination
• Replication Layer: Consistent hashing for data distribution
• Query Engine: SQL-like language processing
• Transaction Layer: ACID properties and concurrency control
)" << std::endl;
}

int main(int argc, char *argv[])
{
    try
    {
        LOG_INFO("Starting Distributed Database");

        // Show architecture overview
        // showArchitectureOverview();

        // Demonstrate configuration system
        demonstrateConfiguration();

        // Demonstrate storage engine
        demonstrateStorageEngine();
    }
    catch (const std::exception &e)
    {
        LOG_ERROR("Application error: %s", e.what());
        return 1;
    }

    return 0;
}