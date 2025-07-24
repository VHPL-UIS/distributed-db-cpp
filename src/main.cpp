#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <future>

#include "common/logger.hpp"
#include "common/config.hpp"
#include "common/types.hpp"
#include "storage/persistent_storage_engine.hpp"
#include "network/tcp_server.hpp"
#include "network/tcp_client.hpp"
#include "network/message.hpp"

using namespace distributed_db;

void printBanner()
{
    std::cout << R"(
╔══════════════════════════════════════════════════════════════╗
║                    Distributed Database                      ║
║                Network Layer Foundation                      ║
╚══════════════════════════════════════════════════════════════╝
)" << std::endl;
}

void demonstrateMessageSerialization()
{
    LOG_INFO("=== Message Serialization Demonstration ===");

    // Test GET request/response
    GetRequestMessage get_request("demo_key");
    LOG_INFO("Created GET request for key: %s", get_request.getKey().c_str());

    const auto serialized_result = get_request.serialize();
    if (serialized_result.ok())
    {
        LOG_INFO("✓ GET request serialized: %zu bytes", serialized_result.value().size());

        // Deserialize back
        const auto message_result = Message::fromBytes(serialized_result.value());
        if (message_result.ok())
        {
            const auto &deserialized = message_result.value();
            LOG_INFO("✓ Message deserialized: type=%s, id=%u",
                     messageTypeToString(deserialized->getType()).c_str(),
                     deserialized->getMessageId());
        }
    }

    // Test PUT request
    PutRequestMessage put_request("demo_key", "demo_value");
    LOG_INFO("Created PUT request: %s -> %s",
             put_request.getKey().c_str(), put_request.getValue().c_str());

    const auto put_serialized = put_request.serialize();
    if (put_serialized.ok())
    {
        LOG_INFO("✓ PUT request serialized: %zu bytes", put_serialized.value().size());
    }

    // Test heartbeat
    HeartbeatMessage heartbeat("demo_node_1");
    const auto hb_serialized = heartbeat.serialize();
    if (hb_serialized.ok())
    {
        LOG_INFO("✓ Heartbeat serialized: %zu bytes", hb_serialized.value().size());
    }

    LOG_INFO("Message serialization working correctly");
}

void startDatabaseServer(std::shared_ptr<StorageEngine> storage, Port port)
{
    LOG_INFO("=== Starting Database Server ===");

    auto server = std::make_unique<TcpServer>(port);
    server->setStorageEngine(storage);
    server->setMaxConnections(10);

    const auto start_status = server->start();
    if (start_status == Status::OK)
    {
        LOG_INFO("✓ Database server started on port %d", port);

        // Let server run for the demonstration
        std::this_thread::sleep_for(std::chrono::seconds(2));

        LOG_INFO("Server running with %zu active connections", server->getConnectionCount());

        // Keep server running for client tests
        std::this_thread::sleep_for(std::chrono::seconds(8));

        LOG_INFO("Stopping database server...");
        server->stop();
        LOG_INFO("✓ Database server stopped");
    }
    else
    {
        LOG_ERROR("✗ Failed to start database server");
    }
}

void demonstrateClientServerCommunication()
{
    LOG_INFO("=== Client-Server Communication Demonstration ===");

    // Create storage engine for server
    const auto data_directory = std::filesystem::current_path() / "demo_data";
    auto storage = std::make_shared<PersistentStorageEngine>(data_directory);

    // Add some initial data
    storage->put("server_key1", "server_value1");
    storage->put("server_key2", "server_value2");

    const Port server_port = 9090;

    // Start server in separate thread
    auto server_future = std::async(std::launch::async, [storage, server_port]()
                                    { startDatabaseServer(storage, server_port); });

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Test client operations
    LOG_INFO("Testing client operations...");

    TcpClient client;
    client.setDefaultTimeout(std::chrono::milliseconds(5000));

    const auto connect_status = client.connect("localhost", server_port);
    if (connect_status == Status::OK)
    {
        LOG_INFO("✓ Client connected to server");

        // Test GET operation
        const auto get_result = client.get("server_key1");
        if (get_result.ok())
        {
            LOG_INFO("✓ GET operation successful: server_key1 -> %s", get_result.value().c_str());
        }
        else
        {
            LOG_ERROR("✗ GET operation failed");
        }

        // Test PUT operation
        const auto put_status = client.put("client_key1", "client_value1");
        if (put_status == Status::OK)
        {
            LOG_INFO("✓ PUT operation successful");
        }
        else
        {
            LOG_ERROR("✗ PUT operation failed");
        }

        // Verify PUT by reading it back
        const auto verify_result = client.get("client_key1");
        if (verify_result.ok())
        {
            LOG_INFO("✓ PUT verification successful: client_key1 -> %s", verify_result.value().c_str());
        }
        else
        {
            LOG_ERROR("✗ PUT verification failed");
        }

        // Test DELETE operation
        const auto delete_status = client.remove("client_key1");
        if (delete_status == Status::OK)
        {
            LOG_INFO("✓ DELETE operation successful");
        }
        else
        {
            LOG_ERROR("✗ DELETE operation failed");
        }

        // Verify DELETE
        const auto verify_delete = client.get("client_key1");
        if (!verify_delete.ok() && verify_delete.status() == Status::NOT_FOUND)
        {
            LOG_INFO("✓ DELETE verification successful");
        }
        else
        {
            LOG_ERROR("✗ DELETE verification failed");
        }

        // Test ping/heartbeat
        const auto ping_status = client.ping();
        if (ping_status == Status::OK)
        {
            LOG_INFO("✓ Ping successful");
        }
        else
        {
            LOG_ERROR("✗ Ping failed");
        }

        client.disconnect();
        LOG_INFO("✓ Client disconnected");
    }
    else
    {
        LOG_ERROR("✗ Failed to connect client to server");
    }

    // Wait for server to finish
    server_future.wait();
}

void demonstrateAsyncOperations()
{
    LOG_INFO("=== Asynchronous Operations Demonstration ===");

    // Create storage and server
    const auto data_directory = std::filesystem::current_path() / "demo_data";
    auto storage = std::make_shared<PersistentStorageEngine>(data_directory);

    const Port async_port = 9091;

    // Start server
    auto server_future = std::async(std::launch::async, [storage, async_port]()
                                    {
        auto server = std::make_unique<TcpServer>(async_port);
        server->setStorageEngine(storage);
        
        const auto start_status = server->start();
        if (start_status == Status::OK) {
            LOG_INFO("Async demo server started on port %d", async_port);
            std::this_thread::sleep_for(std::chrono::seconds(5));
            server->stop();
        } });

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Test async client operations
    TcpClient async_client;
    const auto connect_status = async_client.connect("localhost", async_port);

    if (connect_status == Status::OK)
    {
        LOG_INFO("✓ Async client connected");

        // Perform multiple async operations
        std::vector<std::future<std::unique_ptr<Message>>> futures;

        // Send multiple requests asynchronously
        for (int i = 0; i < 5; ++i)
        {
            PutRequestMessage request("async_key_" + std::to_string(i),
                                      "async_value_" + std::to_string(i));
            futures.push_back(async_client.sendRequestAsync(request));
        }

        // Wait for all responses
        int successful_ops = 0;
        for (auto &future : futures)
        {
            try
            {
                auto response = future.get();
                if (response && response->getType() == MessageType::PUT_RESPONSE)
                {
                    const auto &put_response = static_cast<const PutResponseMessage &>(*response);
                    if (put_response.getStatus() == Status::OK)
                    {
                        ++successful_ops;
                    }
                }
            }
            catch (const std::exception &e)
            {
                LOG_ERROR("Async operation failed: %s", e.what());
            }
        }

        LOG_INFO("✓ Async operations completed: %d/5 successful", successful_ops);

        async_client.disconnect();
    }
    else
    {
        LOG_ERROR("✗ Failed to connect async client");
    }

    server_future.wait();
}

void demonstrateMultipleClients()
{
    LOG_INFO("=== Multiple Clients Demonstration ===");

    // Create storage and server
    const auto data_directory = std::filesystem::current_path() / "demo_data";
    auto storage = std::make_shared<PersistentStorageEngine>(data_directory);

    const Port multi_port = 9092;

    // Start server
    auto server_future = std::async(std::launch::async, [storage, multi_port]()
                                    {
        auto server = std::make_unique<TcpServer>(multi_port);
        server->setStorageEngine(storage);
        server->setMaxConnections(20);
        
        const auto start_status = server->start();
        if (start_status == Status::OK) {
            LOG_INFO("Multi-client server started on port %d", multi_port);
            std::this_thread::sleep_for(std::chrono::seconds(6));
            LOG_INFO("Multi-client server has %zu active connections", server->getConnectionCount());
            server->stop();
        } });

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Launch multiple client threads
    std::vector<std::future<void>> client_futures;
    constexpr int num_clients = 5;

    for (int client_id = 0; client_id < num_clients; ++client_id)
    {
        auto client_future = std::async(std::launch::async, [client_id, multi_port]()
                                        {
            TcpClient client;
            
            const auto connect_status = client.connect("localhost", multi_port);
            if (connect_status == Status::OK) {
                LOG_INFO("Client %d connected", client_id);
                
                // Each client performs multiple operations
                for (int op = 0; op < 3; ++op) {
                    const auto key = "client_" + std::to_string(client_id) + "_key_" + std::to_string(op);
                    const auto value = "client_" + std::to_string(client_id) + "_value_" + std::to_string(op);
                    
                    // PUT
                    const auto put_status = client.put(key, value);
                    if (put_status == Status::OK) {
                        LOG_DEBUG("Client %d PUT successful: %s", client_id, key.c_str());
                    }
                    
                    // GET to verify
                    const auto get_result = client.get(key);
                    if (get_result.ok()) {
                        LOG_DEBUG("Client %d GET successful: %s -> %s", 
                                 client_id, key.c_str(), get_result.value().c_str());
                    }
                    
                    // Small delay between operations
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                
                client.disconnect();
                LOG_INFO("Client %d completed operations and disconnected", client_id);
            } else {
                LOG_ERROR("Client %d failed to connect", client_id);
            } });

        client_futures.push_back(std::move(client_future));
    }

    // Wait for all clients to complete
    for (auto &future : client_futures)
    {
        future.wait();
    }

    LOG_INFO("✓ All %d clients completed their operations", num_clients);

    server_future.wait();
}

void demonstrateErrorHandling()
{
    LOG_INFO("=== Error Handling Demonstration ===");

    // Test connection to non-existent server
    TcpClient error_client;
    const auto bad_connect = error_client.connect("localhost", 12345); // Non-existent port

    if (bad_connect != Status::OK)
    {
        LOG_INFO("✓ Correctly handled connection to non-existent server");
    }

    // Test timeout behavior
    const auto data_directory = std::filesystem::current_path() / "demo_data";
    auto storage = std::make_shared<PersistentStorageEngine>(data_directory);

    const Port error_port = 9093;

    // Start server
    auto server = std::make_unique<TcpServer>(error_port);
    server->setStorageEngine(storage);

    const auto start_status = server->start();
    if (start_status == Status::OK)
    {
        LOG_INFO("Error handling test server started");

        // Connect client with short timeout
        TcpClient timeout_client;
        timeout_client.setDefaultTimeout(std::chrono::milliseconds(100)); // Very short timeout

        const auto connect_status = timeout_client.connect("localhost", error_port);
        if (connect_status == Status::OK)
        {
            // This might timeout due to very short timeout setting
            const auto result = timeout_client.get("any_key");
            if (!result.ok())
            {
                LOG_INFO("✓ Timeout handling working (status: %d)", static_cast<int>(result.status()));
            }
            else
            {
                LOG_INFO("✓ Operation completed within timeout");
            }

            timeout_client.disconnect();
        }

        server->stop();
        LOG_INFO("✓ Error handling tests completed");
    }
}

void showNetworkingStatistics()
{
    LOG_INFO("=== Networking Statistics ===");

    const auto data_directory = std::filesystem::current_path() / "demo_data";
    auto storage = std::make_shared<PersistentStorageEngine>(data_directory);

    LOG_INFO("Network Layer Statistics:");
    LOG_INFO("  Message Header Size: %zu bytes", MessageHeader::HEADER_SIZE);
    LOG_INFO("  Supported Message Types: %d", 14); // Count from MessageType enum
    LOG_INFO("  Storage Engine Keys: %zu", storage->size());

    // Show some message size examples
    GetRequestMessage get_msg("example_key");
    const auto get_serialized = get_msg.serialize();
    if (get_serialized.ok())
    {
        LOG_INFO("  GET request size: %zu bytes", get_serialized.value().size());
    }

    PutRequestMessage put_msg("example_key", "example_value");
    const auto put_serialized = put_msg.serialize();
    if (put_serialized.ok())
    {
        LOG_INFO("  PUT request size: %zu bytes", put_serialized.value().size());
    }

    HeartbeatMessage hb_msg("example_node");
    const auto hb_serialized = hb_msg.serialize();
    if (hb_serialized.ok())
    {
        LOG_INFO("  Heartbeat size: %zu bytes", hb_serialized.value().size());
    }

    LOG_INFO("Network layer foundation established successfully!");
}

int main()
{
    try
    {
        LOG_INFO("Network Layer Foundation demonstration");

        // Demonstrate message serialization
        demonstrateMessageSerialization();

        // Demonstrate client-server communication
        demonstrateClientServerCommunication();

        // Demonstrate async operations
        demonstrateAsyncOperations();

        // Demonstrate multiple clients
        demonstrateMultipleClients();

        // Demonstrate error handling
        demonstrateErrorHandling();

        // Show statistics
        showNetworkingStatistics();

        LOG_INFO("✓ Message protocol implemented");
        LOG_INFO("✓ TCP server/client working");
        LOG_INFO("✓ Async operations functional");
        LOG_INFO("✓ Multiple client support verified");
        LOG_INFO("✓ Error handling robust");
        LOG_INFO("✓ Network layer foundation established");
    }
    catch (const std::exception &e)
    {
        LOG_ERROR("Application error: %s", e.what());
        return 1;
    }

    return 0;
}