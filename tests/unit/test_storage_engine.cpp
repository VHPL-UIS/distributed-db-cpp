#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <chrono>
#include "storage/storage_engine.hpp"

using namespace distributed_db;

class StorageEngineTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        _storage = std::make_unique<InMemoryStorageEngine>();
    }

    void TearDown() override
    {
        _storage.reset();
    }

    std::unique_ptr<InMemoryStorageEngine> _storage;
};

TEST_F(StorageEngineTest, BasicPutAndGet)
{
    const Key key = "test_key";
    const Value value = "test_value";

    // Test put operation
    const auto put_status = _storage->put(key, value);
    EXPECT_EQ(put_status, Status::OK);

    // Test get operation
    const auto get_result = _storage->get(key);
    ASSERT_TRUE(get_result.ok());
    EXPECT_EQ(get_result.value(), value);
}

TEST_F(StorageEngineTest, GetNonExistentKey)
{
    const Key key = "non_existent_key";

    const auto result = _storage->get(key);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status(), Status::NOT_FOUND);
}

TEST_F(StorageEngineTest, UpdateExistingKey)
{
    const Key key = "update_key";
    const Value value1 = "value1";
    const Value value2 = "value2";

    // Put initial value
    _storage->put(key, value1);

    // Update with new value
    const auto put_status = _storage->put(key, value2);
    EXPECT_EQ(put_status, Status::OK);

    // Verify updated value
    const auto get_result = _storage->get(key);
    ASSERT_TRUE(get_result.ok());
    EXPECT_EQ(get_result.value(), value2);
}

TEST_F(StorageEngineTest, RemoveKey)
{
    const Key key = "remove_key";
    const Value value = "remove_value";

    // Put and verify
    _storage->put(key, value);
    EXPECT_TRUE(_storage->exists(key));

    // Remove and verify
    const auto remove_status = _storage->remove(key);
    EXPECT_EQ(remove_status, Status::OK);
    EXPECT_FALSE(_storage->exists(key));

    // Try to get removed key
    const auto get_result = _storage->get(key);
    EXPECT_FALSE(get_result.ok());
    EXPECT_EQ(get_result.status(), Status::NOT_FOUND);
}

TEST_F(StorageEngineTest, RemoveNonExistentKey)
{
    const Key key = "non_existent_key";

    const auto remove_status = _storage->remove(key);
    EXPECT_EQ(remove_status, Status::NOT_FOUND);
}

TEST_F(StorageEngineTest, BatchOperations)
{
    const std::unordered_map<Key, Value> batch = {
        {"batch_key1", "batch_value1"},
        {"batch_key2", "batch_value2"},
        {"batch_key3", "batch_value3"}};

    // Test batch put
    const auto put_status = _storage->putBatch(batch);
    EXPECT_EQ(put_status, Status::OK);

    // Test batch get
    const std::vector<Key> keys = {"batch_key1", "batch_key2", "batch_key3", "non_existent"};
    const auto get_result = _storage->getBatch(keys);

    ASSERT_TRUE(get_result.ok());
    const auto &result_map = get_result.value();

    EXPECT_EQ(result_map.size(), 3);
    EXPECT_EQ(result_map.at("batch_key1"), "batch_value1");
    EXPECT_EQ(result_map.at("batch_key2"), "batch_value2");
    EXPECT_EQ(result_map.at("batch_key3"), "batch_value3");
    EXPECT_EQ(result_map.find("non_existent"), result_map.end());
}

TEST_F(StorageEngineTest, SizeAndClear)
{
    EXPECT_EQ(_storage->size(), 0);

    // Add some data
    _storage->put("key1", "value1");
    _storage->put("key2", "value2");
    _storage->put("key3", "value3");

    EXPECT_EQ(_storage->size(), 3);

    // Clear and verify
    _storage->clear();
    EXPECT_EQ(_storage->size(), 0);
    EXPECT_FALSE(_storage->exists("key1"));
}

TEST_F(StorageEngineTest, GetAllKeys)
{
    const std::unordered_map<Key, Value> test_data = {
        {"alpha", "value_alpha"},
        {"beta", "value_beta"},
        {"gamma", "value_gamma"}};

    // Add test data
    for (const auto &[key, value] : test_data)
    {
        _storage->put(key, value);
    }

    // Get all keys
    const auto keys = _storage->getAllKeys();
    EXPECT_EQ(keys.size(), test_data.size());

    // Verify all keys are present
    for (const auto &key : keys)
    {
        EXPECT_NE(test_data.find(key), test_data.end());
    }
}

TEST_F(StorageEngineTest, ThreadSafety)
{
    constexpr int num_threads = 4;
    constexpr int operations_per_thread = 100;

    std::vector<std::thread> threads;

    // Launch multiple threads performing concurrent operations
    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([this, t, operations_per_thread]()
                             {
            for (int i = 0; i < operations_per_thread; ++i) {
                const auto key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                const auto value = "thread_" + std::to_string(t) + "_value_" + std::to_string(i);
                
                // Put operation
                const auto put_status = _storage->put(key, value);
                EXPECT_EQ(put_status, Status::OK);
                
                // Get operation
                const auto get_result = _storage->get(key);
                EXPECT_TRUE(get_result.ok());
                if (get_result.ok()) {
                    EXPECT_EQ(get_result.value(), value);
                }
                
                // Exists check
                EXPECT_TRUE(_storage->exists(key));
                
                // Small delay to increase contention
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            } });
    }

    // Wait for all threads to complete
    for (auto &thread : threads)
    {
        thread.join();
    }

    // Verify final state
    const auto final_size = _storage->size();
    EXPECT_EQ(final_size, num_threads * operations_per_thread);

    // Verify all keys are accessible
    const auto all_keys = _storage->getAllKeys();
    EXPECT_EQ(all_keys.size(), final_size);
}