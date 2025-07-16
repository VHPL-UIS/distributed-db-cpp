#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <chrono>
#include "storage/persistent_storage_engine.hpp"

using namespace distributed_db;

class PersistentStorageTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        _test_directory = std::filesystem::temp_directory_path() / "persistent_storage_test";
        std::filesystem::remove_all(_test_directory);
        std::filesystem::create_directories(_test_directory);

        _storage = std::make_unique<PersistentStorageEngine>(_test_directory);
    }

    void TearDown() override
    {
        _storage.reset();
        std::filesystem::remove_all(_test_directory);
    }

    std::filesystem::path _test_directory;
    std::unique_ptr<PersistentStorageEngine> _storage;
};

TEST_F(PersistentStorageTest, BasicOperations)
{
    // Test PUT operation
    const auto put_status = _storage->put("key1", "value1");
    EXPECT_EQ(put_status, Status::OK);

    // Test GET operation
    const auto get_result = _storage->get("key1");
    ASSERT_TRUE(get_result.ok());
    EXPECT_EQ(get_result.value(), "value1");

    // Test EXISTS operation
    EXPECT_TRUE(_storage->exists("key1"));
    EXPECT_FALSE(_storage->exists("non_existent_key"));

    // Test REMOVE operation
    const auto remove_status = _storage->remove("key1");
    EXPECT_EQ(remove_status, Status::OK);

    // Verify removal
    EXPECT_FALSE(_storage->exists("key1"));
    const auto get_after_remove = _storage->get("key1");
    EXPECT_FALSE(get_after_remove.ok());
    EXPECT_EQ(get_after_remove.status(), Status::NOT_FOUND);
}

TEST_F(PersistentStorageTest, DataPersistence)
{
    // Store some data
    _storage->put("persistent_key1", "persistent_value1");
    _storage->put("persistent_key2", "persistent_value2");
    _storage->put("persistent_key3", "persistent_value3");

    // Force a checkpoint to save snapshot
    const auto checkpoint_status = _storage->checkpoint();
    EXPECT_EQ(checkpoint_status, Status::OK);

    const auto original_size = _storage->size();
    EXPECT_EQ(original_size, 3);

    // Destroy and recreate storage engine (simulating restart)
    _storage.reset();
    _storage = std::make_unique<PersistentStorageEngine>(_test_directory);

    // Verify data persisted
    EXPECT_EQ(_storage->size(), original_size);

    const auto result1 = _storage->get("persistent_key1");
    ASSERT_TRUE(result1.ok());
    EXPECT_EQ(result1.value(), "persistent_value1");

    const auto result2 = _storage->get("persistent_key2");
    ASSERT_TRUE(result2.ok());
    EXPECT_EQ(result2.value(), "persistent_value2");

    const auto result3 = _storage->get("persistent_key3");
    ASSERT_TRUE(result3.ok());
    EXPECT_EQ(result3.value(), "persistent_value3");
}

TEST_F(PersistentStorageTest, WalRecovery)
{
    // Store initial data and checkpoint
    _storage->put("initial_key", "initial_value");
    _storage->checkpoint();

    // Add more data without checkpointing (will be in WAL only)
    _storage->put("wal_key1", "wal_value1");
    _storage->put("wal_key2", "wal_value2");
    _storage->remove("initial_key"); // This should be recovered from WAL

    // Flush to ensure WAL is written
    const auto flush_status = _storage->flush();
    EXPECT_EQ(flush_status, Status::OK);

    // Destroy and recreate storage engine
    _storage.reset();
    _storage = std::make_unique<PersistentStorageEngine>(_test_directory);

    // Verify WAL recovery worked
    EXPECT_EQ(_storage->size(), 2); // initial_key removed, 2 new keys added

    // initial_key should be removed (from WAL replay)
    EXPECT_FALSE(_storage->exists("initial_key"));

    // WAL entries should be recovered
    const auto result1 = _storage->get("wal_key1");
    ASSERT_TRUE(result1.ok());
    EXPECT_EQ(result1.value(), "wal_value1");

    const auto result2 = _storage->get("wal_key2");
    ASSERT_TRUE(result2.ok());
    EXPECT_EQ(result2.value(), "wal_value2");
}

TEST_F(PersistentStorageTest, BatchOperations)
{
    const std::unordered_map<Key, Value> batch = {
        {"batch_key1", "batch_value1"},
        {"batch_key2", "batch_value2"},
        {"batch_key3", "batch_value3"}};

    // Test batch PUT
    const auto put_status = _storage->putBatch(batch);
    EXPECT_EQ(put_status, Status::OK);

    // Test batch GET
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

TEST_F(PersistentStorageTest, AutomaticCheckpointing)
{
    // Add many entries to trigger automatic checkpointing
    for (int i = 0; i < 150; ++i)
    {
        const auto key = "auto_checkpoint_key_" + std::to_string(i);
        const auto value = "auto_checkpoint_value_" + std::to_string(i);
        _storage->put(key, value);
    }

    // Force flush which should trigger checkpoint due to high WAL entry count
    const auto flush_status = _storage->flush();
    EXPECT_EQ(flush_status, Status::OK);

    // Verify checkpoint occurred by checking WAL entry count is reduced
    const auto wal_entries = _storage->getWalEntryCount();
    EXPECT_LT(wal_entries, 150); // Should be less due to checkpointing

    // Verify all data is still accessible
    for (int i = 0; i < 150; ++i)
    {
        const auto key = "auto_checkpoint_key_" + std::to_string(i);
        const auto expected_value = "auto_checkpoint_value_" + std::to_string(i);

        const auto result = _storage->get(key);
        ASSERT_TRUE(result.ok()) << "Failed to get key: " << key;
        EXPECT_EQ(result.value(), expected_value);
    }
}

TEST_F(PersistentStorageTest, ConcurrentOperations)
{
    constexpr int num_threads = 4;
    constexpr int operations_per_thread = 50;

    std::vector<std::thread> threads;

    // Launch concurrent writers
    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([this, t, operations_per_thread]()
                             {
            for (int i = 0; i < operations_per_thread; ++i) {
                const auto key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                const auto value = "thread_" + std::to_string(t) + "_value_" + std::to_string(i);
                
                // PUT operation
                const auto put_status = _storage->put(key, value);
                EXPECT_EQ(put_status, Status::OK);
                
                // GET operation
                const auto get_result = _storage->get(key);
                EXPECT_TRUE(get_result.ok());
                if (get_result.ok()) {
                    EXPECT_EQ(get_result.value(), value);
                }
                
                // Small delay to increase contention
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            } });
    }

    // Wait for all threads
    for (auto &thread : threads)
    {
        thread.join();
    }

    // Verify final state
    const auto final_size = _storage->size();
    EXPECT_EQ(final_size, num_threads * operations_per_thread);
}

TEST_F(PersistentStorageTest, UpdateOperations)
{
    const Key key = "update_key";
    const Value value1 = "value1";
    const Value value2 = "value2";
    const Value value3 = "value3";

    // Initial PUT
    _storage->put(key, value1);
    auto result = _storage->get(key);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), value1);

    // Update value
    _storage->put(key, value2);
    result = _storage->get(key);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), value2);

    // Update again
    _storage->put(key, value3);
    result = _storage->get(key);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), value3);

    // Verify size is still 1 (not 3)
    EXPECT_EQ(_storage->size(), 1);
}

TEST_F(PersistentStorageTest, GetAllKeys)
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

TEST_F(PersistentStorageTest, ClearOperation)
{
    // Add some data
    _storage->put("key1", "value1");
    _storage->put("key2", "value2");
    _storage->put("key3", "value3");

    EXPECT_EQ(_storage->size(), 3);

    // Clear all data
    _storage->clear();

    // Verify data is cleared
    EXPECT_EQ(_storage->size(), 0);
    EXPECT_FALSE(_storage->exists("key1"));
    EXPECT_FALSE(_storage->exists("key2"));
    EXPECT_FALSE(_storage->exists("key3"));

    // Verify persistence after restart
    _storage.reset();
    _storage = std::make_unique<PersistentStorageEngine>(_test_directory);

    EXPECT_EQ(_storage->size(), 0);
}

TEST_F(PersistentStorageTest, RemoveNonExistentKey)
{
    const auto remove_status = _storage->remove("non_existent_key");
    EXPECT_EQ(remove_status, Status::NOT_FOUND);
}

TEST_F(PersistentStorageTest, EmptyValueHandling)
{
    const Key key = "empty_value_key";
    const Value empty_value = "";

    // Store empty value
    const auto put_status = _storage->put(key, empty_value);
    EXPECT_EQ(put_status, Status::OK);

    // Retrieve empty value
    const auto get_result = _storage->get(key);
    ASSERT_TRUE(get_result.ok());
    EXPECT_TRUE(get_result.value().empty());

    // Verify it exists
    EXPECT_TRUE(_storage->exists(key));
}

TEST_F(PersistentStorageTest, LargeValues)
{
    const Key key = "large_value_key";
    const Value large_value(10000, 'x'); // 10KB value

    // Store large value
    const auto put_status = _storage->put(key, large_value);
    EXPECT_EQ(put_status, Status::OK);

    // Retrieve and verify
    const auto get_result = _storage->get(key);
    ASSERT_TRUE(get_result.ok());
    EXPECT_EQ(get_result.value(), large_value);

    // Test persistence
    _storage->checkpoint();
    _storage.reset();
    _storage = std::make_unique<PersistentStorageEngine>(_test_directory);

    const auto recovered_result = _storage->get(key);
    ASSERT_TRUE(recovered_result.ok());
    EXPECT_EQ(recovered_result.value(), large_value);
}

TEST_F(PersistentStorageTest, WalSequenceNumbers)
{
    const auto initial_sequence = _storage->getCurrentWalSequence();

    _storage->put("key1", "value1");
    const auto after_put = _storage->getCurrentWalSequence();
    EXPECT_GT(after_put, initial_sequence);

    _storage->remove("key1");
    const auto after_remove = _storage->getCurrentWalSequence();
    EXPECT_GT(after_remove, after_put);

    _storage->checkpoint();
    const auto after_checkpoint = _storage->getCurrentWalSequence();
    EXPECT_GT(after_checkpoint, after_remove);
}

TEST_F(PersistentStorageTest, DataDirectoryPath)
{
    EXPECT_EQ(_storage->getDataDirectory(), _test_directory);
}