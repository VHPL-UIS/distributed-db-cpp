#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <chrono>
#include "storage/wal.hpp"

using namespace distributed_db;

class WalTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        _test_directory = std::filesystem::temp_directory_path() / "wal_test";
        std::filesystem::remove_all(_test_directory);
        std::filesystem::create_directories(_test_directory);

        _wal = std::make_unique<WriteAheadLog>(_test_directory);
    }

    void TearDown() override
    {
        _wal.reset();
        std::filesystem::remove_all(_test_directory);
    }

    std::filesystem::path _test_directory;
    std::unique_ptr<WriteAheadLog> _wal;
};

TEST_F(WalTest, BasicLogOperations)
{
    // Test PUT operation
    const auto put_status = _wal->logPut("key1", "value1");
    EXPECT_EQ(put_status, Status::OK);

    // Test DELETE operation
    const auto delete_status = _wal->logDelete("key2");
    EXPECT_EQ(delete_status, Status::OK);

    // Test CHECKPOINT operation
    const auto checkpoint_status = _wal->logCheckpoint();
    EXPECT_EQ(checkpoint_status, Status::OK);

    // Verify sequence numbers are increasing
    EXPECT_GT(_wal->getCurrentSequenceNumber(), 0);
}

TEST_F(WalTest, SequenceNumberIncrement)
{
    const auto initial_sequence = _wal->getCurrentSequenceNumber();

    _wal->logPut("key1", "value1");
    const auto after_put = _wal->getCurrentSequenceNumber();
    EXPECT_EQ(after_put, initial_sequence + 1);

    _wal->logDelete("key1");
    const auto after_delete = _wal->getCurrentSequenceNumber();
    EXPECT_EQ(after_delete, initial_sequence + 2);

    _wal->logCheckpoint();
    const auto after_checkpoint = _wal->getCurrentSequenceNumber();
    EXPECT_EQ(after_checkpoint, initial_sequence + 3);
}

TEST_F(WalTest, ReadAllEntries)
{
    // Log some entries
    _wal->logPut("key1", "value1");
    _wal->logPut("key2", "value2");
    _wal->logDelete("key1");
    _wal->logCheckpoint();

    // Flush to ensure data is written
    const auto flush_status = _wal->flush();
    EXPECT_EQ(flush_status, Status::OK);

    // Read all entries
    const auto entries_result = _wal->getAllEntries();
    ASSERT_TRUE(entries_result.ok());

    const auto &entries = entries_result.value();
    ASSERT_EQ(entries.size(), 4);

    // Verify entry contents
    EXPECT_EQ(entries[0].type, WalEntryType::PUT);
    EXPECT_EQ(entries[0].key, "key1");
    EXPECT_EQ(entries[0].value, "value1");

    EXPECT_EQ(entries[1].type, WalEntryType::PUT);
    EXPECT_EQ(entries[1].key, "key2");
    EXPECT_EQ(entries[1].value, "value2");

    EXPECT_EQ(entries[2].type, WalEntryType::DELETE);
    EXPECT_EQ(entries[2].key, "key1");
    EXPECT_TRUE(entries[2].value.empty());

    EXPECT_EQ(entries[3].type, WalEntryType::CHECKPOINT);
}

TEST_F(WalTest, ReadEntriesSinceSequence)
{
    // Log some entries
    _wal->logPut("key1", "value1");
    const auto sequence_after_first = _wal->getCurrentSequenceNumber();

    _wal->logPut("key2", "value2");
    _wal->logDelete("key1");

    _wal->flush();

    // Read entries since first operation
    const auto entries_result = _wal->getEntriesSinceSequence(sequence_after_first);
    ASSERT_TRUE(entries_result.ok());

    const auto &entries = entries_result.value();
    ASSERT_EQ(entries.size(), 2);

    EXPECT_EQ(entries[0].type, WalEntryType::PUT);
    EXPECT_EQ(entries[0].key, "key2");

    EXPECT_EQ(entries[1].type, WalEntryType::DELETE);
    EXPECT_EQ(entries[1].key, "key1");
}

TEST_F(WalTest, WalPersistence)
{
    // Log some entries
    _wal->logPut("persistent_key1", "persistent_value1");
    _wal->logPut("persistent_key2", "persistent_value2");
    _wal->flush();

    const auto original_sequence = _wal->getCurrentSequenceNumber();

    // Destroy and recreate WAL (simulating restart)
    _wal.reset();
    _wal = std::make_unique<WriteAheadLog>(_test_directory);

    // Verify data persisted
    EXPECT_EQ(_wal->getCurrentSequenceNumber(), original_sequence);

    const auto entries_result = _wal->getAllEntries();
    ASSERT_TRUE(entries_result.ok());

    const auto &entries = entries_result.value();
    ASSERT_EQ(entries.size(), 2);

    EXPECT_EQ(entries[0].key, "persistent_key1");
    EXPECT_EQ(entries[0].value, "persistent_value1");
    EXPECT_EQ(entries[1].key, "persistent_key2");
    EXPECT_EQ(entries[1].value, "persistent_value2");
}

TEST_F(WalTest, TruncateUpTo)
{
    // Log several entries
    _wal->logPut("key1", "value1");
    _wal->logPut("key2", "value2");
    const auto sequence_to_keep = _wal->getCurrentSequenceNumber();

    _wal->logPut("key3", "value3");
    _wal->logPut("key4", "value4");
    _wal->flush();

    // Truncate up to sequence_to_keep
    const auto truncate_status = _wal->truncateUpTo(sequence_to_keep);
    EXPECT_EQ(truncate_status, Status::OK);

    // Verify only entries after sequence_to_keep remain
    const auto entries_result = _wal->getAllEntries();
    ASSERT_TRUE(entries_result.ok());

    const auto &entries = entries_result.value();
    ASSERT_EQ(entries.size(), 2);

    EXPECT_EQ(entries[0].key, "key3");
    EXPECT_EQ(entries[1].key, "key4");
}

TEST_F(WalTest, AutoCheckpoint)
{
    // Log many entries to trigger auto-checkpoint
    for (int i = 0; i < 1005; ++i)
    { // More than MAX_ENTRIES_BEFORE_CHECKPOINT
        const auto key = "key_" + std::to_string(i);
        const auto value = "value_" + std::to_string(i);
        _wal->logPut(key, value);
    }

    _wal->flush();

    // Verify checkpoint was created
    const auto entries_result = _wal->getAllEntries();
    ASSERT_TRUE(entries_result.ok());

    const auto &entries = entries_result.value();

    // Should have at least one checkpoint entry
    const auto checkpoint_count = std::count_if(entries.begin(), entries.end(),
                                                [](const WalEntry &entry)
                                                { return entry.type == WalEntryType::CHECKPOINT; });

    EXPECT_GT(checkpoint_count, 0);
}

TEST_F(WalTest, ConcurrentWrites)
{
    constexpr int num_threads = 4;
    constexpr int writes_per_thread = 50;

    std::vector<std::thread> threads;

    // Launch concurrent writers
    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([this, t, writes_per_thread]()
                             {
            for (int i = 0; i < writes_per_thread; ++i) {
                const auto key = "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                const auto value = "thread_" + std::to_string(t) + "_value_" + std::to_string(i);
                
                const auto status = _wal->logPut(key, value);
                EXPECT_EQ(status, Status::OK);
                
                // Small delay to increase contention
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            } });
    }

    // Wait for all threads
    for (auto &thread : threads)
    {
        thread.join();
    }

    _wal->flush();

    // Verify all entries were written
    const auto entries_result = _wal->getAllEntries();
    ASSERT_TRUE(entries_result.ok());

    const auto &entries = entries_result.value();

    // Count PUT entries (excluding any auto-generated checkpoints)
    const auto put_count = std::count_if(entries.begin(), entries.end(),
                                         [](const WalEntry &entry)
                                         { return entry.type == WalEntryType::PUT; });

    EXPECT_EQ(put_count, num_threads * writes_per_thread);
}

TEST_F(WalTest, EntryTimestamps)
{
    const auto start_time = std::chrono::system_clock::now();

    _wal->logPut("timestamped_key", "timestamped_value");
    _wal->flush();

    const auto end_time = std::chrono::system_clock::now();

    const auto entries_result = _wal->getAllEntries();
    ASSERT_TRUE(entries_result.ok());

    const auto &entries = entries_result.value();
    ASSERT_FALSE(entries.empty());

    const auto &entry = entries[0];
    EXPECT_GE(entry.timestamp, start_time);
    EXPECT_LE(entry.timestamp, end_time);
}

TEST_F(WalTest, EmptyValueHandling)
{
    // Test PUT with empty value
    const auto put_status = _wal->logPut("empty_key", "");
    EXPECT_EQ(put_status, Status::OK);

    _wal->flush();

    const auto entries_result = _wal->getAllEntries();
    ASSERT_TRUE(entries_result.ok());

    const auto &entries = entries_result.value();
    ASSERT_FALSE(entries.empty());

    const auto &entry = entries[0];
    EXPECT_EQ(entry.key, "empty_key");
    EXPECT_TRUE(entry.value.empty());
}

TEST_F(WalTest, LargeEntries)
{
    // Test with large key and value
    const std::string large_key(1000, 'k');
    const std::string large_value(10000, 'v');

    const auto put_status = _wal->logPut(large_key, large_value);
    EXPECT_EQ(put_status, Status::OK);

    _wal->flush();

    const auto entries_result = _wal->getAllEntries();
    ASSERT_TRUE(entries_result.ok());

    const auto &entries = entries_result.value();
    ASSERT_FALSE(entries.empty());

    const auto &entry = entries[0];
    EXPECT_EQ(entry.key, large_key);
    EXPECT_EQ(entry.value, large_value);
}

TEST_F(WalTest, WalDirectoryPath)
{
    EXPECT_EQ(_wal->getWalPath().parent_path(), _test_directory);
}

TEST_F(WalTest, EntryCount)
{
    EXPECT_EQ(_wal->getEntryCount(), 0);

    _wal->logPut("key1", "value1");
    EXPECT_EQ(_wal->getEntryCount(), 1);

    _wal->logPut("key2", "value2");
    EXPECT_EQ(_wal->getEntryCount(), 2);

    // Checkpoint resets entry count
    _wal->logCheckpoint();
    EXPECT_EQ(_wal->getEntryCount(), 0);
}