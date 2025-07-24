#ifndef __WAL_HPP__
#define __WAL_HPP__

#include "../common/types.hpp"
#include <filesystem>
#include <mutex>
#include <memory>
#include <fstream>
#include <vector>

namespace distributed_db
{
    enum class WalEntryType : std::uint8_t
    {
        PUT = 1,
        DELETE = 2,
        CHECKPOINT = 3
    };

    struct WalEntry
    {
        WalEntryType type;
        Key key;
        Value value;
        Timestamp timestamp;
        std::uint64_t sequence_number;

        WalEntry() = default;
        WalEntry(WalEntryType type, Key key, Value value = "")
            : type(type), key(std::move(key)), value(std::move(value)),
              timestamp(std::chrono::system_clock::now()), sequence_number(0) {}
    };

    class WriteAheadLog
    {
    public:
        explicit WriteAheadLog(const std::filesystem::path &wal_directory);
        ~WriteAheadLog();

        WriteAheadLog(const WriteAheadLog &) = delete;
        WriteAheadLog &operator=(const WriteAheadLog &) = delete;
        WriteAheadLog(WriteAheadLog &&) = default;
        WriteAheadLog &operator=(WriteAheadLog &&) = default;

        Status logPut(const Key &key, const Value &value);
        Status logDelete(const Key &key);
        Status logCheckpoint();
        Status flush();

        Result<std::vector<WalEntry>> getAllEntries() const;
        Result<std::vector<WalEntry>> getEntriesSinceSequence(std::uint64_t sequence) const;

        Status truncateUpTo(std::uint64_t sequence_number);
        Status createCheckpoint();

        std::uint64_t getCurrentSequenceNumber() const noexcept;
        std::size_t getEntryCount() const;
        const std::filesystem::path &getWalPath() const noexcept;

    private:
        std::filesystem::path _wal_directory;
        std::filesystem::path _wal_file_path;
        mutable std::mutex _mutex;
        std::unique_ptr<std::ofstream> _wal_file;
        std::uint64_t _current_sequence_number;
        std::size_t _entries_since_checkpoint;

        static constexpr std::size_t MAX_ENTRIES_BEFORE_CHECKPOINT = 1000;
        static constexpr std::uint32_t WAL_MAGIC_NUMBER = 0xDEADBEEF;
        static constexpr std::uint8_t WAL_VERSION = 1;

        bool initializeWalFile();
        Status writeEntry(const WalEntry &entry);
        Status writeHeader();
        Result<WalEntry> readNextEntry(std::ifstream &file) const;
        Status validateWalFile() const;

        Status serializeEntry(const WalEntry &entry, std::vector<std::uint8_t> &buffer) const;
        Result<WalEntry> deserializeEntry(const std::vector<std::uint8_t> &buffer) const;

        std::uint64_t getNextSequenceNumber() noexcept;
        std::filesystem::path generateWalFileName() const;
    };
} // namespace distributed_db

#endif // __WAL_HPP__