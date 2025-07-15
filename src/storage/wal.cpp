#include "wal.hpp"
#include "../common/logger.hpp"

namespace distributed_db
{
    WriteAheadLog::WriteAheadLog(const std::filesystem::path &wal_directory)
        : _wal_directory(wal_directory), _current_sequence_number(0), _entries_since_checkpoint(0)
    {
        std::error_code ec;
        std::filesystem::create_directories(_wal_directory, ec);

        if (ec)
        {
            LOG_ERROR("Failed to create WAL directory: %s", ec.message().c_str());
            throw std::runtime_error("Failed to create WAL directory: " + ec.message());
        }

        _wal_file_path = _wal_directory / "wal.log";

        if (!initializeWalFile())
        {
            throw std::runtime_error("Failed to initialize WAL file!");
        }

        LOG_INFO("WAL initialized at: %s", _wal_file_path.string().c_str());
    }

    WriteAheadLog::~WriteAheadLog()
    {
        const std::loc_guard<std::mutex> lock(_mutex);
        if (_wal_file && _wal_file->is_open())
        {
            _wal_file->flush();
            _wal_file->close();
        }
    }

    Status WriteAheadLog::logPut(const Key &key, const Value &value)
    {
        const std::lock_guard<std::mutex> lock(_mutex);

        WalEntry entry(WalEntryType::PUT, key, value);
        entry.sequence_number = getNextSequenceNumber();

        const auto status = writeEntry(entry);
        if (status == Status::OK)
        {
            ++_entries_since_checkpoint;

            if (_entries_since_checkpoint >= MAX_ENTRIES_BEFORE_CHECKPOINT)
            {
                LOG_DEBUG("Auto-checkpointing after %zu entries!", _entries_since_checkpoint);
                createCheckpoint();
            }
        }

        return status;
    }

    Status WriteAheadLog::logDelete(const Key &key)
    {
        const std::lock_guard<std::mutex> lock(_mutex);

        WalEntry entry(WalEntryType::DELETE, key);
        entry.sequence_number = getNextSequenceNumber();

        const auto status = writeEntry(entry);
        if (status == Status::OK)
        {
            ++_entries_since_checkpoint;
        }

        return status;
    }

    Status WriteAheadLog::logCheckpoint()
    {
        const std::lock_guard<std::mutex> lock(_mutex);
        return createCheckpoint();
    }
}