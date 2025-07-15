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

    Status WriteAheadLog::flush()
    {
        const std::lock_guard<std::mutex> lock(_mutex);

        if (!_wal_file || !_wal_file->is_open())
        {
            return Status::INTERNAL_ERROR;
        }

        _wal_file->flush();
        if (_wal_file->fail())
        {
            LOG_ERROR("Failed to flush WAL file!");
            return Status::INTERNAL_ERROR;
        }

        return Status::OK;
    }

    Result<std::vector<WalEntry>> WriteAheadLog::getAllEntries() const
    {
        const std::lock_guard<std::mutex> lock(_mutex);

        std::ifstream file(_wal_file_path, std::ios::binary);
        if (!file.is_open())
        {
            LOG_ERROR("Failed to open WAL file for reading: %s", _wal_file_path.string().c_str());
            return Result<std::vector<WalEntry>>(Status::INTERNAL_ERROR);
        }

        std::uint32_t magic;
        std::uint8_t version;
        file.read(reinterpret_cast<char *>(&magic), sizeof(magic));
        file.read(reinterpret_cast<char *>(&version), sizeof(version));

        if (magic != WAL_MAGIC_NUMBER || version != WAL_VERSION)
        {
            LOG_ERROR("Invalid WAL file header!");
            return Result<std::vector<WalEntry>>(Status::INTERNAL_ERROR);
        }

        std::vector<WalEntry> entries;

        while (file.good() && !file.eof())
        {
            auto entry_result = readNextEntry(file);
            if (entry_result.ok())
            {
                entries.push_back(entry_result.value());
            }
            else if (file.eof())
            {
                break;
            }
            else
            {
                LOG_WARN("Failed to read WAL entry, stopping recovery!");
                break;
            }
        }

        LOG_DEBUG("Read %zu entries from WAL", entries.size());
        return Result<std::vector<WalEntry>>(std::move(entries));
    }

    Result<std::vector<WalEntry>> WriteAheadLog::getEntriesSinceSequence(std::uint64_t sequence) const
    {
        auto all_entries_result = getAllEntries();
        if (!all_entries_result.ok())
        {
            return all_entries_result;
        }

        std::vector<WalEntry> filtered_entries;
        const auto &all_entries = all_entries_result.value();

        std::copy_if(all_entries.begin(), all_entries.end(),
                     std::back_inserter(filtered_entries),
                     [sequence](const WalEntry &entry)
                     {
                         return entry.sequence_number > sequence;
                     });

        return Result<std::vector<WalEntry>>(std::move(filtered_entries));
    }

    Status WriteAheadLog::truncateUpTo(std::uint64_t sequence_number)
    {
        const std::lock_guard<std::mutex> lock(_mutex);

        auto all_entries_result = getAllEntries();
        if (!all_entries_result.ok())
        {
            return all_entries_result.status();
        }

        const auto &all_entries = all_entries_result.value();

        std::vector<WalEntry> entries_to_keep;
        std::copy_if(all_entries.begin(), all_entries.end(),
                     std::back_inserter(entries_to_keep),
                     [sequence_number](const WalEntry &entry)
                     {
                         return entry.sequence_number > sequence_number;
                     });

        if (_wal_file && _wal_file->is_open())
        {
            _wal_file->close();
        }

        _wal_file = std::make_unique<std::ofstream>(_wal_file_path,
                                                    std::ios::binary | std::ios::trunc);

        if (!_wal_file->is_open())
        {
            LOG_ERROR("Failed to reopen WAL file for truncation!");
            return Status::INTERNAL_ERROR;
        }

        auto status = writeHeader();
        if (status != Status::OK)
        {
            return status;
        }

        for (const auto &entry : entries_to_keep)
        {
            status = writeEntry(entry);
            if (status != Status::OK)
            {
                LOG_ERROR("Failed to write entry during truncation!");
                return status;
            }
        }

        _entries_since_checkpoint = entries_to_keep.size();

        LOG_INFO("WAL truncated, keeping %zu entries after sequence %lu!", entries_to_keep.size(), sequence_number);

        return Status::OK;
    }

    Status WriteAheadLog::createCheckpoint()
    {
        WalEntry checkpoint_entry(WalEntryType::CHECKPOINT, "");
        checkpoint_entry.sequence_number = getNextSequenceNumber();

        const auto status = writeEntry(checkpoint_entry);
        if (status == Status::OK)
        {
            _entries_since_checkpoint = 0;
            LOG_DEBUG("Checkpoint created at sequence %ld", checkpoint_entry.sequence_number);
        }

        return status;
    }

    std::uint64_t WriteAheadLog::getCurrentSequenceNumber() const noexcept
    {
        const std::lock_guard<std::mutex> lock(_mutex);
        return _current_sequence_number;
    }
}