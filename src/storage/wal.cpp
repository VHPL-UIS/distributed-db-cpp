#include "wal.hpp"
#include "../common/logger.hpp"
#include "algorithm"
#include <cstring>

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
        const std::lock_guard<std::mutex> lock(_mutex);
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
                const auto checkpoint_status = createCheckpoint();
                if (checkpoint_status != Status::OK)
                {
                    LOG_WARN("Auto-checkpoint failed, but continuing with operation");
                }
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

        // Skip header
        std::uint32_t magic;
        std::uint8_t version;
        file.read(reinterpret_cast<char *>(&magic), sizeof(magic));
        file.read(reinterpret_cast<char *>(&version), sizeof(version));

        if (file.fail())
        {
            LOG_WARN("WAL file appears to be empty or corrupted");
            return Result<std::vector<WalEntry>>(Status::NOT_FOUND);
        }

        if (magic != WAL_MAGIC_NUMBER || version != WAL_VERSION)
        {
            LOG_ERROR("Invalid WAL file header");
            return Result<std::vector<WalEntry>>(Status::INTERNAL_ERROR);
        }

        std::vector<WalEntry> entries;

        while (file.good())
        {
            // Check if we're at the end before trying to read
            if (file.peek() == EOF)
            {
                break;
            }

            auto entry_result = readNextEntry(file);
            if (entry_result.ok())
            {
                entries.push_back(entry_result.value());
            }
            else
            {
                if (file.eof())
                {
                    break;
                }
                else
                {
                    LOG_WARN("Failed to read WAL entry, stopping recovery");
                    break;
                }
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

        const std::lock_guard<std::mutex> lock(_mutex);

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

        _wal_file->flush();
        if (_wal_file->fail())
        {
            LOG_ERROR("Failed to flush WAL file after truncation");
            return Status::INTERNAL_ERROR;
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

    std::size_t WriteAheadLog::getEntryCount() const
    {
        const std::lock_guard<std::mutex> lock(_mutex);
        return _entries_since_checkpoint;
    }

    const std::filesystem::path &WriteAheadLog::getWalPath() const noexcept
    {
        return _wal_file_path;
    }

    bool WriteAheadLog::initializeWalFile()
    {
        if (std::filesystem::exists(_wal_file_path))
        {
            const auto validation_status = validateWalFile();
            if (validation_status != Status::OK)
            {
                LOG_WARN("Existing WAL file is corrupted, creating new one!");
                std::filesystem::remove(_wal_file_path);
            }
            else
            {
                auto entries_result = getAllEntries();
                if (entries_result.ok() && !entries_result.value().empty())
                {
                    const auto &entries = entries_result.value();
                    _current_sequence_number = entries.back().sequence_number;

                    auto checkpoint_it = std::find_if(entries.rbegin(), entries.rend(),
                                                      [](const WalEntry &entry)
                                                      {
                                                          return entry.type == WalEntryType::CHECKPOINT;
                                                      });

                    if (checkpoint_it != entries.rend())
                    {
                        _entries_since_checkpoint = std::distance(entries.rbegin(), checkpoint_it);
                    }
                    else
                    {
                        _entries_since_checkpoint = entries.size();
                    }

                    LOG_INFO("Recovered WAL with %lu entries, sequence number: %lu", entries.size(), _current_sequence_number);
                }
            }
        }

        _wal_file = std::make_unique<std::ofstream>(_wal_file_path, std::ios::binary | std::ios::app);

        if (!_wal_file->is_open())
        {
            LOG_ERROR("Failed to open WAL file: %s", _wal_file_path.string().c_str());
            return false;
        }

        if (std::filesystem::file_size(_wal_file_path) == 0)
        {
            const auto status = writeHeader();
            if (status != Status::OK)
            {
                LOG_ERROR("Failed to write WAL header!");
                return false;
            }
        }

        return true;
    }

    Status WriteAheadLog::writeEntry(const WalEntry &entry)
    {
        if (!_wal_file || !_wal_file->is_open())
        {
            return Status::INTERNAL_ERROR;
        }

        std::vector<std::uint8_t> buffer;
        const auto status = serializeEntry(entry, buffer);
        if (status != Status::OK)
        {
            return status;
        }

        const auto entry_size = static_cast<std::uint32_t>(buffer.size());
        _wal_file->write(reinterpret_cast<const char *>(&entry_size), sizeof(entry_size));
        _wal_file->write(reinterpret_cast<const char *>(buffer.data()), buffer.size());

        if (_wal_file->fail())
        {
            LOG_ERROR("Failed to write WAL entry");
            return Status::INTERNAL_ERROR;
        }

        LOG_DEBUG("WAL entry written: type=%d, key=%s, sequence=%lu",
                  static_cast<int>(entry.type), entry.key.c_str(), entry.sequence_number);

        return Status::OK;
    }

    Status WriteAheadLog::writeHeader()
    {
        if (!_wal_file || !_wal_file->is_open())
        {
            return Status::INTERNAL_ERROR;
        }

        _wal_file->write(reinterpret_cast<const char *>(&WAL_MAGIC_NUMBER), sizeof(WAL_MAGIC_NUMBER));
        _wal_file->write(reinterpret_cast<const char *>(&WAL_VERSION), sizeof(WAL_VERSION));

        if (_wal_file->fail())
        {
            LOG_ERROR("Failed to write WAL header");
            return Status::INTERNAL_ERROR;
        }

        return Status::OK;
    }

    Result<WalEntry> WriteAheadLog::readNextEntry(std::ifstream &file) const
    {
        std::uint32_t entry_size;
        file.read(reinterpret_cast<char *>(&entry_size), sizeof(entry_size));

        if (file.eof())
        {
            return Result<WalEntry>(Status::NOT_FOUND); // End of file, not an error
        }

        if (file.fail())
        {
            return Result<WalEntry>(Status::INTERNAL_ERROR);
        }

        if (entry_size == 0 || entry_size > 1024 * 1024)
        {
            LOG_ERROR("Invalid WAL entry size: %u", entry_size);
            return Result<WalEntry>(Status::INTERNAL_ERROR);
        }

        std::vector<std::uint8_t> buffer(entry_size);
        file.read(reinterpret_cast<char *>(buffer.data()), entry_size);

        if (file.fail())
        {
            LOG_ERROR("Failed to read WAL entry data");
            return Result<WalEntry>(Status::INTERNAL_ERROR);
        }

        return deserializeEntry(buffer);
    }

    Status WriteAheadLog::validateWalFile() const
    {
        std::ifstream file(_wal_file_path, std::ios::binary);
        if (!file.is_open())
        {
            return Status::INTERNAL_ERROR;
        }

        std::uint32_t magic;
        std::uint8_t version;
        file.read(reinterpret_cast<char *>(&magic), sizeof(magic));
        file.read(reinterpret_cast<char *>(&version), sizeof(version));

        if (file.fail() || magic != WAL_MAGIC_NUMBER || version != WAL_VERSION)
        {
            return Status::INTERNAL_ERROR;
        }

        return Status::OK;
    }

    Status WriteAheadLog::serializeEntry(const WalEntry &entry, std::vector<std::uint8_t> &buffer) const
    {
        buffer.clear();

        buffer.push_back(static_cast<std::uint8_t>(entry.type));

        const auto seq_bytes = reinterpret_cast<const std::uint8_t *>(&entry.sequence_number);
        buffer.insert(buffer.end(), seq_bytes, seq_bytes + sizeof(entry.sequence_number));

        const auto timestamp_value = entry.timestamp.time_since_epoch().count();
        const auto timestamp_bytes = reinterpret_cast<const std::uint8_t *>(&timestamp_value);
        buffer.insert(buffer.end(), timestamp_bytes, timestamp_bytes + sizeof(timestamp_value));

        const auto key_size = static_cast<std::uint32_t>(entry.key.size());
        const auto key_size_bytes = reinterpret_cast<const std::uint8_t *>(&key_size);
        buffer.insert(buffer.end(), key_size_bytes, key_size_bytes + sizeof(key_size));
        buffer.insert(buffer.end(), entry.key.begin(), entry.key.end());

        const auto value_size = static_cast<std::uint32_t>(entry.value.size());
        const auto value_size_bytes = reinterpret_cast<const std::uint8_t *>(&value_size);
        buffer.insert(buffer.end(), value_size_bytes, value_size_bytes + sizeof(value_size));
        buffer.insert(buffer.end(), entry.value.begin(), entry.value.end());

        return Status::OK;
    }

    Result<WalEntry> WriteAheadLog::deserializeEntry(const std::vector<std::uint8_t> &buffer) const
    {
        if (buffer.size() < sizeof(std::uint8_t) + sizeof(std::uint64_t) +
                                sizeof(std::int64_t) + 2 * sizeof(std::uint32_t))
        {
            return Result<WalEntry>(Status::INTERNAL_ERROR);
        }

        std::size_t offset = 0;
        WalEntry entry;

        entry.type = static_cast<WalEntryType>(buffer[offset]);
        offset += sizeof(std::uint8_t);

        std::memcpy(&entry.sequence_number, buffer.data() + offset, sizeof(entry.sequence_number));
        offset += sizeof(entry.sequence_number);

        std::int64_t timestamp_value;
        std::memcpy(&timestamp_value, buffer.data() + offset, sizeof(timestamp_value));
        entry.timestamp = Timestamp(std::chrono::system_clock::duration(timestamp_value));
        offset += sizeof(timestamp_value);

        std::uint32_t key_size;
        std::memcpy(&key_size, buffer.data() + offset, sizeof(key_size));
        offset += sizeof(key_size);

        if (offset + key_size > buffer.size())
        {
            return Result<WalEntry>(Status::INTERNAL_ERROR);
        }

        entry.key = std::string(buffer.begin() + offset, buffer.begin() + offset + key_size);
        offset += key_size;

        std::uint32_t value_size;
        std::memcpy(&value_size, buffer.data() + offset, sizeof(value_size));
        offset += sizeof(value_size);

        if (offset + value_size > buffer.size())
        {
            return Result<WalEntry>(Status::INTERNAL_ERROR);
        }

        entry.value = std::string(buffer.begin() + offset, buffer.begin() + offset + value_size);

        return Result<WalEntry>(std::move(entry));
    }

    std::uint64_t WriteAheadLog::getNextSequenceNumber() noexcept
    {
        return ++_current_sequence_number;
    }

    std::filesystem::path WriteAheadLog::generateWalFileName() const
    {
        const auto now = std::chrono::system_clock::now();
        const auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                                   now.time_since_epoch())
                                   .count();

        return _wal_directory / ("wal_" + std::to_string(timestamp) + ".log");
    }
}