#include "persistent_storage_engine.hpp"
#include "../common/logger.hpp"
#include <cstring>

namespace distributed_db
{
    PersistentStorageEngine::PersistentStorageEngine(const std::filesystem::path &data_directory)
        : _data_directory(data_directory), _last_checkpoint_sequence(0)
    {
        if (!initializeDataDirectory())
        {
            throw std::runtime_error("Failed to initialize data directory: " + _data_dirctory.string());
        }

        _snapshot_file_path = _data_directory / SNAPSHOT_FILENAME;

        const auto wal_directory = _data_directory / "wal";
        _wal = std::make_unique<WriteAheadLog>(wal_directory);

        const auto recovery_status = recover();
        if (recovery_status != Status::OK)
        {
            throw std::runtime_error("Failed to recover data from persistent storage");
        }

        LOG_INFO("Persistent storage engine initialized at: %s", _data_directory.string().c_str());
        LOG_INFO("Recovered %zu keys from storage", _data.size());
    }

    PersistentStorageEngine::~PersistentStorageEngine()
    {
        const auto flush_status = flush();
        if (flush_status != Status::OK)
        {
            LOG_ERROR("Failed to flush data during shutdown");
        }
    }

    Result<Value> PersistentStorageEngine::get(const Key &key)
    {
        const std::shared_lock lock(_mutex);

        const auto it = _data.find(key);
        if (it == _data.end())
        {
            LOG_DEBUG("Key not found: %s", key.c_str());
            return Result<Value>(Status::NOT_FOUND);
        }

        LOG_DEBUG("Retrieved key: %s -> %s", key.c_str(), it->second.c_str());
        return Result<Value>(it->second);
    }

    Status PersistentStorageEngine::put(const Key &key, const Value &value)
    {
        const auto wal_status = _wal->logPut(key, value);
        if (wal_status != Status::OK)
        {
            LOG_ERROR("Failed to log PUT operation to WAL");
            return wal_status;
        }

        {
            const std::unique_lock lock(_mutex);
            _data[key] = value;
        }

        LOG_DEBUG("Stored key: %s -> %s", key.c_str(), value.c_str());
        return Status::OK;
    }

    Status PersistentStorageEngine::remove(const Key &key)
    {
        {
            const std::shared_lock lock(_mutex);
            if (_data.find(key) == _data.end())
            {
                LOG_DEBUG("Key not found for removal: %s", key.c_str());
                return Status::NOT_FOUND;
            }
        }

        const auto wal_status = _wal->logDelete(key);
        if (wal_status != Status::OK)
        {
            LOG_ERROR("Failed to log DELETE operation to WAL");
            return wal_status;
        }

        {
            const std::unique_lock lock(_mutex);
            _data.erase(key);
        }

        LOG_DEBUG("Removed key: %s", key.c_str());
        return Status::OK;
    }

    bool PersistentStorageEngine::exists(const Key &key)
    {
        const std::shared_lock lock(_mutex);
        return _data.find(key) != _data.end();
    }

    Status PersistentStorageEngine::putBatch(const std::unordered_map<Key, Value> &batch)
    {
        for (const auto &[key, value] : batch)
        {
            const auto wal_status = _wal->logPut(key, value);
            if (wal_status != Status::OK)
            {
                LOG_ERROR("Failed to log batch PUT operation to WAL for key: %s", key.c_str());
                return wal_status;
            }
        }

        {
            const std::unique_lock lock(_mutex);
            for (const auto &[key, value] : batch)
            {
                _data[key] = value;
            }
        }

        LOG_DEBUG("Stored batch of %zu items", batch.size());
        return Status::OK;
    }

    Result<std::unordered_map<Key, Value>> PersistentStorageEngine::getBatch(const std::vector<Key> &keys)
    {
        const std::shared_lock lock(_mutex);

        std::unordered_map<Key, Value> result;
        for (const auto &key : keys)
        {
            const auto it = _data.find(key);
            if (it != _data.end())
            {
                result[key] = it->second;
            }
        }

        LOG_DEBUG("Retrieved batch of %zu items", result.size());
        return Result<std::unordered_map<Key, Value>>(std::move(result));
    }

    std::size_t PersistentStorageEngine::size() const
    {
        const std::shared_lock lock(_mutex);
        return _data.size();
    }

    std::vector<Key> PersistentStorageEngine::getAllKeys() const
    {
        const std::shared_lock lock(_mutex);

        std::vector<Key> keys;
        keys.reserve(_data.size());

        for (const auto &[key, _] : _data)
        {
            keys.push_back(key);
        }

        return keys;
    }

    void PersistentStorageEngine::clear()
    {
        _wal->logCheckpoint();

        {
            const std::unique_lock lock(_mutex);
            _data.clear();
        }

        saveSnapshot();

        LOG_DEBUG("Cleared all data");
    }

    Status PersistentStorageEngine::flush()
    {
        const auto wal_status = _wal->flush();
        if (wal_status != Status::OK)
        {
            return wal_status;
        }

        const auto current_wal_entries = _wal->getEntryCount();
        if (current_wal_entries > 100)
        {
            return checkpoint();
        }

        return Status::OK;
    }

    Status PersistentStorageEngine::checkpoint()
    {
        LOG_INFO("Creating checkpoint...");

        const auto wal_status = _wal->logCheckpoint();
        if (wal_status != Status::OK)
        {
            LOG_ERROR("Failed to log checkpoint to WAL");
            return wal_status;
        }

        const auto snapshot_status = saveSnapshot();
        if (snapshot_status != Status::OK)
        {
            LOG_ERROR("Failed to save snapshot during checkpoint");
            return snapshot_status;
        }

        _last_checkpoint_sequence = _wal->getCurrentSequenceNumber();

        const auto truncate_before = _last_checkpoint_sequence - 10; // Keep last 10 entries
        if (truncate_before > 0)
        {
            _wal->truncateUpTo(truncate_before);
        }

        LOG_INFO("Checkpoint completed at sequence %lu", _last_checkpoint_sequence);
        return Status::OK;
    }

    Status PersistentStorageEngine::recover()
    {
        LOG_INFO("Starting recovery process...");

        if (snapshotExists())
        {
            LOG_INFO("Loading snapshot from: %s", _snapshot_file_path.string().c_str());
            const auto snapshot_status = loadSnapshot();
            if (snapshot_status != Status::OK)
            {
                LOG_ERROR("Failed to load snapshot, starting with empty state");
                _data.clear();
                _last_checkpoint_sequence = 0;
            }
        }
        else
        {
            LOG_INFO("No snapshot found, starting with empty state");
            _last_checkpoint_sequence = 0;
        }

        const auto replay_status = replayWalEntries();
        if (replay_status != Status::OK)
        {
            LOG_ERROR("Failed to replay WAL entries");
            return replay_status;
        }

        LOG_INFO("Recovery completed, restored %zu keys", _data.size());
        return Status::OK;
    }

    std::size_t PersistentStorageEngine::getWalEntryCount() const
    {
        return _wal->getEntryCount();
    }

    std::uint64_t PersistentStorageEngine::getCurrentWalSequence() const
    {
        return _wal->getCurrentSequenceNumber();
    }

    const std::filesystem::path &PersistentStorageEngine::getDataDirectory() const noexcept
    {
        return _data_directory;
    }

    bool PersistentStorageEngine::initializeDataDirectory()
    {
        std::error_code ec;
        std::filesystem::create_directories(_data_directory, ec);

        if (ec)
        {
            LOG_ERROR("Failed to create data directory: %s", ec.message().c_str());
            return false;
        }

        return true;
    }

    Status PersistentStorageEngine::loadSnapshot()
    {
        if (!snapshotExists())
        {
            return Status::NOT_FOUND;
        }

        std::ifstream file(_snapshot_file_path, std::ios::binary);
        if (!file.is_open())
        {
            LOG_ERROR("Failed to open snapshot file: %s", _snapshot_file_path.string().c_str());
            return Status::INTERNAL_ERROR;
        }

        const auto status = deserializeSnapshot(file);
        if (status != Status::OK)
        {
            LOG_ERROR("Failed to deserialize snapshot");
            return status;
        }

        LOG_INFO("Loaded snapshot with %zu keys", _data.size());
        return Status::OK;
    }

    Status PersistentStorageEngine::saveSnapshot()
    {
        const auto temp_snapshot_path = _snapshot_file_path.string() + ".tmp";

        std::ofstream file(temp_snapshot_path, std::ios::binary);
        if (!file.is_open())
        {
            LOG_ERROR("Failed to create temporary snapshot file: %s", temp_snapshot_path.c_str());
            return Status::INTERNAL_ERROR;
        }

        {
            const std::shared_lock lock(_mutex);
            const auto status = serializeSnapshot(file);
            if (status != Status::OK)
            {
                LOG_ERROR("Failed to serialize snapshot");
                std::filesystem::remove(temp_snapshot_path);
                return status;
            }
        }

        file.close();

        std::error_code ec;
        std::filesystem::rename(temp_snapshot_path, _snapshot_file_path, ec);
        if (ec)
        {
            LOG_ERROR("Failed to replace snapshot file: %s", ec.message().c_str());
            std::filesystem::remove(temp_snapshot_path);
            return Status::INTERNAL_ERROR;
        }

        LOG_INFO("Saved snapshot with %zu keys", _data.size());
        return Status::OK;
    }

    Status PersistentStorageEngine::replayWalEntries()
    {
        auto entries_result = _wal->getEntriesSinceSequence(_last_checkpoint_sequence);
        if (!entries_result.ok())
        {
            LOG_ERROR("Failed to read WAL entries for replay");
            return entries_result.status();
        }

        const auto &entries = entries_result.value();
        LOG_INFO("Replaying %zu WAL entries since sequence %lu", entries.size(), _last_checkpoint_sequence);

        {
            const std::unique_lock lock(_mutex);
            for (const auto &entry : entries)
            {
                applyWalEntry(entry);
            }
        }

        LOG_INFO("WAL replay completed");
        return Status::OK;
    }

    Status PersistentStorageEngine::serializeSnapshot(std::ofstream &file) const
    {
        file.write(reinterpret_cast<const char *>(&SNAPSHOT_MAGIC_NUMBER), sizeof(SNAPSHOT_MAGIC_NUMBER));
        file.write(reinterpret_cast<const char *>(&SNAPSHOT_VERSION), sizeof(SNAPSHOT_VERSION));
        file.write(reinterpret_cast<const char *>(&_last_checkpoint_sequence), sizeof(_last_checkpoint_sequence));

        const auto entry_count = static_cast<std::uint64_t>(_data.size());
        file.write(reinterpret_cast<const char *>(&entry_count), sizeof(entry_count));

        for (const auto &[key, value] : _data)
        {
            const auto key_size = static_cast<std::uint32_t>(key.size());
            file.write(reinterpret_cast<const char *>(&key_size), sizeof(key_size));
            file.write(key.data(), key.size());

            const auto value_size = static_cast<std::uint32_t>(value.size());
            file.write(reinterpret_cast<const char *>(&value_size), sizeof(value_size));
            file.write(value.data(), value.size());
        }

        if (file.fail())
        {
            LOG_ERROR("Failed to write snapshot data");
            return Status::INTERNAL_ERROR;
        }

        return Status::OK;
    }

    Status PersistentStorageEngine::deserializeSnapshot(std::ifstream &file)
    {
        std::uint32_t magic;
        std::uint8_t version;
        std::uint64_t checkpoint_sequence;

        file.read(reinterpret_cast<char *>(&magic), sizeof(magic));
        file.read(reinterpret_cast<char *>(&version), sizeof(version));
        file.read(reinterpret_cast<char *>(&checkpoint_sequence), sizeof(checkpoint_sequence));

        if (file.fail() || magic != SNAPSHOT_MAGIC_NUMBER || version != SNAPSHOT_VERSION)
        {
            LOG_ERROR("Invalid snapshot file header");
            return Status::INTERNAL_ERROR;
        }

        _last_checkpoint_sequence = checkpoint_sequence;

        std::uint64_t entry_count;
        file.read(reinterpret_cast<char *>(&entry_count), sizeof(entry_count));

        if (file.fail())
        {
            LOG_ERROR("Failed to read entry count from snapshot");
            return Status::INTERNAL_ERROR;
        }

        _data.clear();
        _data.reserve(entry_count);

        for (std::uint64_t i = 0; i < entry_count; ++i)
        {
            std::uint32_t key_size;
            file.read(reinterpret_cast<char *>(&key_size), sizeof(key_size));

            if (file.fail() || key_size > 1024 * 1024)
            {
                LOG_ERROR("Invalid key size in snapshot: %u", key_size);
                return Status::INTERNAL_ERROR;
            }

            std::string key(key_size, '\0');
            file.read(key.data(), key_size);

            std::uint32_t value_size;
            file.read(reinterpret_cast<char *>(&value_size), sizeof(value_size));

            if (file.fail() || value_size > 1024 * 1024)
            {
                LOG_ERROR("Invalid value size in snapshot: %u", value_size);
                return Status::INTERNAL_ERROR;
            }

            std::string value(value_size, '\0');
            file.read(value.data(), value_size);

            if (file.fail())
            {
                LOG_ERROR("Failed to read key-value pair %lu from snapshot", i);
                return Status::INTERNAL_ERROR;
            }

            _data[std::move(key)] = std::move(value);
        }

        return Status::OK;
    }

    bool PersistentStorageEngine::snapshotExists() const
    {
        return std::filesystem::exists(_snapshot_file_path);
    }

    std::filesystem::path PersistentStorageEngine::getSnapshotPath() const
    {
        return _snapshot_file_path;
    }

    void PersistentStorageEngine::applyWalEntry(const WalEntry &entry)
    {
        switch (entry.type)
        {
        case WalEntryType::PUT:
            _data[entry.key] = entry.value;
            LOG_DEBUG("Replayed PUT: %s -> %s", entry.key.c_str(), entry.value.c_str());
            break;

        case WalEntryType::DELETE:
            _data.erase(entry.key);
            LOG_DEBUG("Replayed DELETE: %s", entry.key.c_str());
            break;

        case WalEntryType::CHECKPOINT:
            LOG_DEBUG("Replayed CHECKPOINT at sequence %lu", entry.sequence_number);
            break;

        default:
            LOG_WARN("Unknown WAL entry type: %d", static_cast<int>(entry.type));
            break;
        }
    }

    Status PersistentStorageEngine::validateSnapshot() const
    {
        if (!snapshotExists())
        {
            return Status::NOT_FOUND;
        }

        return isValidSnapshotFile(_snapshot_file_path) ? Status::OK : Status::INTERNAL_ERROR;
    }

    bool PersistentStorageEngine::isValidSnapshotFile(const std::filesystem::path &path) const
    {
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open())
        {
            return false;
        }

        std::uint32_t magic;
        std::uint8_t version;

        file.read(reinterpret_cast<char *>(&magic), sizeof(magic));
        file.read(reinterpret_cast<char *>(&version), sizeof(version));

        return !file.fail() && magic == SNAPSHOT_MAGIC_NUMBER && version == SNAPSHOT_VERSION;
    }
} // namespace distributed_db