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
} // namespace distributed_db