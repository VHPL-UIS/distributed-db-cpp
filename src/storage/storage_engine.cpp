#include "storage_engine.hpp"
#include "../common/logger.hpp"

namespace distributed_db
{
    Result<Value> InMemoryStorageEngine::get(const Key &key)
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

    Status InMemoryStorageEngine::put(const Key &key, const Value &value)
    {
        const std::unique_lock lock(_mutex);

        _data[key] = value;
        LOG_DEBUG("Stored key: %s -> %s", key.c_str(), value.c_str());
        return Status::OK;
    }

    Status InMemoryStorageEngine::remove(const Key &key)
    {
        const std::unique_lock lock(_mutex);

        const auto it = _data.find(key);
        if (it == _data.end())
        {
            LOG_DEBUG("Key not found for removal: %s", key.c_str());
            return Status::NOT_FOUND;
        }

        _data.erase(it);
        LOG_DEBUG("Removed key: %s", key.c_str());
        return Status::OK;
    }

    bool InMemoryStorageEngine::exists(const Key &key)
    {
        const std::shared_lock lock(_mutex);
        return _data.find(key) != _data.end();
    }

    Status InMemoryStorageEngine::putBatch(const std::unordered_map<Key, Value> &batch)
    {
        const std::unique_lock lock(_mutex);

        for (const auto &[key, value] : batch)
        {
            _data[key] = value;
        }

        LOG_DEBUG("Stored batch of %d items", batch.size());
        return Status::OK;
    }

    Result<std::unordered_map<Key, Value>> InMemoryStorageEngine::getBatch(const std::vector<Key> &keys)
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

        LOG_DEBUG("Retrieved batch of %d items", result.size());
        return Result<std::unordered_map<Key, Value>>(std::move(result));
    }

    std::size_t InMemoryStorageEngine::size() const
    {
        const std::shared_lock lock(_mutex);
        return _data.size();
    }

    std::vector<Key> InMemoryStorageEngine::getAllKeys() const
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

    void InMemoryStorageEngine::clear()
    {
        const std::unique_lock lock(_mutex);
        _data.clear();
        LOG_DEBUG("Cleared all data");
    }
} // distributed_db