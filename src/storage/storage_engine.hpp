#ifndef __STORAGE_ENGINE_HPP__
#define __STORAGE_ENGINE_HPP__

#include "../common/types.hpp"
#include <unordered_map>
#include <shared_mutex>
#include <vector>

namespace distributed_db
{
    class StorageEngine
    {
    public:
        StorageEngine() = default;
        virtual ~StorageEngine() = default;

        virtual Result<Value> get(const Key &key) = 0;
        virtual Status put(const Key &key, const Value &value) = 0;
        virtual Status remove(const Key &key) = 0;
        virtual bool exists(const Key &key) = 0;

        virtual Status putBatch(const std::unordered_map<Key, Value> &batch) = 0;
        virtual Result<std::unordered_map<Key, Value>> getBatch(const std::vector<Key> &keys) = 0;

        [[nodiscard]] virtual std::size_t size() const = 0;
        [[nodiscard]] virtual std::vector<Key> getAllKeys() const = 0;
        virtual void clear() = 0;
    };

    class InMemoryStorageEngine : public StorageEngine
    {
    public:
        InMemoryStorageEngine() = default;
        ~InMemoryStorageEngine() override = default;

        // Non-copyable, movable
        InMemoryStorageEngine(const InMemoryStorageEngine &) = delete;
        InMemoryStorageEngine &operator=(const InMemoryStorageEngine &) = delete;
        InMemoryStorageEngine(InMemoryStorageEngine &&) = default;
        InMemoryStorageEngine &operator=(InMemoryStorageEngine &&) = default;

        Result<Value> get(const Key &key) override;
        Status put(const Key &key, const Value &value) override;
        Status remove(const Key &key) override;
        bool exists(const Key &key) override;

        Status putBatch(const std::unordered_map<Key, Value> &batch) override;
        Result<std::unordered_map<Key, Value>> getBatch(const std::vector<Key> &keys) override;

        [[nodiscard]] std::size_t size() const override;
        [[nodiscard]] std::vector<Key> getAllKeys() const override;
        void clear() override;

    private:
        mutable std::shared_mutex _mutex;
        std::unordered_map<Key, Value> _data;
    };
} // distributed_db

#endif // __STORAGE_ENGINE_HPP__