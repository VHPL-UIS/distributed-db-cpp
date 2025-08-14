#ifndef __REPLICATOR_HPP__
#define __REPLICATOR_HPP__

#include "../cluster/key_router.hpp"
#include "../cluster/node_info.hpp"
#include "../common/logger.hpp"
#include "../network/tcp_client.hpp"
#include "../storage/storage_engine.hpp"
#include <future>
#include <unordered_map>

namespace distributed_db
{
    enum class ConsistencyLevel : std::uint8_t
    {
        ONE,    // succeed after 1 ack (primary or any owner)
        QUORUM, // majority of owners
        ALL     // all owners
    };

    struct ReplicationConfig
    {
        ConsistencyLevel write_consistency{ConsistencyLevel::QUORUM};
        ConsistencyLevel read_consistency{ConsistencyLevel::QUORUM};
        std::chrono::milliseconds rpc_timeout{std::chrono::milliseconds(1500)};
        bool enable_read_repair{true};
    };

    // Transport abstraction to make testing easy (TcpClient by default)
    class IKeyValueTransport
    {
    public:
        virtual ~IKeyValueTransport() = default;
        virtual Result<Value> get(const std::string &endpoint, const Key &key,
                                  std::chrono::milliseconds timeout) = 0;
        virtual Status put(const std::string &endpoint, const Key &key, const Value &value,
                           std::chrono::milliseconds timeout) = 0;
        virtual Status remove(const std::string &endpoint, const Key &key,
                              std::chrono::milliseconds timeout) = 0;
    };

    // Default TCP transport
    class TcpKeyValueTransport : public IKeyValueTransport
    {
    public:
        Result<Value> get(const std::string &endpoint, const Key &key,
                          std::chrono::milliseconds timeout) override;
        Status put(const std::string &endpoint, const Key &key, const Value &value,
                   std::chrono::milliseconds timeout) override;
        Status remove(const std::string &endpoint, const Key &key,
                      std::chrono::milliseconds timeout) override;

    private:
        // simple ephemeral client per call
        Result<std::unique_ptr<TcpClient>> connect(const std::string &endpoint,
                                                   std::chrono::milliseconds timeout);
    };

    class Replicator
    {
    public:
        Replicator(std::shared_ptr<NodeInfo> local,
                   std::shared_ptr<NodeRegistry> registry,
                   std::shared_ptr<ConsistentHashRing> ring,
                   std::shared_ptr<StorageEngine> storage,
                   ReplicationConfig cfg = {},
                   std::unique_ptr<IKeyValueTransport> transport = std::make_unique<TcpKeyValueTransport>())
            : _local(std::move(local)),
              _registry(std::move(registry)),
              _ring(std::move(ring)),
              _storage(std::move(storage)),
              _cfg(cfg),
              _transport(std::move(transport)),
              _router(_local, _registry, _ring)
        {
        }

        // Write path (primary-backup)
        Status put(const Key &key, const Value &value, std::optional<ConsistencyLevel> level = std::nullopt);
        Status remove(const Key &key, std::optional<ConsistencyLevel> level = std::nullopt);

        // Read path (quorum + optional read-repair)
        Result<Value> get(const Key &key, std::optional<ConsistencyLevel> level = std::nullopt);

        // Helpers
        [[nodiscard]] std::size_t replicationFactorFor(const Key &key) const;
        [[nodiscard]] std::size_t quorumSizeFor(const Key &key) const;

    private:
        std::shared_ptr<NodeInfo> _local;
        std::shared_ptr<NodeRegistry> _registry;
        std::shared_ptr<ConsistentHashRing> _ring;
        std::shared_ptr<StorageEngine> _storage;
        ReplicationConfig _cfg;
        std::unique_ptr<IKeyValueTransport> _transport;
        KeyRouter _router;

        // Internal ops
        Status replicatePut(const Key &key, const Value &value,
                            const std::vector<std::string> &owner_eps,
                            ConsistencyLevel level);

        Status replicateDelete(const Key &key,
                               const std::vector<std::string> &owner_eps,
                               ConsistencyLevel level);

        Result<Value> readFromOwners(const Key &key,
                                     const std::vector<std::string> &owner_eps,
                                     ConsistencyLevel level);

        // Conflict resolution: primary-wins; if tie/mismatch -> majority wins, fallback to first
        Value resolveConflict(const std::vector<Value> &values,
                              const std::vector<std::string> &from_eps,
                              const std::string &primary_ep) const;

        void bestEffortReadRepair(const Key &key, const Value &canonical,
                                  const std::vector<std::pair<std::string, Value>> &divergent);
    };

} // namespace distributed_db

#endif // __REPLICATOR_HPP__
