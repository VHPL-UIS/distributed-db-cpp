#ifndef __CONSISTENT_HASH_HPP__
#define __CONSISTENT_HASH_HPP__

#include "../common/types.hpp"
#include <map>
#include <unordered_map>
#include <vector>
#include <shared_mutex>
#include <cstdint>
#include <string>

namespace distributed_db
{
    class ConsistentHashRing
    {
    public:
        explicit ConsistentHashRing(std::size_t virtual_nodes = 100, std::size_t replication_factor = 3);

        void setVirtualNodes(std::size_t count);
        void setReplicationFactor(std::size_t factor);

        void addNode(const NodeId &node_id);
        void removeNode(const NodeId &node_it);

        void clear();

        [[nodiscard]] std::vector<NodeId> getNodesForKey(const Key &key) const;
        [[nodiscard]] NodeId getPrimaryNodeForKey(const Key &key) const;

        [[nodiscard]] std::size_t size() const;     // number of distinct physical nodes
        [[nodiscard]] std::size_t ringSize() const; // number of virtual entries in ring

        // debugging / introspection
        [[nodiscard]] std::vector<NodeId> allNodes() const;

    private:
        std::map<std::uint64_t, NodeId> _ring;
        std::unordered_map<NodeId, std::vector<std::uint64_t>> _node_hashes;

        std::size_t _virtual_nodes;
        std::size_t _replication_factor;

        mutable std::shared_mutex _mutex;

        [[nodiscard]] std::uint64_t hashString(const std::string &s) const noexcept;
        static std::uint64_t splitmix64(std::uint64_t x) noexcept;
    };
} // namespace distributed_db

#endif // __CONSISTENT_HASH_HPP__