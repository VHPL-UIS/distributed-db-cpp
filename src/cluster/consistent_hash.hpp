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