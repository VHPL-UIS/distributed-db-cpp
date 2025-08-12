#include "consistent_hash.hpp"
#include <functional>
#include <sstream>
#include <unordered_set>

namespace distributed_db
{
    ConsistentHashRing::ConsistentHashRing(std::size_t virtual_nodes, std::size_t replication_factor)
        : _virtual_nodes(virtual_nodes), _replication_factor(std::max<std::size_t>(1, replication_factor))
    {
    }

    void ConsistentHashRing::setVirtualNodes(std::size_t count)
    {
        std::unique_lock lock(_mutex);
        _virtual_nodes = count;
    }

    void ConsistentHashRing::setReplicationFactor(std::size_t factor)
    {
        std::unique_lock lock(_mutex);
        _replication_factor = std::max<std::size_t>(1, factor);
    }

    void ConsistentHashRing::addNode(const NodeId &node_id)
    {
        std::unique_lock lock(_mutex);
        if (_node_hashes.find(node_id) != _node_hashes.end())
        {
            return; // node already present
        }

        std::vector<std::uint64_t> hashes;
        hashes.reserve(_virtual_nodes);
        for (std::size_t i = 0; i < _virtual_nodes; ++i)
        {
            std::ostringstream oss;
            oss << node_id << "#" << i;
            auto h = hashString(oss.str());
            // if hash exists, still insert (map will overwrite),
            // but we still keep individual hashes so removal works consistently
            _ring[h] = node_id;
            hashes.push_back(h);
        }
        _node_hashes.emplace(node_id, std::move(hashes));
    }

    void ConsistentHashRing::removeNode(const NodeId &node_id)
    {
        std::unique_lock lock(_mutex);
        auto it = _node_hashes.find(node_id);
        if (it == _node_hashes.end())
            return;

        for (auto h : it->second)
        {
            _ring.erase(h);
        }
        _node_hashes.erase(it);
    }

    void ConsistentHashRing::clear()
    {
        std::unique_lock lock(_mutex);
        _ring.clear();
        _node_hashes.clear();
    }

    std::vector<NodeId> ConsistentHashRing::getNodesForKey(const Key &key) const
    {
        std::shared_lock lock(_mutex);
        std::vector<NodeId> result;
        if (_ring.empty())
            return result;

        auto key_hash = hashString(key);
        // find first element >= key_hash
        auto it = _ring.lower_bound(key_hash);
        if (it == _ring.end())
            it = _ring.begin(); // wrap

        // collect unique physical nodes until replication_factor reached
        std::unordered_set<NodeId> seen;
        while (result.size() < _replication_factor && !_ring.empty())
        {
            const NodeId &candidate = it->second;
            if (seen.insert(candidate).second)
            {
                result.push_back(candidate);
            }
            ++it;
            if (it == _ring.end())
                it = _ring.begin(); // wrap
        }
        return result;
    }

    NodeId ConsistentHashRing::getPrimaryNodeForKey(const Key &key) const
    {
        std::shared_lock lock(_mutex);
        if (_ring.empty())
            return NodeId{};
        auto key_hash = hashString(key);
        auto it = _ring.lower_bound(key_hash);
        if (it == _ring.end())
            it = _ring.begin();
        return it->second;
    }

    std::size_t ConsistentHashRing::size() const
    {
        std::shared_lock lock(_mutex);
        return _node_hashes.size();
    }

    std::size_t ConsistentHashRing::ringSize() const
    {
        std::shared_lock lock(_mutex);
        return _ring.size();
    }

    std::vector<NodeId> ConsistentHashRing::allNodes() const
    {
        std::shared_lock lock(_mutex);
        std::vector<NodeId> res;
        res.reserve(_node_hashes.size());
        for (const auto &p : _node_hashes)
            res.push_back(p.first);
        return res;
    }

    std::uint64_t ConsistentHashRing::splitmix64(std::uint64_t x) noexcept
    {
        x += 0x9e3779b97f4a7c15ULL;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        x = x ^ (x >> 31);
        return x;
    }

    std::uint64_t ConsistentHashRing::hashString(const std::string &s) const noexcept
    {
        static std::hash<std::string> hasher;
        auto h = hasher(s);
        return splitmix64(static_cast<std::uint64_t>(h));
    }
}