#include "node_info.hpp"
#include "../common/logger.hpp"
#include <sstream>
#include <algorithm>
#include <cstring>
#include <shared_mutex>

namespace distributed_db
{
    NodeInfo::NodeInfo(NodeId id, std::string address, Port port)
        : _id(std::move(id)), _address(std::move(address)), _port(port),
          _join_time(std::chrono::system_clock::now())
    {
        updateLastSeen();
    }

    NodeInfo::NodeInfo(NodeId id, std::string address, Port port, NodeRole role)
        : NodeInfo(std::move(id), std::move(address), port)
    {
        _role = role;
    }

    NodeInfo::NodeInfo(const NodeInfo &other)
        : _id(other._id), _address(other._address), _port(other._port),
          _state(other._state.load()), _role(other._role.load()),
          _join_time(other._join_time), _last_seen_ms(other._last_seen_ms.load()),
          _heartbeats_sent(other._heartbeats_sent.load()),
          _heartbeats_received(other._heartbeats_received.load()),
          _total_latency_ms(other._total_latency_ms.load()),
          _latency_samples(other._latency_samples.load())
    {

        const std::lock_guard<std::mutex> lock(other._metadata_mutex);
        _metadata = other._metadata;
    }

    NodeInfo &NodeInfo::operator=(const NodeInfo &other)
    {
        if (this != &other)
        {
            _id = other._id;
            _address = other._address;
            _port = other._port;
            _state = other._state.load();
            _role = other._role.load();
            _join_time = other._join_time;
            _last_seen_ms = other._last_seen_ms.load();
            _heartbeats_sent = other._heartbeats_sent.load();
            _heartbeats_received = other._heartbeats_received.load();
            _total_latency_ms = other._total_latency_ms.load();
            _latency_samples = other._latency_samples.load();

            // Lock both mutexes in a consistent order to avoid deadlock
            std::lock(_metadata_mutex, other._metadata_mutex);
            const std::lock_guard<std::mutex> lock1(_metadata_mutex, std::adopt_lock);
            const std::lock_guard<std::mutex> lock2(other._metadata_mutex, std::adopt_lock);
            _metadata = other._metadata;
        }
        return *this;
    }

    NodeInfo::NodeInfo(NodeInfo &&other) noexcept
        : _id(std::move(other._id)), _address(std::move(other._address)), _port(other._port),
          _state(other._state.load()), _role(other._role.load()),
          _join_time(other._join_time), _last_seen_ms(other._last_seen_ms.load()),
          _heartbeats_sent(other._heartbeats_sent.load()),
          _heartbeats_received(other._heartbeats_received.load()),
          _total_latency_ms(other._total_latency_ms.load()),
          _latency_samples(other._latency_samples.load())
    {

        const std::lock_guard<std::mutex> lock(other._metadata_mutex);
        _metadata = std::move(other._metadata);
    }

    NodeInfo &NodeInfo::operator=(NodeInfo &&other) noexcept
    {
        if (this != &other)
        {
            _id = std::move(other._id);
            _address = std::move(other._address);
            _port = other._port;
            _state = other._state.load();
            _role = other._role.load();
            _join_time = other._join_time;
            _last_seen_ms = other._last_seen_ms.load();
            _heartbeats_sent = other._heartbeats_sent.load();
            _heartbeats_received = other._heartbeats_received.load();
            _total_latency_ms = other._total_latency_ms.load();
            _latency_samples = other._latency_samples.load();

            const std::lock_guard<std::mutex> lock1(_metadata_mutex);
            const std::lock_guard<std::mutex> lock2(other._metadata_mutex);
            _metadata = std::move(other._metadata);
        }
        return *this;
    }

    std::string NodeInfo::getEndpoint() const
    {
        return _address + ":" + std::to_string(_port);
    }

    NodeState NodeInfo::getState() const
    {
        return _state.load();
    }

    void NodeInfo::setState(NodeState state)
    {
        _state.store(state);
        updateLastSeen(); // Update activity timestamp when state changes
    }

    NodeRole NodeInfo::getRole() const
    {
        return _role.load();
    }

    void NodeInfo::setRole(NodeRole role)
    {
        _role.store(role);
        updateLastSeen(); // Update activity timestamp when role changes
    }

    Timestamp NodeInfo::getJoinTime() const
    {
        return _join_time;
    }

    Timestamp NodeInfo::getLastSeen() const
    {
        const auto ms = _last_seen_ms.load();
        return Timestamp(std::chrono::milliseconds(ms));
    }

    void NodeInfo::updateLastSeen()
    {
        const auto now_ms = getCurrentTimeMs();
        _last_seen_ms.store(now_ms);
    }

    void NodeInfo::updateLastSeen(Timestamp timestamp)
    {
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            timestamp.time_since_epoch())
                            .count();
        _last_seen_ms.store(ms);
    }

    std::chrono::milliseconds NodeInfo::getLastSeenAge() const
    {
        const auto now_ms = getCurrentTimeMs();
        const auto last_seen_ms = _last_seen_ms.load();
        return std::chrono::milliseconds(now_ms - last_seen_ms);
    }

    bool NodeInfo::isHealthy(std::chrono::milliseconds timeout) const
    {
        return getLastSeenAge() <= timeout;
    }

    bool NodeInfo::isActive() const noexcept
    {
        const auto current_state = _state.load();
        // Consider UNKNOWN as active for newly created nodes
        return current_state == NodeState::ACTIVE || current_state == NodeState::UNKNOWN;
    }

    bool NodeInfo::canVote() const noexcept
    {
        const auto current_role = _role.load();
        const auto current_state = _state.load();

        // A node can vote if:
        // 1. It's not an OBSERVER (observers don't participate in voting)
        // 2. It's not in a FAILED or LEAVING state (these nodes are effectively out of the cluster)
        return current_role != NodeRole::OBSERVER &&
               current_state != NodeState::FAILED &&
               current_state != NodeState::LEAVING;
    }

    void NodeInfo::setMetadata(const std::string &key, const std::string &value)
    {
        const std::lock_guard<std::mutex> lock(_metadata_mutex);
        _metadata[key] = value;
    }

    std::string NodeInfo::getMetadata(const std::string &key) const
    {
        const std::lock_guard<std::mutex> lock(_metadata_mutex);
        const auto it = _metadata.find(key);
        return it != _metadata.end() ? it->second : std::string{};
    }

    bool NodeInfo::hasMetadata(const std::string &key) const
    {
        const std::lock_guard<std::mutex> lock(_metadata_mutex);
        return _metadata.find(key) != _metadata.end();
    }

    void NodeInfo::removeMetadata(const std::string &key)
    {
        const std::lock_guard<std::mutex> lock(_metadata_mutex);
        _metadata.erase(key);
    }

    const std::unordered_map<std::string, std::string> &NodeInfo::getAllMetadata() const
    {
        // Note: This returns a reference, so caller should hold the lock
        // For safety, consider returning a copy instead
        return _metadata;
    }

    void NodeInfo::incrementHeartbeatsSent()
    {
        _heartbeats_sent.fetch_add(1, std::memory_order_relaxed);
    }

    void NodeInfo::incrementHeartbeatsReceived()
    {
        _heartbeats_received.fetch_add(1, std::memory_order_relaxed);
    }

    void NodeInfo::recordLatency(std::chrono::milliseconds latency)
    {
        _total_latency_ms.fetch_add(latency.count(), std::memory_order_relaxed);
        _latency_samples.fetch_add(1, std::memory_order_relaxed);
    }

    std::uint64_t NodeInfo::getHeartbeatsSent() const
    {
        return _heartbeats_sent.load();
    }

    std::uint64_t NodeInfo::getHeartbeatsReceived() const
    {
        return _heartbeats_received.load();
    }

    std::chrono::milliseconds NodeInfo::getAverageLatency() const
    {
        const auto total_ms = _total_latency_ms.load();
        const auto samples = _latency_samples.load();

        if (samples == 0)
        {
            return std::chrono::milliseconds(0);
        }

        return std::chrono::milliseconds(total_ms / samples);
    }

    bool NodeInfo::operator==(const NodeInfo &other) const noexcept
    {
        return _id == other._id && _address == other._address && _port == other._port;
    }

    bool NodeInfo::operator!=(const NodeInfo &other) const noexcept
    {
        return !(*this == other);
    }

    std::string NodeInfo::toString() const
    {
        std::ostringstream oss;
        oss << "NodeInfo{id=" << _id
            << ", endpoint=" << getEndpoint()
            << ", state=" << nodeStateToString(getState())
            << ", role=" << nodeRoleToString(getRole())
            << ", age=" << getLastSeenAge().count() << "ms"
            << ", hb_sent=" << getHeartbeatsSent()
            << ", hb_recv=" << getHeartbeatsReceived()
            << "}";
        return oss.str();
    }

    Result<std::vector<std::uint8_t>> NodeInfo::serialize() const
    {
        std::vector<std::uint8_t> buffer;

        // Serialize basic fields
        // ID length and data
        const auto id_size = static_cast<std::uint32_t>(_id.size());
        const auto id_size_bytes = reinterpret_cast<const std::uint8_t *>(&id_size);
        buffer.insert(buffer.end(), id_size_bytes, id_size_bytes + sizeof(id_size));
        buffer.insert(buffer.end(), _id.begin(), _id.end());

        // Address length and data
        const auto addr_size = static_cast<std::uint32_t>(_address.size());
        const auto addr_size_bytes = reinterpret_cast<const std::uint8_t *>(&addr_size);
        buffer.insert(buffer.end(), addr_size_bytes, addr_size_bytes + sizeof(addr_size));
        buffer.insert(buffer.end(), _address.begin(), _address.end());

        // Port
        const auto port_bytes = reinterpret_cast<const std::uint8_t *>(&_port);
        buffer.insert(buffer.end(), port_bytes, port_bytes + sizeof(_port));

        // State and role
        const auto state = static_cast<std::uint8_t>(_state.load());
        const auto role = static_cast<std::uint8_t>(_role.load());
        buffer.push_back(state);
        buffer.push_back(role);

        // Timestamps
        const auto join_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 _join_time.time_since_epoch())
                                 .count();
        const auto join_bytes = reinterpret_cast<const std::uint8_t *>(&join_ms);
        buffer.insert(buffer.end(), join_bytes, join_bytes + sizeof(join_ms));

        const auto last_seen_ms = _last_seen_ms.load();
        const auto last_seen_bytes = reinterpret_cast<const std::uint8_t *>(&last_seen_ms);
        buffer.insert(buffer.end(), last_seen_bytes, last_seen_bytes + sizeof(last_seen_ms));

        // Statistics
        const auto hb_sent = _heartbeats_sent.load();
        const auto hb_recv = _heartbeats_received.load();
        const auto hb_sent_bytes = reinterpret_cast<const std::uint8_t *>(&hb_sent);
        const auto hb_recv_bytes = reinterpret_cast<const std::uint8_t *>(&hb_recv);
        buffer.insert(buffer.end(), hb_sent_bytes, hb_sent_bytes + sizeof(hb_sent));
        buffer.insert(buffer.end(), hb_recv_bytes, hb_recv_bytes + sizeof(hb_recv));

        // Metadata
        {
            const std::lock_guard<std::mutex> lock(_metadata_mutex);
            const auto metadata_count = static_cast<std::uint32_t>(_metadata.size());
            const auto count_bytes = reinterpret_cast<const std::uint8_t *>(&metadata_count);
            buffer.insert(buffer.end(), count_bytes, count_bytes + sizeof(metadata_count));

            for (const auto &[key, value] : _metadata)
            {
                // Key
                const auto key_size = static_cast<std::uint32_t>(key.size());
                const auto key_size_bytes = reinterpret_cast<const std::uint8_t *>(&key_size);
                buffer.insert(buffer.end(), key_size_bytes, key_size_bytes + sizeof(key_size));
                buffer.insert(buffer.end(), key.begin(), key.end());

                // Value
                const auto value_size = static_cast<std::uint32_t>(value.size());
                const auto value_size_bytes = reinterpret_cast<const std::uint8_t *>(&value_size);
                buffer.insert(buffer.end(), value_size_bytes, value_size_bytes + sizeof(value_size));
                buffer.insert(buffer.end(), value.begin(), value.end());
            }
        }

        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status NodeInfo::deserialize(const std::vector<std::uint8_t> &data)
    {
        if (data.empty())
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = 0;

        try
        {
            // ID
            if (offset + sizeof(std::uint32_t) > data.size())
                return Status::INVALID_REQUEST;
            std::uint32_t id_size;
            std::memcpy(&id_size, data.data() + offset, sizeof(id_size));
            offset += sizeof(id_size);

            if (offset + id_size > data.size())
                return Status::INVALID_REQUEST;
            _id = std::string(data.begin() + offset, data.begin() + offset + id_size);
            offset += id_size;

            // Address
            if (offset + sizeof(std::uint32_t) > data.size())
                return Status::INVALID_REQUEST;
            std::uint32_t addr_size;
            std::memcpy(&addr_size, data.data() + offset, sizeof(addr_size));
            offset += sizeof(addr_size);

            if (offset + addr_size > data.size())
                return Status::INVALID_REQUEST;
            _address = std::string(data.begin() + offset, data.begin() + offset + addr_size);
            offset += addr_size;

            // Port
            if (offset + sizeof(_port) > data.size())
                return Status::INVALID_REQUEST;
            std::memcpy(&_port, data.data() + offset, sizeof(_port));
            offset += sizeof(_port);

            // State and role
            if (offset + 2 > data.size())
                return Status::INVALID_REQUEST;
            _state.store(static_cast<NodeState>(data[offset]));
            _role.store(static_cast<NodeRole>(data[offset + 1]));
            offset += 2;

            // Timestamps
            if (offset + sizeof(std::int64_t) * 2 > data.size())
                return Status::INVALID_REQUEST;
            std::int64_t join_ms;
            std::memcpy(&join_ms, data.data() + offset, sizeof(join_ms));
            _join_time = Timestamp(std::chrono::milliseconds(join_ms));
            offset += sizeof(join_ms);

            std::int64_t last_seen_ms;
            std::memcpy(&last_seen_ms, data.data() + offset, sizeof(last_seen_ms));
            _last_seen_ms.store(last_seen_ms);
            offset += sizeof(last_seen_ms);

            // Statistics
            if (offset + sizeof(std::uint64_t) * 2 > data.size())
                return Status::INVALID_REQUEST;
            std::uint64_t hb_sent, hb_recv;
            std::memcpy(&hb_sent, data.data() + offset, sizeof(hb_sent));
            offset += sizeof(hb_sent);
            std::memcpy(&hb_recv, data.data() + offset, sizeof(hb_recv));
            offset += sizeof(hb_recv);

            _heartbeats_sent.store(hb_sent);
            _heartbeats_received.store(hb_recv);

            // Metadata
            if (offset + sizeof(std::uint32_t) > data.size())
                return Status::INVALID_REQUEST;
            std::uint32_t metadata_count;
            std::memcpy(&metadata_count, data.data() + offset, sizeof(metadata_count));
            offset += sizeof(metadata_count);

            {
                const std::lock_guard<std::mutex> lock(_metadata_mutex);
                _metadata.clear();

                for (std::uint32_t i = 0; i < metadata_count; ++i)
                {
                    // Key
                    if (offset + sizeof(std::uint32_t) > data.size())
                        return Status::INVALID_REQUEST;
                    std::uint32_t key_size;
                    std::memcpy(&key_size, data.data() + offset, sizeof(key_size));
                    offset += sizeof(key_size);

                    if (offset + key_size > data.size())
                        return Status::INVALID_REQUEST;
                    std::string key(data.begin() + offset, data.begin() + offset + key_size);
                    offset += key_size;

                    // Value
                    if (offset + sizeof(std::uint32_t) > data.size())
                        return Status::INVALID_REQUEST;
                    std::uint32_t value_size;
                    std::memcpy(&value_size, data.data() + offset, sizeof(value_size));
                    offset += sizeof(value_size);

                    if (offset + value_size > data.size())
                        return Status::INVALID_REQUEST;
                    std::string value(data.begin() + offset, data.begin() + offset + value_size);
                    offset += value_size;

                    _metadata[std::move(key)] = std::move(value);
                }
            }

            return Status::OK;
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Failed to deserialize NodeInfo: %s", e.what());
            return Status::INVALID_REQUEST;
        }
    }

    std::int64_t NodeInfo::getCurrentTimeMs() const
    {
        const auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   now.time_since_epoch())
            .count();
    }

    void NodeInfo::setLastSeenMs(std::int64_t timestamp_ms)
    {
        _last_seen_ms.store(timestamp_ms);
    }

    // Utility functions
    std::string nodeStateToString(NodeState state)
    {
        switch (state)
        {
        case NodeState::UNKNOWN:
            return "UNKNOWN";
        case NodeState::JOINING:
            return "JOINING";
        case NodeState::ACTIVE:
            return "ACTIVE";
        case NodeState::LEAVING:
            return "LEAVING";
        case NodeState::FAILED:
            return "FAILED";
        case NodeState::SUSPECT:
            return "SUSPECT";
        default:
            return "INVALID";
        }
    }

    std::string nodeRoleToString(NodeRole role)
    {
        switch (role)
        {
        case NodeRole::FOLLOWER:
            return "FOLLOWER";
        case NodeRole::CANDIDATE:
            return "CANDIDATE";
        case NodeRole::LEADER:
            return "LEADER";
        case NodeRole::OBSERVER:
            return "OBSERVER";
        default:
            return "INVALID";
        }
    }

    NodeState nodeStateFromString(const std::string &state_str)
    {
        if (state_str == "UNKNOWN")
            return NodeState::UNKNOWN;
        if (state_str == "JOINING")
            return NodeState::JOINING;
        if (state_str == "ACTIVE")
            return NodeState::ACTIVE;
        if (state_str == "LEAVING")
            return NodeState::LEAVING;
        if (state_str == "FAILED")
            return NodeState::FAILED;
        if (state_str == "SUSPECT")
            return NodeState::SUSPECT;
        return NodeState::UNKNOWN;
    }

    NodeRole nodeRoleFromString(const std::string &role_str)
    {
        if (role_str == "FOLLOWER")
            return NodeRole::FOLLOWER;
        if (role_str == "CANDIDATE")
            return NodeRole::CANDIDATE;
        if (role_str == "LEADER")
            return NodeRole::LEADER;
        if (role_str == "OBSERVER")
            return NodeRole::OBSERVER;
        return NodeRole::FOLLOWER;
    }

    void NodeRegistry::addNode(const NodeInfo &node)
    {
        const std::unique_lock lock(_registry_mutex);
        _nodes[node.getId()] = std::make_shared<NodeInfo>(node);
        LOG_DEBUG("Added node to registry: %s", node.toString().c_str());
    }

    void NodeRegistry::addNode(NodeInfo &&node)
    {
        const auto node_id = node.getId();
        const std::unique_lock lock(_registry_mutex);
        _nodes[node_id] = std::make_shared<NodeInfo>(std::move(node));
        LOG_DEBUG("Added node to registry: %s", node_id.c_str());
    }

    bool NodeRegistry::removeNode(const NodeId &node_id)
    {
        const std::unique_lock lock(_registry_mutex);
        const auto it = _nodes.find(node_id);
        if (it != _nodes.end())
        {
            LOG_DEBUG("Removed node from registry: %s", it->second->toString().c_str());
            _nodes.erase(it);
            return true;
        }
        return false;
    }

    void NodeRegistry::updateNode(const NodeInfo &node)
    {
        const std::unique_lock lock(_registry_mutex);
        const auto it = _nodes.find(node.getId());
        if (it != _nodes.end())
        {
            *it->second = node; // Update existing node
            LOG_DEBUG("Updated node in registry: %s", node.toString().c_str());
        }
        else
        {
            _nodes[node.getId()] = std::make_shared<NodeInfo>(node);
            LOG_DEBUG("Added new node during update: %s", node.toString().c_str());
        }
    }

    std::shared_ptr<NodeInfo> NodeRegistry::getNode(const NodeId &node_id) const
    {
        const std::shared_lock lock(_registry_mutex);
        const auto it = _nodes.find(node_id);
        return it != _nodes.end() ? it->second : nullptr;
    }

    std::vector<std::shared_ptr<NodeInfo>> NodeRegistry::getAllNodes() const
    {
        const std::shared_lock lock(_registry_mutex);
        std::vector<std::shared_ptr<NodeInfo>> result;
        result.reserve(_nodes.size());

        for (const auto &[id, node] : _nodes)
        {
            result.push_back(node);
        }

        return result;
    }

    std::vector<std::shared_ptr<NodeInfo>> NodeRegistry::getActiveNodes() const
    {
        return getNodesWithPredicate([](const NodeInfo &node)
                                     { return node.isActive(); });
    }

    std::vector<std::shared_ptr<NodeInfo>> NodeRegistry::getNodesByState(NodeState state) const
    {
        return getNodesWithPredicate([state](const NodeInfo &node)
                                     { return node.getState() == state; });
    }

    std::vector<std::shared_ptr<NodeInfo>> NodeRegistry::getNodesByRole(NodeRole role) const
    {
        return getNodesWithPredicate([role](const NodeInfo &node)
                                     { return node.getRole() == role; });
    }

    std::size_t NodeRegistry::size() const
    {
        const std::shared_lock lock(_registry_mutex);
        return _nodes.size();
    }

    bool NodeRegistry::empty() const
    {
        const std::shared_lock lock(_registry_mutex);
        return _nodes.empty();
    }

    bool NodeRegistry::contains(const NodeId &node_id) const
    {
        const std::shared_lock lock(_registry_mutex);
        return _nodes.find(node_id) != _nodes.end();
    }

    void NodeRegistry::clear()
    {
        const std::unique_lock lock(_registry_mutex);
        LOG_DEBUG("Clearing node registry (%zu nodes)", _nodes.size());
        _nodes.clear();
    }

    void NodeRegistry::updateLastSeen(const NodeId &node_id)
    {
        const std::shared_lock lock(_registry_mutex);
        const auto it = _nodes.find(node_id);
        if (it != _nodes.end())
        {
            it->second->updateLastSeen();
        }
    }

    void NodeRegistry::updateLastSeen(const NodeId &node_id, Timestamp timestamp)
    {
        const std::shared_lock lock(_registry_mutex);
        const auto it = _nodes.find(node_id);
        if (it != _nodes.end())
        {
            it->second->updateLastSeen(timestamp);
        }
    }

    void NodeRegistry::markNodesFailed(std::chrono::milliseconds timeout)
    {
        const std::shared_lock lock(_registry_mutex);
        std::size_t failed_count = 0;

        for (const auto &[id, node] : _nodes)
        {
            if (node->getState() == NodeState::ACTIVE && !node->isHealthy(timeout))
            {
                node->setState(NodeState::FAILED);
                ++failed_count;
                LOG_WARN("Marked node as failed due to timeout: %s", node->toString().c_str());
            }
        }

        if (failed_count > 0)
        {
            LOG_INFO("Marked %zu nodes as failed due to timeout", failed_count);
        }
    }

    void NodeRegistry::removeFailedNodes()
    {
        const std::unique_lock lock(_registry_mutex);
        std::size_t removed_count = 0;

        auto it = _nodes.begin();
        while (it != _nodes.end())
        {
            if (it->second->getState() == NodeState::FAILED)
            {
                LOG_INFO("Removing failed node: %s", it->second->toString().c_str());
                it = _nodes.erase(it);
                ++removed_count;
            }
            else
            {
                ++it;
            }
        }

        if (removed_count > 0)
        {
            LOG_INFO("Removed %zu failed nodes from registry", removed_count);
        }
    }

    std::size_t NodeRegistry::getActiveNodeCount() const
    {
        const std::shared_lock lock(_registry_mutex);
        return std::count_if(_nodes.begin(), _nodes.end(),
                             [](const auto &pair)
                             { return pair.second->isActive(); });
    }

    std::size_t NodeRegistry::getFailedNodeCount() const
    {
        const std::shared_lock lock(_registry_mutex);
        return std::count_if(_nodes.begin(), _nodes.end(),
                             [](const auto &pair)
                             { return pair.second->getState() == NodeState::FAILED; });
    }

    std::unordered_map<NodeState, std::size_t> NodeRegistry::getNodeStateCounts() const
    {
        const std::shared_lock lock(_registry_mutex);
        std::unordered_map<NodeState, std::size_t> counts;

        for (const auto &[id, node] : _nodes)
        {
            ++counts[node->getState()];
        }

        return counts;
    }

    std::unordered_map<NodeRole, std::size_t> NodeRegistry::getNodeRoleCounts() const
    {
        const std::shared_lock lock(_registry_mutex);
        std::unordered_map<NodeRole, std::size_t> counts;

        for (const auto &[id, node] : _nodes)
        {
            ++counts[node->getRole()];
        }

        return counts;
    }

    void NodeRegistry::withReadLock(std::function<void(const std::unordered_map<NodeId, std::shared_ptr<NodeInfo>> &)> func) const
    {
        const std::shared_lock lock(_registry_mutex);
        func(_nodes);
    }

    void NodeRegistry::withWriteLock(std::function<void(std::unordered_map<NodeId, std::shared_ptr<NodeInfo>> &)> func)
    {
        const std::unique_lock lock(_registry_mutex);
        func(_nodes);
    }

    std::vector<std::shared_ptr<NodeInfo>> NodeRegistry::getNodesWithPredicate(
        std::function<bool(const NodeInfo &)> predicate) const
    {

        const std::shared_lock lock(_registry_mutex);
        std::vector<std::shared_ptr<NodeInfo>> result;

        for (const auto &[id, node] : _nodes)
        {
            if (predicate(*node))
            {
                result.push_back(node);
            }
        }

        return result;
    }

} // namespace distributed_db