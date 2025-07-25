#ifndef __NODE_INFO_HPP__
#define __NODE_INFO_HPP__

#include "../common/types.hpp"
#include <string>
#include <chrono>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <vector>
#include <memory>
#include <shared_mutex>
#include <functional>

namespace distributed_db
{
    enum class NodeState : std::uint8_t
    {
        UNKNOWN = 0,
        JOINING = 1,
        ACTIVE = 2,
        LEAVING = 3,
        FAILED = 4,
        SUSPECT = 5
    };

    enum class NodeRole : std::uint8_t
    {
        FOLLOWER = 0,
        CANDIDATE = 1,
        LEADER = 2,
        OBSERVER = 3
    };

    class NodeInfo
    {
    public:
        NodeInfo() = default;
        NodeInfo(NodeId id, std::string address, Port port);
        NodeInfo(NodeId id, std::string address, Port port, NodeRole role);

        NodeInfo(const NodeInfo &other);
        NodeInfo &operator=(const NodeInfo &other);
        NodeInfo(NodeInfo &&other) noexcept;
        NodeInfo &operator=(NodeInfo &&other) noexcept;

        ~NodeInfo() = default;

        [[nodiscard]] const NodeId &getId() const noexcept { return _id; }
        [[nodiscard]] const std::string &getAddress() const noexcept { return _address; }
        [[nodiscard]] Port getPort() const noexcept { return _port; }
        [[nodiscard]] std::string getEndpoint() const;

        [[nodiscard]] NodeState getState() const;
        void setState(NodeState state);
        [[nodiscard]] NodeRole getRole() const;
        void setRole(NodeRole role);

        [[nodiscard]] Timestamp getJoinTime() const;
        [[nodiscard]] Timestamp getLastSeen() const;
        void updateLastSeen();
        void updateLastSeen(Timestamp timestamp);

        [[nodiscard]] std::chrono::milliseconds getLastSeenAge() const;
        [[nodiscard]] bool isHealthy(std::chrono::milliseconds timeout) const;
        [[nodiscard]] bool isActive() const noexcept;
        [[nodiscard]] bool canVote() const noexcept;

        void setMetadata(const std::string &key, const std::string &value);
        [[nodiscard]] std::string getMetadata(const std::string &key) const;
        [[nodiscard]] bool hasMetadata(const std::string &key) const;
        void removeMetadata(const std::string &key);
        [[nodiscard]] const std::unordered_map<std::string, std::string> &getAllMetadata() const;

        void incrementHeartbeatsSent();
        void incrementHeartbeatsReceived();
        void recordLatency(std::chrono::milliseconds latency);
        [[nodiscard]] std::uint64_t getHeartbeatsSent() const;
        [[nodiscard]] std::uint64_t getHeartbeatsReceived() const;
        [[nodiscard]] std::chrono::milliseconds getAverageLatency() const;

        [[nodiscard]] bool operator==(const NodeInfo &other) const noexcept;
        [[nodiscard]] bool operator!=(const NodeInfo &other) const noexcept;
        [[nodiscard]] std::string toString() const;

        [[nodiscard]] Result<std::vector<std::uint8_t>> serialize() const;
        [[nodiscard]] Status deserialize(const std::vector<std::uint8_t> &data);

    private:
        NodeId _id;
        std::string _address;
        Port _port;

        std::atomic<NodeState> _state{NodeState::UNKNOWN};
        std::atomic<NodeRole> _role{NodeRole::FOLLOWER};

        Timestamp _join_time;
        std::atomic<std::int64_t> _last_seen_ms{0};

        mutable std::mutex _metadata_mutex;
        std::unordered_map<std::string, std::string> _metadata;

        std::atomic<std::uint64_t> _heartbeats_sent{0};
        std::atomic<std::uint64_t> _heartbeats_received{0};
        std::atomic<std::int64_t> _total_latency_ms{0};
        std::atomic<std::uint64_t> _latency_samples{0};

        [[nodiscard]] std::int64_t getCurrentTimeMs() const;
        void setLastSeenMs(std::int64_t timestamp_ms);
    };

    [[nodiscard]] std::string nodeStateToString(NodeState state);
    [[nodiscard]] std::string nodeRoleToString(NodeRole role);
    [[nodiscard]] NodeState nodeStateFromString(const std::string &state_str);
    [[nodiscard]] NodeRole nodeRoleFromString(const std::string &role_str);

    class NodeRegistry
    {
    public:
        NodeRegistry() = default;
        ~NodeRegistry() = default;

        NodeRegistry(const NodeRegistry &) = delete;
        NodeRegistry &operator=(const NodeRegistry &) = delete;
        NodeRegistry(NodeRegistry &&) = default;
        NodeRegistry &operator=(NodeRegistry &&) = default;

        void addNode(const NodeInfo &node);
        void addNode(NodeInfo &&node);
        bool removeNode(const NodeId &node_id);
        void updateNode(const NodeInfo &node);

        [[nodiscard]] std::shared_ptr<NodeInfo> getNode(const NodeId &node_id) const;
        [[nodiscard]] std::vector<std::shared_ptr<NodeInfo>> getAllNodes() const;
        [[nodiscard]] std::vector<std::shared_ptr<NodeInfo>> getActiveNodes() const;
        [[nodiscard]] std::vector<std::shared_ptr<NodeInfo>> getNodesByState(NodeState state) const;
        [[nodiscard]] std::vector<std::shared_ptr<NodeInfo>> getNodesByRole(NodeRole role) const;

        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] bool empty() const;
        [[nodiscard]] bool contains(const NodeId &node_id) const;
        void clear();

        void updateLastSeen(const NodeId &node_id);
        void updateLastSeen(const NodeId &node_id, Timestamp timestamp);
        void markNodesFailed(std::chrono::milliseconds timeout);
        void removeFailedNodes();

        [[nodiscard]] std::size_t getActiveNodeCount() const;
        [[nodiscard]] std::size_t getFailedNodeCount() const;
        [[nodiscard]] std::unordered_map<NodeState, std::size_t> getNodeStateCounts() const;
        [[nodiscard]] std::unordered_map<NodeRole, std::size_t> getNodeRoleCounts() const;

        void withReadLock(std::function<void(const std::unordered_map<NodeId, std::shared_ptr<NodeInfo>> &)> func) const;
        void withWriteLock(std::function<void(std::unordered_map<NodeId, std::shared_ptr<NodeInfo>> &)> func);

    private:
        mutable std::shared_mutex _registry_mutex;
        std::unordered_map<NodeId, std::shared_ptr<NodeInfo>> _nodes;

        [[nodiscard]] std::vector<std::shared_ptr<NodeInfo>> getNodesWithPredicate(
            std::function<bool(const NodeInfo &)> predicate) const;
    };

} // namespace distributed_db

#endif // __NODE_INFO_HPP__