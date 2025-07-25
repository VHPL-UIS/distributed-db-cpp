#ifndef __CLUSTER_MEMBERSHIP_HPP__
#define __CLUSTER_MEMBERSHIP_HPP__

#include "node_info.hpp"
#include "../network/tcp_client.hpp"
#include "../network/message.hpp"
#include <memory>
#include <vector>
#include <functional>
#include <atomic>
#include <thread>
#include <condition_variable>

namespace distributed_db
{
    enum class MembershipEvent
    {
        NODE_JOINED,
        NODE_LEFT,
        NODE_FAILED,
        NODE_RECOVERED,
        ROLE_CHANGED,
        MEMBERSHIP_UPDATED
    };

    struct MembershipEventData
    {
        MembershipEvent event;
        NodeId node_id;
        std::shared_ptr<NodeInfo> node_info;
        std::string details;
        Timestamp timestamp;

        MembershipEventData(MembershipEvent evt, NodeId id, std::shared_ptr<NodeInfo> info = nullptr)
            : event(evt), node_id(std::move(id)), node_info(std::move(info)),
              timestamp(std::chrono::system_clock::now()) {}
    };

    using MembershipEventCallback = std::function<void(const MembershipEventData &)>;

    class JoinRequestMessage : public Message
    {
    public:
        JoinRequestMessage() : Message(MessageType::NODE_JOIN) {}
        explicit JoinRequestMessage(std::shared_ptr<NodeInfo> node_info)
            : Message(MessageType::NODE_JOIN), _node_info(std::move(node_info)) {}

        [[nodiscard]] const std::shared_ptr<NodeInfo> &getNodeInfo() const { return _node_info; }
        void setNodeInfo(std::shared_ptr<NodeInfo> node_info) { _node_info = std::move(node_info); }

        [[nodiscard]] Result<std::vector<std::uint8_t>> serialize() const override;
        [[nodiscard]] Status deserialize(const std::vector<std::uint8_t> &data) override;

    private:
        std::shared_ptr<NodeInfo> _node_info;
    };

    class JoinResponseMessage : public Message
    {
    public:
        JoinResponseMessage() : Message(MessageType::NODE_JOIN) {}
        JoinResponseMessage(std::uint32_t request_id, Status status,
                            std::vector<std::shared_ptr<NodeInfo>> cluster_members = {})
            : Message(MessageType::NODE_JOIN, request_id), _status(status),
              _cluster_members(std::move(cluster_members)) {}

        [[nodiscard]] Status getStatus() const { return _status; }
        [[nodiscard]] const std::vector<std::shared_ptr<NodeInfo>> &getClusterMembers() const { return _cluster_members; }

        void setStatus(Status status) { _status = status; }
        void setClusterMembers(std::vector<std::shared_ptr<NodeInfo>> members) { _cluster_members = std::move(members); }

        [[nodiscard]] Result<std::vector<std::uint8_t>> serialize() const override;
        [[nodiscard]] Status deserialize(const std::vector<std::uint8_t> &data) override;

    private:
        Status _status = Status::OK;
        std::vector<std::shared_ptr<NodeInfo>> _cluster_members;
    };

    class ClusterMembership
    {
    public:
        explicit ClusterMembership(std::shared_ptr<NodeInfo> local_node);
        ~ClusterMembership();

        ClusterMembership(const ClusterMembership &) = delete;
        ClusterMembership &operator=(const ClusterMembership &) = delete;
        ClusterMembership(ClusterMembership &&) = default;
        ClusterMembership &operator=(ClusterMembership &&) = default;

        [[nodiscard]] Status start();
        void stop();
        [[nodiscard]] bool isRunning() const { return _running; }

        [[nodiscard]] Status joinCluster(const std::vector<std::string> &seed_nodes);
        [[nodiscard]] Status leaveCluster();
        [[nodiscard]] Status rejoinCluster();

        [[nodiscard]] Status addNode(const NodeInfo &node);
        [[nodiscard]] Status removeNode(const NodeId &node_id);
        [[nodiscard]] Status updateNodeRole(const NodeId &node_id, NodeRole role);
        [[nodiscard]] Status markNodeFailed(const NodeId &node_id);

        [[nodiscard]] std::shared_ptr<NodeInfo> getLocalNode() const { return _local_node; }
        [[nodiscard]] std::shared_ptr<NodeInfo> getNode(const NodeId &node_id) const;
        [[nodiscard]] std::vector<std::shared_ptr<NodeInfo>> getAllNodes() const;
        [[nodiscard]] std::vector<std::shared_ptr<NodeInfo>> getActiveNodes() const;
        [[nodiscard]] std::vector<std::shared_ptr<NodeInfo>> getVotingNodes() const;
        [[nodiscard]] std::shared_ptr<NodeInfo> getLeaderNode() const;

        [[nodiscard]] std::size_t getClusterSize() const;
        [[nodiscard]] std::size_t getActiveNodeCount() const;
        [[nodiscard]] bool isClusterHealthy() const;
        [[nodiscard]] bool hasQuorum() const;

        void setMembershipEventCallback(MembershipEventCallback callback);
        void clearMembershipEventCallback();

        [[nodiscard]] std::unique_ptr<Message> handleJoinRequest(const JoinRequestMessage &request);
        [[nodiscard]] std::unique_ptr<Message> handleLeaveRequest(const NodeId &node_id);

        void setJoinTimeout(std::chrono::milliseconds timeout) { _join_timeout = timeout; }
        void setMaxRetries(int retries) { _max_retries = retries; }
        void setMinClusterSize(std::size_t min_size) { _min_cluster_size = min_size; }

    private:
        std::shared_ptr<NodeInfo> _local_node;
        std::unique_ptr<NodeRegistry> _node_registry;
        std::atomic<bool> _running{false};
        std::atomic<bool> _should_stop{false};

        std::vector<std::unique_ptr<TcpClient>> _seed_connections;
        mutable std::mutex _connections_mutex;

        MembershipEventCallback _event_callback;
        mutable std::mutex _callback_mutex;

        std::chrono::milliseconds _join_timeout{std::chrono::milliseconds(10000)};
        int _max_retries{3};
        std::size_t _min_cluster_size{1};

        std::unique_ptr<std::thread> _membership_thread;
        std::condition_variable _stop_condition;
        std::mutex _stop_mutex;

        void membershipLoop();
        [[nodiscard]] Status connectToSeedNode(const std::string &seed_endpoint);
        [[nodiscard]] Status sendJoinRequest(TcpClient &client);
        [[nodiscard]] Status processJoinResponse(const JoinResponseMessage &response);
        void updateClusterMembership(const std::vector<std::shared_ptr<NodeInfo>> &members);
        void notifyMembershipEvent(const MembershipEventData &event);

        [[nodiscard]] bool validateNodeForJoin(const NodeInfo &node) const;
        [[nodiscard]] bool isNodeIdUnique(const NodeId &node_id) const;
        [[nodiscard]] bool isEndpointUnique(const std::string &endpoint) const;

        [[nodiscard]] std::vector<std::string> parseEndpoints(const std::vector<std::string> &seed_nodes) const;
        [[nodiscard]] std::string getLocalEndpoint() const;
        void cleanupConnections();
    };

    struct MembershipStats
    {
        std::size_t total_nodes;
        std::size_t active_nodes;
        std::size_t failed_nodes;
        std::size_t joining_nodes;
        std::size_t leaving_nodes;
        std::unordered_map<NodeRole, std::size_t> role_counts;
        Timestamp last_membership_change;
        std::chrono::milliseconds cluster_uptime;

        [[nodiscard]] std::string toString() const;
    };

    class MembershipUtils
    {
    public:
        [[nodiscard]] static Result<std::pair<std::string, Port>> parseEndpoint(const std::string &endpoint);
        [[nodiscard]] static bool isValidEndpoint(const std::string &endpoint);
        [[nodiscard]] static std::string formatEndpoint(const std::string &host, Port port);

        [[nodiscard]] static bool hasQuorum(std::size_t active_nodes, std::size_t total_nodes);
        [[nodiscard]] static std::size_t calculateQuorumSize(std::size_t total_nodes);
        [[nodiscard]] static bool isClusterHealthy(const std::vector<std::shared_ptr<NodeInfo>> &nodes);

        [[nodiscard]] static std::vector<std::shared_ptr<NodeInfo>> selectHealthyNodes(
            const std::vector<std::shared_ptr<NodeInfo>> &nodes,
            std::chrono::milliseconds health_timeout);

        [[nodiscard]] static std::shared_ptr<NodeInfo> findLeaderNode(
            const std::vector<std::shared_ptr<NodeInfo>> &nodes);

        [[nodiscard]] static MembershipStats calculateStats(
            const std::vector<std::shared_ptr<NodeInfo>> &nodes,
            Timestamp cluster_start_time);
    };

} // namespace distributed_db

#endif // __CLUSTER_MEMBERSHIP_HPP__