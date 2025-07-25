#ifndef __HEARTBEAT_MANAGER_HPP__
#define __HEARTBEAT_MANAGER_HPP__

#include "node_info.hpp"
#include "../network/tcp_client.hpp"
#include "../network/message.hpp"
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <functional>

namespace distributed_db
{
    enum class HeartbeatEvent
    {
        HEARTBEAT_SENT,
        HEARTBEAT_RECEIVED,
        HEARTBEAT_TIMEOUT,
        NODE_SUSPECTED,
        NODE_RECOVERED,
        LATENCY_UPDATED
    };

    struct HeartbeatEventData
    {
        HeartbeatEvent event;
        NodeId node_id;
        std::chrono::milliseconds latency{0};
        std::string details;
        Timestamp timestamp;

        HeartbeatEventData(HeartbeatEvent evt, NodeId id)
            : event(evt), node_id(std::move(id)), timestamp(std::chrono::system_clock::now()) {}

        HeartbeatEventData(HeartbeatEvent evt, NodeId id, std::chrono::milliseconds lat)
            : event(evt), node_id(std::move(id)), latency(lat), timestamp(std::chrono::system_clock::now()) {}
    };

    // Callback type for heartbeat events
    using HeartbeatEventCallback = std::function<void(const HeartbeatEventData &)>;

    // Heartbeat statistics for a node
    struct NodeHeartbeatStats
    {
        NodeId node_id;
        std::uint64_t heartbeats_sent{0};
        std::uint64_t heartbeats_received{0};
        std::uint64_t consecutive_failures{0};
        std::chrono::milliseconds average_latency{0};
        std::chrono::milliseconds min_latency{std::chrono::milliseconds::max()};
        std::chrono::milliseconds max_latency{0};
        Timestamp last_heartbeat_sent;
        Timestamp last_heartbeat_received;
        std::atomic<bool> is_suspected{false};

        [[nodiscard]] double getSuccessRate() const
        {
            return heartbeats_sent > 0 ? static_cast<double>(heartbeats_received) / heartbeats_sent : 0.0;
        }

        [[nodiscard]] std::string toString() const;
    };

    // Heartbeat manager for cluster health monitoring
    class HeartbeatManager
    {
    public:
        explicit HeartbeatManager(std::shared_ptr<NodeInfo> local_node);
        ~HeartbeatManager();

        HeartbeatManager(const HeartbeatManager &) = delete;
        HeartbeatManager &operator=(const HeartbeatManager &) = delete;
        HeartbeatManager(HeartbeatManager &&) = default;
        HeartbeatManager &operator=(HeartbeatManager &&) = default;

        [[nodiscard]] Status start();
        void stop();
        [[nodiscard]] bool isRunning() const { return _running; }

        // Node management
        void addNode(std::shared_ptr<NodeInfo> node);
        void removeNode(const NodeId &node_id);
        void updateNodeList(const std::vector<std::shared_ptr<NodeInfo>> &nodes);

        // Heartbeat operations
        [[nodiscard]] Status sendHeartbeat(const NodeId &node_id);
        [[nodiscard]] Status sendHeartbeatToAll();
        void processHeartbeatResponse(const HeartbeatResponseMessage &response);

        // Statistics and monitoring
        [[nodiscard]] std::shared_ptr<NodeHeartbeatStats> getNodeStats(const NodeId &node_id) const;
        [[nodiscard]] std::vector<std::shared_ptr<NodeHeartbeatStats>> getAllNodeStats() const;
        [[nodiscard]] std::vector<NodeId> getSuspectedNodes() const;
        [[nodiscard]] std::vector<NodeId> getHealthyNodes() const;

        // Configuration
        void setHeartbeatInterval(std::chrono::milliseconds interval) { _heartbeat_interval = interval; }
        void setHeartbeatTimeout(std::chrono::milliseconds timeout) { _heartbeat_timeout = timeout; }
        void setMaxConsecutiveFailures(std::uint64_t max_failures) { _max_consecutive_failures = max_failures; }
        void setSuspicionThreshold(std::uint64_t threshold) { _suspicion_threshold = threshold; }

        [[nodiscard]] std::chrono::milliseconds getHeartbeatInterval() const { return _heartbeat_interval; }
        [[nodiscard]] std::chrono::milliseconds getHeartbeatTimeout() const { return _heartbeat_timeout; }

        // Event handling
        void setHeartbeatEventCallback(HeartbeatEventCallback callback);
        void clearHeartbeatEventCallback();

        // Network message creation (for integration with message handlers)
        [[nodiscard]] std::unique_ptr<Message> handleHeartbeatRequest(const HeartbeatMessage &request);

    private:
        std::shared_ptr<NodeInfo> _local_node;
        std::atomic<bool> _running{false};
        std::atomic<bool> _should_stop{false};

        // Node tracking
        mutable std::shared_mutex _nodes_mutex;
        std::unordered_map<NodeId, std::shared_ptr<NodeInfo>> _monitored_nodes;
        std::unordered_map<NodeId, std::unique_ptr<TcpClient>> _node_connections;
        std::unordered_map<NodeId, std::shared_ptr<NodeHeartbeatStats>> _node_stats;

        // Threading
        std::unique_ptr<std::thread> _heartbeat_thread;
        std::condition_variable _heartbeat_condition;
        std::mutex _heartbeat_mutex;

        // Event handling
        HeartbeatEventCallback _event_callback;
        mutable std::mutex _callback_mutex;

        // Configuration
        std::chrono::milliseconds _heartbeat_interval{std::chrono::milliseconds(1000)}; // 1 second
        std::chrono::milliseconds _heartbeat_timeout{std::chrono::milliseconds(5000)};  // 5 seconds
        std::uint64_t _max_consecutive_failures{3};
        std::uint64_t _suspicion_threshold{2};

        // Internal methods
        void heartbeatLoop();
        void sendHeartbeatsToNodes();
        void checkNodeTimeouts();
        void updateNodeStatistics();
        void cleanupConnections();

        // Connection management
        [[nodiscard]] TcpClient *getOrCreateConnection(const NodeId &node_id, const std::string &endpoint);
        void removeConnection(const NodeId &node_id);

        // Statistics management
        [[nodiscard]] std::shared_ptr<NodeHeartbeatStats> getOrCreateStats(const NodeId &node_id);
        void updateLatencyStats(const NodeId &node_id, std::chrono::milliseconds latency);
        void markHeartbeatSent(const NodeId &node_id);
        void markHeartbeatReceived(const NodeId &node_id, std::chrono::milliseconds latency);
        void markHeartbeatTimeout(const NodeId &node_id);

        // Failure detection
        void checkNodeSuspicion(const NodeId &node_id, std::shared_ptr<NodeHeartbeatStats> stats);
        void suspectNode(const NodeId &node_id);
        void recoverNode(const NodeId &node_id);

        // Event notification
        void notifyHeartbeatEvent(const HeartbeatEventData &event);

        [[nodiscard]] std::chrono::milliseconds calculateLatency(Timestamp start_time) const;
        [[nodiscard]] bool shouldSuspectNode(const std::shared_ptr<NodeHeartbeatStats> &stats) const;
    };

    class HeartbeatUtils
    {
    public:
        // Failure detection algorithms
        [[nodiscard]] static bool isNodeSuspected(const NodeHeartbeatStats &stats,
                                                  std::uint64_t failure_threshold);

        [[nodiscard]] static std::chrono::milliseconds calculateTimeout(
            std::chrono::milliseconds base_timeout,
            std::chrono::milliseconds average_latency,
            double safety_factor = 2.0);

        // Statistics calculations
        [[nodiscard]] static double calculateNetworkHealth(
            const std::vector<std::shared_ptr<NodeHeartbeatStats>> &stats);

        [[nodiscard]] static std::chrono::milliseconds calculateClusterLatency(
            const std::vector<std::shared_ptr<NodeHeartbeatStats>> &stats);

        // Adaptive timing
        [[nodiscard]] static std::chrono::milliseconds adaptiveHeartbeatInterval(
            std::size_t cluster_size,
            std::chrono::milliseconds base_interval);

        // Node health assessment
        [[nodiscard]] static bool isNodeHealthy(const NodeHeartbeatStats &stats,
                                                std::chrono::milliseconds max_age);

        [[nodiscard]] static std::vector<NodeId> selectHealthiestNodes(
            const std::vector<std::shared_ptr<NodeHeartbeatStats>> &stats,
            std::size_t count);
    };

    // Heartbeat configuration
    struct HeartbeatConfig
    {
        std::chrono::milliseconds interval{1000};  // Heartbeat interval
        std::chrono::milliseconds timeout{5000};   // Response timeout
        std::uint64_t max_consecutive_failures{3}; // Max failures before suspicion
        std::uint64_t suspicion_threshold{2};      // Failures needed for suspicion
        double adaptive_factor{1.5};               // Adaptive timeout factor
        bool enable_adaptive_timing{true};         // Enable adaptive intervals
        std::size_t max_concurrent_heartbeats{10}; // Max parallel heartbeats

        [[nodiscard]] bool isValid() const
        {
            return interval > std::chrono::milliseconds(0) &&
                   timeout > interval &&
                   max_consecutive_failures > 0 &&
                   suspicion_threshold > 0 &&
                   adaptive_factor > 1.0 &&
                   max_concurrent_heartbeats > 0;
        }

        [[nodiscard]] std::string toString() const
        {
            std::ostringstream oss;
            oss << "HeartbeatConfig{interval=" << interval.count() << "ms"
                << ", timeout=" << timeout.count() << "ms"
                << ", max_failures=" << max_consecutive_failures
                << ", suspicion_threshold=" << suspicion_threshold
                << ", adaptive_factor=" << adaptive_factor
                << ", adaptive_timing=" << (enable_adaptive_timing ? "enabled" : "disabled")
                << ", max_concurrent=" << max_concurrent_heartbeats << "}";
            return oss.str();
        }
    };

} // namespace distributed_db

#endif // __HEARTBEAT_MANAGER_HPP__