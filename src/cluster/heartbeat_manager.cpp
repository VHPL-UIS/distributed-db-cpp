#include "heartbeat_manager.hpp"
#include "../common/logger.hpp"
#include <algorithm>
#include <sstream>

namespace distributed_db
{
    std::string NodeHeartbeatStats::toString() const
    {
        std::ostringstream oss;
        oss << "NodeHeartbeatStats{node=" << node_id
            << ", sent=" << heartbeats_sent
            << ", received=" << heartbeats_received
            << ", failures=" << consecutive_failures
            << ", success_rate=" << (getSuccessRate() * 100.0) << "%"
            << ", avg_latency=" << average_latency.count() << "ms"
            << ", suspected=" << (is_suspected.load() ? "true" : "false") << "}";
        return oss.str();
    }

    HeartbeatManager::HeartbeatManager(std::shared_ptr<NodeInfo> local_node)
        : _local_node(std::move(local_node))
    {

        if (!_local_node)
        {
            throw std::invalid_argument("Local node cannot be null");
        }

        LOG_INFO("Heartbeat manager initialized for node: %s", _local_node->getId().c_str());
    }

    HeartbeatManager::~HeartbeatManager()
    {
        stop();
    }

    Status HeartbeatManager::start()
    {
        if (_running)
        {
            return Status::OK;
        }

        _should_stop = false;
        _running = true;

        // Start heartbeat thread
        _heartbeat_thread = std::make_unique<std::thread>(&HeartbeatManager::heartbeatLoop, this);

        LOG_INFO("Heartbeat manager started with interval: %ldms", _heartbeat_interval.count());
        return Status::OK;
    }

    void HeartbeatManager::stop()
    {
        if (!_running)
        {
            return;
        }

        _should_stop = true;
        _running = false;

        // Notify heartbeat thread
        {
            const std::lock_guard<std::mutex> lock(_heartbeat_mutex);
            _heartbeat_condition.notify_all();
        }

        // Wait for heartbeat thread
        if (_heartbeat_thread && _heartbeat_thread->joinable())
        {
            _heartbeat_thread->join();
        }
        _heartbeat_thread.reset();

        // Cleanup connections
        cleanupConnections();

        LOG_INFO("Heartbeat manager stopped");
    }

    void HeartbeatManager::addNode(std::shared_ptr<NodeInfo> node)
    {
        if (!node || node->getId() == _local_node->getId())
        {
            return; // Skip null or self
        }

        const std::unique_lock lock(_nodes_mutex);
        _monitored_nodes[node->getId()] = node;

        // Initialize statistics for this node
        getOrCreateStats(node->getId());

        LOG_DEBUG("Added node to heartbeat monitoring: %s", node->toString().c_str());
    }

    void HeartbeatManager::removeNode(const NodeId &node_id)
    {
        const std::unique_lock lock(_nodes_mutex);

        _monitored_nodes.erase(node_id);
        _node_stats.erase(node_id);
        removeConnection(node_id);

        LOG_DEBUG("Removed node from heartbeat monitoring: %s", node_id.c_str());
    }

    void HeartbeatManager::updateNodeList(const std::vector<std::shared_ptr<NodeInfo>> &nodes)
    {
        const std::unique_lock lock(_nodes_mutex);

        // Clear existing nodes
        _monitored_nodes.clear();
        cleanupConnections();

        // Add new nodes (excluding self)
        for (const auto &node : nodes)
        {
            if (node && node->getId() != _local_node->getId())
            {
                _monitored_nodes[node->getId()] = node;
                getOrCreateStats(node->getId());
            }
        }

        LOG_INFO("Updated heartbeat monitoring list: %zu nodes", _monitored_nodes.size());
    }

    Status HeartbeatManager::sendHeartbeat(const NodeId &node_id)
    {
        std::shared_ptr<NodeInfo> target_node;

        {
            const std::shared_lock lock(_nodes_mutex);
            const auto it = _monitored_nodes.find(node_id);
            if (it == _monitored_nodes.end())
            {
                return Status::NOT_FOUND;
            }
            target_node = it->second;
        }

        // Get or create connection
        auto *client = getOrCreateConnection(node_id, target_node->getEndpoint());
        if (!client)
        {
            LOG_WARN("Failed to establish connection to node: %s", node_id.c_str());
            markHeartbeatTimeout(node_id);
            return Status::NETWORK_ERROR;
        }

        // Create heartbeat message
        HeartbeatMessage heartbeat(_local_node->getId());
        const auto start_time = std::chrono::steady_clock::now();

        // Send heartbeat
        const auto response_result = client->sendRequest(heartbeat, _heartbeat_timeout);

        markHeartbeatSent(node_id);

        if (!response_result.ok())
        {
            LOG_DEBUG("Heartbeat to %s failed: status=%d", node_id.c_str(),
                      static_cast<int>(response_result.status()));
            markHeartbeatTimeout(node_id);
            return response_result.status();
        }

        // Calculate latency
        const auto latency = calculateLatency(start_time);

        // Process response
        const auto &response = response_result.value();
        if (response->getType() == MessageType::HEARTBEAT_RESPONSE)
        {
            markHeartbeatReceived(node_id, latency);

            const auto &hb_response = static_cast<const HeartbeatResponseMessage &>(*response);
            LOG_DEBUG("Heartbeat response from %s: latency=%ldms",
                      hb_response.getNodeId().c_str(), latency.count());

            return Status::OK;
        }
        else
        {
            LOG_WARN("Unexpected response type for heartbeat from %s", node_id.c_str());
            markHeartbeatTimeout(node_id);
            return Status::INTERNAL_ERROR;
        }
    }

    Status HeartbeatManager::sendHeartbeatToAll()
    {
        std::vector<NodeId> node_ids;

        {
            const std::shared_lock lock(_nodes_mutex);
            node_ids.reserve(_monitored_nodes.size());
            for (const auto &[id, node] : _monitored_nodes)
            {
                if (node->isActive())
                {
                    node_ids.push_back(id);
                }
            }
        }

        if (node_ids.empty())
        {
            return Status::OK;
        }

        LOG_DEBUG("Sending heartbeats to %zu nodes", node_ids.size());

        std::size_t successful = 0;
        for (const auto &node_id : node_ids)
        {
            if (sendHeartbeat(node_id) == Status::OK)
            {
                ++successful;
            }
        }

        LOG_DEBUG("Heartbeat round completed: %zu/%zu successful", successful, node_ids.size());
        return Status::OK;
    }

    void HeartbeatManager::processHeartbeatResponse(const HeartbeatResponseMessage &response)
    {
        const auto &responding_node_id = response.getNodeId();

        // Find the corresponding request (in a real implementation, we'd track message IDs)
        const std::shared_lock lock(_nodes_mutex);
        const auto it = _monitored_nodes.find(responding_node_id);
        if (it != _monitored_nodes.end())
        {
            // Update last seen time
            it->second->updateLastSeen();

            // If node was suspected, recover it
            const auto stats = getOrCreateStats(responding_node_id);
            if (stats->is_suspected.load())
            {
                recoverNode(responding_node_id);
            }
        }
    }

    std::shared_ptr<NodeHeartbeatStats> HeartbeatManager::getNodeStats(const NodeId &node_id) const
    {
        const std::shared_lock lock(_nodes_mutex);
        const auto it = _node_stats.find(node_id);
        return it != _node_stats.end() ? it->second : nullptr;
    }

    std::vector<std::shared_ptr<NodeHeartbeatStats>> HeartbeatManager::getAllNodeStats() const
    {
        const std::shared_lock lock(_nodes_mutex);

        std::vector<std::shared_ptr<NodeHeartbeatStats>> result;
        result.reserve(_node_stats.size());

        for (const auto &[id, stats] : _node_stats)
        {
            result.push_back(stats);
        }

        return result;
    }

    std::vector<NodeId> HeartbeatManager::getSuspectedNodes() const
    {
        const std::shared_lock lock(_nodes_mutex);

        std::vector<NodeId> suspected;
        for (const auto &[id, stats] : _node_stats)
        {
            if (stats->is_suspected.load())
            {
                suspected.push_back(id);
            }
        }

        return suspected;
    }

    std::vector<NodeId> HeartbeatManager::getHealthyNodes() const
    {
        const std::shared_lock lock(_nodes_mutex);

        std::vector<NodeId> healthy;
        for (const auto &[id, stats] : _node_stats)
        {
            if (!stats->is_suspected.load() && stats->consecutive_failures < _max_consecutive_failures)
            {
                healthy.push_back(id);
            }
        }

        return healthy;
    }

    void HeartbeatManager::setHeartbeatEventCallback(HeartbeatEventCallback callback)
    {
        const std::lock_guard<std::mutex> lock(_callback_mutex);
        _event_callback = std::move(callback);
    }

    void HeartbeatManager::clearHeartbeatEventCallback()
    {
        const std::lock_guard<std::mutex> lock(_callback_mutex);
        _event_callback = nullptr;
    }

    std::unique_ptr<Message> HeartbeatManager::handleHeartbeatRequest(const HeartbeatMessage &request)
    {
        const auto &requesting_node_id = request.getNodeId();

        LOG_DEBUG("Received heartbeat from node: %s", requesting_node_id.c_str());

        // Update node's last seen time if we know about it
        {
            const std::shared_lock lock(_nodes_mutex);
            const auto it = _monitored_nodes.find(requesting_node_id);
            if (it != _monitored_nodes.end())
            {
                it->second->updateLastSeen();
            }
        }

        // Respond with our node ID
        return std::make_unique<HeartbeatResponseMessage>(request.getMessageId(), _local_node->getId());
    }

    void HeartbeatManager::heartbeatLoop()
    {
        LOG_DEBUG("Heartbeat loop started");

        while (!_should_stop)
        {
            try
            {
                // Send heartbeats to all monitored nodes
                sendHeartbeatsToNodes();

                // Check for timeouts and update statistics
                checkNodeTimeouts();
                updateNodeStatistics();

                // Wait for next heartbeat interval or stop signal
                std::unique_lock<std::mutex> lock(_heartbeat_mutex);
                _heartbeat_condition.wait_for(lock, _heartbeat_interval);
            }
            catch (const std::exception &e)
            {
                LOG_ERROR("Exception in heartbeat loop: %s", e.what());
            }
        }

        LOG_DEBUG("Heartbeat loop finished");
    }

    void HeartbeatManager::sendHeartbeatsToNodes()
    {
        // Send heartbeats in parallel for better performance
        std::vector<std::future<void>> heartbeat_futures;

        {
            const std::shared_lock lock(_nodes_mutex);

            for (const auto &[node_id, node] : _monitored_nodes)
            {
                if (!node->isActive())
                {
                    continue; // Skip inactive nodes
                }

                // Launch async heartbeat
                auto future = std::async(std::launch::async, [this, node_id]()
                                         { sendHeartbeat(node_id); });

                heartbeat_futures.push_back(std::move(future));
            }
        }

        // Wait for all heartbeats to complete (with timeout)
        for (auto &future : heartbeat_futures)
        {
            try
            {
                const auto status = future.wait_for(_heartbeat_timeout);
                if (status == std::future_status::timeout)
                {
                    LOG_WARN("Heartbeat operation timed out");
                }
            }
            catch (const std::exception &e)
            {
                LOG_WARN("Exception waiting for heartbeat completion: %s", e.what());
            }
        }
    }

    void HeartbeatManager::checkNodeTimeouts()
    {
        const std::shared_lock lock(_nodes_mutex);

        for (const auto &[node_id, node] : _monitored_nodes)
        {
            const auto stats = getOrCreateStats(node_id);

            // Check if node should be suspected
            checkNodeSuspicion(node_id, stats);

            // Check if node has been unresponsive for too long
            if (!node->isHealthy(_heartbeat_timeout * 3))
            { // 3x timeout threshold
                if (stats->consecutive_failures < _max_consecutive_failures)
                {
                    LOG_WARN("Node %s appears unresponsive", node_id.c_str());
                    markHeartbeatTimeout(node_id);
                }
            }
        }
    }

    void HeartbeatManager::updateNodeStatistics()
    {
        const std::shared_lock lock(_nodes_mutex);

        for (const auto &[node_id, stats] : _node_stats)
        {
            // Update node's heartbeat statistics in NodeInfo
            const auto node_it = _monitored_nodes.find(node_id);
            if (node_it != _monitored_nodes.end())
            {
                const auto &node = node_it->second;

                // Sync statistics (this is a simplified approach)
                // In a real implementation, we might want more sophisticated syncing
                while (node->getHeartbeatsSent() < stats->heartbeats_sent)
                {
                    node->incrementHeartbeatsSent();
                }
                while (node->getHeartbeatsReceived() < stats->heartbeats_received)
                {
                    node->incrementHeartbeatsReceived();
                }

                if (stats->average_latency.count() > 0)
                {
                    node->recordLatency(stats->average_latency);
                }
            }
        }
    }

    void HeartbeatManager::cleanupConnections()
    {
        const std::unique_lock lock(_nodes_mutex);

        for (auto &[node_id, client] : _node_connections)
        {
            if (client && client->isConnected())
            {
                client->disconnect();
            }
        }

        _node_connections.clear();
    }

    TcpClient *HeartbeatManager::getOrCreateConnection(const NodeId &node_id, const std::string &endpoint)
    {
        const std::unique_lock lock(_nodes_mutex);

        // Check if connection already exists
        const auto it = _node_connections.find(node_id);
        if (it != _node_connections.end() && it->second && it->second->isConnected())
        {
            return it->second.get();
        }

        // Parse endpoint
        const auto endpoint_result = MembershipUtils::parseEndpoint(endpoint);
        if (!endpoint_result.ok())
        {
            LOG_ERROR("Invalid endpoint for node %s: %s", node_id.c_str(), endpoint.c_str());
            return nullptr;
        }

        const auto [host, port] = endpoint_result.value();

        // Create new connection
        auto client = std::make_unique<TcpClient>();
        client->setDefaultTimeout(_heartbeat_timeout);

        const auto connect_status = client->connect(host, port);
        if (connect_status != Status::OK)
        {
            LOG_DEBUG("Failed to connect to node %s at %s", node_id.c_str(), endpoint.c_str());
            return nullptr;
        }

        auto *client_ptr = client.get();
        _node_connections[node_id] = std::move(client);

        LOG_DEBUG("Created connection to node %s at %s", node_id.c_str(), endpoint.c_str());
        return client_ptr;
    }

    void HeartbeatManager::removeConnection(const NodeId &node_id)
    {
        const auto it = _node_connections.find(node_id);
        if (it != _node_connections.end())
        {
            if (it->second && it->second->isConnected())
            {
                it->second->disconnect();
            }
            _node_connections.erase(it);
        }
    }

    std::shared_ptr<NodeHeartbeatStats> HeartbeatManager::getOrCreateStats(const NodeId &node_id)
    {
        const auto it = _node_stats.find(node_id);
        if (it != _node_stats.end())
        {
            return it->second;
        }

        auto stats = std::make_shared<NodeHeartbeatStats>();
        stats->node_id = node_id;
        stats->last_heartbeat_sent = std::chrono::system_clock::now();
        stats->last_heartbeat_received = std::chrono::system_clock::now();

        _node_stats[node_id] = stats;
        return stats;
    }

    void HeartbeatManager::updateLatencyStats(const NodeId &node_id, std::chrono::milliseconds latency)
    {
        const auto stats = getOrCreateStats(node_id);

        // Update min/max latency
        if (latency < stats->min_latency)
        {
            stats->min_latency = latency;
        }
        if (latency > stats->max_latency)
        {
            stats->max_latency = latency;
        }

        // Update average latency (simple moving average)
        if (stats->heartbeats_received == 0)
        {
            stats->average_latency = latency;
        }
        else
        {
            const auto total_ms = stats->average_latency.count() * stats->heartbeats_received + latency.count();
            stats->average_latency = std::chrono::milliseconds(total_ms / (stats->heartbeats_received + 1));
        }
    }

    void HeartbeatManager::markHeartbeatSent(const NodeId &node_id)
    {
        const auto stats = getOrCreateStats(node_id);
        ++stats->heartbeats_sent;
        stats->last_heartbeat_sent = std::chrono::system_clock::now();

        notifyHeartbeatEvent(HeartbeatEventData(HeartbeatEvent::HEARTBEAT_SENT, node_id));
    }

    void HeartbeatManager::markHeartbeatReceived(const NodeId &node_id, std::chrono::milliseconds latency)
    {
        const auto stats = getOrCreateStats(node_id);
        ++stats->heartbeats_received;
        stats->last_heartbeat_received = std::chrono::system_clock::now();
        stats->consecutive_failures = 0; // Reset failure count

        updateLatencyStats(node_id, latency);

        notifyHeartbeatEvent(HeartbeatEventData(HeartbeatEvent::HEARTBEAT_RECEIVED, node_id, latency));

        // If node was suspected, recover it
        if (stats->is_suspected.load())
        {
            recoverNode(node_id);
        }
    }

    void HeartbeatManager::markHeartbeatTimeout(const NodeId &node_id)
    {
        const auto stats = getOrCreateStats(node_id);
        ++stats->consecutive_failures;

        notifyHeartbeatEvent(HeartbeatEventData(HeartbeatEvent::HEARTBEAT_TIMEOUT, node_id));

        // Check if node should be suspected
        checkNodeSuspicion(node_id, stats);
    }

    void HeartbeatManager::checkNodeSuspicion(const NodeId &node_id, std::shared_ptr<NodeHeartbeatStats> stats)
    {
        if (shouldSuspectNode(stats) && !stats->is_suspected.load())
        {
            suspectNode(node_id);
        }
    }

    void HeartbeatManager::suspectNode(const NodeId &node_id)
    {
        const auto stats = getOrCreateStats(node_id);
        stats->is_suspected.store(true);

        LOG_WARN("Node suspected of failure: %s (failures: %lu)",
                 node_id.c_str(), stats->consecutive_failures);

        notifyHeartbeatEvent(HeartbeatEventData(HeartbeatEvent::NODE_SUSPECTED, node_id));
    }

    void HeartbeatManager::recoverNode(const NodeId &node_id)
    {
        const auto stats = getOrCreateStats(node_id);
        if (stats->is_suspected.exchange(false))
        {
            LOG_INFO("Node recovered from suspected failure: %s", node_id.c_str());
            notifyHeartbeatEvent(HeartbeatEventData(HeartbeatEvent::NODE_RECOVERED, node_id));
        }
    }

    void HeartbeatManager::notifyHeartbeatEvent(const HeartbeatEventData &event)
    {
        const std::lock_guard<std::mutex> lock(_callback_mutex);
        if (_event_callback)
        {
            try
            {
                _event_callback(event);
            }
            catch (const std::exception &e)
            {
                LOG_ERROR("Exception in heartbeat event callback: %s", e.what());
            }
        }
    }

    std::chrono::milliseconds HeartbeatManager::calculateLatency(Timestamp start_time) const
    {
        const auto end_time = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    }

    bool HeartbeatManager::shouldSuspectNode(const std::shared_ptr<NodeHeartbeatStats> &stats) const
    {
        return stats->consecutive_failures >= _suspicion_threshold;
    }

    // HeartbeatUtils implementation
    bool HeartbeatUtils::isNodeSuspected(const NodeHeartbeatStats &stats, std::uint64_t failure_threshold)
    {
        return stats.consecutive_failures >= failure_threshold || stats.is_suspected.load();
    }

    std::chrono::milliseconds HeartbeatUtils::calculateTimeout(
        std::chrono::milliseconds base_timeout,
        std::chrono::milliseconds average_latency,
        double safety_factor)
    {

        const auto adjusted_timeout = static_cast<long>(base_timeout.count() +
                                                        average_latency.count() * safety_factor);
        return std::chrono::milliseconds(std::max(adjusted_timeout, base_timeout.count()));
    }

    double HeartbeatUtils::calculateNetworkHealth(
        const std::vector<std::shared_ptr<NodeHeartbeatStats>> &stats)
    {

        if (stats.empty())
        {
            return 0.0;
        }

        double total_success_rate = 0.0;
        for (const auto &stat : stats)
        {
            total_success_rate += stat->getSuccessRate();
        }

        return total_success_rate / stats.size();
    }

    std::chrono::milliseconds HeartbeatUtils::calculateClusterLatency(
        const std::vector<std::shared_ptr<NodeHeartbeatStats>> &stats)
    {

        if (stats.empty())
        {
            return std::chrono::milliseconds(0);
        }

        std::int64_t total_latency = 0;
        std::size_t count = 0;

        for (const auto &stat : stats)
        {
            if (stat->average_latency.count() > 0)
            {
                total_latency += stat->average_latency.count();
                ++count;
            }
        }

        return count > 0 ? std::chrono::milliseconds(total_latency / count) : std::chrono::milliseconds(0);
    }

    std::chrono::milliseconds HeartbeatUtils::adaptiveHeartbeatInterval(
        std::size_t cluster_size,
        std::chrono::milliseconds base_interval)
    {

        // Scale interval based on cluster size to reduce network traffic
        const auto scale_factor = std::max(1.0, std::log2(static_cast<double>(cluster_size)));
        const auto scaled_interval = static_cast<long>(base_interval.count() * scale_factor);

        return std::chrono::milliseconds(scaled_interval);
    }

    bool HeartbeatUtils::isNodeHealthy(const NodeHeartbeatStats &stats,
                                       std::chrono::milliseconds max_age)
    {

        const auto now = std::chrono::system_clock::now();
        const auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - stats.last_heartbeat_received);

        return age <= max_age && !stats.is_suspected.load() && stats.consecutive_failures == 0;
    }

    std::vector<NodeId> HeartbeatUtils::selectHealthiestNodes(
        const std::vector<std::shared_ptr<NodeHeartbeatStats>> &stats,
        std::size_t count)
    {

        // Sort by health score (success rate + inverse latency)
        auto sorted_stats = stats;
        std::sort(sorted_stats.begin(), sorted_stats.end(),
                  [](const std::shared_ptr<NodeHeartbeatStats> &a,
                     const std::shared_ptr<NodeHeartbeatStats> &b)
                  {
                      const auto score_a = a->getSuccessRate() - (a->average_latency.count() / 1000.0);
                      const auto score_b = b->getSuccessRate() - (b->average_latency.count() / 1000.0);
                      return score_a > score_b;
                  });

        std::vector<NodeId> result;
        const auto select_count = std::min(count, sorted_stats.size());

        for (std::size_t i = 0; i < select_count; ++i)
        {
            result.push_back(sorted_stats[i]->node_id);
        }

        return result;
    }

} // namespace distributed_db