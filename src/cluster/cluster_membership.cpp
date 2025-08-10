#include "cluster_membership.hpp"
#include "../common/logger.hpp"
#include <algorithm>
#include <sstream>
#include <regex>
#include <cstring>

namespace distributed_db
{
    Result<std::vector<std::uint8_t>> JoinRequestMessage::serialize() const
    {
        if (!_node_info)
        {
            return Result<std::vector<std::uint8_t>>(Status::INTERNAL_ERROR);
        }

        const auto node_serialized = _node_info->serialize();
        if (!node_serialized.ok())
        {
            return node_serialized;
        }

        // Update payload size
        const_cast<JoinRequestMessage *>(this)->updatePayloadSize(
            static_cast<std::uint32_t>(node_serialized.value().size()));

        std::vector<std::uint8_t> buffer;
        const auto header_status = serializeHeader(buffer);
        if (header_status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(header_status);
        }

        const auto &node_data = node_serialized.value();
        buffer.insert(buffer.end(), node_data.begin(), node_data.end());

        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status JoinRequestMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + _header.payload_size)
        {
            return Status::INVALID_REQUEST;
        }

        // Extract node data
        const std::vector<std::uint8_t> node_data(
            data.begin() + MessageHeader::HEADER_SIZE,
            data.begin() + MessageHeader::HEADER_SIZE + _header.payload_size);

        _node_info = std::make_shared<NodeInfo>();
        return _node_info->deserialize(node_data);
    }

    Result<std::vector<std::uint8_t>> JoinResponseMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;

        // Status
        payload.push_back(static_cast<std::uint8_t>(_status));

        // Number of cluster members
        const auto member_count = static_cast<std::uint32_t>(_cluster_members.size());
        const auto count_bytes = reinterpret_cast<const std::uint8_t *>(&member_count);
        payload.insert(payload.end(), count_bytes, count_bytes + sizeof(member_count));

        // Serialize each member
        for (const auto &member : _cluster_members)
        {
            if (!member)
                continue;

            const auto member_serialized = member->serialize();
            if (!member_serialized.ok())
            {
                return Result<std::vector<std::uint8_t>>(member_serialized.status());
            }

            const auto &member_data = member_serialized.value();
            const auto member_size = static_cast<std::uint32_t>(member_data.size());
            const auto size_bytes = reinterpret_cast<const std::uint8_t *>(&member_size);

            payload.insert(payload.end(), size_bytes, size_bytes + sizeof(member_size));
            payload.insert(payload.end(), member_data.begin(), member_data.end());
        }

        // Update payload size
        const_cast<JoinResponseMessage *>(this)->updatePayloadSize(
            static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto header_status = serializeHeader(buffer);
        if (header_status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(header_status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status JoinResponseMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + _header.payload_size)
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = MessageHeader::HEADER_SIZE;

        // Status
        if (offset >= data.size())
            return Status::INVALID_REQUEST;
        _status = static_cast<Status>(data[offset]);
        offset += sizeof(std::uint8_t);

        // Number of members
        if (offset + sizeof(std::uint32_t) > data.size())
            return Status::INVALID_REQUEST;
        std::uint32_t member_count;
        memcpy(&member_count, data.data() + offset, sizeof(member_count));
        offset += sizeof(member_count);

        // Deserialize members
        _cluster_members.clear();
        _cluster_members.reserve(member_count);

        for (std::uint32_t i = 0; i < member_count; ++i)
        {
            // Member size
            if (offset + sizeof(std::uint32_t) > data.size())
                return Status::INVALID_REQUEST;
            std::uint32_t member_size;
            memcpy(&member_size, data.data() + offset, sizeof(member_size));
            offset += sizeof(member_size);

            // Member data
            if (offset + member_size > data.size())
                return Status::INVALID_REQUEST;
            const std::vector<std::uint8_t> member_data(data.begin() + offset,
                                                        data.begin() + offset + member_size);
            offset += member_size;

            auto member = std::make_shared<NodeInfo>();
            const auto deserialize_status = member->deserialize(member_data);
            if (deserialize_status != Status::OK)
            {
                LOG_WARN("Failed to deserialize cluster member %u", i);
                continue;
            }

            _cluster_members.push_back(std::move(member));
        }

        return Status::OK;
    }

    // ClusterMembership implementation
    ClusterMembership::ClusterMembership(std::shared_ptr<NodeInfo> local_node)
        : _local_node(std::move(local_node)), _node_registry(std::make_unique<NodeRegistry>())
    {

        if (!_local_node)
        {
            throw std::invalid_argument("Local node cannot be null");
        }

        // Add local node to registry
        _node_registry->addNode(*_local_node);
        LOG_INFO("Cluster membership initialized for node: %s", _local_node->toString().c_str());
    }

    ClusterMembership::~ClusterMembership()
    {
        stop();
    }

    Status ClusterMembership::start()
    {
        if (_running)
        {
            return Status::OK;
        }

        _should_stop = false;
        _running = true;

        // Start membership management thread
        _membership_thread = std::make_unique<std::thread>(&ClusterMembership::membershipLoop, this);

        LOG_INFO("Cluster membership started");
        return Status::OK;
    }

    void ClusterMembership::stop()
    {
        if (!_running)
        {
            return;
        }

        _should_stop = true;
        _running = false;

        // Notify stop condition
        {
            const std::lock_guard<std::mutex> lock(_stop_mutex);
            _stop_condition.notify_all();
        }

        // Wait for membership thread
        if (_membership_thread && _membership_thread->joinable())
        {
            _membership_thread->join();
        }
        _membership_thread.reset();

        // Cleanup connections
        cleanupConnections();

        LOG_INFO("Cluster membership stopped");
    }

    Status ClusterMembership::joinCluster(const std::vector<std::string> &seed_nodes)
    {
        if (seed_nodes.empty())
        {
            LOG_INFO("No seed nodes provided, starting as single-node cluster");

            // Update local node state
            _local_node->setState(NodeState::ACTIVE);

            // IMPORTANT: Also update the node in the registry since they're separate objects
            _node_registry->updateNode(*_local_node);

            notifyMembershipEvent(MembershipEventData(MembershipEvent::NODE_JOINED, _local_node->getId(), _local_node));
            return Status::OK;
        }

        LOG_INFO("Attempting to join cluster using %zu seed nodes", seed_nodes.size());

        // Parse and validate endpoints
        const auto endpoints = parseEndpoints(seed_nodes);
        if (endpoints.empty())
        {
            LOG_ERROR("No valid seed endpoints found");
            return Status::INVALID_REQUEST;
        }

        _local_node->setState(NodeState::JOINING);
        // Update registry as well
        _node_registry->updateNode(*_local_node);

        // Try to connect to seed nodes
        bool joined = false;
        for (const auto &endpoint : endpoints)
        {
            LOG_DEBUG("Trying to connect to seed node: %s", endpoint.c_str());

            const auto connect_status = connectToSeedNode(endpoint);
            if (connect_status == Status::OK)
            {
                joined = true;
                break;
            }

            LOG_DEBUG("Failed to connect to seed node %s, trying next", endpoint.c_str());
        }

        if (!joined)
        {
            LOG_ERROR("Failed to join cluster - no seed nodes were reachable");
            _local_node->setState(NodeState::FAILED);
            _node_registry->updateNode(*_local_node);
            return Status::NETWORK_ERROR;
        }

        _local_node->setState(NodeState::ACTIVE);
        _node_registry->updateNode(*_local_node);
        notifyMembershipEvent(MembershipEventData(MembershipEvent::NODE_JOINED, _local_node->getId(), _local_node));

        LOG_INFO("Successfully joined cluster");
        return Status::OK;
    }

    Status ClusterMembership::leaveCluster()
    {
        if (!_running)
        {
            return Status::OK;
        }

        LOG_INFO("Leaving cluster gracefully");

        _local_node->setState(NodeState::LEAVING);

        // Notify other nodes about leaving (implementation would send leave messages)
        // For now, just mark as leaving and stop

        notifyMembershipEvent(MembershipEventData(MembershipEvent::NODE_LEFT, _local_node->getId(), _local_node));

        stop();
        return Status::OK;
    }

    Status ClusterMembership::rejoinCluster()
    {
        LOG_INFO("Rejoining cluster");

        // Reset local node state
        _local_node->setState(NodeState::JOINING);

        // Try to reconnect to known nodes
        const auto active_nodes = _node_registry->getActiveNodes();
        std::vector<std::string> endpoints;

        for (const auto &node : active_nodes)
        {
            if (node->getId() != _local_node->getId())
            {
                endpoints.push_back(node->getEndpoint());
            }
        }

        return joinCluster(endpoints);
    }

    Status ClusterMembership::addNode(const NodeInfo &node)
    {
        if (!validateNodeForJoin(node))
        {
            LOG_WARN("Node failed validation for join: %s", node.toString().c_str());
            return Status::INVALID_REQUEST;
        }

        // Create a copy and set proper state
        NodeInfo node_copy = node;
        node_copy.setState(NodeState::ACTIVE); // Set to ACTIVE when adding to cluster

        _node_registry->addNode(node_copy);
        notifyMembershipEvent(MembershipEventData(MembershipEvent::NODE_JOINED, node.getId()));

        LOG_INFO("Added node to cluster: %s", node_copy.toString().c_str());
        return Status::OK;
    }

    Status ClusterMembership::removeNode(const NodeId &node_id)
    {
        const auto node = _node_registry->getNode(node_id);
        if (!node)
        {
            return Status::NOT_FOUND;
        }

        _node_registry->removeNode(node_id);
        notifyMembershipEvent(MembershipEventData(MembershipEvent::NODE_LEFT, node_id, node));

        LOG_INFO("Removed node from cluster: %s", node_id.c_str());
        return Status::OK;
    }

    Status ClusterMembership::updateNodeRole(const NodeId &node_id, NodeRole role)
    {
        const auto node = _node_registry->getNode(node_id);
        if (!node)
        {
            return Status::NOT_FOUND;
        }

        const auto old_role = node->getRole();
        node->setRole(role);

        notifyMembershipEvent(MembershipEventData(MembershipEvent::ROLE_CHANGED, node_id, node));

        LOG_INFO("Updated node role: %s from %s to %s",
                 node_id.c_str(), nodeRoleToString(old_role).c_str(), nodeRoleToString(role).c_str());

        return Status::OK;
    }

    Status ClusterMembership::markNodeFailed(const NodeId &node_id)
    {
        const auto node = _node_registry->getNode(node_id);
        if (!node)
        {
            return Status::NOT_FOUND;
        }

        node->setState(NodeState::FAILED);
        notifyMembershipEvent(MembershipEventData(MembershipEvent::NODE_FAILED, node_id, node));

        LOG_WARN("Marked node as failed: %s", node->toString().c_str());
        return Status::OK;
    }

    std::shared_ptr<NodeInfo> ClusterMembership::getNode(const NodeId &node_id) const
    {
        return _node_registry->getNode(node_id);
    }

    std::vector<std::shared_ptr<NodeInfo>> ClusterMembership::getAllNodes() const
    {
        return _node_registry->getAllNodes();
    }

    std::vector<std::shared_ptr<NodeInfo>> ClusterMembership::getActiveNodes() const
    {
        return _node_registry->getActiveNodes();
    }

    std::vector<std::shared_ptr<NodeInfo>> ClusterMembership::getVotingNodes() const
    {
        const auto all_nodes = _node_registry->getAllNodes();
        std::vector<std::shared_ptr<NodeInfo>> voting_nodes;

        std::copy_if(all_nodes.begin(), all_nodes.end(), std::back_inserter(voting_nodes),
                     [](const std::shared_ptr<NodeInfo> &node)
                     { return node->canVote(); });

        return voting_nodes;
    }

    std::shared_ptr<NodeInfo> ClusterMembership::getLeaderNode() const
    {
        return MembershipUtils::findLeaderNode(_node_registry->getAllNodes());
    }

    std::size_t ClusterMembership::getClusterSize() const
    {
        return _node_registry->size();
    }

    std::size_t ClusterMembership::getActiveNodeCount() const
    {
        return _node_registry->getActiveNodeCount();
    }

    bool ClusterMembership::isClusterHealthy() const
    {
        const auto active_nodes = getActiveNodes();
        return MembershipUtils::isClusterHealthy(active_nodes);
    }

    bool ClusterMembership::hasQuorum() const
    {
        const auto active_count = getActiveNodeCount();
        const auto total_count = getClusterSize();
        return MembershipUtils::hasQuorum(active_count, total_count);
    }

    void ClusterMembership::setMembershipEventCallback(MembershipEventCallback callback)
    {
        const std::lock_guard<std::mutex> lock(_callback_mutex);
        _event_callback = std::move(callback);
    }

    void ClusterMembership::clearMembershipEventCallback()
    {
        const std::lock_guard<std::mutex> lock(_callback_mutex);
        _event_callback = nullptr;
    }

    std::unique_ptr<Message> ClusterMembership::handleJoinRequest(const JoinRequestMessage &request)
    {
        const auto &joining_node = request.getNodeInfo();
        if (!joining_node)
        {
            return std::make_unique<JoinResponseMessage>(request.getMessageId(), Status::INVALID_REQUEST);
        }

        LOG_INFO("Processing join request from node: %s", joining_node->toString().c_str());

        // Validate the joining node
        if (!validateNodeForJoin(*joining_node))
        {
            LOG_WARN("Rejecting join request - node failed validation: %s", joining_node->toString().c_str());
            return std::make_unique<JoinResponseMessage>(request.getMessageId(), Status::INVALID_REQUEST);
        }

        // Add the node to our registry
        _node_registry->addNode(*joining_node);
        notifyMembershipEvent(MembershipEventData(MembershipEvent::NODE_JOINED, joining_node->getId(), joining_node));

        // Prepare response with current cluster membership
        const auto cluster_members = _node_registry->getAllNodes();

        LOG_INFO("Accepted join request from node: %s", joining_node->getId().c_str());
        return std::make_unique<JoinResponseMessage>(request.getMessageId(), Status::OK, cluster_members);
    }

    std::unique_ptr<Message> ClusterMembership::handleLeaveRequest(const NodeId &node_id)
    {
        LOG_INFO("Processing leave request from node: %s", node_id.c_str());

        const auto node = _node_registry->getNode(node_id);
        if (node)
        {
            node->setState(NodeState::LEAVING);
            notifyMembershipEvent(MembershipEventData(MembershipEvent::NODE_LEFT, node_id, node));

            // Remove after a delay to allow cleanup
            // For now, just mark as leaving
        }

        return std::make_unique<PutResponseMessage>(0, Status::OK); // Generic OK response
    }

    void ClusterMembership::membershipLoop()
    {
        LOG_DEBUG("Membership management loop started");

        while (!_should_stop)
        {
            try
            {
                // Periodically check node health and update membership
                _node_registry->markNodesFailed(std::chrono::milliseconds(30000)); // 30 second timeout

                // Clean up failed nodes periodically
                _node_registry->removeFailedNodes();

                // Wait for stop signal or timeout
                std::unique_lock<std::mutex> lock(_stop_mutex);
                _stop_condition.wait_for(lock, std::chrono::milliseconds(5000));
            }
            catch (const std::exception &e)
            {
                LOG_ERROR("Exception in membership loop: %s", e.what());
            }
        }

        LOG_DEBUG("Membership management loop finished");
    }

    Status ClusterMembership::connectToSeedNode(const std::string &seed_endpoint)
    {
        const auto endpoint_result = MembershipUtils::parseEndpoint(seed_endpoint);
        if (!endpoint_result.ok())
        {
            LOG_ERROR("Invalid seed endpoint: %s", seed_endpoint.c_str());
            return endpoint_result.status();
        }

        const auto [host, port] = endpoint_result.value();

        auto client = std::make_unique<TcpClient>();
        client->setDefaultTimeout(_join_timeout);

        const auto connect_status = client->connect(host, port);
        if (connect_status != Status::OK)
        {
            LOG_DEBUG("Failed to connect to seed node %s:%d", host.c_str(), port);
            return connect_status;
        }

        // Send join request
        const auto join_status = sendJoinRequest(*client);
        if (join_status == Status::OK)
        {
            // Store connection for future use
            const std::lock_guard<std::mutex> lock(_connections_mutex);
            _seed_connections.push_back(std::move(client));
        }

        return join_status;
    }

    Status ClusterMembership::sendJoinRequest(TcpClient &client)
    {
        JoinRequestMessage request(_local_node);

        const auto response_result = client.sendRequest(request, _join_timeout);
        if (!response_result.ok())
        {
            LOG_ERROR("Failed to send join request: status=%d", static_cast<int>(response_result.status()));
            return response_result.status();
        }

        const auto &response = response_result.value();
        if (response->getType() != MessageType::NODE_JOIN)
        {
            LOG_ERROR("Unexpected response type for join request");
            return Status::INTERNAL_ERROR;
        }

        const auto &join_response = static_cast<const JoinResponseMessage &>(*response);
        return processJoinResponse(join_response);
    }

    Status ClusterMembership::processJoinResponse(const JoinResponseMessage &response)
    {
        if (response.getStatus() != Status::OK)
        {
            LOG_ERROR("Join request rejected by seed node: status=%d", static_cast<int>(response.getStatus()));
            return response.getStatus();
        }

        // Update cluster membership with received member list
        updateClusterMembership(response.getClusterMembers());

        LOG_INFO("Join request accepted, received %zu cluster members", response.getClusterMembers().size());
        return Status::OK;
    }

    void ClusterMembership::updateClusterMembership(const std::vector<std::shared_ptr<NodeInfo>> &members)
    {
        for (const auto &member : members)
        {
            if (!member || member->getId() == _local_node->getId())
            {
                continue; // Skip null or self
            }

            const auto existing_node = _node_registry->getNode(member->getId());
            if (existing_node)
            {
                // Update existing node
                _node_registry->updateNode(*member);
            }
            else
            {
                // Add new node
                _node_registry->addNode(*member);
                notifyMembershipEvent(MembershipEventData(MembershipEvent::NODE_JOINED, member->getId(), member));
            }
        }

        notifyMembershipEvent(MembershipEventData(MembershipEvent::MEMBERSHIP_UPDATED, _local_node->getId()));
    }

    void ClusterMembership::notifyMembershipEvent(const MembershipEventData &event)
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
                LOG_ERROR("Exception in membership event callback: %s", e.what());
            }
        }
    }

    bool ClusterMembership::validateNodeForJoin(const NodeInfo &node) const
    {
        // Check for unique node ID
        if (!isNodeIdUnique(node.getId()))
        {
            LOG_WARN("Node ID already exists in cluster: %s", node.getId().c_str());
            return false;
        }

        // Check for unique endpoint
        if (!isEndpointUnique(node.getEndpoint()))
        {
            LOG_WARN("Node endpoint already exists in cluster: %s", node.getEndpoint().c_str());
            return false;
        }

        // Additional validation can be added here
        return true;
    }

    bool ClusterMembership::isNodeIdUnique(const NodeId &node_id) const
    {
        return !_node_registry->contains(node_id);
    }

    bool ClusterMembership::isEndpointUnique(const std::string &endpoint) const
    {
        const auto all_nodes = _node_registry->getAllNodes();
        return std::none_of(all_nodes.begin(), all_nodes.end(),
                            [&endpoint](const std::shared_ptr<NodeInfo> &node)
                            {
                                return node->getEndpoint() == endpoint;
                            });
    }

    std::vector<std::string> ClusterMembership::parseEndpoints(const std::vector<std::string> &seed_nodes) const
    {
        std::vector<std::string> valid_endpoints;

        for (const auto &seed : seed_nodes)
        {
            if (MembershipUtils::isValidEndpoint(seed))
            {
                valid_endpoints.push_back(seed);
            }
            else
            {
                LOG_WARN("Invalid seed endpoint format: %s", seed.c_str());
            }
        }

        return valid_endpoints;
    }

    std::string ClusterMembership::getLocalEndpoint() const
    {
        return _local_node->getEndpoint();
    }

    void ClusterMembership::cleanupConnections()
    {
        const std::lock_guard<std::mutex> lock(_connections_mutex);

        for (auto &client : _seed_connections)
        {
            if (client && client->isConnected())
            {
                client->disconnect();
            }
        }

        _seed_connections.clear();
    }

    std::string MembershipStats::toString() const
    {
        std::ostringstream oss;
        oss << "MembershipStats{total=" << total_nodes
            << ", active=" << active_nodes
            << ", failed=" << failed_nodes
            << ", joining=" << joining_nodes
            << ", leaving=" << leaving_nodes
            << ", uptime=" << cluster_uptime.count() << "ms}";
        return oss.str();
    }

    Result<std::pair<std::string, Port>> MembershipUtils::parseEndpoint(const std::string &endpoint)
    {
        const std::regex endpoint_regex(R"(^([^:]+):(\d+)$)");
        std::smatch match;

        if (!std::regex_match(endpoint, match, endpoint_regex))
        {
            return Result<std::pair<std::string, Port>>(Status::INVALID_REQUEST);
        }

        const std::string host = match[1].str();
        const auto port_str = match[2].str();

        try
        {
            // Parse as unsigned long first to check full range
            const auto port_ul = std::stoul(port_str);

            // Check if port is in valid range (1-65535)
            if (port_ul == 0 || port_ul > 65535)
            {
                return Result<std::pair<std::string, Port>>(Status::INVALID_REQUEST);
            }

            // Now safely cast to Port type
            const auto port = static_cast<Port>(port_ul);

            return Result<std::pair<std::string, Port>>(std::make_pair(host, port));
        }
        catch (const std::exception &)
        {
            return Result<std::pair<std::string, Port>>(Status::INVALID_REQUEST);
        }
    }

    bool MembershipUtils::isValidEndpoint(const std::string &endpoint)
    {
        return parseEndpoint(endpoint).ok();
    }

    std::string MembershipUtils::formatEndpoint(const std::string &host, Port port)
    {
        return host + ":" + std::to_string(port);
    }

    bool MembershipUtils::hasQuorum(std::size_t active_nodes, std::size_t total_nodes)
    {
        if (total_nodes == 0)
            return false;
        const auto quorum_size = calculateQuorumSize(total_nodes);
        return active_nodes >= quorum_size;
    }

    std::size_t MembershipUtils::calculateQuorumSize(std::size_t total_nodes)
    {
        return (total_nodes / 2) + 1;
    }

    bool MembershipUtils::isClusterHealthy(const std::vector<std::shared_ptr<NodeInfo>> &nodes)
    {
        if (nodes.empty())
            return false;

        const auto active_count = std::count_if(nodes.begin(), nodes.end(),
                                                [](const std::shared_ptr<NodeInfo> &node)
                                                {
                                                    return node->isActive();
                                                });

        return hasQuorum(active_count, nodes.size());
    }

    std::vector<std::shared_ptr<NodeInfo>> MembershipUtils::selectHealthyNodes(
        const std::vector<std::shared_ptr<NodeInfo>> &nodes,
        std::chrono::milliseconds health_timeout)
    {

        std::vector<std::shared_ptr<NodeInfo>> healthy_nodes;

        std::copy_if(nodes.begin(), nodes.end(), std::back_inserter(healthy_nodes),
                     [health_timeout](const std::shared_ptr<NodeInfo> &node)
                     {
                         return node->isHealthy(health_timeout);
                     });

        return healthy_nodes;
    }

    std::shared_ptr<NodeInfo> MembershipUtils::findLeaderNode(
        const std::vector<std::shared_ptr<NodeInfo>> &nodes)
    {

        const auto leader_it = std::find_if(nodes.begin(), nodes.end(),
                                            [](const std::shared_ptr<NodeInfo> &node)
                                            {
                                                return node->getRole() == NodeRole::LEADER && node->isActive();
                                            });

        return leader_it != nodes.end() ? *leader_it : nullptr;
    }

    MembershipStats MembershipUtils::calculateStats(
        const std::vector<std::shared_ptr<NodeInfo>> &nodes,
        Timestamp cluster_start_time)
    {

        MembershipStats stats{};
        stats.total_nodes = nodes.size();

        Timestamp latest_change = cluster_start_time;

        for (const auto &node : nodes)
        {
            const auto state = node->getState();
            const auto role = node->getRole();

            switch (state)
            {
            case NodeState::ACTIVE:
                ++stats.active_nodes;
                break;
            case NodeState::FAILED:
                ++stats.failed_nodes;
                break;
            case NodeState::JOINING:
                ++stats.joining_nodes;
                break;
            case NodeState::LEAVING:
                ++stats.leaving_nodes;
                break;
            default:
                break;
            }

            ++stats.role_counts[role];

            // Track latest membership change
            if (node->getLastSeen() > latest_change)
            {
                latest_change = node->getLastSeen();
            }
        }

        stats.last_membership_change = latest_change;

        const auto now = std::chrono::system_clock::now();
        stats.cluster_uptime = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - cluster_start_time);

        return stats;
    }

} // namespace distributed_db