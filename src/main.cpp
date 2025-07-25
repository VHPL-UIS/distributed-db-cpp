#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <future>
#include <vector>

#include "common/logger.hpp"
#include "common/config.hpp"
#include "common/types.hpp"
#include "storage/persistent_storage_engine.hpp"
#include "network/tcp_server.hpp"
#include "network/tcp_client.hpp"
#include "cluster/node_info.hpp"
#include "cluster/cluster_membership.hpp"

using namespace distributed_db;

void printBanner()
{
    std::cout << R"(
╔══════════════════════════════════════════════════════════════╗
║                    Distributed Database                      ║
║            Node Discovery & Cluster Formation                ║
╚══════════════════════════════════════════════════════════════╝
)" << std::endl;
}

void demonstrateNodeInfo()
{
    LOG_INFO("=== Node Information Demonstration ===");

    // Create node with basic information
    NodeInfo node("demo_node_1", "192.168.1.100", 8080, NodeRole::FOLLOWER);
    LOG_INFO("Created node: %s", node.toString().c_str());

    // Test state transitions
    LOG_INFO("Testing state transitions...");
    node.setState(NodeState::JOINING);
    LOG_INFO("State changed to JOINING: %s", nodeStateToString(node.getState()).c_str());

    node.setState(NodeState::ACTIVE);
    LOG_INFO("State changed to ACTIVE: %s", nodeStateToString(node.getState()).c_str());

    LOG_INFO("✓ Node is active: %s", node.isActive() ? "true" : "false");

    // Test role changes
    LOG_INFO("Testing role changes...");
    node.setRole(NodeRole::LEADER);
    LOG_INFO("Role changed to LEADER: %s", nodeRoleToString(node.getRole()).c_str());
    LOG_INFO("✓ Can vote: %s", node.canVote() ? "true" : "false");

    // Test metadata
    LOG_INFO("Testing metadata operations...");
    node.setMetadata("version", "1.0.0");
    node.setMetadata("datacenter", "us-west-1");
    node.setMetadata("instance_type", "m5.large");

    LOG_INFO("✓ Metadata - version: %s", node.getMetadata("version").c_str());
    LOG_INFO("✓ Metadata - datacenter: %s", node.getMetadata("datacenter").c_str());
    LOG_INFO("✓ Has version metadata: %s", node.hasMetadata("version") ? "true" : "false");

    // Test statistics
    LOG_INFO("Testing heartbeat statistics...");
    for (int i = 0; i < 5; ++i)
    {
        node.incrementHeartbeatsSent();
        if (i < 4)
        { // 4 out of 5 responses
            node.incrementHeartbeatsReceived();
            node.recordLatency(std::chrono::milliseconds(50 + i * 10));
        }
    }

    LOG_INFO("✓ Heartbeats sent: %lu", node.getHeartbeatsSent());
    LOG_INFO("✓ Heartbeats received: %lu", node.getHeartbeatsReceived());
    LOG_INFO("✓ Average latency: %ldms", node.getAverageLatency().count());

    // Test health check
    LOG_INFO("Testing health checks...");
    LOG_INFO("✓ Is healthy (1s timeout): %s",
             node.isHealthy(std::chrono::milliseconds(1000)) ? "true" : "false");

    // Test serialization
    LOG_INFO("Testing serialization...");
    const auto serialized = node.serialize();
    if (serialized.ok())
    {
        LOG_INFO("✓ Node serialized: %zu bytes", serialized.value().size());

        NodeInfo deserialized;
        if (deserialized.deserialize(serialized.value()) == Status::OK)
        {
            LOG_INFO("✓ Node deserialized successfully");
            LOG_INFO("  Deserialized: %s", deserialized.toString().c_str());
        }
    }
}

void demonstrateNodeRegistry()
{
    LOG_INFO("=== Node Registry Demonstration ===");

    NodeRegistry registry;
    LOG_INFO("Created empty registry: size=%zu", registry.size());

    // Add multiple nodes
    LOG_INFO("Adding nodes to registry...");
    std::vector<NodeInfo> test_nodes = {
        NodeInfo("node_1", "127.0.0.1", 8081, NodeRole::FOLLOWER),
        NodeInfo("node_2", "127.0.0.1", 8082, NodeRole::FOLLOWER),
        NodeInfo("node_3", "127.0.0.1", 8083, NodeRole::LEADER),
        NodeInfo("node_4", "127.0.0.1", 8084, NodeRole::OBSERVER)};

    // Set different states
    test_nodes[0].setState(NodeState::ACTIVE);
    test_nodes[1].setState(NodeState::ACTIVE);
    test_nodes[2].setState(NodeState::ACTIVE);
    test_nodes[3].setState(NodeState::FAILED);

    for (const auto &node : test_nodes)
    {
        registry.addNode(node);
    }

    LOG_INFO("✓ Registry size after adding nodes: %zu", registry.size());

    // Test filtering
    const auto active_nodes = registry.getActiveNodes();
    LOG_INFO("✓ Active nodes: %zu", active_nodes.size());

    const auto leaders = registry.getNodesByRole(NodeRole::LEADER);
    LOG_INFO("✓ Leader nodes: %zu", leaders.size());

    const auto failed_nodes = registry.getNodesByState(NodeState::FAILED);
    LOG_INFO("✓ Failed nodes: %zu", failed_nodes.size());

    // Test statistics
    const auto state_counts = registry.getNodeStateCounts();
    LOG_INFO("✓ Node state distribution:");
    for (const auto &[state, count] : state_counts)
    {
        LOG_INFO("  %s: %zu nodes", nodeStateToString(state).c_str(), count);
    }

    const auto role_counts = registry.getNodeRoleCounts();
    LOG_INFO("✓ Node role distribution:");
    for (const auto &[role, count] : role_counts)
    {
        LOG_INFO("  %s: %zu nodes", nodeRoleToString(role).c_str(), count);
    }

    // Test node retrieval
    const auto retrieved_node = registry.getNode("node_2");
    if (retrieved_node)
    {
        LOG_INFO("✓ Retrieved node: %s", retrieved_node->toString().c_str());
    }

    // Test node removal
    if (registry.removeNode("node_4"))
    {
        LOG_INFO("✓ Removed failed node, new size: %zu", registry.size());
    }
}

void demonstrateSingleNodeCluster()
{
    LOG_INFO("=== Single Node Cluster Demonstration ===");

    // Create local node
    auto local_node = std::make_shared<NodeInfo>("single_node", "127.0.0.1", 9000);
    auto membership = std::make_unique<ClusterMembership>(local_node);

    LOG_INFO("Created cluster membership for: %s", local_node->toString().c_str());

    // Start membership management
    const auto start_status = membership->start();
    if (start_status == Status::OK)
    {
        LOG_INFO("✓ Membership management started");
    }

    // Join as single node cluster (empty seed list)
    const auto join_status = membership->joinCluster({});
    if (join_status == Status::OK)
    {
        LOG_INFO("✓ Joined cluster as single node");
    }

    // Check cluster state
    LOG_INFO("Cluster status:");
    LOG_INFO("  Size: %zu", membership->getClusterSize());
    LOG_INFO("  Active nodes: %zu", membership->getActiveNodeCount());
    LOG_INFO("  Has quorum: %s", membership->hasQuorum() ? "true" : "false");
    LOG_INFO("  Is healthy: %s", membership->isClusterHealthy() ? "true" : "false");

    const auto leader = membership->getLeaderNode();
    if (leader)
    {
        LOG_INFO("  Leader: %s", leader->getId().c_str());
    }
    else
    {
        LOG_INFO("  Leader: none");
    }

    // Demonstrate node management
    LOG_INFO("Adding additional nodes to cluster...");

    NodeInfo additional_node1("additional_1", "127.0.0.1", 9001);
    NodeInfo additional_node2("additional_2", "127.0.0.1", 9002);

    additional_node1.setState(NodeState::ACTIVE);
    additional_node2.setState(NodeState::ACTIVE);

    membership->addNode(additional_node1);
    membership->addNode(additional_node2);

    LOG_INFO("✓ Added 2 additional nodes");
    LOG_INFO("Updated cluster size: %zu", membership->getClusterSize());

    // Promote a node to leader
    const auto promote_status = membership->updateNodeRole("additional_1", NodeRole::LEADER);
    if (promote_status == Status::OK)
    {
        LOG_INFO("✓ Promoted additional_1 to leader");

        const auto new_leader = membership->getLeaderNode();
        if (new_leader)
        {
            LOG_INFO("  New leader: %s", new_leader->getId().c_str());
        }
    }

    // Get voting nodes
    const auto voting_nodes = membership->getVotingNodes();
    LOG_INFO("✓ Voting nodes: %zu", voting_nodes.size());
    for (const auto &node : voting_nodes)
    {
        LOG_INFO("  Voter: %s (%s)", node->getId().c_str(), nodeRoleToString(node->getRole()).c_str());
    }

    // Simulate node failure
    LOG_INFO("Simulating node failure...");
    const auto fail_status = membership->markNodeFailed("additional_2");
    if (fail_status == Status::OK)
    {
        LOG_INFO("✓ Marked additional_2 as failed");
        LOG_INFO("  Active nodes after failure: %zu", membership->getActiveNodeCount());
        LOG_INFO("  Still has quorum: %s", membership->hasQuorum() ? "true" : "false");
    }

    // Clean shutdown
    membership->stop();
    LOG_INFO("✓ Cluster membership stopped");
}

void demonstrateMembershipEvents()
{
    LOG_INFO("=== Membership Events Demonstration ===");

    auto local_node = std::make_shared<NodeInfo>("event_demo_node", "127.0.0.1", 9100);
    auto membership = std::make_unique<ClusterMembership>(local_node);

    std::vector<MembershipEventData> captured_events;

    // Set up event callback
    membership->setMembershipEventCallback([&captured_events](const MembershipEventData &event)
                                           {
        captured_events.push_back(event);
        LOG_INFO("📧 Membership Event: %s for node %s", 
                 [](MembershipEvent evt) {
                     switch (evt) {
                         case MembershipEvent::NODE_JOINED: return "NODE_JOINED";
                         case MembershipEvent::NODE_LEFT: return "NODE_LEFT";
                         case MembershipEvent::NODE_FAILED: return "NODE_FAILED";
                         case MembershipEvent::ROLE_CHANGED: return "ROLE_CHANGED";
                         case MembershipEvent::MEMBERSHIP_UPDATED: return "MEMBERSHIP_UPDATED";
                         default: return "UNKNOWN";
                     }
                 }(event.event),
                 event.node_id.c_str()); });

    membership->start();
    membership->joinCluster({}); // Single node cluster

    // Give time for initial events
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    LOG_INFO("Performing membership operations to generate events...");

    // Add nodes (should generate NODE_JOINED events)
    NodeInfo event_node1("event_node_1", "127.0.0.1", 9101);
    NodeInfo event_node2("event_node_2", "127.0.0.1", 9102);

    membership->addNode(event_node1);
    membership->addNode(event_node2);

    // Change role (should generate ROLE_CHANGED event)
    membership->updateNodeRole("event_node_1", NodeRole::LEADER);

    // Mark node as failed (should generate NODE_FAILED event)
    membership->markNodeFailed("event_node_2");

    // Remove node (should generate NODE_LEFT event)
    membership->removeNode("event_node_2");

    // Give time for event processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    LOG_INFO("✓ Captured %zu membership events", captured_events.size());

    // Summarize events
    std::unordered_map<MembershipEvent, int> event_counts;
    for (const auto &event : captured_events)
    {
        event_counts[event.event]++;
    }

    LOG_INFO("Event summary:");
    for (const auto &[event_type, count] : event_counts)
    {
        const char *event_name = [](MembershipEvent evt)
        {
            switch (evt)
            {
            case MembershipEvent::NODE_JOINED:
                return "NODE_JOINED";
            case MembershipEvent::NODE_LEFT:
                return "NODE_LEFT";
            case MembershipEvent::NODE_FAILED:
                return "NODE_FAILED";
            case MembershipEvent::ROLE_CHANGED:
                return "ROLE_CHANGED";
            case MembershipEvent::MEMBERSHIP_UPDATED:
                return "MEMBERSHIP_UPDATED";
            default:
                return "UNKNOWN";
            }
        }(event_type);
        LOG_INFO("  %s: %d events", event_name, count);
    }

    membership->stop();
}

void demonstrateMembershipUtilities()
{
    LOG_INFO("=== Membership Utilities Demonstration ===");

    // Test endpoint parsing
    LOG_INFO("Testing endpoint parsing...");

    const std::vector<std::string> test_endpoints = {
        "127.0.0.1:8080",
        "192.168.1.100:9000",
        "localhost:3000",
        "invalid-endpoint",
        "host:",
        ":8080",
        "host:99999"};

    for (const auto &endpoint : test_endpoints)
    {
        const auto result = MembershipUtils::parseEndpoint(endpoint);
        if (result.ok())
        {
            const auto [host, port] = result.value();
            LOG_INFO("✓ Valid endpoint: %s -> %s:%d", endpoint.c_str(), host.c_str(), port);
        }
        else
        {
            LOG_INFO("✗ Invalid endpoint: %s", endpoint.c_str());
        }
    }

    // Test quorum calculations
    LOG_INFO("Testing quorum calculations...");

    const std::vector<std::size_t> cluster_sizes = {1, 2, 3, 4, 5, 7, 10};
    for (const auto size : cluster_sizes)
    {
        const auto quorum_size = MembershipUtils::calculateQuorumSize(size);
        const auto has_quorum_all = MembershipUtils::hasQuorum(size, size);
        const auto has_quorum_half = MembershipUtils::hasQuorum(size / 2, size);

        LOG_INFO("Cluster size %zu: quorum=%zu, all_active=%s, half_active=%s",
                 size, quorum_size,
                 has_quorum_all ? "true" : "false",
                 has_quorum_half ? "true" : "false");
    }

    // Test cluster health assessment
    LOG_INFO("Testing cluster health assessment...");

    std::vector<std::shared_ptr<NodeInfo>> test_cluster;

    // Create a mix of healthy and unhealthy nodes
    for (int i = 1; i <= 5; ++i)
    {
        auto node = std::make_shared<NodeInfo>("health_test_" + std::to_string(i),
                                               "127.0.0.1", 8000 + i);
        if (i <= 3)
        {
            node->setState(NodeState::ACTIVE);
        }
        else
        {
            node->setState(NodeState::FAILED);
        }
        test_cluster.push_back(node);
    }

    const auto is_healthy = MembershipUtils::isClusterHealthy(test_cluster);
    LOG_INFO("✓ Test cluster health (3/5 active): %s", is_healthy ? "healthy" : "unhealthy");

    // Test healthy node selection
    const auto healthy_nodes = MembershipUtils::selectHealthyNodes(
        test_cluster, std::chrono::milliseconds(10000));
    LOG_INFO("✓ Healthy nodes selected: %zu out of %zu", healthy_nodes.size(), test_cluster.size());

    // Test leader finding
    test_cluster[1]->setRole(NodeRole::LEADER); // Make node 2 the leader
    const auto leader = MembershipUtils::findLeaderNode(test_cluster);
    if (leader)
    {
        LOG_INFO("✓ Found leader: %s", leader->getId().c_str());
    }
    else
    {
        LOG_INFO("✗ No leader found");
    }
}

void demonstrateClusterStatistics()
{
    LOG_INFO("=== Cluster Statistics Demonstration ===");

    // Create a realistic cluster scenario
    std::vector<std::shared_ptr<NodeInfo>> cluster_nodes;

    // Add various types of nodes
    const std::vector<std::tuple<std::string, NodeState, NodeRole>> node_configs = {
        {"leader_node", NodeState::ACTIVE, NodeRole::LEADER},
        {"follower_1", NodeState::ACTIVE, NodeRole::FOLLOWER},
        {"follower_2", NodeState::ACTIVE, NodeRole::FOLLOWER},
        {"follower_3", NodeState::JOINING, NodeRole::FOLLOWER},
        {"observer_1", NodeState::ACTIVE, NodeRole::OBSERVER},
        {"failed_node", NodeState::FAILED, NodeRole::FOLLOWER},
        {"leaving_node", NodeState::LEAVING, NodeRole::FOLLOWER}};

    for (const auto &[id, state, role] : node_configs)
    {
        auto node = std::make_shared<NodeInfo>(id, "127.0.0.1", 8000);
        node->setState(state);
        node->setRole(role);

        // Add some metadata
        node->setMetadata("datacenter", "us-west-1");
        node->setMetadata("version", "1.0.0");

        // Simulate some heartbeat activity
        for (int i = 0; i < 10; ++i)
        {
            node->incrementHeartbeatsSent();
            if (state == NodeState::ACTIVE && i < 9)
            { // 90% success rate for active nodes
                node->incrementHeartbeatsReceived();
                node->recordLatency(std::chrono::milliseconds(50 + i * 5));
            }
        }

        cluster_nodes.push_back(node);
    }

    // Calculate statistics
    const auto cluster_start = std::chrono::system_clock::now() - std::chrono::hours(1);
    const auto stats = MembershipUtils::calculateStats(cluster_nodes, cluster_start);

    LOG_INFO("Cluster Statistics:");
    LOG_INFO("  Total nodes: %zu", stats.total_nodes);
    LOG_INFO("  Active nodes: %zu", stats.active_nodes);
    LOG_INFO("  Failed nodes: %zu", stats.failed_nodes);
    LOG_INFO("  Joining nodes: %zu", stats.joining_nodes);
    LOG_INFO("  Leaving nodes: %zu", stats.leaving_nodes);
    LOG_INFO("  Cluster uptime: %ldms", stats.cluster_uptime.count());

    LOG_INFO("Role distribution:");
    for (const auto &[role, count] : stats.role_counts)
    {
        LOG_INFO("  %s: %zu", nodeRoleToString(role).c_str(), count);
    }

    // Calculate additional metrics
    const auto active_percentage = (static_cast<double>(stats.active_nodes) / stats.total_nodes) * 100.0;
    const auto has_quorum = MembershipUtils::hasQuorum(stats.active_nodes, stats.total_nodes);

    LOG_INFO("Cluster health metrics:");
    LOG_INFO("  Active percentage: %.1f%%", active_percentage);
    LOG_INFO("  Has quorum: %s", has_quorum ? "yes" : "no");
    LOG_INFO("  Is healthy: %s", MembershipUtils::isClusterHealthy(cluster_nodes) ? "yes" : "no");

    // Show individual node health
    LOG_INFO("Individual node status:");
    for (const auto &node : cluster_nodes)
    {
        const auto success_rate = node->getHeartbeatsSent() > 0 ? (static_cast<double>(node->getHeartbeatsReceived()) / node->getHeartbeatsSent()) * 100.0 : 0.0;

        LOG_INFO("  %s: %s/%s, %.0f%% success, %ldms avg latency",
                 node->getId().c_str(),
                 nodeStateToString(node->getState()).c_str(),
                 nodeRoleToString(node->getRole()).c_str(),
                 success_rate,
                 node->getAverageLatency().count());
    }
}

int main()
{
    try
    {
        printBanner();

        LOG_INFO("Starting Distributed Database");
        LOG_INFO("Node Discovery and Cluster Formation demonstration");

        // Demonstrate node information management
        demonstrateNodeInfo();

        // Demonstrate node registry operations
        demonstrateNodeRegistry();

        // Demonstrate single node cluster
        demonstrateSingleNodeCluster();

        // Demonstrate membership events
        demonstrateMembershipEvents();

        // Demonstrate membership utilities
        demonstrateMembershipUtilities();

        // Demonstrate cluster statistics
        demonstrateClusterStatistics();

        LOG_INFO("✓ Node information system implemented");
        LOG_INFO("✓ Cluster membership management working");
        LOG_INFO("✓ Event-driven architecture established");
        LOG_INFO("✓ Membership utilities and statistics functional");
        LOG_INFO("✓ Foundation ready for consensus algorithms");
    }
    catch (const std::exception &e)
    {
        LOG_ERROR("Application error: %s", e.what());
        return 1;
    }

    return 0;
}