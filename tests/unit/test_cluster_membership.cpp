#include <gtest/gtest.h>
#include "cluster/node_info.hpp"
#include "cluster/cluster_membership.hpp"
#include <thread>
#include <chrono>

using namespace distributed_db;

class ClusterMembershipTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        _local_node = std::make_shared<NodeInfo>("test_node_1", "127.0.0.1", 8080);
        _membership = std::make_unique<ClusterMembership>(_local_node);
    }

    void TearDown() override
    {
        if (_membership)
        {
            _membership->stop();
        }
        _membership.reset();
    }

    std::shared_ptr<NodeInfo> _local_node;
    std::unique_ptr<ClusterMembership> _membership;
};

TEST_F(ClusterMembershipTest, NodeInfoBasicOperations)
{
    NodeInfo node("test_node", "192.168.1.100", 9090);

    EXPECT_EQ(node.getId(), "test_node");
    EXPECT_EQ(node.getAddress(), "192.168.1.100");
    EXPECT_EQ(node.getPort(), 9090);
    EXPECT_EQ(node.getEndpoint(), "192.168.1.100:9090");
    EXPECT_EQ(node.getState(), NodeState::UNKNOWN);
    EXPECT_EQ(node.getRole(), NodeRole::FOLLOWER);
}

TEST_F(ClusterMembershipTest, NodeInfoStateManagement)
{
    NodeInfo node("state_test_node", "127.0.0.1", 8080);

    // Test state transitions
    node.setState(NodeState::JOINING);
    EXPECT_EQ(node.getState(), NodeState::JOINING);

    node.setState(NodeState::ACTIVE);
    EXPECT_EQ(node.getState(), NodeState::ACTIVE);
    EXPECT_TRUE(node.isActive());

    node.setState(NodeState::SUSPECT);
    EXPECT_EQ(node.getState(), NodeState::SUSPECT);
    EXPECT_FALSE(node.isActive());

    // Test role changes
    node.setRole(NodeRole::LEADER);
    EXPECT_EQ(node.getRole(), NodeRole::LEADER);
    EXPECT_TRUE(node.canVote());

    node.setRole(NodeRole::OBSERVER);
    EXPECT_EQ(node.getRole(), NodeRole::OBSERVER);
    EXPECT_FALSE(node.canVote());
}

TEST_F(ClusterMembershipTest, NodeInfoHealthCheck)
{
    NodeInfo node("health_test_node", "127.0.0.1", 8080);

    // Node should be healthy immediately after creation
    EXPECT_TRUE(node.isHealthy(std::chrono::milliseconds(1000)));

    // Simulate passage of time by sleeping
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Should still be healthy with reasonable timeout
    EXPECT_TRUE(node.isHealthy(std::chrono::milliseconds(1000)));

    // Should be unhealthy with very short timeout
    EXPECT_FALSE(node.isHealthy(std::chrono::milliseconds(1)));
}

TEST_F(ClusterMembershipTest, NodeInfoMetadata)
{
    NodeInfo node("metadata_test_node", "127.0.0.1", 8080);

    // Test metadata operations
    EXPECT_FALSE(node.hasMetadata("test_key"));
    EXPECT_TRUE(node.getMetadata("test_key").empty());

    node.setMetadata("test_key", "test_value");
    EXPECT_TRUE(node.hasMetadata("test_key"));
    EXPECT_EQ(node.getMetadata("test_key"), "test_value");

    node.setMetadata("another_key", "another_value");
    EXPECT_EQ(node.getMetadata("another_key"), "another_value");

    node.removeMetadata("test_key");
    EXPECT_FALSE(node.hasMetadata("test_key"));
    EXPECT_TRUE(node.hasMetadata("another_key"));
}

TEST_F(ClusterMembershipTest, NodeInfoStatistics)
{
    NodeInfo node("stats_test_node", "127.0.0.1", 8080);

    // Initial statistics
    EXPECT_EQ(node.getHeartbeatsSent(), 0);
    EXPECT_EQ(node.getHeartbeatsReceived(), 0);
    EXPECT_EQ(node.getAverageLatency(), std::chrono::milliseconds(0));

    // Update statistics
    node.incrementHeartbeatsSent();
    node.incrementHeartbeatsSent();
    EXPECT_EQ(node.getHeartbeatsSent(), 2);

    node.incrementHeartbeatsReceived();
    EXPECT_EQ(node.getHeartbeatsReceived(), 1);

    // Record latency
    node.recordLatency(std::chrono::milliseconds(100));
    node.recordLatency(std::chrono::milliseconds(200));
    EXPECT_EQ(node.getAverageLatency(), std::chrono::milliseconds(150));
}

TEST_F(ClusterMembershipTest, NodeInfoSerialization)
{
    NodeInfo original("serial_test_node", "192.168.1.1", 9999, NodeRole::LEADER);
    original.setState(NodeState::ACTIVE);
    original.setMetadata("version", "1.0.0");
    original.incrementHeartbeatsSent();
    original.recordLatency(std::chrono::milliseconds(50));

    // Serialize
    const auto serialized_result = original.serialize();
    ASSERT_TRUE(serialized_result.ok());

    // Deserialize
    NodeInfo deserialized;
    const auto deserialize_status = deserialized.deserialize(serialized_result.value());
    EXPECT_EQ(deserialize_status, Status::OK);

    // Verify deserialized data
    EXPECT_EQ(deserialized.getId(), "serial_test_node");
    EXPECT_EQ(deserialized.getAddress(), "192.168.1.1");
    EXPECT_EQ(deserialized.getPort(), 9999);
    EXPECT_EQ(deserialized.getState(), NodeState::ACTIVE);
    EXPECT_EQ(deserialized.getRole(), NodeRole::LEADER);
    EXPECT_EQ(deserialized.getMetadata("version"), "1.0.0");
    EXPECT_EQ(deserialized.getHeartbeatsSent(), 1);
}

TEST_F(ClusterMembershipTest, NodeRegistryOperations)
{
    NodeRegistry registry;

    EXPECT_TRUE(registry.empty());
    EXPECT_EQ(registry.size(), 0);

    // Add nodes
    NodeInfo node1("node_1", "127.0.0.1", 8081);
    NodeInfo node2("node_2", "127.0.0.1", 8082);
    NodeInfo node3("node_3", "127.0.0.1", 8083);

    registry.addNode(node1);
    registry.addNode(node2);
    registry.addNode(std::move(node3));

    EXPECT_FALSE(registry.empty());
    EXPECT_EQ(registry.size(), 3);
    EXPECT_TRUE(registry.contains("node_1"));
    EXPECT_TRUE(registry.contains("node_2"));
    EXPECT_TRUE(registry.contains("node_3"));
    EXPECT_FALSE(registry.contains("non_existent"));

    // Get node
    const auto retrieved_node = registry.getNode("node_1");
    ASSERT_NE(retrieved_node, nullptr);
    EXPECT_EQ(retrieved_node->getId(), "node_1");

    // Remove node
    EXPECT_TRUE(registry.removeNode("node_2"));
    EXPECT_FALSE(registry.removeNode("non_existent"));
    EXPECT_EQ(registry.size(), 2);
    EXPECT_FALSE(registry.contains("node_2"));
}

TEST_F(ClusterMembershipTest, NodeRegistryFiltering)
{
    NodeRegistry registry;

    // Add nodes with different states and roles
    NodeInfo active_follower("active_follower", "127.0.0.1", 8081, NodeRole::FOLLOWER);
    active_follower.setState(NodeState::ACTIVE);

    NodeInfo active_leader("active_leader", "127.0.0.1", 8082, NodeRole::LEADER);
    active_leader.setState(NodeState::ACTIVE);

    NodeInfo failed_node("failed_node", "127.0.0.1", 8083);
    failed_node.setState(NodeState::FAILED);

    NodeInfo observer_node("observer", "127.0.0.1", 8084, NodeRole::OBSERVER);
    observer_node.setState(NodeState::ACTIVE);

    registry.addNode(active_follower);
    registry.addNode(active_leader);
    registry.addNode(failed_node);
    registry.addNode(observer_node);

    // Test state filtering
    const auto active_nodes = registry.getActiveNodes();
    EXPECT_EQ(active_nodes.size(), 3); // active_follower, active_leader, observer

    const auto failed_nodes = registry.getNodesByState(NodeState::FAILED);
    EXPECT_EQ(failed_nodes.size(), 1);
    EXPECT_EQ(failed_nodes[0]->getId(), "failed_node");

    // Test role filtering
    const auto leaders = registry.getNodesByRole(NodeRole::LEADER);
    EXPECT_EQ(leaders.size(), 1);
    EXPECT_EQ(leaders[0]->getId(), "active_leader");

    const auto observers = registry.getNodesByRole(NodeRole::OBSERVER);
    EXPECT_EQ(observers.size(), 1);
    EXPECT_EQ(observers[0]->getId(), "observer");
}

TEST_F(ClusterMembershipTest, ClusterMembershipLifecycle)
{
    EXPECT_FALSE(_membership->isRunning());
    EXPECT_EQ(_membership->getClusterSize(), 1); // Only local node

    // Start membership
    const auto start_status = _membership->start();
    EXPECT_EQ(start_status, Status::OK);
    EXPECT_TRUE(_membership->isRunning());

    // Stop membership
    _membership->stop();
    EXPECT_FALSE(_membership->isRunning());
}

TEST_F(ClusterMembershipTest, SingleNodeCluster)
{
    // Join with empty seed list (single node cluster)
    const auto join_status = _membership->joinCluster({});
    EXPECT_EQ(join_status, Status::OK);

    EXPECT_EQ(_membership->getClusterSize(), 1);
    EXPECT_EQ(_membership->getActiveNodeCount(), 1);
    EXPECT_TRUE(_membership->hasQuorum());
    EXPECT_TRUE(_membership->isClusterHealthy());

    const auto local_node = _membership->getLocalNode();
    ASSERT_NE(local_node, nullptr);
    EXPECT_EQ(local_node->getState(), NodeState::ACTIVE);
}

TEST_F(ClusterMembershipTest, NodeManagement)
{
    _membership->start();

    // Add nodes manually
    NodeInfo new_node1("new_node_1", "127.0.0.1", 8081);
    NodeInfo new_node2("new_node_2", "127.0.0.1", 8082);

    const auto add_status1 = _membership->addNode(new_node1);
    EXPECT_EQ(add_status1, Status::OK);
    EXPECT_EQ(_membership->getClusterSize(), 2);

    const auto add_status2 = _membership->addNode(new_node2);
    EXPECT_EQ(add_status2, Status::OK);
    EXPECT_EQ(_membership->getClusterSize(), 3);

    // Verify nodes exist
    const auto retrieved_node = _membership->getNode("new_node_1");
    ASSERT_NE(retrieved_node, nullptr);
    EXPECT_EQ(retrieved_node->getId(), "new_node_1");

    // Update node role
    const auto role_update_status = _membership->updateNodeRole("new_node_1", NodeRole::LEADER);
    EXPECT_EQ(role_update_status, Status::OK);

    const auto leader_node = _membership->getLeaderNode();
    ASSERT_NE(leader_node, nullptr);
    EXPECT_EQ(leader_node->getId(), "new_node_1");

    // Mark node as failed
    const auto fail_status = _membership->markNodeFailed("new_node_2");
    EXPECT_EQ(fail_status, Status::OK);

    // Remove node
    const auto remove_status = _membership->removeNode("new_node_2");
    EXPECT_EQ(remove_status, Status::OK);
    EXPECT_EQ(_membership->getClusterSize(), 2);
}

TEST_F(ClusterMembershipTest, MembershipEventCallback)
{
    _membership->start();

    std::vector<MembershipEventData> received_events;

    // Set event callback
    _membership->setMembershipEventCallback([&received_events](const MembershipEventData &event)
                                            { received_events.push_back(event); });

    // Add a node (should trigger event)
    NodeInfo new_node("event_test_node", "127.0.0.1", 8081);
    _membership->addNode(new_node);

    // Give some time for event processing
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Verify event was received
    EXPECT_FALSE(received_events.empty());
    const auto &event = received_events.back();
    EXPECT_EQ(event.event, MembershipEvent::NODE_JOINED);
    EXPECT_EQ(event.node_id, "event_test_node");

    // Clear callback
    _membership->clearMembershipEventCallback();

    // Add another node (should not trigger event)
    const auto initial_event_count = received_events.size();
    NodeInfo another_node("no_event_node", "127.0.0.1", 8082);
    _membership->addNode(another_node);

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Event count should not have changed
    EXPECT_EQ(received_events.size(), initial_event_count);
}

TEST_F(ClusterMembershipTest, MembershipUtilities)
{
    // Test endpoint parsing
    const auto endpoint_result = MembershipUtils::parseEndpoint("192.168.1.1:8080");
    ASSERT_TRUE(endpoint_result.ok());

    const auto [host, port] = endpoint_result.value();
    EXPECT_EQ(host, "192.168.1.1");
    EXPECT_EQ(port, 8080);

    // Test invalid endpoints
    EXPECT_FALSE(MembershipUtils::parseEndpoint("invalid").ok());
    EXPECT_FALSE(MembershipUtils::parseEndpoint("host:").ok());
    EXPECT_FALSE(MembershipUtils::parseEndpoint(":8080").ok());
    EXPECT_FALSE(MembershipUtils::parseEndpoint("host:99999").ok());

    // Test endpoint validation
    EXPECT_TRUE(MembershipUtils::isValidEndpoint("localhost:8080"));
    EXPECT_TRUE(MembershipUtils::isValidEndpoint("192.168.1.1:443"));
    EXPECT_FALSE(MembershipUtils::isValidEndpoint("invalid-endpoint"));

    // Test endpoint formatting
    EXPECT_EQ(MembershipUtils::formatEndpoint("127.0.0.1", 9090), "127.0.0.1:9090");

    // Test quorum calculations
    EXPECT_TRUE(MembershipUtils::hasQuorum(3, 5));  // 3 out of 5
    EXPECT_FALSE(MembershipUtils::hasQuorum(2, 5)); // 2 out of 5
    EXPECT_TRUE(MembershipUtils::hasQuorum(1, 1));  // 1 out of 1

    EXPECT_EQ(MembershipUtils::calculateQuorumSize(5), 3);
    EXPECT_EQ(MembershipUtils::calculateQuorumSize(4), 3);
    EXPECT_EQ(MembershipUtils::calculateQuorumSize(3), 2);
    EXPECT_EQ(MembershipUtils::calculateQuorumSize(1), 1);
}

TEST_F(ClusterMembershipTest, UtilityFunctions)
{
    // Test state to string conversion
    EXPECT_EQ(nodeStateToString(NodeState::ACTIVE), "ACTIVE");
    EXPECT_EQ(nodeStateToString(NodeState::JOINING), "JOINING");
    EXPECT_EQ(nodeStateToString(NodeState::FAILED), "FAILED");

    // Test role to string conversion
    EXPECT_EQ(nodeRoleToString(NodeRole::LEADER), "LEADER");
    EXPECT_EQ(nodeRoleToString(NodeRole::FOLLOWER), "FOLLOWER");
    EXPECT_EQ(nodeRoleToString(NodeRole::OBSERVER), "OBSERVER");

    // Test string to state conversion
    EXPECT_EQ(nodeStateFromString("ACTIVE"), NodeState::ACTIVE);
    EXPECT_EQ(nodeStateFromString("JOINING"), NodeState::JOINING);
    EXPECT_EQ(nodeStateFromString("INVALID"), NodeState::UNKNOWN);

    // Test string to role conversion
    EXPECT_EQ(nodeRoleFromString("LEADER"), NodeRole::LEADER);
    EXPECT_EQ(nodeRoleFromString("FOLLOWER"), NodeRole::FOLLOWER);
    EXPECT_EQ(nodeRoleFromString("INVALID"), NodeRole::FOLLOWER);
}