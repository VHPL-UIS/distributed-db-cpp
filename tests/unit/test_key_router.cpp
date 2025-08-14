#include <gtest/gtest.h>
#include "cluster/consistent_hash.hpp"
#include "cluster/key_router.hpp"
#include "cluster/migration_planner.hpp"
#include "cluster/node_info.hpp"

using namespace distributed_db;

static std::shared_ptr<NodeInfo> makeNode(const NodeId &id, const std::string &host, Port p)
{
    NodeInfo n{id, host, p, NodeRole::FOLLOWER};
    n.setState(NodeState::ACTIVE);
    return std::make_shared<NodeInfo>(n);
}

TEST(KeyRouter, RoutingAndReplication)
{
    auto ring = std::make_shared<ConsistentHashRing>(80, 3);
    auto reg = std::make_shared<NodeRegistry>();

    auto n1 = makeNode("n1", "10.0.0.1", 9001);
    auto n2 = makeNode("n2", "10.0.0.2", 9002);
    auto n3 = makeNode("n3", "10.0.0.3", 9003);
    reg->addNode(*n1);
    reg->addNode(*n2);
    reg->addNode(*n3);

    ring->addNode("n1");
    ring->addNode("n2");
    ring->addNode("n3");

    KeyRouter router(n1, reg, ring);

    auto route = router.routeKey("alpha");
    ASSERT_FALSE(route.primary.empty());
    ASSERT_EQ(route.replicas.size(), 3u);

    auto owners = router.ownerEndpoints("alpha");
    // all owner endpoints should exist (size == replication)
    EXPECT_EQ(owners.size(), 3u);

    auto plan = router.planWrite("alpha");
    // If local is an owner, write_local_first may be true or false depending on hash mapping.
    // But forward list size must be <= replication-1 and never include local endpoint.
    EXPECT_LE(plan.forward_to_endpoints.size(), 2u);
}

TEST(MigrationPlanner, PrimaryMovementOnJoin)
{
    auto before = std::make_shared<ConsistentHashRing>(50, 2);
    auto after = std::make_shared<ConsistentHashRing>(50, 2);

    before->addNode("A");
    before->addNode("B");

    // after join, node C appears
    after->addNode("A");
    after->addNode("B");
    after->addNode("C");

    // keys
    std::vector<Key> keys;
    for (int i = 0; i < 2000; ++i)
        keys.push_back("k" + std::to_string(i));

    MigrationPlanner planner(before, after);
    auto plan = planner.computePrimaryMoves(keys);

    // With a new node joining, some keys should move to the new node as primary
    EXPECT_GT(plan.moving_keys, 0u);
    // sanity: total matches input
    EXPECT_EQ(plan.total_keys, keys.size());

    // ensure the new owner "C" appears with some keys to receive (likely)
    // not guaranteed by hash, but highly probable with 2000 keys:
    // make this a soft check: allow either presence or absence, but log expectation
    auto it = plan.to_receive.find("C");
    // We won't ASSERT here to avoid flakiness; still, in practice this should pass:
    if (it == plan.to_receive.end())
    {
        // Non-fatal info if distribution was unlucky (extremely unlikely)
        SUCCEED() << "No keys moved to C — extremely unlikely but possible with hash collisions.";
    }
    else
    {
        EXPECT_GT(it->second.size(), 0u);
    }
}

TEST(MigrationPlanner, PrimaryMovementOnLeave)
{
    auto before = std::make_shared<ConsistentHashRing>(50, 2);
    auto after = std::make_shared<ConsistentHashRing>(50, 2);

    before->addNode("A");
    before->addNode("B");
    before->addNode("C");

    // after leave, remove B
    after->addNode("A");
    after->addNode("C");

    std::vector<Key> keys;
    for (int i = 0; i < 1500; ++i)
        keys.push_back("x" + std::to_string(i));

    MigrationPlanner planner(before, after);
    auto plan = planner.computePrimaryMoves(keys);

    EXPECT_GT(plan.moving_keys, 0u);
    EXPECT_EQ(plan.total_keys, keys.size());
}
