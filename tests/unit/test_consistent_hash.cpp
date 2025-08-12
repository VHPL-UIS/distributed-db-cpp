#include <gtest/gtest.h>
#include "cluster/consistent_hash.hpp"
#include <unordered_map>
#include <random>

using namespace distributed_db;

TEST(ConsistentHash, BasicAddRemove)
{
    ConsistentHashRing ring(50, 2);
    ring.addNode("nodeA");
    ring.addNode("nodeB");
    ring.addNode("nodeC");

    EXPECT_EQ(ring.size(), 3u);
    EXPECT_GE(ring.ringSize(), 150u); // 3 * 50 virtual nodes

    // keys map to some node
    auto n1 = ring.getPrimaryNodeForKey("key1");
    EXPECT_FALSE(n1.empty());
    auto nodes = ring.getNodesForKey("key1");
    EXPECT_GE(nodes.size(), 1u);
    EXPECT_LE(nodes.size(), 2u);

    // remove a node and verify size changes
    ring.removeNode("nodeB");
    EXPECT_EQ(ring.size(), 2u);
}

TEST(ConsistentHash, DistributionAndReplication)
{
    const std::size_t virtual_nodes = 100;
    const std::size_t replication = 3;
    ConsistentHashRing ring(virtual_nodes, replication);

    ring.addNode("n1");
    ring.addNode("n2");
    ring.addNode("n3");
    ring.addNode("n4");

    const std::size_t key_count = 2000;
    std::unordered_map<NodeId, std::size_t> counts;
    counts["n1"] = counts["n2"] = counts["n3"] = counts["n4"] = 0;

    for (std::size_t i = 0; i < key_count; ++i)
    {
        std::string key = "key_" + std::to_string(i);
        auto primary = ring.getPrimaryNodeForKey(key);
        ASSERT_FALSE(primary.empty());
        counts[primary]++;
        auto nodes = ring.getNodesForKey(key);
        // replication should return distinct nodes
        std::unordered_set<NodeId> set(nodes.begin(), nodes.end());
        EXPECT_EQ(set.size(), nodes.size());
        EXPECT_EQ(nodes.size(), replication);
    }

    // distribution should be reasonably balanced: allow +-50% tolerance
    std::size_t max_count = 0, min_count = SIZE_MAX;
    for (auto &p : counts)
    {
        max_count = std::max(max_count, p.second);
        min_count = std::min(min_count, p.second);
    }
    EXPECT_LT(max_count, min_count * 3); // rough check: no node > 3x of smallest
}

TEST(ConsistentHash, KeyMovementOnNodeRemoval)
{
    ConsistentHashRing ring(80, 2);
    ring.addNode("A");
    ring.addNode("B");
    ring.addNode("C");

    const std::size_t key_count = 2000;
    std::vector<Key> keys;
    keys.reserve(key_count);
    for (std::size_t i = 0; i < key_count; ++i)
        keys.push_back("k" + std::to_string(i));

    std::unordered_map<Key, NodeId> before;
    for (const auto &k : keys)
        before[k] = ring.getPrimaryNodeForKey(k);

    // remove node B
    ring.removeNode("B");

    std::size_t moved = 0;
    for (const auto &k : keys)
    {
        auto after = ring.getPrimaryNodeForKey(k);
        if (after != before[k])
            ++moved;
    }

    double moved_ratio = static_cast<double>(moved) / key_count;
    // roughly, removing 1 of 3 nodes should move about 1/3 of keys; allow a generous bound
    EXPECT_LT(moved_ratio, 0.8);
    EXPECT_GT(moved_ratio, 0.05); // ensure some movement happened
}
