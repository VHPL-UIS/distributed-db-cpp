#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include "common/config.hpp"

using namespace distributed_db;

class ConfigTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Reset config to defaults for each test
        auto &config = Config::getInstance();
        config.setPort(8080);
        config.setHost("localhost");
        config.setNodeId("node_1");
        config.setReplicationFactor(3);
        config.setElectionTimeout(std::chrono::milliseconds(5000));
        config.setHeartbeatInterval(std::chrono::milliseconds(1000));
        config.setDataDirectory("./data");
        config.setSeeds({});
    }

    void TearDown() override
    {
        // Clean up test files
        std::filesystem::remove("test_config.conf");
        std::filesystem::remove("invalid_config.conf");
    }
};

TEST_F(ConfigTest, DefaultValues)
{
    const auto &config = Config::getInstance();

    EXPECT_EQ(config.getPort(), 8080);
    EXPECT_EQ(config.getHost(), "localhost");
    EXPECT_EQ(config.getNodeId(), "node_1");
    EXPECT_EQ(config.getReplicationFactor(), 3);
    EXPECT_EQ(config.getElectionTimeout(), std::chrono::milliseconds(5000));
    EXPECT_EQ(config.getHeartbeatInterval(), std::chrono::milliseconds(1000));
    EXPECT_EQ(config.getDataDirectory(), "./data");
    EXPECT_TRUE(config.getSeeds().empty());
}

TEST_F(ConfigTest, SettersAndGetters)
{
    auto &config = Config::getInstance();

    // Test port
    config.setPort(9090);
    EXPECT_EQ(config.getPort(), 9090);

    // Test host
    config.setHost("192.168.1.100");
    EXPECT_EQ(config.getHost(), "192.168.1.100");

    // Test node ID
    config.setNodeId("test_node_123");
    EXPECT_EQ(config.getNodeId(), "test_node_123");

    // Test replication factor
    config.setReplicationFactor(5);
    EXPECT_EQ(config.getReplicationFactor(), 5);

    // Test election timeout
    config.setElectionTimeout(std::chrono::milliseconds(10000));
    EXPECT_EQ(config.getElectionTimeout(), std::chrono::milliseconds(10000));

    // Test heartbeat interval
    config.setHeartbeatInterval(std::chrono::milliseconds(500));
    EXPECT_EQ(config.getHeartbeatInterval(), std::chrono::milliseconds(500));

    // Test data directory
    config.setDataDirectory("/tmp/test_data");
    EXPECT_EQ(config.getDataDirectory(), "/tmp/test_data");

    // Test seeds
    const std::vector<std::string> seeds = {"node1:8080", "node2:8080", "node3:8080"};
    config.setSeeds(seeds);
    EXPECT_EQ(config.getSeeds(), seeds);
}

TEST_F(ConfigTest, SaveToFile)
{
    auto &config = Config::getInstance();

    // Set some test values
    config.setPort(9999);
    config.setHost("test.example.com");
    config.setNodeId("save_test_node");
    config.setReplicationFactor(7);
    config.setElectionTimeout(std::chrono::milliseconds(15000));
    config.setHeartbeatInterval(std::chrono::milliseconds(750));
    config.setDataDirectory("/opt/distributed_db");
    config.setSeeds({"seed1:8080", "seed2:8080"});

    // Save to file
    const std::string filename = "test_config.conf";
    EXPECT_TRUE(config.saveToFile(filename));

    // Verify file exists and contains expected content
    std::ifstream file(filename);
    ASSERT_TRUE(file.is_open());

    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());

    EXPECT_NE(content.find("port=9999"), std::string::npos);
    EXPECT_NE(content.find("host=test.example.com"), std::string::npos);
    EXPECT_NE(content.find("node_id=save_test_node"), std::string::npos);
    EXPECT_NE(content.find("replication_factor=7"), std::string::npos);
    EXPECT_NE(content.find("election_timeout=15000"), std::string::npos);
    EXPECT_NE(content.find("heartbeat_interval=750"), std::string::npos);
    EXPECT_NE(content.find("data_directory=/opt/distributed_db"), std::string::npos);
    EXPECT_NE(content.find("seeds=seed1:8080,seed2:8080"), std::string::npos);
}

TEST_F(ConfigTest, LoadFromFile)
{
    const std::string filename = "test_config.conf";

    // Create test config file
    std::ofstream file(filename);
    ASSERT_TRUE(file.is_open());

    file << "# Test configuration file\n";
    file << "port=7777\n";
    file << "host=load.test.com\n";
    file << "node_id=load_test_node\n";
    file << "replication_factor=4\n";
    file << "election_timeout=12000\n";
    file << "heartbeat_interval=600\n";
    file << "data_directory=/var/lib/distdb\n";
    file << "seeds=node_a:8080,node_b:8080,node_c:8080\n";
    file << "\n";
    file << "# Comment line\n";
    file.close();

    auto &config = Config::getInstance();

    // Load from file
    EXPECT_TRUE(config.loadFromFile(filename));

    // Verify loaded values
    EXPECT_EQ(config.getPort(), 7777);
    EXPECT_EQ(config.getHost(), "load.test.com");
    EXPECT_EQ(config.getNodeId(), "load_test_node");
    EXPECT_EQ(config.getReplicationFactor(), 4);
    EXPECT_EQ(config.getElectionTimeout(), std::chrono::milliseconds(12000));
    EXPECT_EQ(config.getHeartbeatInterval(), std::chrono::milliseconds(600));
    EXPECT_EQ(config.getDataDirectory(), "/var/lib/distdb");

    const auto seeds = config.getSeeds();
    ASSERT_EQ(seeds.size(), 3);
    EXPECT_EQ(seeds[0], "node_a:8080");
    EXPECT_EQ(seeds[1], "node_b:8080");
    EXPECT_EQ(seeds[2], "node_c:8080");
}

TEST_F(ConfigTest, LoadFromNonExistentFile)
{
    auto &config = Config::getInstance();

    EXPECT_FALSE(config.loadFromFile("non_existent_file.conf"));
}

TEST_F(ConfigTest, LoadInvalidConfigFile)
{
    const std::string filename = "invalid_config.conf";

    // Create invalid config file
    std::ofstream file(filename);
    ASSERT_TRUE(file.is_open());

    file << "invalid_line_without_equals\n";
    file << "port=invalid_number\n";
    file << "valid_setting=valid_value\n";
    file.close();

    auto &config = Config::getInstance();

    // Should return false due to invalid number
    EXPECT_FALSE(config.loadFromFile(filename));
}

TEST_F(ConfigTest, SaveAndLoadRoundTrip)
{
    auto &config = Config::getInstance();

    // Set test values
    const Port test_port = 6543;
    const std::string test_host = "roundtrip.test.com";
    const NodeId test_node_id = "roundtrip_node";
    const std::size_t test_replication = 6;
    const auto test_election_timeout = std::chrono::milliseconds(8000);
    const auto test_heartbeat_interval = std::chrono::milliseconds(400);
    const std::string test_data_dir = "/tmp/roundtrip_test";
    const std::vector<std::string> test_seeds = {"rt1:8080", "rt2:8080"};

    config.setPort(test_port);
    config.setHost(test_host);
    config.setNodeId(test_node_id);
    config.setReplicationFactor(test_replication);
    config.setElectionTimeout(test_election_timeout);
    config.setHeartbeatInterval(test_heartbeat_interval);
    config.setDataDirectory(test_data_dir);
    config.setSeeds(test_seeds);

    // Save to file
    const std::string filename = "test_config.conf";
    EXPECT_TRUE(config.saveToFile(filename));

    // Reset to different values
    config.setPort(1111);
    config.setHost("different.host.com");
    config.setNodeId("different_node");
    config.setReplicationFactor(1);
    config.setElectionTimeout(std::chrono::milliseconds(1000));
    config.setHeartbeatInterval(std::chrono::milliseconds(100));
    config.setDataDirectory("/different/path");
    config.setSeeds({"different:8080"});

    // Load from file and verify original values are restored
    EXPECT_TRUE(config.loadFromFile(filename));

    EXPECT_EQ(config.getPort(), test_port);
    EXPECT_EQ(config.getHost(), test_host);
    EXPECT_EQ(config.getNodeId(), test_node_id);
    EXPECT_EQ(config.getReplicationFactor(), test_replication);
    EXPECT_EQ(config.getElectionTimeout(), test_election_timeout);
    EXPECT_EQ(config.getHeartbeatInterval(), test_heartbeat_interval);
    EXPECT_EQ(config.getDataDirectory(), test_data_dir);
    EXPECT_EQ(config.getSeeds(), test_seeds);
}