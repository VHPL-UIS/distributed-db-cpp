#ifndef __CONFIG_HPP__
#define __CONFIG_HPP__

#include "types.hpp"
#include <string>
#include <vector>

namespace distributed_db
{
    class Config
    {
    public:
        static Config &getInstance()
        {
            static Config instance;
            return instance;
        }

        Port getPort() const { return _port; }
        void setPort(Port port) { _port = port; }

        std::string getHost() const { return _host; }
        void setHost(const std::string &host) { _host = host; }

        NodeId getNodeId() const { return _node_id; }
        void setNodeId(const NodeId &id) { _node_id = id; }

        std::vector<std::string> getSeeds() const { return _seeds; }
        void setSeeds(const std::vector<std::string> &seeds) { _seeds = seeds; }

        size_t getReplicationFactor() const { return _replication_factor; }
        void setReplicationFactor(size_t factor) { _replication_factor = factor; }

        std::chrono::milliseconds getElectionTimeout() const { return _election_timeout; }
        void setElectionTimeout(std::chrono::milliseconds timeout) { _election_timeout = timeout; }

        std::chrono::milliseconds getHeartbeatInterval() const { return _heartbeat_interval; }
        void setHeartbeatInterval(std::chrono::milliseconds interval) { _heartbeat_interval = interval; }

        std::string getDataDirectory() const { return _data_directory; }
        void setDataDirectory(const std::string &dir) { _data_directory = dir; }

        bool loadFromFile(const std::string &filename);
        bool saveToFile(const std::string &filename) const;

    private:
        Config() = default;

        Port _port = 8080;
        std::string _host = "localhost";
        NodeId _node_id = "node_1";

        std::vector<std::string> _seeds;
        size_t _replication_factor = 3;

        std::chrono::milliseconds _election_timeout{5000};
        std::chrono::milliseconds _heartbeat_interval{1000};

        std::string _data_directory = "./data";
    };
} // distributed_db

#endif // __CONFIG_HPP__