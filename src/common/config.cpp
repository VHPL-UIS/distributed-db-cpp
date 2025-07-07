#include "config.h"
#include "logger.h"
#include <fstream>
#include <sstream>

namespace distributed_db
{
    bool Config::loadFromFile(const std::string &filename)
    {
        std::ifstream file(filename);
        if (!file.is_open())
        {
            LOG_ERROR("Failed to open config file: {}", filename);
            return false;
        }

        std::string line;
        while (std::getline(file, line))
        {
            if (line.empty() || line[0] == '#')
            {
                continue;
            }

            const auto pos = line.find('=');
            if (pos == std::string::npos)
            {
                LOG_WARN("Invalid config line: {}", line);
                continue;
            }

            const auto key = line.substr(0, pos);
            const auto value = line.substr(pos + 1);

            try
            {
                if (key == "port")
                {
                    _port = static_cast<Port>(std::stoul(value));
                }
                else if (key == "host")
                {
                    _host = value;
                }
                else if (key == "node_id")
                {
                    _node_id = value;
                }
                else if (key == "replication_factor")
                {
                    _replication_factor = std::stoul(value);
                }
                else if (key == "election_timeout")
                {
                    _election_timeout = std::chrono::milliseconds(std::stoul(value));
                }
                else if (key == "heartbeat_interval")
                {
                    _heartbeat_interval = std::chrono::milliseconds(std::stoul(value));
                }
                else if (key == "data_directory")
                {
                    _data_directory = value;
                }
                else if (key == "seeds")
                {
                    _seeds.clear();
                    std::stringstream ss(value);
                    std::string seed;
                    while (std::getline(ss, seed, ','))
                    {
                        // Trim whitespace
                        seed.erase(0, seed.find_first_not_of(" \t"));
                        seed.erase(seed.find_last_not_of(" \t") + 1);
                        if (!seed.empty())
                        {
                            _seeds.push_back(seed);
                        }
                    }
                }
                else
                {
                    LOG_WARN("Unknown config key: {}", key);
                }
            }
            catch (const std::exception &e)
            {
                LOG_ERROR("Error parsing config value for key '{}': {}", key, e.what());
                return false;
            }
        }

        LOG_INFO("Configuration loaded from: {}", filename);
        return true;
    }

    bool Config::saveToFile(const std::string &filename) const
    {
        std::ofstream file(filename);
        if (!file.is_open())
        {
            LOG_ERROR("Failed to create config file: {}", filename);
            return false;
        }

        file << "# Distributed Database Configuration\n";
        file << "# Generated automatically\n\n";

        file << "# Server Configuration\n";
        file << "port=" << _port << "\n";
        file << "host=" << _host << "\n";
        file << "node_id=" << _node_id << "\n\n";

        file << "# Cluster Configuration\n";
        file << "replication_factor=" << _replication_factor << "\n";

        if (!_seeds.empty())
        {
            file << "seeds=";
            for (std::size_t i = 0; i < _seeds.size(); ++i)
            {
                if (i > 0)
                    file << ",";
                file << _seeds[i];
            }
            file << "\n";
        }
        file << "\n";

        file << "# Raft Configuration\n";
        file << "election_timeout=" << _election_timeout.count() << "\n";
        file << "heartbeat_interval=" << _heartbeat_interval.count() << "\n\n";

        file << "# Storage Configuration\n";
        file << "data_directory=" << _data_directory << "\n";

        if (!file.good())
        {
            LOG_ERROR("Error writing to config file: {}", filename);
            return false;
        }

        LOG_INFO("Configuration saved to: {}", filename);
        return true;
    }
} // namespace distributed_db