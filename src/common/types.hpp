#ifndef __TYPES_HPP__
#define __TYPES_HPP__

#include <string>
#include <chrono>
#include <optional>
#include <stdexcept>

namespace distributed_db
{
    using NodeId = std::string;
    using Key = std::string;
    using Value = std::string;
    using Term = uint64_t;
    using Index = uint64_t;
    using Timestamp = std::chrono::system_clock::time_point;

    using Port = uint16_t;
    using IPAddress = std::string;

    enum class Status
    {
        OK,
        NOT_FOUND,
        NETWORK_ERROR,
        TIMEOUT,
        INTERNAL_ERROR,
        INVALID_REQUEST
    };

    template <typename T>
    class Result
    {
    public:
        Result(T value) : _value(std::move(value)), _status(Status::OK)
        {
        }
        Result(Status status) : _status(status)
        {
        }

        bool ok() const { return _status == Status::OK; }
        Status status() const { return _status; }

        const T &value() const
        {
            if (!ok())
            {
                throw std::runtime_error("Accessing value of failed result");
            }
            return _value.value();
        }

        T &value()
        {
            if (!ok())
            {
                throw std::runtime_error("Accessing value of failed result");
            }
            return _value.value();
        }

    private:
        std::optional<T> _value;
        Status _status;
    };

    struct NodeInfo
    {
        NodeId id;
        IPAddress ip;
        Port port;
        Timestamp last_heartbeat;

        NodeInfo(NodeId id, IPAddress ip, Port port) : id(std::move(id)), ip(std::move(ip)), port(port), last_heartbeat(std::chrono::system_clock::now()) {}
    };
} // namespace distributed_db

#endif // __TYPES_HPP__