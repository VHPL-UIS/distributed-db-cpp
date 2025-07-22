#ifndef __TCP_SERVER_HPP__
#define __TCP_SERVER_HPP__

#include "../common/types.hpp"
#include "message.hpp"
#include <atomic>
#include <mutex>
#include <vector>
#include <memory>

#ifdef _WIN32
using socket_t = uintptr_t;
#else
using socket_t = int;
#endif

namespace distributed_db
{
    class StorageEngine;

    class Connection
    {
    public:
        Connection(socket_t socket, const std::string &remote_address);
        ~Connection();

        Connection(const Connection &) = delete;
        Connection &operator=(const Connection &) = delete;
        Connection(Connection &&) = default;
        Connection &operator=(Connection &&) = default;

        [[nodiscard]] socket_t getSocket() const noexcept { return _socket; }
        [[nodiscard]] const std::string &getRemoteAddress() const noexcept { return _remote_address; }
        [[nodiscard]] std::uint64_t getConnectionId() const noexcept { return _connection_id; }
        [[nodiscard]] Timestamp getConnectedAt() const noexcept { return _connected_at; }

        [[nodiscard]] Status sendMessage(const Message &message);

        [[nodiscard]] Result<std::unique_ptr<Message>> receiveMessage();

        void close();

        [[nodiscard]] bool isConnected() const noexcept { return _connected; }

    private:
        socket_t _socket;
        std::string _remote_address;
        std::uint64_t _connection_id;
        Timestamp _connected_at;
        std::atomic<bool> _connected;
        mutable std::mutex _send_mutex;

        static std::atomic<std::uint64_t> _next_connection_id;

        [[nodiscard]] Status sendBytes(const std::vector<std::uint8_t> &data);
        [[nodiscard]] Result<std::vector<std::uint8_t>> receiveBytes(std::size_t size);
        [[nodiscard]] Result<std::vector<std::uint8_t>> receiveExactBytes(std::size_t size);
    };
} // distributed_db

#endif // __TCP_SERVER_HPP__