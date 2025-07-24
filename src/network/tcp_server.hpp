#ifndef __TCP_SERVER_HPP__
#define __TCP_SERVER_HPP__

#include "../common/types.hpp"
#include "message.hpp"
#include <atomic>
#include <mutex>
#include <vector>
#include <memory>
#include <thread>
#include <condition_variable>
#include <functional>
#include <unordered_map>

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

        socket_t getSocket() const noexcept { return _socket; }
        const std::string &getRemoteAddress() const noexcept { return _remote_address; }
        std::uint64_t getConnectionId() const noexcept { return _connection_id; }
        Timestamp getConnectedAt() const noexcept { return _connected_at; }

        Status sendMessage(const Message &message);

        Result<std::unique_ptr<Message>> receiveMessage();

        void close();

        bool isConnected() const noexcept { return _connected; }

    private:
        socket_t _socket;
        std::string _remote_address;
        std::uint64_t _connection_id;
        Timestamp _connected_at;
        std::atomic<bool> _connected;
        mutable std::mutex _send_mutex;

        static std::atomic<std::uint64_t> _next_connection_id;

        Status sendBytes(const std::vector<std::uint8_t> &data);
        Result<std::vector<std::uint8_t>> receiveBytes(std::size_t size);
        Result<std::vector<std::uint8_t>> receiveExactBytes(std::size_t size);
    };

    using MessageHandler = std::function<std::unique_ptr<Message>(const Message &, const Connection &)>;

    class TcpServer
    {
    public:
        explicit TcpServer(Port port);
        explicit TcpServer(Port port, const std::string &bind_address);
        ~TcpServer();

        TcpServer(const TcpServer &) = delete;
        TcpServer &operator=(const TcpServer &) = delete;
        TcpServer(TcpServer &&) = default;
        TcpServer &operator=(TcpServer &&) = default;

        Status start();
        void stop();
        bool isRunning() const noexcept { return _running; }

        void setMessageHandler(MessageHandler handler) { _message_handler = std::move(handler); }
        void setStorageEngine(std::shared_ptr<StorageEngine> storage) { _storage_engine = std::move(storage); }

        Port getPort() const noexcept { return _port; }
        const std::string &getBindAddress() const noexcept { return _bind_address; }
        std::size_t getConnectionCount() const;
        std::vector<std::uint64_t> getActiveConnections() const;

        void setMaxConnections(std::size_t max_connections) noexcept { _max_connections = max_connections; }
        void setReceiveTimeout(std::chrono::milliseconds timeout) noexcept { _receive_timeout = timeout; }

    private:
        Port _port;
        std::string _bind_address;
        socket_t _server_socket;
        std::atomic<bool> _running;
        std::atomic<bool> _should_stop;

        std::unique_ptr<std::thread> _accept_thread;
        std::vector<std::unique_ptr<std::thread>> _worker_threads;
        mutable std::mutex _connections_mutex;
        std::condition_variable _stop_condition;

        std::unordered_map<std::uint64_t, std::unique_ptr<Connection>> _active_connections;
        std::size_t _max_connections;
        std::chrono::milliseconds _receive_timeout;

        MessageHandler _message_handler;
        std::shared_ptr<StorageEngine> _storage_engine;

        Status initializeSocket();
        void acceptLoop();
        void handleConnection(std::unique_ptr<Connection> connection);
        void processMessage(const std::unique_ptr<Message> &request, Connection &connection);
        void cleanupConnections();

        std::unique_ptr<Message> handleGetRequest(const GetRequestMessage &request);
        std::unique_ptr<Message> handlePutRequest(const PutRequestMessage &request);
        std::unique_ptr<Message> handleDeleteRequest(const DeleteRequestMessage &request);
        std::unique_ptr<Message> handleHeartbeat(const HeartbeatMessage &request);

        std::string getSocketAddress(socket_t socket) const;
        void closeSocket(socket_t socket);

        Status setSocketOptions(socket_t socket);
        Status setNonBlocking(socket_t socket, bool non_blocking);
    };

    namespace socket_utils
    {
        Status initializeNetworking();
        void cleanupNetworking();
        std::string getLastSocketError();
        bool isSocketValid(socket_t socket);
        Status closeSocket(socket_t socket);
    }
} // distributed_db

#endif // __TCP_SERVER_HPP__