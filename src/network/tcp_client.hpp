#ifndef __TCP_CLIENT_HP__
#define __TCP_CLIENT_HP__

#include "../common/types.hpp"
#include "message.hpp"
#include <future>
#include <memory>
#include <unordered_map>

#ifdef _WIN32
using socket_t = uintptr_t;
#else
using socket_t = int;
#endif

namespace distributed_db
{
    struct PendingRequest
    {
        std::uint32_t message_id;
        std::promise<std::unique_ptr<Message>> promise;
        std::chrono::steady_clock::time_point created_at;

        PendingRequest(std::uint32_t id)
            : message_id(id), created_at(std::chrono::steady_clock::now()) {}
    };

    class TcpClient
    {
    public:
        TcpClient();
        explicit TcpClient(std::chrono::milliseconds connect_timeout);
        ~TcpClient();

        // Non-copyable, moveable
        TcpClient(const TcpClient &) = delete;
        TcpClient &operator=(const TcpClient &) = delete;
        TcpClient(TcpClient &&) = default;
        TcpClient &operator=(TcpClient &&) = default;

        // Connection management
        Status connect(const std::string &host, Port port);
        void disconnect();
        bool isConnected() const noexcept { return _connected; }

        // Synchronous operations
        Result<std::unique_ptr<Message>> sendRequest(const Message &request);
        Result<std::unique_ptr<Message>> sendRequest(const Message &request,
                                                     std::chrono::milliseconds timeout);

        // Asynchronous operations
        std::future<std::unique_ptr<Message>> sendRequestAsync(const Message &request);
        std::future<std::unique_ptr<Message>> sendRequestAsync(const Message &request,
                                                               std::chrono::milliseconds timeout);

        // High-level database operations
        Result<Value> get(const Key &key);
        Result<Value> get(const Key &key, std::chrono::milliseconds timeout);
        Status put(const Key &key, const Value &value);
        Status put(const Key &key, const Value &value, std::chrono::milliseconds timeout);
        Status remove(const Key &key);
        Status remove(const Key &key, std::chrono::milliseconds timeout);

        // Heartbeat/ping
        Status ping();
        Status ping(std::chrono::milliseconds timeout);

        // Configuration
        void setDefaultTimeout(std::chrono::milliseconds timeout) noexcept { _default_timeout = timeout; }
        void setConnectTimeout(std::chrono::milliseconds timeout) noexcept { _connect_timeout = timeout; }
        void setRetryCount(int count) noexcept { _retry_count = count; }
        void setRetryDelay(std::chrono::milliseconds delay) noexcept { _retry_delay = delay; }

        // Connection info
        const std::string &getHost() const noexcept { return _host; }
        Port getPort() const noexcept { return _port; }
        std::chrono::milliseconds getDefaultTimeout() const noexcept { return _default_timeout; }

    private:
        socket_t _socket;
        std::string _host;
        Port _port;
        std::atomic<bool> _connected;
        std::atomic<bool> _should_stop;

        std::unique_ptr<std::thread> _receive_thread;
        mutable std::mutex _socket_mutex;
        mutable std::mutex _pending_requests_mutex;

        std::unordered_map<std::uint32_t, std::unique_ptr<PendingRequest>> _pending_requests;

        std::chrono::milliseconds _default_timeout;
        std::chrono::milliseconds _connect_timeout;
        int _retry_count;
        std::chrono::milliseconds _retry_delay;

        Status initializeSocket();
        void receiveLoop();
        void handleResponse(std::unique_ptr<Message> response);
        void cleanupPendingRequests();
        void timeoutExpiredRequests();

        Status sendMessage(const Message &message);
        Result<std::unique_ptr<Message>> receiveMessage();
        Status sendBytes(const std::vector<std::uint8_t> &data);
        Result<std::vector<std::uint8_t>> receiveBytes(std::size_t size);
        Result<std::vector<std::uint8_t>> receiveExactBytes(std::size_t size);

        std::uint32_t generateMessageId();
        Status connectWithRetry(const std::string &host, Port port);
        void closeSocket();

        template <typename RequestType, typename ResponseType>
        Result<ResponseType> performRequest(const RequestType &request,
                                            std::chrono::milliseconds timeout);
    };

    class ConnectionPool
    {
    public:
        explicit ConnectionPool(std::size_t pool_size = 5);
        ~ConnectionPool();

        ConnectionPool(const ConnectionPool &) = delete;
        ConnectionPool &operator=(const ConnectionPool &) = delete;
        ConnectionPool(ConnectionPool &&) = default;
        ConnectionPool &operator=(ConnectionPool &&) = default;

        Status addServer(const std::string &host, Port port);
        void removeServer(const std::string &host, Port port);
        void clear();

        Result<Value> get(const Key &key);
        Status put(const Key &key, const Value &value);
        Status remove(const Key &key);

        void setDefaultTimeout(std::chrono::milliseconds timeout);
        void setMaxConnections(std::size_t max_connections) { _max_connections = max_connections; }

        std::size_t getActiveConnections() const;
        std::size_t getTotalServers() const;

    private:
        struct ServerInfo
        {
            std::string host;
            Port port;
            std::vector<std::unique_ptr<TcpClient>> connections;
            std::atomic<std::size_t> round_robin_index;

            ServerInfo(std::string h, Port p) : host(std::move(h)), port(p), round_robin_index(0) {}
        };

        std::vector<std::unique_ptr<ServerInfo>> _servers;
        mutable std::mutex _pool_mutex;
        std::size_t _max_connections;
        std::chrono::milliseconds _default_timeout;

        TcpClient *getConnection();
        void returnConnection(TcpClient *client);
        TcpClient *createConnection(ServerInfo &server);
    };
} // distributed_db

#endif // __TCP_CLIENT_HP__