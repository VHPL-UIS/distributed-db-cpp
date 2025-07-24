#include "tcp_client.hpp"
#include "../common/logger.hpp"
#include <algorithm>
#include <cstring>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#define INVALID_SOCKET_VALUE INVALID_SOCKET
#define SOCKET_ERROR_VALUE SOCKET_ERROR
#define close_socket closesocket
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#define INVALID_SOCKET_VALUE -1
#define SOCKET_ERROR_VALUE -1
#define close_socket close
#endif

namespace distributed_db
{

    TcpClient::TcpClient() : TcpClient(std::chrono::milliseconds(5000)) {}

    TcpClient::TcpClient(std::chrono::milliseconds connect_timeout)
        : _socket(INVALID_SOCKET_VALUE), _port(0), _connected(false), _should_stop(false),
          _default_timeout(std::chrono::milliseconds(10000)), _connect_timeout(connect_timeout),
          _retry_count(3), _retry_delay(std::chrono::milliseconds(1000)) {}

    TcpClient::~TcpClient()
    {
        disconnect();
    }

    Status TcpClient::connect(const std::string &host, Port port)
    {
        if (_connected)
        {
            disconnect();
        }

        _host = host;
        _port = port;

        const auto status = connectWithRetry(host, port);
        if (status != Status::OK)
        {
            return status;
        }

        _should_stop = false;
        _receive_thread = std::make_unique<std::thread>(&TcpClient::receiveLoop, this);

        LOG_INFO("Connected to %s:%d", host.c_str(), port);
        return Status::OK;
    }

    void TcpClient::disconnect()
    {
        if (!_connected)
        {
            return;
        }

        _should_stop = true;
        _connected = false;

        closeSocket();

        if (_receive_thread && _receive_thread->joinable())
        {
            _receive_thread->join();
        }
        _receive_thread.reset();

        cleanupPendingRequests();

        LOG_INFO("Disconnected from %s:%d", _host.c_str(), _port);
    }

    Result<std::unique_ptr<Message>> TcpClient::sendRequest(const Message &request)
    {
        return sendRequest(request, _default_timeout);
    }

    Result<std::unique_ptr<Message>> TcpClient::sendRequest(const Message &request,
                                                            std::chrono::milliseconds timeout)
    {
        if (!_connected)
        {
            return Result<std::unique_ptr<Message>>(Status::NETWORK_ERROR);
        }

        auto pending = std::make_unique<PendingRequest>(request.getMessageId());
        auto future = pending->promise.get_future();

        // Store pending request
        {
            const std::lock_guard<std::mutex> lock(_pending_requests_mutex);
            _pending_requests[request.getMessageId()] = std::move(pending);
        }

        // Send message
        const auto send_status = sendMessage(request);
        if (send_status != Status::OK)
        {
            // Remove pending request on send failure
            {
                const std::lock_guard<std::mutex> lock(_pending_requests_mutex);
                _pending_requests.erase(request.getMessageId());
            }
            return Result<std::unique_ptr<Message>>(send_status);
        }

        // Wait for response
        const auto future_status = future.wait_for(timeout);
        if (future_status == std::future_status::timeout)
        {
            // Remove timed out request
            {
                const std::lock_guard<std::mutex> lock(_pending_requests_mutex);
                _pending_requests.erase(request.getMessageId());
            }
            return Result<std::unique_ptr<Message>>(Status::TIMEOUT);
        }

        try
        {
            auto response = future.get();
            return Result<std::unique_ptr<Message>>(std::move(response));
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Exception waiting for response: %s", e.what());
            return Result<std::unique_ptr<Message>>(Status::INTERNAL_ERROR);
        }
    }

    std::future<std::unique_ptr<Message>> TcpClient::sendRequestAsync(const Message &request)
    {
        return sendRequestAsync(request, _default_timeout);
    }

    std::future<std::unique_ptr<Message>> TcpClient::sendRequestAsync(const Message &request,
                                                                      std::chrono::milliseconds timeout)
    {
        // Create pending request
        auto pending = std::make_unique<PendingRequest>(request.getMessageId());
        auto future = pending->promise.get_future();

        // Store pending request
        {
            const std::lock_guard<std::mutex> lock(_pending_requests_mutex);
            _pending_requests[request.getMessageId()] = std::move(pending);
        }

        // Send message asynchronously
        // std::thread([this, request, timeout]()
        std::async(std::launch::async, [this, &request, timeout]()
                   {
        const auto send_status = sendMessage(request);
        if (send_status != Status::OK) {
            // Remove pending request and set exception
            std::unique_ptr<PendingRequest> pending_req;
            {
                const std::lock_guard<std::mutex> lock(_pending_requests_mutex);
                auto it = _pending_requests.find(request.getMessageId());
                if (it != _pending_requests.end()) {
                    pending_req = std::move(it->second);
                    _pending_requests.erase(it);
                }
            }
            
            if (pending_req) {
                pending_req->promise.set_exception(
                    std::make_exception_ptr(std::runtime_error("Failed to send message")));
            }
        } });

        return future;
    }

    Result<Value> TcpClient::get(const Key &key)
    {
        return get(key, _default_timeout);
    }

    Result<Value> TcpClient::get(const Key &key, std::chrono::milliseconds timeout)
    {
        GetRequestMessage request(key);

        const auto response_result = sendRequest(request, timeout);
        if (!response_result.ok())
        {
            return Result<Value>(response_result.status());
        }

        const auto &response = response_result.value();
        if (response->getType() != MessageType::GET_RESPONSE)
        {
            return Result<Value>(Status::INTERNAL_ERROR);
        }

        const auto &get_response = static_cast<const GetResponseMessage &>(*response);
        if (get_response.getStatus() != Status::OK)
        {
            return Result<Value>(get_response.getStatus());
        }

        return Result<Value>(get_response.getValue());
    }

    Status TcpClient::put(const Key &key, const Value &value)
    {
        return put(key, value, _default_timeout);
    }

    Status TcpClient::put(const Key &key, const Value &value, std::chrono::milliseconds timeout)
    {
        PutRequestMessage request(key, value);

        const auto response_result = sendRequest(request, timeout);
        if (!response_result.ok())
        {
            return response_result.status();
        }

        const auto &response = response_result.value();
        if (response->getType() != MessageType::PUT_RESPONSE)
        {
            return Status::INTERNAL_ERROR;
        }

        const auto &put_response = static_cast<const PutResponseMessage &>(*response);
        return put_response.getStatus();
    }

    Status TcpClient::remove(const Key &key)
    {
        return remove(key, _default_timeout);
    }

    Status TcpClient::remove(const Key &key, std::chrono::milliseconds timeout)
    {
        DeleteRequestMessage request(key);

        const auto response_result = sendRequest(request, timeout);
        if (!response_result.ok())
        {
            return response_result.status();
        }

        const auto &response = response_result.value();
        if (response->getType() != MessageType::DELETE_RESPONSE)
        {
            return Status::INTERNAL_ERROR;
        }

        const auto &delete_response = static_cast<const DeleteResponseMessage &>(*response);
        return delete_response.getStatus();
    }

    Status TcpClient::ping()
    {
        return ping(_default_timeout);
    }

    Status TcpClient::ping(std::chrono::milliseconds timeout)
    {
        HeartbeatMessage request("client_node");

        const auto response_result = sendRequest(request, timeout);
        if (!response_result.ok())
        {
            return response_result.status();
        }

        const auto &response = response_result.value();
        if (response->getType() != MessageType::HEARTBEAT_RESPONSE)
        {
            return Status::INTERNAL_ERROR;
        }

        return Status::OK;
    }

    Status TcpClient::initializeSocket()
    {
        _socket = socket(AF_INET, SOCK_STREAM, 0);
        if (_socket == INVALID_SOCKET_VALUE)
        {
            LOG_ERROR("Failed to create socket");
            return Status::NETWORK_ERROR;
        }

        // Set socket options
        int reuse = 1;
        setsockopt(_socket, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char *>(&reuse), sizeof(reuse));

        return Status::OK;
    }

    void TcpClient::receiveLoop()
    {
        LOG_DEBUG("Client receive loop started");

        while (_connected && !_should_stop)
        {
            try
            {
                auto message_result = receiveMessage();
                if (message_result.ok())
                {
                    handleResponse(std::move(message_result.value()));
                }
                else
                {
                    if (message_result.status() == Status::TIMEOUT)
                    {
                        // Timeout expired requests periodically
                        timeoutExpiredRequests();
                        continue;
                    }

                    LOG_DEBUG("Receive failed, disconnecting: status=%d",
                              static_cast<int>(message_result.status()));
                    break;
                }
            }
            catch (const std::exception &e)
            {
                LOG_ERROR("Exception in receive loop: %s", e.what());
                break;
            }
        }

        _connected = false;
        LOG_DEBUG("Client receive loop finished");
    }

    void TcpClient::handleResponse(std::unique_ptr<Message> response)
    {
        const auto message_id = response->getMessageId();

        std::unique_ptr<PendingRequest> pending_request;

        // Find and remove pending request
        {
            const std::lock_guard<std::mutex> lock(_pending_requests_mutex);
            auto it = _pending_requests.find(message_id);
            if (it != _pending_requests.end())
            {
                pending_request = std::move(it->second);
                _pending_requests.erase(it);
            }
        }

        if (pending_request)
        {
            // Fulfill the promise
            pending_request->promise.set_value(std::move(response));
            LOG_DEBUG("Response delivered for message ID: %u", message_id);
        }
        else
        {
            LOG_WARN("Received response for unknown message ID: %u", message_id);
        }
    }

    void TcpClient::cleanupPendingRequests()
    {
        const std::lock_guard<std::mutex> lock(_pending_requests_mutex);

        // Set exception for all pending requests
        for (auto &[id, pending] : _pending_requests)
        {
            pending->promise.set_exception(
                std::make_exception_ptr(std::runtime_error("Connection closed")));
        }

        _pending_requests.clear();
    }

    void TcpClient::timeoutExpiredRequests()
    {
        const auto now = std::chrono::steady_clock::now();
        std::vector<std::unique_ptr<PendingRequest>> expired_requests;

        {
            const std::lock_guard<std::mutex> lock(_pending_requests_mutex);

            auto it = _pending_requests.begin();
            while (it != _pending_requests.end())
            {
                const auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - it->second->created_at);

                if (age > _default_timeout)
                {
                    expired_requests.push_back(std::move(it->second));
                    it = _pending_requests.erase(it);
                }
                else
                {
                    ++it;
                }
            }
        }

        // Set timeout exception for expired requests
        for (auto &expired : expired_requests)
        {
            expired->promise.set_exception(
                std::make_exception_ptr(std::runtime_error("Request timeout")));
        }

        if (!expired_requests.empty())
        {
            LOG_DEBUG("Cleaned up %zu expired requests", expired_requests.size());
        }
    }

    Status TcpClient::sendMessage(const Message &message)
    {
        const std::lock_guard<std::mutex> lock(_socket_mutex);

        if (!_connected)
        {
            return Status::NETWORK_ERROR;
        }

        const auto serialized_result = message.serialize();
        if (!serialized_result.ok())
        {
            LOG_ERROR("Failed to serialize message");
            return serialized_result.status();
        }

        return sendBytes(serialized_result.value());
    }

    Result<std::unique_ptr<Message>> TcpClient::receiveMessage()
    {
        // First, receive the message header
        auto header_result = receiveExactBytes(MessageHeader::HEADER_SIZE);
        if (!header_result.ok())
        {
            return Result<std::unique_ptr<Message>>(header_result.status());
        }

        const auto &header_data = header_result.value();

        // Parse header to get payload size
        if (header_data.size() < MessageHeader::HEADER_SIZE)
        {
            return Result<std::unique_ptr<Message>>(Status::INVALID_REQUEST);
        }

        std::uint32_t payload_size;
        std::memcpy(&payload_size, header_data.data() + sizeof(std::uint32_t) + sizeof(std::uint8_t) + sizeof(MessageType) + sizeof(std::uint32_t),
                    sizeof(payload_size));

        // Sanity check on payload size
        if (payload_size > 1024 * 1024)
        { // Max 1MB payload
            LOG_ERROR("Payload size too large: %u bytes", payload_size);
            return Result<std::unique_ptr<Message>>(Status::INVALID_REQUEST);
        }

        // Receive payload if present
        std::vector<std::uint8_t> full_message = header_data;
        if (payload_size > 0)
        {
            auto payload_result = receiveExactBytes(payload_size);
            if (!payload_result.ok())
            {
                return Result<std::unique_ptr<Message>>(payload_result.status());
            }

            const auto &payload_data = payload_result.value();
            full_message.insert(full_message.end(), payload_data.begin(), payload_data.end());
        }

        // Deserialize complete message
        return Message::fromBytes(full_message);
    }

    Status TcpClient::sendBytes(const std::vector<std::uint8_t> &data)
    {
        std::size_t total_sent = 0;
        const auto *buffer = reinterpret_cast<const char *>(data.data());

        while (total_sent < data.size())
        {
            const auto bytes_to_send = data.size() - total_sent;
            const auto bytes_sent = send(_socket, buffer + total_sent, bytes_to_send, 0);

            if (bytes_sent == SOCKET_ERROR_VALUE)
            {
#ifdef _WIN32
                const auto error = WSAGetLastError();
                if (error == WSAEWOULDBLOCK)
                {
                    continue; // Try again
                }
#else
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    continue; // Try again
                }
#endif
                LOG_ERROR("Send failed");
                _connected = false;
                return Status::NETWORK_ERROR;
            }

            if (bytes_sent == 0)
            {
                LOG_DEBUG("Connection closed by peer during send");
                _connected = false;
                return Status::NETWORK_ERROR;
            }

            total_sent += static_cast<std::size_t>(bytes_sent);
        }

        return Status::OK;
    }

    Result<std::vector<std::uint8_t>> TcpClient::receiveBytes(std::size_t size)
    {
        std::vector<std::uint8_t> buffer(size);
        const auto bytes_received = recv(_socket, reinterpret_cast<char *>(buffer.data()), size, 0);

        if (bytes_received == SOCKET_ERROR_VALUE)
        {
#ifdef _WIN32
            const auto error = WSAGetLastError();
            if (error == WSAEWOULDBLOCK)
            {
                return Result<std::vector<std::uint8_t>>(Status::TIMEOUT);
            }
#else
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return Result<std::vector<std::uint8_t>>(Status::TIMEOUT);
            }
#endif
            LOG_ERROR("Receive failed");
            _connected = false;
            return Result<std::vector<std::uint8_t>>(Status::NETWORK_ERROR);
        }

        if (bytes_received == 0)
        {
            LOG_DEBUG("Connection closed by peer during receive");
            _connected = false;
            return Result<std::vector<std::uint8_t>>(Status::NETWORK_ERROR);
        }

        buffer.resize(static_cast<std::size_t>(bytes_received));
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Result<std::vector<std::uint8_t>> TcpClient::receiveExactBytes(std::size_t size)
    {
        std::vector<std::uint8_t> buffer;
        buffer.reserve(size);

        while (buffer.size() < size)
        {
            const auto remaining = size - buffer.size();
            auto chunk_result = receiveBytes(remaining);

            if (!chunk_result.ok())
            {
                return chunk_result;
            }

            const auto &chunk = chunk_result.value();
            buffer.insert(buffer.end(), chunk.begin(), chunk.end());
        }

        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    std::uint32_t TcpClient::generateMessageId()
    {
        static std::atomic<std::uint32_t> counter{1};
        return counter.fetch_add(1, std::memory_order_relaxed);
    }

    Status TcpClient::connectWithRetry(const std::string &host, Port port)
    {
        for (int attempt = 0; attempt <= _retry_count; ++attempt)
        {
            if (attempt > 0)
            {
                LOG_DEBUG("Connection attempt %d to %s:%d", attempt + 1, host.c_str(), port);
                std::this_thread::sleep_for(_retry_delay);
            }

            const auto status = initializeSocket();
            if (status != Status::OK)
            {
                closeSocket();
                continue;
            }

            // Resolve hostname
            struct sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);

            if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0)
            {
                // Try hostname resolution
                struct hostent *he = gethostbyname(host.c_str());
                if (!he)
                {
                    LOG_ERROR("Failed to resolve hostname: %s", host.c_str());
                    closeSocket();
                    continue;
                }

                std::memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);
            }

            // Connect
            if (::connect(_socket, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) == 0)
            {
                _connected = true;
                return Status::OK;
            }

            LOG_DEBUG("Connect failed for attempt %d: %s", attempt + 1, std::strerror(errno));
            closeSocket();
        }

        LOG_ERROR("Failed to connect to %s:%d after %d attempts", host.c_str(), port, _retry_count + 1);
        return Status::NETWORK_ERROR;
    }

    void TcpClient::closeSocket()
    {
        if (_socket != INVALID_SOCKET_VALUE)
        {
            close_socket(_socket);
            _socket = INVALID_SOCKET_VALUE;
        }
    }

    ConnectionPool::ConnectionPool(std::size_t pool_size)
        : _max_connections(pool_size), _default_timeout(std::chrono::milliseconds(10000)) {}

    ConnectionPool::~ConnectionPool()
    {
        clear();
    }

    Status ConnectionPool::addServer(const std::string &host, Port port)
    {
        const std::lock_guard<std::mutex> lock(_pool_mutex);

        // Check if server already exists
        for (const auto &server : _servers)
        {
            if (server->host == host && server->port == port)
            {
                return Status::OK; // Already exists
            }
        }

        auto server = std::make_unique<ServerInfo>(host, port);
        _servers.push_back(std::move(server));

        LOG_INFO("Added server to pool: %s:%d", host.c_str(), port);
        return Status::OK;
    }

    void ConnectionPool::removeServer(const std::string &host, Port port)
    {
        const std::lock_guard<std::mutex> lock(_pool_mutex);

        _servers.erase(
            std::remove_if(_servers.begin(), _servers.end(),
                           [&host, port](const std::unique_ptr<ServerInfo> &server)
                           {
                               return server->host == host && server->port == port;
                           }),
            _servers.end());

        LOG_INFO("Removed server from pool: %s:%d", host.c_str(), port);
    }

    void ConnectionPool::clear()
    {
        const std::lock_guard<std::mutex> lock(_pool_mutex);
        _servers.clear();
    }

    Result<Value> ConnectionPool::get(const Key &key)
    {
        auto *client = getConnection();
        if (!client)
        {
            return Result<Value>(Status::NETWORK_ERROR);
        }

        const auto result = client->get(key, _default_timeout);
        returnConnection(client);
        return result;
    }

    Status ConnectionPool::put(const Key &key, const Value &value)
    {
        auto *client = getConnection();
        if (!client)
        {
            return Status::NETWORK_ERROR;
        }

        const auto status = client->put(key, value, _default_timeout);
        returnConnection(client);
        return status;
    }

    Status ConnectionPool::remove(const Key &key)
    {
        auto *client = getConnection();
        if (!client)
        {
            return Status::NETWORK_ERROR;
        }

        const auto status = client->remove(key, _default_timeout);
        returnConnection(client);
        return status;
    }

    void ConnectionPool::setDefaultTimeout(std::chrono::milliseconds timeout)
    {
        _default_timeout = timeout;

        const std::lock_guard<std::mutex> lock(_pool_mutex);
        for (auto &server : _servers)
        {
            for (auto &connection : server->connections)
            {
                connection->setDefaultTimeout(timeout);
            }
        }
    }

    std::size_t ConnectionPool::getActiveConnections() const
    {
        const std::lock_guard<std::mutex> lock(_pool_mutex);

        std::size_t total = 0;
        for (const auto &server : _servers)
        {
            total += server->connections.size();
        }
        return total;
    }

    std::size_t ConnectionPool::getTotalServers() const
    {
        const std::lock_guard<std::mutex> lock(_pool_mutex);
        return _servers.size();
    }

    TcpClient *ConnectionPool::getConnection()
    {
        const std::lock_guard<std::mutex> lock(_pool_mutex);

        if (_servers.empty())
        {
            LOG_ERROR("No servers configured in connection pool");
            return nullptr;
        }

        // Simple round-robin server selection
        static std::atomic<std::size_t> server_index{0};
        const auto selected_index = server_index.fetch_add(1, std::memory_order_relaxed) % _servers.size();
        auto &server = _servers[selected_index];

        // Try to find an existing connection
        for (auto &connection : server->connections)
        {
            if (connection && connection->isConnected())
            {
                return connection.get();
            }
        }

        // Create new connection if under limit
        if (server->connections.size() < _max_connections)
        {
            return createConnection(*server);
        }

        LOG_WARN("Connection pool exhausted for server %s:%d", server->host.c_str(), server->port);
        return nullptr;
    }

    void ConnectionPool::returnConnection(TcpClient *client)
    {
        // In this simple implementation, we don't need to do anything
        // The connection remains in the pool for reuse
        (void)client; // Suppress unused parameter warning
    }

    TcpClient *ConnectionPool::createConnection(ServerInfo &server)
    {
        auto client = std::make_unique<TcpClient>();
        client->setDefaultTimeout(_default_timeout);

        const auto status = client->connect(server.host, server.port);
        if (status != Status::OK)
        {
            LOG_ERROR("Failed to create connection to %s:%d", server.host.c_str(), server.port);
            return nullptr;
        }

        auto *client_ptr = client.get();
        server.connections.push_back(std::move(client));

        LOG_DEBUG("Created new connection to %s:%d", server.host.c_str(), server.port);
        return client_ptr;
    }

} // namespace distributed_db