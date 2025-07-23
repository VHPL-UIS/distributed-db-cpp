#include "tcp_server.hpp"
#include "../common/logger.hpp"
#include "../storage/storage_engine.hpp"
#include <algorithm>
#include <cstring>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#define INVALID_SOCKET_VALUE INVALID_SOCKET
#define SOCKET_ERROR_VALUE SOCKET_ERROR
#define close_socket closesocket
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#define INVALID_SOCKET_VALUE -1
#define SOCKET_ERROR_VALUE -1
#define close_socket close
#endif

namespace distributed_db
{
    std::atomic<std::uint64_t> Connection::_next_connection_id{1};

    Connection::Connection(socket_t socket, const std::string &remote_address)
        : _socket(socket), _remote_address(remote_address),
          _connection_id(_next_connection_id.fetch_add(1, std::memory_order_relaxed)),
          _connected_at(std::chrono::system_clock::now()),
          _connected(true)
    {
    }

    Connection::~Connection()
    {
        close();
    }

    Status Connection::sendMessage(const Message &message)
    {
        const std::lock_guard<std::mutex> lock(_send_mutex);

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

    Result<std::unique_ptr<Message>> Connection::receiveMessage()
    {
        if (!_connected)
        {
            return Result<std::unique_ptr<Message>>(Status::NETWORK_ERROR);
        }

        auto header_result = receiveExactBytes(MessageHeader::HEADER_SIZE);
        if (!header_result.ok())
        {
            return Result<std::unique_ptr<Message>>(header_result.status());
        }

        const auto &header_data = header_result.value();

        if (header_data.size() < MessageHeader::HEADER_SIZE)
        {
            return Result<std::unique_ptr<Message>>(Status::INVALID_REQUEST);
        }

        std::uint32_t payload_size;
        std::memcpy(&payload_size, header_data.data() + sizeof(std::uint32_t) + sizeof(std::uint8_t) + sizeof(MessageType) + sizeof(std::uint32_t),
                    sizeof(payload_size));

        if (payload_size > 1024 * 1024)
        {
            LOG_ERROR("Payload size too large: %u bytes", payload_size);
            return Result<std::unique_ptr<Message>>(Status::INVALID_REQUEST);
        }

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

        return Message::fromBytes(full_message);
    }

    void Connection::close()
    {
        if (_connected.exchange(false))
        {
            socket_utils::closeSocket(_socket);
            LOG_DEBUG("Connection %lu closed", _connection_id);
        }
    }

    Status Connection::sendBytes(const std::vector<std::uint8_t> &data)
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
                LOG_ERROR("Send failed: %s", socket_utils::getLastSocketError().c_str());
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

    Result<std::vector<std::uint8_t>> Connection::receiveBytes(std::size_t size)
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
            LOG_ERROR("Receive failed: %s", socket_utils::getLastSocketError().c_str());
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

    Result<std::vector<std::uint8_t>> Connection::receiveExactBytes(std::size_t size)
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

    TcpServer::TcpServer(Port port)
        : TcpServer(port, "0.0.0.0") {}

    TcpServer::TcpServer(Port port, const std::string &bind_address)
        : _port(port), _bind_address(bind_address), _server_socket(INVALID_SOCKET_VALUE),
          _running(false), _should_stop(false), _max_connections(100),
          _receive_timeout(std::chrono::milliseconds(30000))
    {

        socket_utils::initializeNetworking();
    }

    TcpServer::~TcpServer()
    {
        stop();
        socket_utils::cleanupNetworking();
    }

    Status TcpServer::start()
    {
        if (_running)
        {
            return Status::OK;
        }

        const auto init_status = initializeSocket();
        if (init_status != Status::OK)
        {
            return init_status;
        }

        _should_stop = false;
        _running = true;

        _accept_thread = std::make_unique<std::thread>(&TcpServer::acceptLoop, this);

        LOG_INFO("TCP server started on %s:%d", _bind_address.c_str(), _port);
        return Status::OK;
    }

    void TcpServer::stop()
    {
        if (!_running)
        {
            return;
        }

        _should_stop = true;
        _running = false;

        if (_server_socket != INVALID_SOCKET_VALUE)
        {
            close_socket(_server_socket);
            _server_socket = INVALID_SOCKET_VALUE;
        }

        if (_accept_thread && _accept_thread->joinable())
        {
            _accept_thread->join();
        }

        for (auto &thread : _worker_threads)
        {
            if (thread && thread->joinable())
            {
                thread->join();
            }
        }
        _worker_threads.clear();

        {
            const std::lock_guard<std::mutex> lock(_connections_mutex);
            for (auto &[id, connection] : _active_connections)
            {
                connection->close();
            }
            _active_connections.clear();
        }

        LOG_INFO("TCP server stopped");
    }

    std::size_t TcpServer::getConnectionCount() const
    {
        const std::lock_guard<std::mutex> lock(_connections_mutex);
        return _active_connections.size();
    }

    std::vector<std::uint64_t> TcpServer::getActiveConnections() const
    {
        const std::lock_guard<std::mutex> lock(_connections_mutex);

        std::vector<std::uint64_t> connection_ids;
        connection_ids.reserve(_active_connections.size());

        for (const auto &[id, connection] : _active_connections)
        {
            connection_ids.push_back(id);
        }

        return connection_ids;
    }

    Status TcpServer::initializeSocket()
    {
        _server_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (_server_socket == INVALID_SOCKET_VALUE)
        {
            LOG_ERROR("Failed to create socket: %s", socket_utils::getLastSocketError().c_str());
            return Status::NETWORK_ERROR;
        }

        const auto options_status = setSocketOptions(_server_socket);
        if (options_status != Status::OK)
        {
            close_socket(_server_socket);
            return options_status;
        }

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(_port);

        if (_bind_address == "0.0.0.0")
        {
            addr.sin_addr.s_addr = INADDR_ANY;
        }
        else
        {
            if (inet_pton(AF_INET, _bind_address.c_str(), &addr.sin_addr) <= 0)
            {
                LOG_ERROR("Invalid bind address: %s", _bind_address.c_str());
                close_socket(_server_socket);
                return Status::INVALID_REQUEST;
            }
        }

        if (bind(_server_socket, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) == SOCKET_ERROR_VALUE)
        {
            LOG_ERROR("Failed to bind socket: %s", socket_utils::getLastSocketError().c_str());
            close_socket(_server_socket);
            return Status::NETWORK_ERROR;
        }

        if (listen(_server_socket, 10) == SOCKET_ERROR_VALUE)
        {
            LOG_ERROR("Failed to listen on socket: %s", socket_utils::getLastSocketError().c_str());
            close_socket(_server_socket);
            return Status::NETWORK_ERROR;
        }

        return Status::OK;
    }

    void TcpServer::acceptLoop()
    {
        LOG_DEBUG("Accept loop started");

        while (!_should_stop)
        {
            struct sockaddr_in client_addr{};
            socklen_t client_addr_len = sizeof(client_addr);

            const auto client_socket = accept(_server_socket,
                                              reinterpret_cast<struct sockaddr *>(&client_addr),
                                              &client_addr_len);

            if (client_socket == INVALID_SOCKET_VALUE)
            {
                if (_should_stop)
                {
                    break;
                }

#ifdef _WIN32
                const auto error = WSAGetLastError();
                if (error == WSAEINTR || error == WSAENOTSOCK)
                {
                    break; // Server socket closed
                }
#else
                if (errno == EINTR || errno == EBADF)
                {
                    break; // Server socket closed
                }
#endif

                LOG_WARN("Accept failed: %s", socket_utils::getLastSocketError().c_str());
                continue;
            }

            if (getConnectionCount() >= _max_connections)
            {
                LOG_WARN("Connection limit reached, rejecting new connection");
                close_socket(client_socket);
                continue;
            }

            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
            const auto remote_address = std::string(client_ip) + ":" + std::to_string(ntohs(client_addr.sin_port));

            auto connection = std::make_unique<Connection>(client_socket, remote_address);
            const auto connection_id = connection->getConnectionId();

            LOG_INFO("New connection from %s (ID: %lu)", remote_address.c_str(), connection_id);

            {
                const std::lock_guard<std::mutex> lock(_connections_mutex);
                _active_connections[connection_id] = std::move(connection);
            }

            auto worker = std::make_unique<std::thread>([this, connection_id]()
                                                        {
            std::unique_ptr<Connection> connection;
            
            {
                const std::lock_guard<std::mutex> lock(_connections_mutex);
                auto it = _active_connections.find(connection_id);
                if (it != _active_connections.end()) {
                    connection = std::move(it->second);
                    _active_connections.erase(it);
                }
            }
            
            if (connection) {
                handleConnection(std::move(connection));
            } });

            _worker_threads.push_back(std::move(worker));

            cleanupConnections();
        }

        LOG_DEBUG("Accept loop finished");
    }

    void TcpServer::handleConnection(std::unique_ptr<Connection> connection)
    {
        const auto connection_id = connection->getConnectionId();
        const auto &remote_address = connection->getRemoteAddress();

        LOG_DEBUG("Handling connection %lu from %s", connection_id, remote_address.c_str());

        try
        {
            while (connection->isConnected() && !_should_stop)
            {
                // Receive message with timeout
                auto message_result = connection->receiveMessage();

                if (!message_result.ok())
                {
                    if (message_result.status() == Status::TIMEOUT)
                    {
                        continue; // Try again
                    }
                    LOG_DEBUG("Failed to receive message from connection %lu: status=%d",
                              connection_id, static_cast<int>(message_result.status()));
                    break;
                }

                auto &message = message_result.value();
                LOG_DEBUG("Received %s message from connection %lu",
                          messageTypeToString(message->getType()).c_str(), connection_id);

                processMessage(message, *connection);
            }
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Exception in connection handler %lu: %s", connection_id, e.what());
        }

        connection->close();
        LOG_INFO("Connection %lu from %s closed", connection_id, remote_address.c_str());
    }

    void TcpServer::processMessage(const std::unique_ptr<Message> &request, Connection &connection)
    {
        std::unique_ptr<Message> response;

        try
        {
            // Use custom message handler if set
            if (_message_handler)
            {
                response = _message_handler(*request, connection);
            }
            else
            {
                // Use default handlers
                switch (request->getType())
                {
                case MessageType::GET_REQUEST:
                    response = handleGetRequest(static_cast<const GetRequestMessage &>(*request));
                    break;

                case MessageType::PUT_REQUEST:
                    response = handlePutRequest(static_cast<const PutRequestMessage &>(*request));
                    break;

                case MessageType::DELETE_REQUEST:
                    response = handleDeleteRequest(static_cast<const DeleteRequestMessage &>(*request));
                    break;

                case MessageType::HEARTBEAT:
                    response = handleHeartbeat(static_cast<const HeartbeatMessage &>(*request));
                    break;

                default:
                    LOG_WARN("Unhandled message type: %s",
                             messageTypeToString(request->getType()).c_str());
                    response = std::make_unique<ErrorResponseMessage>(
                        request->getMessageId(), Status::INVALID_REQUEST,
                        "Unsupported message type");
                    break;
                }
            }

            if (response)
            {
                const auto send_status = connection.sendMessage(*response);
                if (send_status != Status::OK)
                {
                    LOG_ERROR("Failed to send response to connection %lu", connection.getConnectionId());
                }
            }
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Exception processing message: %s", e.what());

            auto error_response = std::make_unique<ErrorResponseMessage>(
                request->getMessageId(), Status::INTERNAL_ERROR, e.what());

            connection.sendMessage(*error_response);
        }
    }

    void TcpServer::cleanupConnections()
    {
        _worker_threads.erase(
            std::remove_if(_worker_threads.begin(), _worker_threads.end(),
                           [](const std::unique_ptr<std::thread> &thread)
                           {
                               return thread && !thread->joinable();
                           }),
            _worker_threads.end());
    }

    std::unique_ptr<Message> TcpServer::handleGetRequest(const GetRequestMessage &request)
    {
        if (!_storage_engine)
        {
            return std::make_unique<ErrorResponseMessage>(
                request.getMessageId(), Status::INTERNAL_ERROR, "No storage engine configured");
        }

        const auto result = _storage_engine->get(request.getKey());

        if (result.ok())
        {
            return std::make_unique<GetResponseMessage>(
                request.getMessageId(), Status::OK, result.value());
        }
        else
        {
            return std::make_unique<GetResponseMessage>(
                request.getMessageId(), result.status());
        }
    }

    std::unique_ptr<Message> TcpServer::handlePutRequest(const PutRequestMessage &request)
    {
        if (!_storage_engine)
        {
            return std::make_unique<ErrorResponseMessage>(
                request.getMessageId(), Status::INTERNAL_ERROR, "No storage engine configured");
        }

        const auto status = _storage_engine->put(request.getKey(), request.getValue());
        return std::make_unique<PutResponseMessage>(request.getMessageId(), status);
    }

    std::unique_ptr<Message> TcpServer::handleDeleteRequest(const DeleteRequestMessage &request)
    {
        if (!_storage_engine)
        {
            return std::make_unique<ErrorResponseMessage>(
                request.getMessageId(), Status::INTERNAL_ERROR, "No storage engine configured");
        }

        const auto status = _storage_engine->remove(request.getKey());
        return std::make_unique<DeleteResponseMessage>(request.getMessageId(), status);
    }

    std::unique_ptr<Message> TcpServer::handleHeartbeat(const HeartbeatMessage &request)
    {
        // For now, just echo back with our node ID
        return std::make_unique<HeartbeatResponseMessage>(
            request.getMessageId(), "tcp_server_node");
    }

    std::string TcpServer::getSocketAddress(socket_t socket) const
    {
        struct sockaddr_in addr{};
        socklen_t addr_len = sizeof(addr);

        if (getsockname(socket, reinterpret_cast<struct sockaddr *>(&addr), &addr_len) == 0)
        {
            char ip_str[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &addr.sin_addr, ip_str, INET_ADDRSTRLEN);
            return std::string(ip_str) + ":" + std::to_string(ntohs(addr.sin_port));
        }

        return "unknown";
    }

    void TcpServer::closeSocket(socket_t socket)
    {
        socket_utils::closeSocket(socket);
    }

    Status TcpServer::setSocketOptions(socket_t socket)
    {
        // Enable address reuse
        int reuse = 1;
        if (setsockopt(socket, SOL_SOCKET, SO_REUSEADDR,
                       reinterpret_cast<const char *>(&reuse), sizeof(reuse)) == SOCKET_ERROR_VALUE)
        {
            LOG_WARN("Failed to set SO_REUSEADDR: %s", socket_utils::getLastSocketError().c_str());
        }

        // Set receive timeout
        const auto timeout_ms = static_cast<int>(_receive_timeout.count());

#ifdef _WIN32
        DWORD timeout = timeout_ms;
        if (setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO,
                       reinterpret_cast<const char *>(&timeout), sizeof(timeout)) == SOCKET_ERROR_VALUE)
        {
            LOG_WARN("Failed to set receive timeout: %s", socket_utils::getLastSocketError().c_str());
        }
#else
        struct timeval timeout{};
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;

        if (setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == SOCKET_ERROR_VALUE)
        {
            LOG_WARN("Failed to set receive timeout: %s", socket_utils::getLastSocketError().c_str());
        }
#endif

        return Status::OK;
    }

    Status TcpServer::setNonBlocking(socket_t socket, bool non_blocking)
    {
#ifdef _WIN32
        u_long mode = non_blocking ? 1 : 0;
        if (ioctlsocket(socket, FIONBIO, &mode) == SOCKET_ERROR_VALUE)
        {
            return Status::NETWORK_ERROR;
        }
#else
        const auto flags = fcntl(socket, F_GETFL, 0);
        if (flags == -1)
        {
            return Status::NETWORK_ERROR;
        }

        const auto new_flags = non_blocking ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
        if (fcntl(socket, F_SETFL, new_flags) == -1)
        {
            return Status::NETWORK_ERROR;
        }
#endif

        return Status::OK;
    }

    namespace socket_utils
    {

        Status initializeNetworking()
        {
#ifdef _WIN32
            WSADATA wsa_data;
            const auto result = WSAStartup(MAKEWORD(2, 2), &wsa_data);
            if (result != 0)
            {
                LOG_ERROR("WSAStartup failed: %d", result);
                return Status::NETWORK_ERROR;
            }
#endif
            return Status::OK;
        }

        void cleanupNetworking()
        {
#ifdef _WIN32
            WSACleanup();
#endif
        }

        std::string getLastSocketError()
        {
#ifdef _WIN32
            const auto error = WSAGetLastError();
            char *message = nullptr;
            FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
                           nullptr, error, 0, reinterpret_cast<char *>(&message), 0, nullptr);

            std::string result = message ? message : "Unknown error";
            if (message)
            {
                LocalFree(message);
            }
            return result;
#else
            return std::strerror(errno);
#endif
        }

        bool isSocketValid(socket_t socket)
        {
            return socket != INVALID_SOCKET_VALUE;
        }

        Status closeSocket(socket_t socket)
        {
            if (socket != INVALID_SOCKET_VALUE)
            {
                if (close_socket(socket) == SOCKET_ERROR_VALUE)
                {
                    LOG_WARN("Failed to close socket: %s", getLastSocketError().c_str());
                    return Status::NETWORK_ERROR;
                }
            }
            return Status::OK;
        }

    } // namespace socket_utils
} // distributed_db