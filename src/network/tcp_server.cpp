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

} // distributed_db