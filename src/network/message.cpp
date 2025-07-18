#include "message.hpp"
#include "../common/logger.hpp"
#include <cstring>
#include <atomic>

namespace distributed_db
{
    MessageHeader::MessageHeader(MessageType type, std::uint32_t message_id, std::uint32_t payload_size)
        : magic_number(MAGIC_NUMBER), version(VERSION), type(type),
          message_id(message_id), payload_size(payload_size)
    {
        const auto now = std::chrono::system_clock::now();
        timestamp = std::chrono::duration_cast<std::chrono::millisecond>(now.time_since_epoch()).count();
    }

    bool MessageHeader::isValid() const noexcept
    {
        return magic_number == MAGIC_NUMBER && version == VERSION;
    }

    Message::Message(MessageType type)
        : _header(type, generateMessageId(), 0) {}

    Message::Message(MessageType type, std::uint32_t message_id)
        : _header(type, message_id, 0) {}

    std::uint32_t Message::generateMessageId() noexcept
    {
        static std::atomic<std::uint32_t> counter{1};
        return counter.fetch_add(1, std::memory_order_relaxed);
    }

    Status Message::serializeHeader(std::vector<std::uint8_t> &buffer) const
    {
        buffer.reserve(buffer.size() + MessageHeader::HEADER_SIZE);

        const auto magic_bytes = reinterpret_cast<const std::uint8_t *>(&_header.magic_number);
        buffer.insert(buffer.end(), magic_bytes, magic_bytes + sizeof(_header.magic_number));

        buffer.push_back(_header.version);

        buffer.push_back(static_cast<std::uint8_t>(_header.type));

        const auto id_bytes = reinterpret_cast<const std::uint8_t *>(&_header.message_id);
        buffer.insert(buffer.end(), id_bytes, id_bytes + sizeof(_header.message_id));

        const auto size_bytes = reinterpret_cast<const std::uint8_t *>(&_header.payload_size);
        buffer.insert(buffer.end(), size_bytes, size_bytes + sizeof(_header.payload_size));

        const auto timestamp_bytes = reinterpret_cast<const std::uint8_t *>(&_header.timestamp);
        buffer.insert(buffer.end(), timestamp_bytes, timestamp_bytes + sizeof(_header.timestamp));

        return Status::OK;
    }

    Status Message::deserializeHeader(const std::vector<std::uint8_t> &data)
    {
        if (data.size() < MessageHeader::HEADER_SIZE)
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = 0;

        std::memcpy(&_header.magic_number, data.data() + offset, sizeof(_header.magic_number));
        offset += sizeof(_header.magic_number);

        _header.version = data[offset];
        offset += sizeof(_header.version);

        _header.type = static_cast<MessageType>(data[offset]);
        offset += sizeof(_header.type);

        std::memcpy(&_header.message_id, data.data() + offset, sizeof(_header.message_id));
        offset += sizeof(_header.message_id);

        std::memcpy(&_header.payload_size, data.data() + offset, sizeof(_header.payload_size));
        offset += sizeof(_header.payload_size);

        std::memcpy(&_header.timestamp, data.data() + offset, sizeof(_header.timestamp));

        if (!_header.isValid())
        {
            return Status::INVALID_REQUEST;
        }

        return Status::OK;
    }

    std::unique_ptr<Message> Message::createMessage(MessageType type)
    {
        switch (type)
        {
        case MessageType::GET_REQUEST:
            return std::make_unique<GetRequestMessage>();
        case MessageType::GET_RESPONSE:
            return std::make_unique<GetResponseMessage>();
        case MessageType::PUT_REQUEST:
            return std::make_unique<PutRequestMessage>();
        case MessageType::PUT_RESPONSE:
            return std::make_unique<PutResponseMessage>();
        case MessageType::DELETE_REQUEST:
            return std::make_unique<DeleteRequestMessage>();
        case MessageType::DELETE_RESPONSE:
            return std::make_unique<DeleteResponseMessage>();
        case MessageType::HEARTBEAT:
            return std::make_unique<HeartbeatMessage>();
        case MessageType::HEARTBEAT_RESPONSE:
            return std::make_unique<HeartbeatResponseMessage>();
        case MessageType::ERROR_RESPONSE:
            return std::make_unique<ErrorResponseMessage>();
        default:
            return nullptr;
        }
    }

    Result<std::unique_ptr<Message>> Message::fromBytes(const std::vector<std::uint8_t> &data)
    {
        if (data.size() < MessageHeader::HEADER_SIZE)
        {
            return Result<std::unique_ptr<Message>>(Status::INVALID_REQUEST);
        }

        const auto type = static_cast<MessageType>(data[sizeof(std::uint32_t) + sizeof(std::uint8_t)]);

        auto message = createMessage(type);
        if (!message)
        {
            LOG_ERROR("Unknown message type: %d", static_cast<int>(type));
            return Result<std::unique_ptr<Message>>(Status::INVALID_REQUEST);
        }

        const auto status = message->deserialize(data);
        if (status != Status::OK)
        {
            return Result<std::unique_ptr<Message>>(status);
        }

        return Result<std::unique_ptr<Message>>(std::move(message));
    }
} // namespace distributed_db