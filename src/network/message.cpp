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
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    }

    bool MessageHeader::isValid() const noexcept
    {
        return magic_number == MAGIC_NUMBER && version == VERSION;
    }

    Message::Message(MessageType type)
        : _header(type, generateMessageId(), 0) {}

    Message::Message(MessageType type, std::uint32_t message_id)
        : _header(type, message_id, 0) {}

    Message::~Message()
    {
        LOG_DEBUG("Destroying message type: %d", static_cast<int>(_header.type));
    }

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

    static void serializeString(const std::string &str, std::vector<std::uint8_t> &buffer)
    {
        const auto size = static_cast<std::uint32_t>(str.size());
        const auto size_bytes = reinterpret_cast<const std::uint8_t *>(&size);
        buffer.insert(buffer.end(), size_bytes, size_bytes + sizeof(size));
        buffer.insert(buffer.end(), str.begin(), str.end());
    }

    static Status deserializeString(const std::vector<std::uint8_t> &data, std::size_t &offset, std::string &str)
    {
        if (offset + sizeof(std::uint32_t) > data.size())
        {
            return Status::INVALID_REQUEST;
        }

        std::uint32_t size;
        std::memcpy(&size, data.data() + offset, sizeof(size));
        offset += sizeof(size);

        if (offset + size > data.size())
        {
            return Status::INVALID_REQUEST;
        }

        str = std::string(data.begin() + offset, data.begin() + offset + size);
        offset += size;

        return Status::OK;
    }

    Result<std::vector<std::uint8_t>> GetRequestMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;
        serializeString(_key, payload);

        const_cast<GetRequestMessage *>(this)->updatePayloadSize(static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto status = serializeHeader(buffer);
        if (status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status GetRequestMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + _header.payload_size)
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = MessageHeader::HEADER_SIZE;
        return deserializeString(data, offset, _key);
    }

    Result<std::vector<std::uint8_t>> GetResponseMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;

        const auto status_byte = static_cast<std::uint8_t>(_status);
        payload.push_back(status_byte);

        serializeString(_value, payload);

        const_cast<GetResponseMessage *>(this)->updatePayloadSize(static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto status = serializeHeader(buffer);
        if (status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status GetResponseMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + _header.payload_size)
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = MessageHeader::HEADER_SIZE;

        if (offset >= data.size())
        {
            return Status::INVALID_REQUEST;
        }
        _status = static_cast<Status>(data[offset]);
        offset += sizeof(std::uint8_t);

        return deserializeString(data, offset, _value);
    }

    Result<std::vector<std::uint8_t>> PutRequestMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;
        serializeString(_key, payload);
        serializeString(_value, payload);

        const_cast<PutRequestMessage *>(this)->updatePayloadSize(static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto status = serializeHeader(buffer);
        if (status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status PutRequestMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + _header.payload_size)
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = MessageHeader::HEADER_SIZE;

        auto status = deserializeString(data, offset, _key);
        if (status != Status::OK)
        {
            return status;
        }

        return deserializeString(data, offset, _value);
    }

    Result<std::vector<std::uint8_t>> PutResponseMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;
        const auto status_byte = static_cast<std::uint8_t>(_status);
        payload.push_back(status_byte);

        const_cast<PutResponseMessage *>(this)->updatePayloadSize(static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto status = serializeHeader(buffer);
        if (status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status PutResponseMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + 1)
        {
            return Status::INVALID_REQUEST;
        }

        const std::size_t offset = MessageHeader::HEADER_SIZE;
        _status = static_cast<Status>(data[offset]);

        return Status::OK;
    }

    Result<std::vector<std::uint8_t>> DeleteRequestMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;
        serializeString(_key, payload);

        const_cast<DeleteRequestMessage *>(this)->updatePayloadSize(static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto status = serializeHeader(buffer);
        if (status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status DeleteRequestMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + _header.payload_size)
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = MessageHeader::HEADER_SIZE;
        return deserializeString(data, offset, _key);
    }

    Result<std::vector<std::uint8_t>> DeleteResponseMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;
        const auto status_byte = static_cast<std::uint8_t>(_status);
        payload.push_back(status_byte);

        const_cast<DeleteResponseMessage *>(this)->updatePayloadSize(static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto status = serializeHeader(buffer);
        if (status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status DeleteResponseMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + 1)
        {
            return Status::INVALID_REQUEST;
        }

        const std::size_t offset = MessageHeader::HEADER_SIZE;
        _status = static_cast<Status>(data[offset]);

        return Status::OK;
    }

    Result<std::vector<std::uint8_t>> HeartbeatMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;
        serializeString(_node_id, payload);

        const_cast<HeartbeatMessage *>(this)->updatePayloadSize(static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto status = serializeHeader(buffer);
        if (status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status HeartbeatMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + _header.payload_size)
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = MessageHeader::HEADER_SIZE;
        return deserializeString(data, offset, _node_id);
    }

    Result<std::vector<std::uint8_t>> HeartbeatResponseMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;
        serializeString(_node_id, payload);

        const_cast<HeartbeatResponseMessage *>(this)->updatePayloadSize(static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto status = serializeHeader(buffer);
        if (status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status HeartbeatResponseMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + _header.payload_size)
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = MessageHeader::HEADER_SIZE;
        return deserializeString(data, offset, _node_id);
    }

    Result<std::vector<std::uint8_t>> ErrorResponseMessage::serialize() const
    {
        std::vector<std::uint8_t> payload;

        const auto status_byte = static_cast<std::uint8_t>(_error_status);
        payload.push_back(status_byte);

        serializeString(_error_message, payload);

        const_cast<ErrorResponseMessage *>(this)->updatePayloadSize(static_cast<std::uint32_t>(payload.size()));

        std::vector<std::uint8_t> buffer;
        const auto status = serializeHeader(buffer);
        if (status != Status::OK)
        {
            return Result<std::vector<std::uint8_t>>(status);
        }

        buffer.insert(buffer.end(), payload.begin(), payload.end());
        return Result<std::vector<std::uint8_t>>(std::move(buffer));
    }

    Status ErrorResponseMessage::deserialize(const std::vector<std::uint8_t> &data)
    {
        const auto header_status = deserializeHeader(data);
        if (header_status != Status::OK)
        {
            return header_status;
        }

        if (data.size() < MessageHeader::HEADER_SIZE + _header.payload_size)
        {
            return Status::INVALID_REQUEST;
        }

        std::size_t offset = MessageHeader::HEADER_SIZE;

        if (offset >= data.size())
        {
            return Status::INVALID_REQUEST;
        }
        _error_status = static_cast<Status>(data[offset]);
        offset += sizeof(std::uint8_t);

        return deserializeString(data, offset, _error_message);
    }

    std::string messageTypeToString(MessageType type)
    {
        switch (type)
        {
        case MessageType::GET_REQUEST:
            return "GET_REQUEST";
        case MessageType::PUT_REQUEST:
            return "PUT_REQUEST";
        case MessageType::DELETE_REQUEST:
            return "DELETE_REQUEST";
        case MessageType::BATCH_GET_REQUEST:
            return "BATCH_GET_REQUEST";
        case MessageType::BATCH_PUT_REQUEST:
            return "BATCH_PUT_REQUEST";
        case MessageType::GET_RESPONSE:
            return "GET_RESPONSE";
        case MessageType::PUT_RESPONSE:
            return "PUT_RESPONSE";
        case MessageType::DELETE_RESPONSE:
            return "DELETE_RESPONSE";
        case MessageType::BATCH_GET_RESPONSE:
            return "BATCH_GET_RESPONSE";
        case MessageType::BATCH_PUT_RESPONSE:
            return "BATCH_PUT_RESPONSE";
        case MessageType::HEARTBEAT:
            return "HEARTBEAT";
        case MessageType::HEARTBEAT_RESPONSE:
            return "HEARTBEAT_RESPONSE";
        case MessageType::NODE_JOIN:
            return "NODE_JOIN";
        case MessageType::NODE_LEAVE:
            return "NODE_LEAVE";
        case MessageType::VOTE_REQUEST:
            return "VOTE_REQUEST";
        case MessageType::VOTE_RESPONSE:
            return "VOTE_RESPONSE";
        case MessageType::APPEND_ENTRIES:
            return "APPEND_ENTRIES";
        case MessageType::APPEND_ENTRIES_RESPONSE:
            return "APPEND_ENTRIES_RESPONSE";
        case MessageType::ERROR_RESPONSE:
            return "ERROR_RESPONSE";
        default:
            return "UNKNOWN";
        }
    }

    bool isRequestMessage(MessageType type) noexcept
    {
        switch (type)
        {
        case MessageType::GET_REQUEST:
        case MessageType::PUT_REQUEST:
        case MessageType::DELETE_REQUEST:
        case MessageType::BATCH_GET_REQUEST:
        case MessageType::BATCH_PUT_REQUEST:
        case MessageType::HEARTBEAT:
        case MessageType::NODE_JOIN:
        case MessageType::NODE_LEAVE:
        case MessageType::VOTE_REQUEST:
        case MessageType::APPEND_ENTRIES:
            return true;
        default:
            return false;
        }
    }

    bool isResponseMessage(MessageType type) noexcept
    {
        switch (type)
        {
        case MessageType::GET_RESPONSE:
        case MessageType::PUT_RESPONSE:
        case MessageType::DELETE_RESPONSE:
        case MessageType::BATCH_GET_RESPONSE:
        case MessageType::BATCH_PUT_RESPONSE:
        case MessageType::HEARTBEAT_RESPONSE:
        case MessageType::VOTE_RESPONSE:
        case MessageType::APPEND_ENTRIES_RESPONSE:
        case MessageType::ERROR_RESPONSE:
            return true;
        default:
            return false;
        }
    }

    MessageType getResponseType(MessageType request_type)
    {
        switch (request_type)
        {
        case MessageType::GET_REQUEST:
            return MessageType::GET_RESPONSE;
        case MessageType::PUT_REQUEST:
            return MessageType::PUT_RESPONSE;
        case MessageType::DELETE_REQUEST:
            return MessageType::DELETE_RESPONSE;
        case MessageType::BATCH_GET_REQUEST:
            return MessageType::BATCH_GET_RESPONSE;
        case MessageType::BATCH_PUT_REQUEST:
            return MessageType::BATCH_PUT_RESPONSE;
        case MessageType::HEARTBEAT:
            return MessageType::HEARTBEAT_RESPONSE;
        case MessageType::VOTE_REQUEST:
            return MessageType::VOTE_RESPONSE;
        case MessageType::APPEND_ENTRIES:
            return MessageType::APPEND_ENTRIES_RESPONSE;
        default:
            return MessageType::ERROR_RESPONSE;
        }
    }
} // namespace distributed_db