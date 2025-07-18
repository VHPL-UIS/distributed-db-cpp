#ifndef __MESSAGE_HPP__
#define __MESSAGE_HPP__

#include "../common/types.hpp"
#include <vector>

namespace distributed_db
{
    enum class MessageType : std::uint8_t
    {
        // Client requests
        GET_REQUEST = 1,
        PUT_REQUEST = 2,
        DELETE_REQUEST = 3,
        BATCH_GET_REQUEST = 4,
        BATCH_PUT_REQUEST = 5,

        // Client responses
        GET_RESPONSE = 10,
        PUT_RESPONSE = 11,
        DELETE_RESPONSE = 12,
        BATCH_GET_RESPONSE = 13,
        BATCH_PUT_RESPONSE = 14,

        // Node-to-node communication
        HEARTBEAT = 20,
        HEARTBEAT_RESPONSE = 21,
        NODE_JOIN = 22,
        NODE_LEAVE = 23,

        // Raft consensus
        VOTE_REQUEST = 30,
        VOTE_RESPONSE = 31,
        APPEND_ENTRIES = 32,
        APPEND_ENTRIES_RESPONSE = 33,

        // Error responses
        ERROR_RESPONSE = 99
    };

    struct MessageHeader
    {
        std::uint32_t magic_number; // for protocol validation
        std::uint8_t version;
        MessageType type;
        std::uint32_t message_id;
        std::uint32_t payload_size;
        std::uint64_t timestamp;

        MessageHeader() = default;
        MessageHeader(MessageType type, std::uint32_t message_id, uint32_t payload_size);

        [[nodiscard]] bool isValid() const noexcept;

        static constexpr std::uint32_t MAGIC_NUMBER = 0xDBDBDBDB;
        static constexpr std::uint8_t VERSION = 1;
        static constexpr std::size_t HEADER_SIZE = sizeof(std::uint32_t) + // magic
                                                   sizeof(std::uint8_t) +  // version
                                                   sizeof(MessageType) +   // type
                                                   sizeof(std::uint32_t) + // message_id
                                                   sizeof(std::uint32_t) + // payload_size
                                                   sizeof(std::uint64_t);  // timestamp
    };

    class Message
    {
    public:
        Message() = default;
        explicit Message(MessageType type);
        Message(MessageType type, std::uint32_t message_id);
        virtual ~Message() = default;

        Message(const Message &) = delete;
        Message &operator=(const Message &) = delete;
        Message(Message &&) = default;
        Message &operator=(Message &&) = default;

        [[nodiscard]] MessageType getType() const noexcept { return _header.type; }
        [[nodiscard]] std::uint32_t getMessageId() const noexcept { return _header.message_id; }
        [[nodiscard]] std::uint32_t getPayloadSize() const noexcept { return _header.payload_size; }
        [[nodiscard]] std::uint64_t getTimestamp() const noexcept { return _header.timestamp; }
        [[nodiscard]] const MessageHeader &getHeader() const noexcept { return _header; }

        [[nodiscard]] virtual Result<std::vector<std::uint8_t>> serialize() const = 0;
        [[nodiscard]] virtual Status deserialize(const std::vector<std::uint8_t> &data) = 0;

        // Factory method
        [[nodiscard]] static std::unique_ptr<Message> createMessage(MessageType type);
        [[nodiscard]] static Result<std::unique_ptr<Message>> fromBytes(const std::vector<std::uint8_t> &data);

    protected:
        MessageHeader _header;

        void updatePayloadSize(std::uint32_t size) noexcept { _header.payload_size = size; }
        [[nodiscard]] static std::uint32_t generateMessageId() noexcept;

        [[nodiscard]] Status serializeHeader(std::vector<std::uint8_t> &buffer) const;
        [[nodiscard]] Status deserializeHeader(const std::vector<std::uint8_t> &data);
    };

    class GetRequestMessage : public Message
    {
    public:
        GetRequestMessage() : Message(MessageType::GET_REQUEST)
        {
        }
        explicit GetRequestMessage(Key key) : Message(MessageType::GET_REQUEST), _key(std::move(key))
        {
        }

        [[nodiscard]] const Key &getKey() const noexcept { return _key; }
        void setKey(Key key) { _key = std::move(key); }

        [[nodiscard]] Result<std::vector<std::uint8_t>> serialize() const override;
        [[nodiscard]] Status deserialize(const std::vector<std::uint8_t> &data) override;

    private:
        Key _key;
    };

    class GetResponseMessage : public Message
    {
    public:
        GetResponseMessage() : Message(MessageType::GET_RESPONSE) {}
        GetResponseMessage(std::uint32_t request_id, Status status, Value value = "")
            : Message(MessageType::GET_RESPONSE, request_id), _status(status), _value(std::move(value)) {}

        [[nodiscard]] Status getStatus() const noexcept { return _status; }
        [[nodiscard]] const Value &getValue() const noexcept { return _value; }

        void setStatus(Status status) noexcept { _status = status; }
        void setValue(Value value) { _value = std::move(value); }

        [[nodiscard]] Result<std::vector<std::uint8_t>> serialize() const override;
        [[nodiscard]] Status deserialize(const std::vector<std::uint8_t> &data) override;

    private:
        Status _status = Status::OK;
        Value _value;
    };
} // namespace distributed_db

#endif // __MESSAGE_HPP__