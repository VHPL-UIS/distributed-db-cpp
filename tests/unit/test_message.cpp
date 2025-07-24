#include <gtest/gtest.h>
#include "network/message.hpp"

using namespace distributed_db;

class MessageTest : public ::testing::Test
{
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(MessageTest, MessageHeaderValidation)
{
    MessageHeader header(MessageType::GET_REQUEST, 123, 456);

    EXPECT_TRUE(header.isValid());
    EXPECT_EQ(header.magic_number, MessageHeader::MAGIC_NUMBER);
    EXPECT_EQ(header.version, MessageHeader::VERSION);
    EXPECT_EQ(header.type, MessageType::GET_REQUEST);
    EXPECT_EQ(header.message_id, 123);
    EXPECT_EQ(header.payload_size, 456);
    EXPECT_GT(header.timestamp, 0);
}

TEST_F(MessageTest, GetRequestSerialization)
{
    GetRequestMessage request("test_key");

    const auto serialized_result = request.serialize();
    ASSERT_TRUE(serialized_result.ok());

    const auto &data = serialized_result.value();
    EXPECT_GT(data.size(), MessageHeader::HEADER_SIZE);

    // Deserialize and verify
    GetRequestMessage deserialized;
    const auto deserialize_status = deserialized.deserialize(data);
    EXPECT_EQ(deserialize_status, Status::OK);
    EXPECT_EQ(deserialized.getKey(), "test_key");
    EXPECT_EQ(deserialized.getType(), MessageType::GET_REQUEST);
}

TEST_F(MessageTest, GetResponseSerialization)
{
    GetResponseMessage response(123, Status::OK, "test_value");

    const auto serialized_result = response.serialize();
    ASSERT_TRUE(serialized_result.ok());

    const auto &data = serialized_result.value();
    EXPECT_GT(data.size(), MessageHeader::HEADER_SIZE);

    // Deserialize and verify
    GetResponseMessage deserialized;
    const auto deserialize_status = deserialized.deserialize(data);
    EXPECT_EQ(deserialize_status, Status::OK);
    EXPECT_EQ(deserialized.getMessageId(), 123);
    EXPECT_EQ(deserialized.getStatus(), Status::OK);
    EXPECT_EQ(deserialized.getValue(), "test_value");
    EXPECT_EQ(deserialized.getType(), MessageType::GET_RESPONSE);
}

TEST_F(MessageTest, PutRequestSerialization)
{
    PutRequestMessage request("test_key", "test_value");

    const auto serialized_result = request.serialize();
    ASSERT_TRUE(serialized_result.ok());

    const auto &data = serialized_result.value();
    EXPECT_GT(data.size(), MessageHeader::HEADER_SIZE);

    // Deserialize and verify
    PutRequestMessage deserialized;
    const auto deserialize_status = deserialized.deserialize(data);
    EXPECT_EQ(deserialize_status, Status::OK);
    EXPECT_EQ(deserialized.getKey(), "test_key");
    EXPECT_EQ(deserialized.getValue(), "test_value");
    EXPECT_EQ(deserialized.getType(), MessageType::PUT_REQUEST);
}

TEST_F(MessageTest, PutResponseSerialization)
{
    PutResponseMessage response(456, Status::OK);

    const auto serialized_result = response.serialize();
    ASSERT_TRUE(serialized_result.ok());

    const auto &data = serialized_result.value();
    EXPECT_GT(data.size(), MessageHeader::HEADER_SIZE);

    // Deserialize and verify
    PutResponseMessage deserialized;
    const auto deserialize_status = deserialized.deserialize(data);
    EXPECT_EQ(deserialize_status, Status::OK);
    EXPECT_EQ(deserialized.getMessageId(), 456);
    EXPECT_EQ(deserialized.getStatus(), Status::OK);
    EXPECT_EQ(deserialized.getType(), MessageType::PUT_RESPONSE);
}

TEST_F(MessageTest, DeleteRequestSerialization)
{
    DeleteRequestMessage request("delete_key");

    const auto serialized_result = request.serialize();
    ASSERT_TRUE(serialized_result.ok());

    const auto &data = serialized_result.value();
    EXPECT_GT(data.size(), MessageHeader::HEADER_SIZE);

    // Deserialize and verify
    DeleteRequestMessage deserialized;
    const auto deserialize_status = deserialized.deserialize(data);
    EXPECT_EQ(deserialize_status, Status::OK);
    EXPECT_EQ(deserialized.getKey(), "delete_key");
    EXPECT_EQ(deserialized.getType(), MessageType::DELETE_REQUEST);
}

TEST_F(MessageTest, DeleteResponseSerialization)
{
    DeleteResponseMessage response(789, Status::NOT_FOUND);

    const auto serialized_result = response.serialize();
    ASSERT_TRUE(serialized_result.ok());

    const auto &data = serialized_result.value();
    EXPECT_GT(data.size(), MessageHeader::HEADER_SIZE);

    // Deserialize and verify
    DeleteResponseMessage deserialized;
    const auto deserialize_status = deserialized.deserialize(data);
    EXPECT_EQ(deserialize_status, Status::OK);
    EXPECT_EQ(deserialized.getMessageId(), 789);
    EXPECT_EQ(deserialized.getStatus(), Status::NOT_FOUND);
    EXPECT_EQ(deserialized.getType(), MessageType::DELETE_RESPONSE);
}

TEST_F(MessageTest, HeartbeatSerialization)
{
    HeartbeatMessage heartbeat("node_123");

    const auto serialized_result = heartbeat.serialize();
    ASSERT_TRUE(serialized_result.ok());

    const auto &data = serialized_result.value();
    EXPECT_GT(data.size(), MessageHeader::HEADER_SIZE);

    // Deserialize and verify
    HeartbeatMessage deserialized;
    const auto deserialize_status = deserialized.deserialize(data);
    EXPECT_EQ(deserialize_status, Status::OK);
    EXPECT_EQ(deserialized.getNodeId(), "node_123");
    EXPECT_EQ(deserialized.getType(), MessageType::HEARTBEAT);
}

TEST_F(MessageTest, HeartbeatResponseSerialization)
{
    HeartbeatResponseMessage response(999, "responding_node");

    const auto serialized_result = response.serialize();
    ASSERT_TRUE(serialized_result.ok());

    const auto &data = serialized_result.value();
    EXPECT_GT(data.size(), MessageHeader::HEADER_SIZE);

    // Deserialize and verify
    HeartbeatResponseMessage deserialized;
    const auto deserialize_status = deserialized.deserialize(data);
    EXPECT_EQ(deserialize_status, Status::OK);
    EXPECT_EQ(deserialized.getMessageId(), 999);
    EXPECT_EQ(deserialized.getNodeId(), "responding_node");
    EXPECT_EQ(deserialized.getType(), MessageType::HEARTBEAT_RESPONSE);
}

TEST_F(MessageTest, ErrorResponseSerialization)
{
    ErrorResponseMessage error(111, Status::TIMEOUT, "Connection timed out");

    const auto serialized_result = error.serialize();
    ASSERT_TRUE(serialized_result.ok());

    const auto &data = serialized_result.value();
    EXPECT_GT(data.size(), MessageHeader::HEADER_SIZE);

    // Deserialize and verify
    ErrorResponseMessage deserialized;
    const auto deserialize_status = deserialized.deserialize(data);
    EXPECT_EQ(deserialize_status, Status::OK);
    EXPECT_EQ(deserialized.getMessageId(), 111);
    EXPECT_EQ(deserialized.getErrorStatus(), Status::TIMEOUT);
    EXPECT_EQ(deserialized.getErrorMessage(), "Connection timed out");
    EXPECT_EQ(deserialized.getType(), MessageType::ERROR_RESPONSE);
}

TEST_F(MessageTest, MessageFactory)
{
    auto get_msg = Message::createMessage(MessageType::GET_REQUEST);
    ASSERT_NE(get_msg, nullptr);
    EXPECT_EQ(get_msg->getType(), MessageType::GET_REQUEST);

    auto put_msg = Message::createMessage(MessageType::PUT_REQUEST);
    ASSERT_NE(put_msg, nullptr);
    EXPECT_EQ(put_msg->getType(), MessageType::PUT_REQUEST);

    auto unknown_msg = Message::createMessage(static_cast<MessageType>(255));
    EXPECT_EQ(unknown_msg, nullptr);
}

TEST_F(MessageTest, MessageFromBytes)
{
    // Create and serialize a message
    GetRequestMessage original("test_key_from_bytes");
    const auto serialized_result = original.serialize();
    ASSERT_TRUE(serialized_result.ok());

    // Reconstruct from bytes
    const auto reconstructed_result = Message::fromBytes(serialized_result.value());
    ASSERT_TRUE(reconstructed_result.ok());

    const auto &reconstructed = reconstructed_result.value();
    EXPECT_EQ(reconstructed->getType(), MessageType::GET_REQUEST);

    const auto &get_msg = static_cast<const GetRequestMessage &>(*reconstructed);
    EXPECT_EQ(get_msg.getKey(), "test_key_from_bytes");
}

TEST_F(MessageTest, EmptyValues)
{
    // Test with empty key
    GetRequestMessage empty_key("");
    const auto serialized = empty_key.serialize();
    ASSERT_TRUE(serialized.ok());

    GetRequestMessage deserialized;
    EXPECT_EQ(deserialized.deserialize(serialized.value()), Status::OK);
    EXPECT_TRUE(deserialized.getKey().empty());

    // Test with empty value
    PutRequestMessage empty_value("key", "");
    const auto serialized2 = empty_value.serialize();
    ASSERT_TRUE(serialized2.ok());

    PutRequestMessage deserialized2;
    EXPECT_EQ(deserialized2.deserialize(serialized2.value()), Status::OK);
    EXPECT_EQ(deserialized2.getKey(), "key");
    EXPECT_TRUE(deserialized2.getValue().empty());
}

TEST_F(MessageTest, LargeMessages)
{
    // Test with large key and value
    const std::string large_key(1000, 'k');
    const std::string large_value(10000, 'v');

    PutRequestMessage large_msg(large_key, large_value);
    const auto serialized = large_msg.serialize();
    ASSERT_TRUE(serialized.ok());

    PutRequestMessage deserialized;
    EXPECT_EQ(deserialized.deserialize(serialized.value()), Status::OK);
    EXPECT_EQ(deserialized.getKey(), large_key);
    EXPECT_EQ(deserialized.getValue(), large_value);
}

TEST_F(MessageTest, UtilityFunctions)
{
    // Test message type to string conversion
    EXPECT_EQ(messageTypeToString(MessageType::GET_REQUEST), "GET_REQUEST");
    EXPECT_EQ(messageTypeToString(MessageType::PUT_RESPONSE), "PUT_RESPONSE");
    EXPECT_EQ(messageTypeToString(MessageType::HEARTBEAT), "HEARTBEAT");
    EXPECT_EQ(messageTypeToString(static_cast<MessageType>(255)), "UNKNOWN");

    // Test request/response classification
    EXPECT_TRUE(isRequestMessage(MessageType::GET_REQUEST));
    EXPECT_TRUE(isRequestMessage(MessageType::PUT_REQUEST));
    EXPECT_FALSE(isRequestMessage(MessageType::GET_RESPONSE));

    EXPECT_TRUE(isResponseMessage(MessageType::GET_RESPONSE));
    EXPECT_TRUE(isResponseMessage(MessageType::ERROR_RESPONSE));
    EXPECT_FALSE(isResponseMessage(MessageType::GET_REQUEST));

    // Test response type mapping
    EXPECT_EQ(getResponseType(MessageType::GET_REQUEST), MessageType::GET_RESPONSE);
    EXPECT_EQ(getResponseType(MessageType::PUT_REQUEST), MessageType::PUT_RESPONSE);
    EXPECT_EQ(getResponseType(MessageType::HEARTBEAT), MessageType::HEARTBEAT_RESPONSE);
}

TEST_F(MessageTest, InvalidMessageDeserialization)
{
    // Test with invalid data
    std::vector<std::uint8_t> invalid_data = {1, 2, 3, 4, 5};

    GetRequestMessage msg;
    const auto status = msg.deserialize(invalid_data);
    EXPECT_NE(status, Status::OK);

    // Test Message::fromBytes with invalid data
    const auto result = Message::fromBytes(invalid_data);
    EXPECT_FALSE(result.ok());
}

TEST_F(MessageTest, MessageIdGeneration)
{
    GetRequestMessage msg1("key1");
    GetRequestMessage msg2("key2");

    // Message IDs should be different
    EXPECT_NE(msg1.getMessageId(), msg2.getMessageId());

    // Message IDs should be positive
    EXPECT_GT(msg1.getMessageId(), 0);
    EXPECT_GT(msg2.getMessageId(), 0);
}