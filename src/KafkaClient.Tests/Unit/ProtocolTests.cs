using System;
using System.Collections.Generic;
using System.Linq;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using Xunit;

namespace KafkaClient.Tests.Unit
{
    public class ProtocolTests
    {
        private readonly Random _randomizer = new Random();

        [Fact]
        public void HeaderShouldCorrectPackByteLengths()
        {
            var result = new ApiVersionsRequest().ToBytes(new RequestContext(123456789, clientId: "test"));

            var withoutLength = new byte[result.Count - 4];
            Buffer.BlockCopy(result.Array, 4, withoutLength, 0, result.Count - 4);
            Assert.Equal(14, withoutLength.Length);
            Assert.Equal(new byte[] { 0, 18, 0, 0, 7, 91, 205, 21, 0, 4, 116, 101, 115, 116 }, withoutLength);
        }

        #region Messages

        [Fact]
        public void DecodeMessageShouldThrowWhenCrcFails()
        {
            var testMessage = new Message(value: "kafka test message.", key: "test");

            using (var writer = new KafkaWriter())
            {
                testMessage.WriteTo(writer);
                var encoded = writer.ToSegment(false);
                encoded.Array[encoded.Offset] += 1;
                using (var reader = new KafkaReader(encoded))
                {
                    Assert.Throws<CrcValidationException>(() => reader.ReadMessage(encoded.Count, 0).First());
                }
            }
        }

        [Theory]
        [InlineData("test key", "test message")]
        [InlineData(null, "test message")]
        [InlineData("test key", null)]
        [InlineData(null, null)]
        public void EnsureMessageEncodeAndDecodeAreCompatible(string key, string value)
        {
            var testMessage = new Message(key: key, value: value);

            using (var writer = new KafkaWriter())
            {
                testMessage.WriteTo(writer);
                var encoded = writer.ToSegment(false);
                using (var reader = new KafkaReader(encoded))
                {
                    var result = reader.ReadMessage(encoded.Count, 0).First();

                    Assert.Equal(testMessage.Key, result.Key);
                    Assert.Equal(testMessage.Value, result.Value);
                }
            }
        }

        [Fact]
        public void EncodeMessageSetEncodesMultipleMessages()
        {
            //expected generated from python library
            var expected = new byte[]
                {
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 45, 70, 24, 62, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 48, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 16, 90, 65, 40, 168, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 16, 195, 72, 121, 18, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 50
                };

            var messages = new[]
                {
                    new Message("0", "1"),
                    new Message("1", "1"),
                    new Message("2", "1")
                };

            using (var writer = new KafkaWriter())
            {
                writer.Write(messages);
                var result = writer.ToSegment(false);
                Assert.Equal(expected, result);
            }
        }

        [Fact]
        public void DecodeMessageSetShouldHandleResponseWithMaxBufferSizeHit()
        {
            using (var reader = new KafkaReader(MessageHelper.FetchResponseMaxBytesOverflow))
            {
                //This message set has a truncated message bytes at the end of it
                var result = reader.ReadMessages(0);

                var message = result.First().Value.ToUtf8String();

                Assert.Equal("test", message);
                Assert.Equal(529, result.Count);
            }
        }

        [Fact]
        public void WhenMessageIsTruncatedThenBufferUnderRunExceptionIsThrown()
        {
            // arrange
            var message = new byte[] { };
            var messageSize = message.Length + 1;
            using (var writer = new KafkaWriter())
            {
                writer.Write(0L)
                       .Write(messageSize)
                       .Write(new ArraySegment<byte>(message));
                var segment = writer.ToSegment();
                using (var reader = new KafkaReader(segment))
                {
                    // act/assert
                    Assert.Throws<BufferUnderRunException>(() => reader.ReadMessages(0));
                }
            }
        }

        [Fact]
        public void WhenMessageIsExactlyTheSizeOfBufferThenMessageIsDecoded()
        {
            // arrange
            var expectedPayloadBytes = new ArraySegment<byte>(new byte[] { 1, 2, 3, 4 });
            using (var writer = new KafkaWriter())
            {
                writer.Write(0L);
                using (writer.MarkForLength())
                {
                    new Message(expectedPayloadBytes, new ArraySegment<byte>(new byte[] { 0 }), 0, version: 0).WriteTo(writer);
                }
                var segment = writer.ToSegment();

                // act/assert
                using (var reader = new KafkaReader(segment))
                {
                    var messages = reader.ReadMessages(0);
                    var actualPayload = messages.First().Value;

                    // assert
                    var expectedPayload = new byte[] { 1, 2, 3, 4 };
                    Assert.True(actualPayload.HasEqualElementsInOrder(expectedPayload));
                }
            }
        }

        #endregion

        #region Request / Response

#if DOTNETSTANDARD
        [Theory, CombinatorialData]
        public void ProduceRequest(
            [CombinatorialValues(0, 1, 2)] short version,
            [CombinatorialValues(0, 2, -1)] short acks, 
            [CombinatorialValues(0, 1000)] int timeoutMilliseconds, 
            [CombinatorialValues("testTopic")] string topic, 
            [CombinatorialValues(1, 10)] int topicsPerRequest, 
            [CombinatorialValues(1, 5)] int totalPartitions, 
            [CombinatorialValues(3)] int messagesPerSet,
            [CombinatorialValues(MessageCodec.None, MessageCodec.Gzip, MessageCodec.Snappy)] MessageCodec codec)
        {
            var payloads = new List<ProduceRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partition = 1 + t%totalPartitions;
                payloads.Add(new ProduceRequest.Topic(topic + t, partition, GenerateMessages(messagesPerSet, (byte) (version >= 2 ? 1 : 0), codec), codec));
            }
            var request = new ProduceRequest(payloads, TimeSpan.FromMilliseconds(timeoutMilliseconds), acks);
            var requestWithUpdatedAttribute = new ProduceRequest(request.topics.Select(t => new ProduceRequest.Topic(t.topic, t.partition_id,
                t.Messages.Select(m => m.Attribute == 0 ? m : new Message(m.Value, m.Key, 0, m.Offset, m.MessageVersion, m.Timestamp)))),
                request.timeout, request.acks);

            request.AssertCanEncodeDecodeRequest(version, forComparison: requestWithUpdatedAttribute);
        }
#endif

        [Theory, CombinatorialData]
        public void ProduceResponse(
            [CombinatorialValues(0, 1, 2)] short version,
            [CombinatorialValues(-1, 0, 10000000)] long timestampMilliseconds, 
            [CombinatorialValues("testTopic")] string topicName, 
            [CombinatorialValues(1, 10)] int topicsPerRequest, 
            [CombinatorialValues(1, 5)] int totalPartitions, 
            [CombinatorialValues(
                ErrorCode.NONE,
                ErrorCode.CORRUPT_MESSAGE
            )] ErrorCode errorCode,
            [CombinatorialValues(0, 100000)] int throttleTime)
        {
            var topics = new List<ProduceResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new ProduceResponse.Topic(topicName + t, t % totalPartitions, errorCode, _randomizer.Next(), version >= 2 ? DateTimeOffset.FromUnixTimeMilliseconds(timestampMilliseconds) : (DateTimeOffset?)null));
            }
            var response = new ProduceResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Theory, CombinatorialData]
        public void FetchRequest(
            [CombinatorialValues(0, 1, 2, 3)] short version,
            [CombinatorialValues(0, 100)] int maxWaitMilliseconds, 
            [CombinatorialValues(0, 64000)] int minBytes, 
            [CombinatorialValues("testTopic")] string topic, 
            [CombinatorialValues(1, 10)] int topicsPerRequest, 
            [CombinatorialValues(1, 5)] int totalPartitions, 
            [CombinatorialValues(25600000)] int maxBytes)
        {
            var fetches = new List<FetchRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                fetches.Add(new FetchRequest.Topic(topic + t, t % totalPartitions, _randomizer.Next(0, int.MaxValue), maxBytes));
            }
            var request = new FetchRequest(fetches, TimeSpan.FromMilliseconds(maxWaitMilliseconds), minBytes, version >= 3 ? maxBytes / _randomizer.Next(1, maxBytes) : 0);
            request.AssertCanEncodeDecodeRequest(version);
        }

#if DOTNETSTANDARD
        [Theory, CombinatorialData]
        public void FetchResponse(
            [CombinatorialValues(0, 1, 2, 3)] short version,
            [CombinatorialValues(0, 1234)] int throttleTime,
            [CombinatorialValues("testTopic")] string topicName, 
            [CombinatorialValues(1, 10)] int topicsPerRequest, 
            [CombinatorialValues(1, 5)] int totalPartitions, 
            [CombinatorialValues(MessageCodec.None, MessageCodec.Gzip, MessageCodec.Snappy)] MessageCodec codec, 
            [CombinatorialValues(
                ErrorCode.NONE,
                ErrorCode.OFFSET_OUT_OF_RANGE
            )] ErrorCode errorCode, 
            [CombinatorialValues(3)] int messagesPerSet
            )
        {
            var topics = new List<FetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                var messages = GenerateMessages(messagesPerSet, (byte) (version >= 2 ? 1 : 0), codec);
                topics.Add(new FetchResponse.Topic(topicName + t, partitionId, _randomizer.Next(), errorCode, messages));
            }
            var response = new FetchResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);
            var responseWithUpdatedAttribute = new FetchResponse(response.responses.Select(t => new FetchResponse.Topic(t.topic, t.partition_id, t.high_watermark, t.error_code, 
                t.Messages.Select(m => m.Attribute == 0 ? m : new Message(m.Value, m.Key, 0, m.Offset, m.MessageVersion, m.Timestamp)))), 
                response.throttle_time_ms);

            response.AssertCanEncodeDecodeResponse(version, forComparison: responseWithUpdatedAttribute);
        }
#endif

        [Theory, CombinatorialData]
        public void OffsetsRequest(
            [CombinatorialValues(0, 1)] short version,
            [CombinatorialValues("testTopic")] string topic, 
            [CombinatorialValues(1, 10)] int topicsPerRequest, 
            [CombinatorialValues(1, 5)] int totalPartitions, 
            [CombinatorialValues(-2, -1, 123456, 10000000)] long time,
            [CombinatorialValues(1, 10)] int maxOffsets)
        {
            var topics = new List<OffsetsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var offset = new OffsetsRequest.Topic(topic + t, t % totalPartitions, time, version == 0 ? maxOffsets : 1);
                topics.Add(offset);
            }
            var request = new OffsetsRequest(topics);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Theory, CombinatorialData]
        public void OffsetsResponse(
            [CombinatorialValues(0, 1)] short version,
            [CombinatorialValues("testTopic")] string topicName, 
            [CombinatorialValues(1, 10)] int topicsPerRequest, 
            [CombinatorialValues(5)] int totalPartitions, 
            [CombinatorialValues(
                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                ErrorCode.NOT_LEADER_FOR_PARTITION,
                ErrorCode.UNKNOWN
            )] ErrorCode errorCode, 
            [CombinatorialValues(1, 5)] int offsetsPerPartition)
        {
            var topics = new List<OffsetsResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                for (var o = 0; o < offsetsPerPartition; o++) {
                    topics.Add(new OffsetsResponse.Topic(topicName + t, partitionId, errorCode, _randomizer.Next(-1, int.MaxValue), version >= 1 ? (DateTimeOffset?)DateTimeOffset.UtcNow : null));
                }
            }
            var response = new OffsetsResponse(topics);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Theory, CombinatorialData]
        public void MetadataRequest(
            [CombinatorialValues("testTopic")] string topic,
            [CombinatorialValues(0, 1, 10)] int topicsPerRequest)
        {
            var topics = new List<string>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(topic + t);
            }
            var request = new MetadataRequest(topics);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void MetadataResponse(
            [CombinatorialValues(0, 1, 2)] short version,
            [CombinatorialValues(1, 15)] int brokersPerRequest,
            [CombinatorialValues("testTopic")] string topicName,
            [CombinatorialValues(1, 10)] int topicsPerRequest,
            [CombinatorialValues(1, 5)] int partitionsPerTopic,
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
             )] ErrorCode errorCode)
        {
            var brokers = new List<Server>();
            for (var b = 0; b < brokersPerRequest; b++) {
                string rack = null;
                if (version >= 1) {
                    rack = "Rack" + b;
                }
                brokers.Add(new Server(b, "broker-" + b, 9092 + b, rack));
            }
            var topics = new List<MetadataResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitions = new List<MetadataResponse.Partition>();
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var leader = _randomizer.Next(0, brokersPerRequest - 1);
                    var replica = 0;
                    var replicas = _randomizer.Next(0, brokersPerRequest - 1).Repeat(() => replica++);
                    var isr = 0;
                    var isrs = _randomizer.Next(0, replica).Repeat(() => isr++);
                    partitions.Add(new MetadataResponse.Partition(partitionId, leader, errorCode, replicas, isrs));
                }
                topics.Add(new MetadataResponse.Topic(topicName + t, errorCode, partitions, version >= 1 ? topicsPerRequest%2 == 0 : (bool?)null));
            }
            var response = new MetadataResponse(brokers, topics, version >= 1 ? brokersPerRequest : (int?)null, version >= 2 ? $"cluster-{version}" : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Theory, CombinatorialData]
        public void OffsetCommitRequest(
            [CombinatorialValues(0, 1, 2)] short version,
            [CombinatorialValues("group1", "group2")] string groupId,
            [CombinatorialValues(0, 5)] int generation,
            [CombinatorialValues(-1, 20000)] int retentionTime,
            [CombinatorialValues("testTopic")] string topic,
            [CombinatorialValues(1, 10)] int topicsPerRequest,
            [CombinatorialValues(5)] int maxPartitions,
            [CombinatorialValues(10)] int maxOffsets,
            [CombinatorialValues(null, "something useful for the client")] string metadata)
        {
            var timestamp = retentionTime;
            var offsetCommits = new List<OffsetCommitRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                offsetCommits.Add(new OffsetCommitRequest.Topic(
                                      topic + t,
                                      t%maxPartitions,
                                      _randomizer.Next(0, int.MaxValue),
                                      metadata,
                                      version == 1 ? timestamp : (long?)null));
            }
            var request = new OffsetCommitRequest(
                groupId,
                offsetCommits,
                version >= 1 ? "member" + generation : null,
                version >= 1 ? generation : 0,
                version >= 2 && retentionTime >= 0 ? (TimeSpan?) TimeSpan.FromMilliseconds(retentionTime) : null);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Theory, CombinatorialData]
        public void OffsetCommitResponse(
            [CombinatorialValues("testTopic")] string topicName,
            [CombinatorialValues(1, 10)] int topicsPerRequest,
            [CombinatorialValues(1, 5)] int partitionsPerTopic,
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode)
        {
            var topics = new List<TopicResponse>();
            for (var t = 0; t < topicsPerRequest; t++) {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    topics.Add(new TopicResponse(topicName + t, partitionId, errorCode));
                }
            }
            var response = new OffsetCommitResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Theory, CombinatorialData]
        public void OffsetFetchRequest(
            [CombinatorialValues("group1", "group2")] string groupId,
            [CombinatorialValues("testTopic")] string topic,
            [CombinatorialValues(1, 10)] int topicsPerRequest,
            [CombinatorialValues(5)] int maxPartitions)
        {
            var topics = new List<TopicPartition>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new TopicPartition(topic + t, t % maxPartitions));
            }
            var request = new OffsetFetchRequest(groupId, topics);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void OffsetFetchResponse(
            [CombinatorialValues("testTopic")] string topicName,
            [CombinatorialValues(1, 10)] int topicsPerRequest,
            [CombinatorialValues(1, 5)] int partitionsPerTopic,
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                 ErrorCode.GROUP_LOAD_IN_PROGRESS,
                 ErrorCode.NOT_COORDINATOR_FOR_GROUP,
                 ErrorCode.ILLEGAL_GENERATION,
                 ErrorCode.UNKNOWN_MEMBER_ID,
                 ErrorCode.TOPIC_AUTHORIZATION_FAILED,
                 ErrorCode.GROUP_AUTHORIZATION_FAILED
             )] ErrorCode errorCode)
        {
            var topics = new List<OffsetFetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var offset = (long)_randomizer.Next(int.MinValue, int.MaxValue);
                    topics.Add(new OffsetFetchResponse.Topic(topicName + t, partitionId, errorCode, offset, offset >= 0 ? topicName : string.Empty));
                }
            }
            var response = new OffsetFetchResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Theory, CombinatorialData]
        public void GroupCoordinatorRequest([CombinatorialValues("group1", "group2")] string groupId)
        {
            var request = new GroupCoordinatorRequest(groupId);
            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void GroupCoordinatorResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE,
                 ErrorCode.GROUP_AUTHORIZATION_FAILED
             )] ErrorCode errorCode,
            [CombinatorialValues(0, 1)] int coordinatorId
            )
        {
            var response = new GroupCoordinatorResponse(errorCode, coordinatorId, "broker-" + coordinatorId, 9092 + coordinatorId);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Fact]
        public void ApiVersionsRequest()
        {
            var request = new ApiVersionsRequest();
            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void ApiVersionsResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.BROKER_NOT_AVAILABLE
             )] ErrorCode errorCode
            )
        {
            var supported = new List<ApiVersionsResponse.VersionSupport>();
            for (short apiKey = 0; apiKey <= 18; apiKey++) {
                supported.Add(new ApiVersionsResponse.VersionSupport((ApiKey)apiKey, 0, (short)_randomizer.Next(0, 2)));
            }
            var response = new ApiVersionsResponse(errorCode, supported);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Theory, CombinatorialData]
        public void JoinGroupRequest(
            [CombinatorialValues(0, 1)] short version,
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues(1, 20000)] int sessionTimeout,
            [CombinatorialValues("", "an existing member")] string memberId, 
            [CombinatorialValues("consumer", "other")] string protocolType, 
            [CombinatorialValues(1, 10)] int protocolsPerRequest)
        {
            var protocols = new List<JoinGroupRequest.GroupProtocol>();
            for (var p = 0; p < protocolsPerRequest; p++) {
                var bytes = new byte[protocolsPerRequest*100];
                _randomizer.NextBytes(bytes);
                protocols.Add(new JoinGroupRequest.GroupProtocol(new ByteTypeMetadata("known", new ArraySegment<byte>(bytes))));
            }
            var request = new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout), memberId, protocolType, protocols, version >= 1 ? (TimeSpan?)TimeSpan.FromMilliseconds(sessionTimeout * 2) : null);

            request.AssertCanEncodeDecodeRequest(version, new ByteMembershipEncoder(protocolType));
        }

        [Theory, CombinatorialData]
        public void JoinGroupResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [CombinatorialValues(0, 1, 20000)] int generationId,
            [CombinatorialValues("consumer", "other")] string protocol, 
            [CombinatorialValues("test", "a groupId")] string leaderId, 
            [CombinatorialValues("", "an existing member")] string memberId, 
            [CombinatorialValues(1, 10)] int memberCount)
        {
            var members = new List<JoinGroupResponse.Member>();
            for (var m = 0; m < memberCount; m++) {
                var bytes = new byte[memberCount*100];
                _randomizer.NextBytes(bytes);
                members.Add(new JoinGroupResponse.Member(memberId + m, new ByteTypeMetadata("known", new ArraySegment<byte>(bytes))));
            }
            var response = new JoinGroupResponse(errorCode, generationId, "known", leaderId, memberId, members);

            response.AssertCanEncodeDecodeResponse(0, new ByteMembershipEncoder(protocol));
        }

        [Theory, CombinatorialData]
        public void JoinConsumerGroupRequest(
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues(1, 20000)] int sessionTimeout,
            [CombinatorialValues("", "an existing member")] string memberId, 
            [CombinatorialValues("mine", "yours")] string protocol, 
            [CombinatorialValues(1, 10)] int protocolsPerRequest)
        {
            var encoder = new ConsumerEncoder();
            var protocols = new List<JoinGroupRequest.GroupProtocol>();
            for (var p = 0; p < protocolsPerRequest; p++) {
                var userData = new byte[protocolsPerRequest*100];
                _randomizer.NextBytes(userData);
                var metadata = new ConsumerProtocolMetadata(new []{ groupId, memberId, protocol }, protocol + p, new ArraySegment<byte>(userData), 0);
                protocols.Add(new JoinGroupRequest.GroupProtocol(metadata));
            }
            var request = new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout), memberId, ConsumerEncoder.Protocol, protocols);

            request.AssertCanEncodeDecodeRequest(0, encoder);
        }

        [Theory, CombinatorialData]
        public void JoinConsumerGroupResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [CombinatorialValues(0, 1, 20000)] int generationId,
            [CombinatorialValues("consumer")] string protocol, 
            [CombinatorialValues("test", "a groupId")] string leaderId, 
            [CombinatorialValues("", "an existing member")] string memberId, 
            [CombinatorialValues(1, 10)] int memberCount)
        {
            var encoder = new ConsumerEncoder();
            var members = new List<JoinGroupResponse.Member>();
            for (var m = 0; m < memberCount; m++) {
                var userData = new byte[memberCount*100];
                _randomizer.NextBytes(userData);
                var metadata = new ConsumerProtocolMetadata(new []{ protocol, memberId, leaderId }, protocol, new ArraySegment<byte>(userData), 0);
                members.Add(new JoinGroupResponse.Member(memberId + m, metadata));
            }
            var response = new JoinGroupResponse(errorCode, generationId, protocol, leaderId, memberId, members);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Theory, CombinatorialData]
        public void HeartbeatRequest(
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues(0, 1, 20000)] int generationId,
            [CombinatorialValues("", "an existing member")] string memberId)
        {
            var request = new HeartbeatRequest(groupId, generationId, memberId);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void HeartbeatResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode)
        {
            var response = new HeartbeatResponse(errorCode);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Theory, CombinatorialData]
        public void LeaveGroupRequest(
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues("", "an existing member")] string memberId)
        {
            var request = new LeaveGroupRequest(groupId, memberId);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void LeaveGroupResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode)
        {
            var response = new LeaveGroupResponse(errorCode);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Theory, CombinatorialData]
        public void SyncGroupRequest(
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues(0, 1, 20000)] int generationId,
            [CombinatorialValues("", "an existing member")] string memberId, 
            [CombinatorialValues("consumer", "other")] string protocolType, 
            [CombinatorialValues(1, 10)] int assignmentsPerRequest)
        {
            var assignments = new List<SyncGroupRequest.GroupAssignment>();
            for (var a = 0; a < assignmentsPerRequest; a++) {
                var bytes = new byte[assignmentsPerRequest*100];
                _randomizer.NextBytes(bytes);
                assignments.Add(new SyncGroupRequest.GroupAssignment(protocolType + a, new ByteTypeAssignment(new ArraySegment<byte>(bytes))));
            }
            var request = new SyncGroupRequest(groupId, generationId, memberId, assignments);

            request.AssertCanEncodeDecodeRequest(0, new ByteMembershipEncoder(protocolType));
        }

        [Theory, CombinatorialData]
        public void SyncGroupResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode)
        {
            var bytes = new byte[1000];
            _randomizer.NextBytes(bytes);
            var response = new SyncGroupResponse(errorCode, new ByteTypeAssignment(new ArraySegment<byte>(bytes)));

            response.AssertCanEncodeDecodeResponse(0, new ByteMembershipEncoder("protocolType"));
        }

        [Theory, CombinatorialData]
        public void SyncConsumerGroupRequest(
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues(0, 1, 20000)] int generationId,
            [CombinatorialValues("", "an existing member")] string memberId, 
            [CombinatorialValues("consumer")] string protocolType, 
            [CombinatorialValues(1, 10)] int assignmentsPerRequest)
        {
            var encoder = new ConsumerEncoder();
            var assignments = new List<SyncGroupRequest.GroupAssignment>();
            for (var a = 0; a < assignmentsPerRequest; a++) {
                var topics = new List<TopicPartition>();
                for (var t = 0; t < assignmentsPerRequest; t++) {
                    topics.Add(new TopicPartition(groupId + t, t));
                }
                var userData = new byte[assignmentsPerRequest*100];
                _randomizer.NextBytes(userData);
                var assignment = new ConsumerMemberAssignment(topics, new ArraySegment<byte>(userData), 0);
                assignments.Add(new SyncGroupRequest.GroupAssignment(protocolType + a, assignment));
            }
            var request = new SyncGroupRequest(groupId, generationId, memberId, assignments);

            request.AssertCanEncodeDecodeRequest(0, encoder);
        }

        [Theory, CombinatorialData]
        public void SyncConsumerGroupResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [CombinatorialValues(1, 10)] int memberCount)
        {
            var encoder = new ConsumerEncoder();
            var topics = new List<TopicPartition>();
            for (var t = 0; t < memberCount; t++) {
                topics.Add(new TopicPartition("topic foo" + t, t));
            }
            var userData = new byte[memberCount*100];
            _randomizer.NextBytes(userData);
            var assignment = new ConsumerMemberAssignment(topics, new ArraySegment<byte>(userData), 0);
            var response = new SyncGroupResponse(errorCode, assignment);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Theory, CombinatorialData]
        public void DescribeGroupsRequest(
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues(1, 5, 10)] int count)
        {
            var groups = new string[count];
            for (var g = 0; g < count; g++) {
                groups[g] = groupId + g;
            }
            var request = new DescribeGroupsRequest(groups);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void DescribeGroupsResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues(2, 3)] int count,
            [CombinatorialValues(Protocol.DescribeGroupsResponse.Group.States.Stable, Protocol.DescribeGroupsResponse.Group.States.Dead)] string state, 
            [CombinatorialValues("consumer", "unknown")] string protocolType,
            [CombinatorialValues("good", "bad", "ugly")] string protocol)
        {
            var groups = new DescribeGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                var members = new List<DescribeGroupsResponse.Member>();
                for (var m = 0; m < count; m++) {
                    var metadata = new byte[count*100];
                    var assignment = new byte[count*10];
                    _randomizer.NextBytes(metadata);
                    _randomizer.NextBytes(assignment);

                    members.Add(new DescribeGroupsResponse.Member("member" + m, "client" + m, "host-" + m, new ByteTypeMetadata(protocol, new ArraySegment<byte>(metadata)), new ByteTypeAssignment(new ArraySegment<byte>(assignment))));
                }
                groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId + g, state, protocolType, protocol, members);
            }
            var response = new DescribeGroupsResponse(groups);

            response.AssertCanEncodeDecodeResponse(0, new ByteMembershipEncoder(protocolType));
        }

        [Theory, CombinatorialData]
        public void DescribeConsumerGroupsResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues(2, 3)] int count,
            [CombinatorialValues(Protocol.DescribeGroupsResponse.Group.States.Stable, Protocol.DescribeGroupsResponse.Group.States.AwaitingSync)] string state, 
            [CombinatorialValues("consumer")] string protocolType,
            [CombinatorialValues("good", "bad", "ugly")] string protocol)
        {
            var encoder = new ConsumerEncoder();
            var groups = new DescribeGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                var members = new List<DescribeGroupsResponse.Member>();
                for (var m = 0; m < count; m++) {
                    var memberId = "member" + m;
                    var userData = new byte[count*100];
                    _randomizer.NextBytes(userData);
                    var metadata = new ConsumerProtocolMetadata(new []{ protocol, memberId, memberId }, protocol, new ArraySegment<byte>(userData), 0);

                    var topics = new List<TopicPartition>();
                    for (var t = 0; t < count; t++) {
                        topics.Add(new TopicPartition("topic foo" + t, t));
                    }
                    var assignment = new ConsumerMemberAssignment(topics, new ArraySegment<byte>(userData), 0);

                    members.Add(new DescribeGroupsResponse.Member(memberId, "client" + m, "host-" + m, metadata, assignment));
                }
                groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId + g, state, protocolType, protocol, members);
            }
            var response = new DescribeGroupsResponse(groups);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Fact]
        public void ListGroupsRequest()
        {
            var request = new ListGroupsRequest();
            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void ListGroupsResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [CombinatorialValues("test", "a groupId")] string groupId, 
            [CombinatorialValues(2, 3)] int count,
            [CombinatorialValues("consumer")] string protocolType)
        {
            var groups = new ListGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                groups[g] = new ListGroupsResponse.Group(groupId + g, protocolType);
            }
            var response = new ListGroupsResponse(errorCode, groups);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Theory, CombinatorialData]
        public void SaslHandshakeRequest(
            [CombinatorialValues("EXTERNAL", "ANONYMOUS", "PLAIN", "OTP", "SKEY", "CRAM-MD5", "DIGEST-MD5", "SCRAM", "NTLM", "GSSAPI", "OAUTHBEARER")] string mechanism)
        {
            var request = new SaslHandshakeRequest(mechanism);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, PairwiseData]
        public void SaslHandshakeResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.OFFSET_METADATA_TOO_LARGE
             )] ErrorCode errorCode,
            [CombinatorialValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)] int count)
        {
            var mechanisms = new[] { "EXTERNAL", "ANONYMOUS", "PLAIN", "OTP", "SKEY", "CRAM-MD5", "DIGEST-MD5", "SCRAM", "NTLM", "GSSAPI", "OAUTHBEARER" };
            var response = new SaslHandshakeResponse(errorCode, mechanisms.Take(count));

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Theory, CombinatorialData]
        public void DeleteTopicsRequest(
            [CombinatorialValues("test", "anotherNameForATopic")] string topicName, 
            [CombinatorialValues(2, 3)] int count,
            [CombinatorialValues(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new string[count];
            for (var t = 0; t < count; t++) {
                topics[t] = topicName + t;
            }
            var request = new DeleteTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void DeleteTopicsResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.NOT_CONTROLLER
             )] ErrorCode errorCode,
            [CombinatorialValues("test", "anotherNameForATopic")] string topicName, 
            [CombinatorialValues(1, 5, 11)] int count)
        {
            var topics = new TopicsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicsResponse.Topic(topicName + t, errorCode);
            }
            var response = new DeleteTopicsResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Theory, CombinatorialData]
        public void CreateTopicsRequest(
            [CombinatorialValues("testTopic")] string topicName,
            [CombinatorialValues(1, 10)] int topicsPerRequest,
            [CombinatorialValues(1, 5)] int partitionsPerTopic,
            [CombinatorialValues(1, 3)] short replicationFactor,
            [CombinatorialValues(0, 3)] int configCount,
            [CombinatorialValues(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new List<CreateTopicsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var configs = new Dictionary<string, string>();
                for (var c = 0; c < configCount; c++) {
                    configs["config-" + c] = Guid.NewGuid().ToString("N");
                }
                if (configs.Count == 0 && _randomizer.Next() % 2 == 0) {
                    configs = null;
                }
                topics.Add(new CreateTopicsRequest.Topic(topicName + t, partitionsPerTopic, replicationFactor, configs));
            }
            var request = new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void CreateTopicsExplicitRequest(
            [CombinatorialValues("testTopic")] string topicName,
            [CombinatorialValues(1, 10)] int topicsPerRequest,
            [CombinatorialValues(1, 5)] int partitionsPerTopic,
            [CombinatorialValues(1, 3)] short replicationFactor,
            [CombinatorialValues(0, 3)] int configCount,
            [CombinatorialValues(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new List<CreateTopicsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var configs = new Dictionary<string, string>();
                for (var c = 0; c < configCount; c++) {
                    configs["config-" + c] = Guid.NewGuid().ToString("N");
                }
                if (configs.Count == 0 && _randomizer.Next() % 2 == 0) {
                    configs = null;
                }

                var assignments = new List<CreateTopicsRequest.ReplicaAssignment>();
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var replica = 0;
                    var replicas = _randomizer.Next(0, replicationFactor - 1).Repeat(() => replica++);
                    assignments.Add(new CreateTopicsRequest.ReplicaAssignment(partitionId, replicas));
                }
                topics.Add(new CreateTopicsRequest.Topic(topicName + t, assignments, configs));
            }
            var request = new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Theory, CombinatorialData]
        public void CreateTopicsResponse(
            [CombinatorialValues(
                 ErrorCode.NONE,
                 ErrorCode.INVALID_TOPIC_EXCEPTION,
                ErrorCode.INVALID_PARTITIONS
             )] ErrorCode errorCode,
            [CombinatorialValues("test", "anotherNameForATopic")] string topicName, 
            [CombinatorialValues(1, 5, 11)] int count)
        {
            var topics = new TopicsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicsResponse.Topic(topicName + t, errorCode);
            }
            var response = new CreateTopicsResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        private IEnumerable<Message> GenerateMessages(int count, byte version, MessageCodec codec = MessageCodec.None)
        {
            var random = new Random(42);
            var messages = new List<Message>();
            for (var m = 0; m < count; m++) {
                var key = m > 0 ? new byte[8] : null;
                var value = new byte[8*(m + 1)];
                if (key != null) {
                    random.NextBytes(key);
                }
                random.NextBytes(value);

                messages.Add(new Message(new ArraySegment<byte>(value), key != null ? new ArraySegment<byte>(key) : new ArraySegment<byte>(), (byte)codec, version: version, timestamp: version > 0 ? DateTimeOffset.UtcNow : (DateTimeOffset?)null));
            }
            return messages;
        }

        #endregion
    }
}