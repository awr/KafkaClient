using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public static class KafkaEncoder
    {
        public const int IntegerByteSize = 4;
        public const int CorrelationSize = IntegerByteSize;
        public const int ResponseHeaderSize = IntegerByteSize + CorrelationSize;

        public static T Decode<T>(IRequestContext context, ApiKey requstType, ArraySegment<byte> bytes) where T : class, IResponse
        {
            switch (requstType) {
                case ApiKey.Produce:
                    return (T)ProduceResponse(context, bytes);
                case ApiKey.Fetch:
                    return (T)FetchResponse(context, bytes);
                case ApiKey.Offsets:
                    return (T)OffsetResponse(context, bytes);
                case ApiKey.Metadata:
                    return (T)MetadataResponse(context, bytes);
                case ApiKey.OffsetCommit:
                    return (T)OffsetCommitResponse(context, bytes);
                case ApiKey.OffsetFetch:
                    return (T)OffsetFetchResponse(context, bytes);
                case ApiKey.GroupCoordinator:
                    return (T)GroupCoordinatorResponse(context, bytes);
                case ApiKey.JoinGroup:
                    return (T)JoinGroupResponse(context, bytes);
                case ApiKey.Heartbeat:
                    return (T)HeartbeatResponse(context, bytes);
                case ApiKey.LeaveGroup:
                    return (T)LeaveGroupResponse(context, bytes);
                case ApiKey.SyncGroup:
                    return (T)SyncGroupResponse(context, bytes);
                case ApiKey.DescribeGroups:
                    return (T)DescribeGroupsResponse(context, bytes);
                case ApiKey.ListGroups:
                    return (T)ListGroupsResponse(context, bytes);
                case ApiKey.SaslHandshake:
                    return (T)SaslHandshakeResponse(context, bytes);
                case ApiKey.ApiVersions:
                    return (T)ApiVersionsResponse(context, bytes);
                case ApiKey.CreateTopics:
                    return (T)CreateTopicsResponse(context, bytes);
                case ApiKey.DeleteTopics:
                    return (T)DeleteTopicsResponse(context, bytes);
                default:
                    return default (T);
            }
        }

        #region Decode

        private const int MessageHeaderSize = 12;

        /// <summary>
        /// Decode a byte[] that represents a collection of messages.
        /// </summary>
        /// <param name="reader">The reader</param>
        /// <param name="codec">The codec of the containing messageset, if any</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[]</returns>
        public static IImmutableList<Message> ReadMessages(this IKafkaReader reader, MessageCodec codec = MessageCodec.None)
        {
            var expectedLength = reader.ReadInt32();
            if (!reader.HasBytes(expectedLength)) throw new BufferUnderRunException($"Message set size of {expectedLength} is not fully available (codec {codec}).");

            var messages = ImmutableList<Message>.Empty;
            var finalPosition = reader.Position + expectedLength;
            while (reader.Position < finalPosition) {
                // this checks that we have at least the minimum amount of data to retrieve a header
                if (reader.HasBytes(MessageHeaderSize) == false) break;

                var offset = reader.ReadInt64();
                var messageSize = reader.ReadInt32();

                // if the stream does not have enough left in the payload, we got only a partial message
                if (reader.HasBytes(messageSize) == false) throw new BufferUnderRunException($"Message header size of {MessageHeaderSize} is not fully available (codec {codec}).");

                try {
                    messages = messages.AddRange(reader.ReadMessage(messageSize, offset));
                } catch (EndOfStreamException ex) {
                    throw new BufferUnderRunException($"Message size of {messageSize} is not available (codec {codec}).", ex);
                }
            }
            return messages;
        }

        /// <summary>
        /// Decode messages from a payload and assign it a given kafka offset.
        /// </summary>
        /// <param name="reader">The reader</param>
        /// <param name="messageSize">The size of the message, for Crc Hash calculation</param>
        /// <param name="offset">The offset represting the log entry from kafka of this message.</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[].</returns>
        /// <remarks>The return type is an Enumerable as the message could be a compressed message set.</remarks>
        public static IImmutableList<Message> ReadMessage(this IKafkaReader reader, int messageSize, long offset)
        {
            var crc = reader.ReadUInt32();
            var crcHash = reader.ReadCrc(messageSize - 4);
            if (crc != crcHash) throw new CrcValidationException(crc, crcHash);

            var messageVersion = reader.ReadByte();
            var attribute = reader.ReadByte();
            DateTimeOffset? timestamp = null;
            if (messageVersion >= 1) {
                var milliseconds = reader.ReadInt64();
                if (milliseconds >= 0) {
                    timestamp = DateTimeOffset.FromUnixTimeMilliseconds(milliseconds);
                }
            }
            var key = reader.ReadBytes();
            var value = reader.ReadBytes();

            var codec = (MessageCodec)(Message.CodecMask & attribute);
            if (codec == MessageCodec.None) {
                return ImmutableList<Message>.Empty.Add(new Message(value, key, attribute, offset, messageVersion, timestamp));
            }
            var uncompressedBytes = value.ToUncompressed(codec);
            using (var messageSetReader = new KafkaReader(uncompressedBytes)) {
                return messageSetReader.ReadMessages(codec);
            }
        }

        private static IResponse ProduceResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                TimeSpan? throttleTime = null;

                var topics = new List<ProduceResponse.Topic>();
                var topicCount = reader.ReadInt32();
                for (var i = 0; i < topicCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();
                        var offset = reader.ReadInt64();
                        DateTimeOffset? timestamp = null;

                        if (context.ApiVersion >= 3) {
                            var milliseconds = reader.ReadInt64();
                            if (milliseconds >= 0) {
                                timestamp = DateTimeOffset.FromUnixTimeMilliseconds(milliseconds);
                            }
                        }

                        topics.Add(new ProduceResponse.Topic(topicName, partitionId, errorCode, offset, timestamp));
                    }
                }

                if (context.ApiVersion >= 1) {
                    throttleTime = TimeSpan.FromMilliseconds(reader.ReadInt32());
                }
                return new ProduceResponse(topics, throttleTime);
            }
        }

        private static IResponse FetchResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                TimeSpan? throttleTime = null;

                if (context.ApiVersion >= 1) {
                    throttleTime = TimeSpan.FromMilliseconds(reader.ReadInt32());
                }

                var topics = new List<FetchResponse.Topic>();
                var topicCount = reader.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();
                        var highWaterMarkOffset = reader.ReadInt64();
                        var messages = reader.ReadMessages();

                        topics.Add(new FetchResponse.Topic(topicName, partitionId, highWaterMarkOffset, errorCode, messages));
                    }
                }
                return new FetchResponse(topics, throttleTime);
            }
        }

        private static IResponse OffsetResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var topics = new List<OffsetsResponse.Topic>();
                var topicCount = reader.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();

                        if (context.ApiVersion == 0) {
                            var offsetsCount = reader.ReadInt32();
                            for (var o = 0; o < offsetsCount; o++) {
                                var offset = reader.ReadInt64();
                                topics.Add(new OffsetsResponse.Topic(topicName, partitionId, errorCode, offset));
                            }
                        } else {
                            var timestamp = reader.ReadInt64();
                            var offset = reader.ReadInt64();
                            topics.Add(new OffsetsResponse.Topic(topicName, partitionId, errorCode, offset, DateTimeOffset.FromUnixTimeMilliseconds(timestamp)));
                        }
                    }
                }
                return new OffsetsResponse(topics);
            }
        }

        private static IResponse MetadataResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var brokers = new Server[reader.ReadInt32()];
                for (var b = 0; b < brokers.Length; b++) {
                    var brokerId = reader.ReadInt32();
                    var host = reader.ReadString();
                    var port = reader.ReadInt32();
                    string rack = null;
                    if (context.ApiVersion >= 2) {
                        rack = reader.ReadString();
                    }

                    brokers[b] = new Server(brokerId, host, port, rack);
                }

                string clusterId = null;
                if (context.ApiVersion >= 2) {
                    clusterId = reader.ReadString();
                }

                int? controllerId = null;
                if (context.ApiVersion >= 2) {
                    controllerId = reader.ReadInt32();
                }

                var topics = new MetadataResponse.Topic[reader.ReadInt32()];
                for (var t = 0; t < topics.Length; t++) {
                    var topicError = (ErrorCode) reader.ReadInt16();
                    var topicName = reader.ReadString();
                    bool? isInternal = null;
                    if (context.ApiVersion >= 2) {
                        isInternal = reader.ReadBoolean();
                    }

                    var partitions = new MetadataResponse.Partition[reader.ReadInt32()];
                    for (var p = 0; p < partitions.Length; p++) {
                        var partitionError = (ErrorCode) reader.ReadInt16();
                        var partitionId = reader.ReadInt32();
                        var leaderId = reader.ReadInt32();

                        var replicaCount = reader.ReadInt32();
                        var replicas = replicaCount.Repeat(reader.ReadInt32).ToArray();

                        var isrCount = reader.ReadInt32();
                        var isrs = isrCount.Repeat(reader.ReadInt32).ToArray();

                        partitions[p] = new MetadataResponse.Partition(partitionId, leaderId, partitionError, replicas, isrs);

                    }
                    topics[t] = new MetadataResponse.Topic(topicName, topicError, partitions, isInternal);
                }

                return new MetadataResponse(brokers, topics, controllerId, clusterId);
            }
        }
        
        private static IResponse OffsetCommitResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var topics = new List<TopicResponse>();
                var topicCount = reader.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();

                        topics.Add(new TopicResponse(topicName, partitionId, errorCode));
                    }
                }

                return new OffsetCommitResponse(topics);
            }
        }

        private static IResponse OffsetFetchResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var topics = new List<OffsetFetchResponse.Topic>();
                var topicCount = reader.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var offset = reader.ReadInt64();
                        var metadata = reader.ReadString();
                        var errorCode = (ErrorCode) reader.ReadInt16();

                        topics.Add(new OffsetFetchResponse.Topic(topicName, partitionId, errorCode, offset, metadata));
                    }
                }

                return new OffsetFetchResponse(topics);
            }
        }
        
        private static IResponse GroupCoordinatorResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var coordinatorId = reader.ReadInt32();
                var coordinatorHost = reader.ReadString();
                var coordinatorPort = reader.ReadInt32();

                return new GroupCoordinatorResponse(errorCode, coordinatorId, coordinatorHost, coordinatorPort);
            }
        }

        private static IResponse JoinGroupResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var generationId = reader.ReadInt32();
                var groupProtocol = reader.ReadString();
                var leaderId = reader.ReadString();
                var memberId = reader.ReadString();

                var encoder = context.GetEncoder(context.ProtocolType);
                var members = new JoinGroupResponse.Member[reader.ReadInt32()];
                for (var m = 0; m < members.Length; m++) {
                    var id = reader.ReadString();
                    var metadata = encoder.DecodeMetadata(groupProtocol, reader);
                    members[m] = new JoinGroupResponse.Member(id, metadata);
                }

                return new JoinGroupResponse(errorCode, generationId, groupProtocol, leaderId, memberId, members);
            }
        }

        private static IResponse HeartbeatResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var errorCode = (ErrorCode)reader.ReadInt16();

                return new HeartbeatResponse(errorCode);
            }
        }

        private static IResponse LeaveGroupResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var errorCode = (ErrorCode)reader.ReadInt16();

                return new LeaveGroupResponse(errorCode);
            }
        }

        private static IResponse SyncGroupResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var errorCode = (ErrorCode)reader.ReadInt16();

                var encoder = context.GetEncoder();
                var memberAssignment = encoder.DecodeAssignment(reader);
                return new SyncGroupResponse(errorCode, memberAssignment);
            }
        }

        private static IResponse DescribeGroupsResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var groups = new DescribeGroupsResponse.Group[reader.ReadInt32()];
                for (var g = 0; g < groups.Length; g++) {
                    var errorCode = (ErrorCode)reader.ReadInt16();
                    var groupId = reader.ReadString();
                    var state = reader.ReadString();
                    var protocolType = reader.ReadString();
                    var protocol = reader.ReadString();

                    IMembershipEncoder encoder = null;
                    var members = new DescribeGroupsResponse.Member[reader.ReadInt32()];
                    for (var m = 0; m < members.Length; m++) {
                        encoder = encoder ?? context.GetEncoder(protocolType);
                        var memberId = reader.ReadString();
                        var clientId = reader.ReadString();
                        var clientHost = reader.ReadString();
                        var memberMetadata = encoder.DecodeMetadata(protocol, reader);
                        var memberAssignment = encoder.DecodeAssignment(reader);
                        members[m] = new DescribeGroupsResponse.Member(memberId, clientId, clientHost, memberMetadata, memberAssignment);
                    }
                    groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId, state, protocolType, protocol, members);
                }

                return new DescribeGroupsResponse(groups);
            }
        }

        private static IResponse ListGroupsResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var groups = new ListGroupsResponse.Group[reader.ReadInt32()];
                for (var g = 0; g < groups.Length; g++) {
                    var groupId = reader.ReadString();
                    var protocolType = reader.ReadString();
                    groups[g] = new ListGroupsResponse.Group(groupId, protocolType);
                }

                return new ListGroupsResponse(errorCode, groups);
            }
        }

        private static IResponse SaslHandshakeResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var enabledMechanisms = new string[reader.ReadInt32()];
                for (var m = 0; m < enabledMechanisms.Length; m++) {
                    enabledMechanisms[m] = reader.ReadString();
                }

                return new SaslHandshakeResponse(errorCode, enabledMechanisms);
            }
        }

        private static IResponse ApiVersionsResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var errorCode = (ErrorCode)reader.ReadInt16();

                var apiKeys = new ApiVersionsResponse.VersionSupport[reader.ReadInt32()];
                for (var i = 0; i < apiKeys.Length; i++) {
                    var apiKey = (ApiKey)reader.ReadInt16();
                    var minVersion = reader.ReadInt16();
                    var maxVersion = reader.ReadInt16();
                    apiKeys[i] = new ApiVersionsResponse.VersionSupport(apiKey, minVersion, maxVersion);
                }
                return new ApiVersionsResponse(errorCode, apiKeys);
            }
        }        

        private static IResponse CreateTopicsResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var topics = new TopicsResponse.Topic[reader.ReadInt32()];
                for (var i = 0; i < topics.Length; i++) {
                    var topicName = reader.ReadString();
                    var errorCode = reader.ReadErrorCode();
                    topics[i] = new TopicsResponse.Topic(topicName, errorCode);
                }
                return new CreateTopicsResponse(topics);
            }
        }        

        private static IResponse DeleteTopicsResponse(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = new KafkaReader(payload)) {
                var topics = new TopicsResponse.Topic[reader.ReadInt32()];
                for (var i = 0; i < topics.Length; i++) {
                    var topicName = reader.ReadString();
                    var errorCode = reader.ReadErrorCode();
                    topics[i] = new TopicsResponse.Topic(topicName, errorCode);
                }
                return new DeleteTopicsResponse(topics);
            }
        }        

        #endregion
    }
}