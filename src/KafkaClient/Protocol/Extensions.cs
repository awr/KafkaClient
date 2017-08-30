using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;

namespace KafkaClient.Protocol
{
    public static class Extensions
    {
        #region Exception

        public static Exception ExtractExceptions<TResponse>(this IRequest<TResponse> request, TResponse response, Endpoint endpoint = null) where TResponse : IResponse
        {
            var exceptions = new List<Exception>();
            if (response != null) {
                foreach (var errorCode in response.Errors.Where(e => !e.IsSuccess())) {
                    exceptions.Add(ExtractException(request, errorCode, endpoint));
                }
            }
            if (exceptions.Count == 0) return new RequestException(request.ApiKey, ErrorCode.NONE, endpoint);
            if (exceptions.Count == 1) return exceptions[0];
            return new AggregateException(exceptions);
        }

        private static Exception ExtractException(this IRequest request, ErrorCode errorCode, Endpoint endpoint) 
        {
            return ExtractFetchException(request as FetchRequest, errorCode, endpoint) ??
                   ExtractMemberException(request, errorCode, endpoint) ??
                   new RequestException(request.ApiKey, errorCode, endpoint);
        }

        private static MemberRequestException ExtractMemberException(IRequest request, ErrorCode errorCode, Endpoint endpoint)
        {
            var member = request as IGroupMember;
            if (member != null && 
                (errorCode == ErrorCode.UNKNOWN_MEMBER_ID ||
                errorCode == ErrorCode.ILLEGAL_GENERATION || 
                errorCode == ErrorCode.INCONSISTENT_GROUP_PROTOCOL))
            {
                return new MemberRequestException(member, request.ApiKey, errorCode, endpoint);
            }
            return null;
        } 

        private static FetchOutOfRangeException ExtractFetchException(FetchRequest request, ErrorCode errorCode, Endpoint endpoint)
        {
            if (errorCode == ErrorCode.OFFSET_OUT_OF_RANGE && request?.Topics?.Count == 1) {
                return new FetchOutOfRangeException(request.Topics.First(), errorCode, endpoint);
            }
            return null;
        }        

        #endregion

        #region Encoding

        public static IMembershipEncoder GetEncoder(this IRequestContext context, string protocolType = null)
        {
            var type = protocolType ?? context.ProtocolType;
            if (type != null && context.Encoders != null && context.Encoders.TryGetValue(type, out IMembershipEncoder encoder) && encoder != null) return encoder;

            throw new ArgumentOutOfRangeException(nameof(protocolType), $"Unknown protocol type {protocolType}");
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IMemberMetadata metadata, IMembershipEncoder encoder)
        {
            encoder.EncodeMetadata(writer, metadata);
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IMemberAssignment assignment, IMembershipEncoder encoder)
        {
            encoder.EncodeAssignment(writer, assignment);
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IImmutableList<int> values)
        {
            writer.Write(values.Count);
            foreach (var value in values) {
                writer.Write(value);
            }
            return writer;
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, ErrorCode errorCode)
        {
            return writer.Write((short)errorCode);
        }

        public static IKafkaWriter WriteMilliseconds(this IKafkaWriter writer, TimeSpan? span)
        {
            return writer.WriteMilliseconds(span.GetValueOrDefault());
        }

        public static IKafkaWriter WriteMilliseconds(this IKafkaWriter writer, TimeSpan span)
        {
            return writer.Write((int)Math.Min(int.MaxValue, span.TotalMilliseconds));
        }

        public static IKafkaWriter Write(this IKafkaWriter writer, IEnumerable<string> values, bool includeLength = false)
        {
            if (includeLength) {
                var valuesList = values.ToList();
                writer.Write(valuesList.Count);
                writer.Write(valuesList); // NOTE: !includeLength passed next time
                return writer;
            }

            foreach (var item in values) {
                writer.Write(item);
            }
            return writer;
        }

        public static IKafkaWriter WriteGroupedTopics<T>(this IKafkaWriter writer, IEnumerable<T> topics, Action<T> partitionWriter = null) where T : TopicPartition
        {
            var groupedTopics = topics.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();
                writer.Write(topic.Key)
                      .Write(partitions.Count);
                foreach (var partition in partitions) {
                    if (partitionWriter != null) {
                        partitionWriter(partition);
                    } else {
                        writer.Write(partition.PartitionId);
                    }
                }
            }
            return writer;
        }

        public static IKafkaWriter WriteMessages(this IKafkaWriter writer, IEnumerable<Message> messages, ITransactionContext transaction, byte version, MessageCodec codec, out int compressedBytes)
        {
            // RecordBatch was introduced with version 2, and is significantly different from the MessageSet approach (see MessageBatch for details)
            var context = new MessageContext(codec, version);
            return version >= 2
                ? writer.WriteRecordBatch(messages.ToList(), transaction, context, out compressedBytes)
                : writer.WriteMessageSet(messages, context, out compressedBytes);
        }

        /// <summary>
        /// The fifth lowest bit indicates whether the RecordBatch is part of a transaction or not. 0 indicates that the RecordBatch is not transactional, while 1 indicates that it is. (since 0.11.0.0)
        /// </summary>
        public const byte IsTransactionMask = 0x10; 

        internal static IKafkaWriter WriteRecordBatch(this IKafkaWriter writer, IList<Message> messages, ITransactionContext transaction, IMessageContext context, out int compressedBytes)
        {
            // Denotes the first offset in the RecordBatch. The 'offsetDelta' of each Record in the batch would be be computed relative to this FirstOffset. 
            // In particular, the offset of each Record in the Batch is its 'OffsetDelta' + 'FirstOffset'.
            var firstOffset = 0L;

            // Attributes int16
            // The lowest 3 bits contain the compression codec used for the message.
            // The fourth lowest bit represents the timestamp type. 0 stands for CreateTime and 1 stands for LogAppendTime. The producer should always set this bit to 0. (since 0.10.0)
            // The fifth lowest bit indicates whether the RecordBatch is part of a transaction or not. 0 indicates that the RecordBatch is not transactional, while 1 indicates that it is. (since 0.11.0.0)
            // 
            // The sixth lowest bit indicates whether the RecordBatch includes a control message. 1 indicates that the RecordBatch is contains a control message, 0 indicates that it doesn't. 
            // Control messages are used to enable transactions in Kafka and are generated by the broker. Clients should not return control batches (ie. those with this bit set) to applications. (since 0.11.0.0)
            // 
            // All other bits should be set to 0.
            var attributes = (short) (Message.CodecMask & (short) context.Codec);
            if (transaction?.ProducerId > 0L && transaction?.ProducerEpoch > 0) {
                attributes = (short) (attributes | IsTransactionMask);
            }

            // The timestamp of the first Record in the batch. The timestamp of each Record in the RecordBatch is its 'TimestampDelta' + 'FirstTimestamp'.
            var firstTimestamp = 0L;

            // The offset of the last message in the RecordBatch. This is used by the broker to ensure correct behavior even when Records within a batch are compacted out.
            var lastOffsetDelta = messages.Count;
            DateTimeOffset? timestamp = null;

            // The timestamp of the last Record in the batch. This is used by the broker to ensure the correct behavior even when Records within the batch are compacted out.
            var maxTimestamp = 0L;
            if (messages.Count > 0) {
                firstOffset = messages[0].Offset;
                timestamp = messages[0].Timestamp.GetValueOrDefault(DateTimeOffset.UtcNow);
                var first = timestamp.GetValueOrDefault(DateTimeOffset.UtcNow);
                firstTimestamp = first.ToUnixTimeMilliseconds();
                maxTimestamp = messages
                                .Where(m => m.Timestamp.HasValue).Max(m => m.Timestamp)
                                .GetValueOrDefault(first).ToUnixTimeMilliseconds();
            }

            void WriteUncompressedTo(IKafkaWriter uncompressedWriter)
            {
                uncompressedWriter.Write(messages.Count);
                foreach (var record in messages) {
                    uncompressedWriter.WriteRecord(record, MessageCodec.None, firstOffset, timestamp, out int _);
                }
            }

            writer.Write(firstOffset);
            using (writer.MarkForLength()) {
                writer.Write(0) // PartitionLeaderEpoch int32
                      .Write(context.MessageVersion.GetValueOrDefault()); // AKA Magic

                // the CRC includes everything from the attribute on
                using (writer.MarkForCrc(castagnoli: true)) {
                    writer.Write(attributes)
                          .Write(lastOffsetDelta)
                          .Write(firstTimestamp)
                          .Write(maxTimestamp)
                          .Write(transaction?.ProducerId ?? 0L)
                          .Write(transaction?.ProducerEpoch ?? (short)0)
                          .Write(transaction?.FirstSequence ?? 0);

                    if (context.Codec == MessageCodec.None) {
                        WriteUncompressedTo(writer);
                        compressedBytes = 0;
                        return writer;
                    }

                    using (var uncompressedWriter = new KafkaWriter()) {
                        WriteUncompressedTo(uncompressedWriter);
                        var recordsValue = uncompressedWriter.ToSegment(false);
                        var uncompressedRecord = new Message(recordsValue, 0, firstOffset);

                        writer.Write(1) // record count = 1 for compressed set
                              .WriteRecord(uncompressedRecord, context.Codec, firstOffset, timestamp, out compressedBytes);
                        compressedBytes -= 8; // minimum size of record used to do the compression -- actual size *might* be larger for the varint for the value
                        return writer;
                    }                    
                }
            }
        }

        internal static IKafkaWriter WriteRecord(this IKafkaWriter writer, Message record, MessageCodec codec, long firstOffset, DateTimeOffset? firstTimestamp, out int compressedBytes)
        {
            var timestamp = firstTimestamp.HasValue && record.Timestamp.HasValue
                ? (ulong) Math.Max(0L, record.Timestamp.Value.Subtract(firstTimestamp.Value).TotalMilliseconds)
                : 0L;

            var timestampDelta = timestamp.ToVarint();
            var offsetDelta = ((ulong)Math.Max(0L, record.Offset - firstOffset)).ToVarint();
            var keyLenth = ((uint) record.Key.Count).ToVarint();
            var valueLenth = ((uint) record.Value.Count).ToVarint();
            var headerCount = ((uint) record.Headers.Count).ToVarint();

            var expectedLength = 1 // attribute
                + timestampDelta.Count 
                + offsetDelta.Count 
                + keyLenth.Count + record.Key.Count 
                + valueLenth.Count + record.Value.Count 
                + headerCount.Count 
                + record.Headers.Sum(h => 5 + (2 * (h.Key ?? "").Length) + 5 + h.Value.Count);

            using (writer.MarkForVarintLength(expectedLength)) {
                writer.Write((byte) 0) // attributes
                      .Write(timestampDelta, false)
                      .Write(offsetDelta, false)
                      .Write(keyLenth, false)
                      .Write(record.Key, false);
                if (codec == MessageCodec.None) {
                    writer.Write(valueLenth, false)
                          .Write(record.Value, false);
                    compressedBytes = 0;
                } else {
                    var expectedCompressedLength = record.Value.Count;
                    using (writer.MarkForVarintLength(expectedCompressedLength)) {
                        var initialPosition = writer.Position;
                        writer.WriteCompressed(record.Value, codec);
                        var compressedRecordLength = writer.Position - initialPosition;
                        compressedBytes = record.Value.Count - compressedRecordLength;
                    }
                }
                writer.Write(headerCount, false);
                foreach (var header in record.Headers) {
                    writer.Write(header.Key, varint: true)
                          .WriteVarint((uint)header.Value.Count)
                          .Write(header.Value, false);
                }
            }
            return writer;
        }

        internal static IKafkaWriter WriteMessageSet(this IKafkaWriter writer, IEnumerable<Message> messages, IMessageContext context, out int compressedBytes)
        {
            if (context.Codec == MessageCodec.None) {
                foreach (var message in messages) {
                    writer.Write(message.Offset);
                    using (writer.MarkForLength()) { // message length
                        writer.WriteMessage(message, context, out int _);
                    }
                }
                compressedBytes = 0;
                return writer;
            }
            using (var uncompressedWriter = new KafkaWriter()) {
                var uncompressedContext = new MessageContext(version: context.MessageVersion);
                var offset = 0L;
                foreach (var message in messages) {
                    uncompressedWriter.Write(offset++); // offset increasing by oneto avoid server side recompression
                    using (uncompressedWriter.MarkForLength()) { // message length
                        uncompressedWriter.WriteMessage(message, uncompressedContext, out int _);
                    }
                }
                var messageSetBytes = uncompressedWriter.ToSegment(false);
                var uncompressedMessage = new Message(messageSetBytes, (byte)context.Codec);

                writer.Write(0L); // offset
                using (writer.MarkForLength()) { // message length
                    writer.WriteMessage(uncompressedMessage, context, out compressedBytes);
                    compressedBytes -= 34; // minimum size of MessageSet used to do the compression
                    return writer;
                }
            }
        }

        internal static IKafkaWriter WriteMessage(this IKafkaWriter writer, Message message, IMessageContext context, out int compressedBytes)
        {
            using (writer.MarkForCrc()) {
                writer.Write(context.MessageVersion.GetValueOrDefault())
                      .Write((byte) context.Codec);
                if (context.MessageVersion >= 1) {
                    writer.Write(message.Timestamp.GetValueOrDefault(DateTimeOffset.UtcNow).ToUnixTimeMilliseconds());
                }
                writer.Write(message.Key);

                if (context.Codec == MessageCodec.None) {
                    writer.Write(message.Value);
                    compressedBytes = 0;
                    return writer;
                }
                using (writer.MarkForLength()) {
                    var initialPosition = writer.Position;
                    writer.WriteCompressed(message.Value, context.Codec);
                    var compressedMessageLength = writer.Position - initialPosition;
                    compressedBytes = message.Value.Count - compressedMessageLength;
                    return writer;
                }
            }
        }

        #endregion

        #region Decoding

        private const int MessageSetVersionOffset = 16;

        public static MessageTransaction ReadMessages(this IKafkaReader reader, int? messageSetSize = null)
        {
            messageSetSize = messageSetSize ?? reader.ReadInt32();
            if (!reader.HasBytes(messageSetSize.Value)) throw new BufferUnderRunException($"Message set of {messageSetSize} is not available.");

            reader.Position += MessageSetVersionOffset;
            var version = reader.ReadByte();
            reader.Position -= MessageSetVersionOffset + 1;

            if (version >= 2) {
                return reader.ReadRecordBatch();
            }

            return new MessageTransaction(reader.ReadMessageSet(MessageCodec.None, (reader.Position - 4) + messageSetSize.Value));
        }

        internal static MessageTransaction ReadRecordBatch(this IKafkaReader reader)
        {
            var firstOffset = reader.ReadInt64();
            var length = reader.ReadInt32();
            var partitionLeaderEpoch = reader.ReadInt32();
            var version = reader.ReadByte();

            var crc = reader.ReadUInt32();
            var crcHash = reader.ReadCrc(length - 9, castagnoli: true);
            if (crc != crcHash) throw new CrcValidationException(crc, crcHash);

            var attributes = reader.ReadInt16();
            var lastOffsetDelta = reader.ReadInt32();
            var firstTimestampMilliseconds = reader.ReadInt64();
            var maxTimestamp = reader.ReadInt64();
            var producerId = reader.ReadInt64();
            var producerEpoch = reader.ReadInt16();
            var firstSequence = reader.ReadInt32();

            var firstTimestamp = firstTimestampMilliseconds >= 0 ? (DateTimeOffset?)DateTimeOffset.FromUnixTimeMilliseconds(firstTimestampMilliseconds) : null;
            var codec = (MessageCodec) (attributes & Message.CodecMask);
            var messages = reader.ReadRecords(new MessageContext(codec, version), firstOffset, firstTimestamp);
            var transaction = new TransactionContext(producerId, producerEpoch, firstSequence);
            return new MessageTransaction(messages, transaction);
        }

        private static readonly ArraySegment<byte> EmptySegment = new ArraySegment<byte>(new byte[0]);

        internal static IEnumerable<Message> ReadRecords(this IKafkaReader reader, IMessageContext context, long firstOffset, DateTimeOffset? firstTimestamp)
        {
            var messages = new List<Message>();
            var messageCount = reader.ReadInt32();

            for (var m = 0; m < messageCount; m++) {
                var length = reader.ReadVarint32();
                if (!reader.HasBytes(length)) throw new BufferUnderRunException($"Record size of {length} is not fully available (codec {context.Codec}).");

                var attribute = reader.ReadByte();
                var timestampDelta = reader.ReadVarint64();
                var offsetDelta = reader.ReadVarint64();
                var keyLength = reader.ReadVarint32();
                var key = keyLength > 0 ? reader.ReadBytes(keyLength) : EmptySegment;
                var valueLength = reader.ReadVarint32();
                var value = valueLength > 0 ? reader.ReadBytes(valueLength) : EmptySegment;

                MessageHeader[] headers = null;
                var headerCount = reader.ReadVarint32();
                if (headerCount > 0) {
                    headers = new MessageHeader[headerCount];
                    for (var h = 0; h < headerCount; h++) {
                        var headerKeyLength = reader.ReadVarint32();
                        var headerKey = headerKeyLength > 0 ? reader.ReadString(headerKeyLength) : null;
                        var headerValueLength = reader.ReadVarint32();
                        var headerValue = headerKeyLength > 0 ? reader.ReadBytes(headerValueLength) : EmptySegment;
                        headers[h] = new MessageHeader(headerKey, headerValue);
                    }
                }

                var timestamp = firstTimestamp?.Add(TimeSpan.FromMilliseconds(timestampDelta));

                if (context.Codec == MessageCodec.None) {
                    messages.Add(new Message(value, key, attribute, firstOffset + offsetDelta, timestamp, headers));
                } else {
                    var uncompressedBytes = value.ToUncompressed(context.Codec);
                    using (var messageRecordsReader = new KafkaReader(uncompressedBytes)) {
                        messages.AddRange(messageRecordsReader.ReadRecords(new MessageContext(version: context.MessageVersion), firstOffset, firstTimestamp));
                    }
                }
            }
            return messages;
        }

        internal static IEnumerable<Message> ReadMessageSet(this IKafkaReader reader, MessageCodec codec, int finalPosition)
        {
            var messages = new List<Message>();
            while (reader.Position < finalPosition) {
                int? length = null;
                try {
                    var offset = reader.ReadInt64();
                    length = reader.ReadInt32();

                    var crc = reader.ReadUInt32();
                    var crcHash = reader.ReadCrc(length.Value - 4);
                    if (crc != crcHash) throw new CrcValidationException(crc, crcHash);

                    var version = reader.ReadByte();
                    var attribute = reader.ReadByte();
                    DateTimeOffset? timestamp = null;
                    if (version >= 1) {
                        var milliseconds = reader.ReadInt64();
                        if (milliseconds >= 0) {
                            timestamp = DateTimeOffset.FromUnixTimeMilliseconds(milliseconds);
                        }
                    }
                    var key = reader.ReadBytes();
                    var value = reader.ReadBytes();

                    codec = (MessageCodec)(Message.CodecMask & attribute);
                    if (codec == MessageCodec.None) {
                        messages.Add(new Message(value, key, attribute, offset, timestamp));
                    } else {
                        var uncompressedBytes = value.ToUncompressed(codec);
                        using (var messageSetReader = new KafkaReader(uncompressedBytes)) {
                            messages.AddRange(messageSetReader.ReadMessageSet(codec, uncompressedBytes.Count));
                        }
                    }
                } catch (EndOfStreamException ex) {
                    throw new BufferUnderRunException($"Message size of {length} is not available (codec {codec}).", ex);
                }
            }
            return messages;
        }

        public static IResponse ToResponse(this ApiKey apiKey, IRequestContext context, ArraySegment<byte> bytes)
        {
            switch (apiKey) {
                case ApiKey.Produce:
                    return ProduceResponse.FromBytes(context, bytes);
                case ApiKey.Fetch:
                    return FetchResponse.FromBytes(context, bytes);
                case ApiKey.Offsets:
                    return OffsetsResponse.FromBytes(context, bytes);
                case ApiKey.Metadata:
                    return MetadataResponse.FromBytes(context, bytes);
                case ApiKey.OffsetCommit:
                    return OffsetCommitResponse.FromBytes(context, bytes);
                case ApiKey.OffsetFetch:
                    return OffsetFetchResponse.FromBytes(context, bytes);
                case ApiKey.FindCoordinator:
                    return FindCoordinatorResponse.FromBytes(context, bytes);
                case ApiKey.JoinGroup:
                    return JoinGroupResponse.FromBytes(context, bytes);
                case ApiKey.Heartbeat:
                    return HeartbeatResponse.FromBytes(context, bytes);
                case ApiKey.LeaveGroup:
                    return LeaveGroupResponse.FromBytes(context, bytes);
                case ApiKey.SyncGroup:
                    return SyncGroupResponse.FromBytes(context, bytes);
                case ApiKey.DescribeGroups:
                    return DescribeGroupsResponse.FromBytes(context, bytes);
                case ApiKey.ListGroups:
                    return ListGroupsResponse.FromBytes(context, bytes);
                case ApiKey.SaslHandshake:
                    return SaslHandshakeResponse.FromBytes(context, bytes);
                case ApiKey.ApiVersions:
                    return ApiVersionsResponse.FromBytes(context, bytes);
                case ApiKey.CreateTopics:
                    return CreateTopicsResponse.FromBytes(context, bytes);
                case ApiKey.DeleteTopics:
                    return DeleteTopicsResponse.FromBytes(context, bytes);
                case ApiKey.DeleteRecords:
                    return DeleteRecordsResponse.FromBytes(context, bytes);
                case ApiKey.InitProducerId:
                    return InitProducerIdResponse.FromBytes(context, bytes);
                case ApiKey.OffsetForLeaderEpoch:
                    return OffsetForLeaderEpochResponse.FromBytes(context, bytes);
                case ApiKey.AddPartitionsToTxn:
                    return AddPartitionsToTxnResponse.FromBytes(context, bytes);
                case ApiKey.AddOffsetsToTxn:
                    return AddOffsetsToTxnResponse.FromBytes(context, bytes);
                case ApiKey.EndTxn:
                    return EndTxnResponse.FromBytes(context, bytes);
                case ApiKey.WriteTxnMarkers:
                    return WriteTxnMarkersResponse.FromBytes(context, bytes);
                case ApiKey.TxnOffsetCommit:
                    return TxnOffsetCommitResponse.FromBytes(context, bytes);
                case ApiKey.DescribeAcls:
                    return DescribeAclsResponse.FromBytes(context, bytes);
                case ApiKey.CreateAcls:
                    return CreateAclsResponse.FromBytes(context, bytes);
                case ApiKey.DeleteAcls:
                    return DeleteAclsResponse.FromBytes(context, bytes);
                case ApiKey.DescribeConfigs:
                    return DescribeConfigsResponse.FromBytes(context, bytes);
                case ApiKey.AlterConfigs:
                    return AlterConfigsResponse.FromBytes(context, bytes);
                default:
                    throw new NotImplementedException($"Unknown response type {apiKey}");
            }
        }

        public static ErrorCode ReadErrorCode(this IKafkaReader reader)
        {
            return (ErrorCode) reader.ReadInt16();
        }

        public static TimeSpan? ReadThrottleTime(this IKafkaReader reader, bool inThisVersion = true)
        {
            if (!inThisVersion) return null;
            return TimeSpan.FromMilliseconds(reader.ReadInt32());
        }

        public static bool IsSuccess(this ErrorCode code)
        {
            return code == ErrorCode.NONE;
        }

        /// <summary>
        /// See http://kafka.apache.org/protocol.html#protocol_error_codes for details
        /// </summary>
        public static bool IsRetryable(this ErrorCode code)
        {
            return code == ErrorCode.CORRUPT_MESSAGE
                || code == ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
                || code == ErrorCode.LEADER_NOT_AVAILABLE
                || code == ErrorCode.NOT_LEADER_FOR_PARTITION
                || code == ErrorCode.REQUEST_TIMED_OUT
                || code == ErrorCode.NETWORK_EXCEPTION
                || code == ErrorCode.GROUP_LOAD_IN_PROGRESS
                || code == ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE
                || code == ErrorCode.NOT_COORDINATOR_FOR_GROUP
                || code == ErrorCode.NOT_ENOUGH_REPLICAS
                || code == ErrorCode.NOT_ENOUGH_REPLICAS_AFTER_APPEND
                || code == ErrorCode.NOT_CONTROLLER
                || code == ErrorCode.DUPLICATE_SEQUENCE_NUMBER;
        }

        public static bool IsFromStaleMetadata(this ErrorCode code)
        {
            return code == ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
                || code == ErrorCode.LEADER_NOT_AVAILABLE
                || code == ErrorCode.NOT_LEADER_FOR_PARTITION
                || code == ErrorCode.GROUP_LOAD_IN_PROGRESS
                || code == ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE
                || code == ErrorCode.NOT_COORDINATOR_FOR_GROUP;
        }

        #endregion

        #region Router

        internal static async Task<T> SendToAnyAsync<T>(this IRouter router, IRequest<T> request, CancellationToken cancellationToken, IRequestContext context = null) where T : class, IResponse
        {
            Exception lastException = null;
            var endpoints = new List<Endpoint>();
            foreach (var connection in router.Connections) {
                var endpoint = connection.Endpoint;
                try {
                    return await connection.SendAsync(request, cancellationToken, context).ConfigureAwait(false);
                } catch (Exception ex) {
                    lastException = ex;
                    endpoints.Add(endpoint);
                    router.Log.Info(() => LogEvent.Create(ex, $"Failed to contact {endpoint} -> Trying next server"));
                }
            }

            throw new ConnectionException(endpoints, lastException);
        }

        internal static async Task<bool> RefreshGroupMetadataIfInvalidAsync(this IRouter router, string groupId, bool? metadataInvalid, CancellationToken cancellationToken)
        {
            if (metadataInvalid.GetValueOrDefault(true)) {
                // unknown metadata status should not force the issue
                await router.RefreshGroupConnectionAsync(groupId, metadataInvalid.GetValueOrDefault(), cancellationToken).ConfigureAwait(false);
            }
            return false;
        }

        internal static async Task<bool> RefreshTopicMetadataIfInvalidAsync(this IRouter router, string topicName, bool? metadataInvalid, CancellationToken cancellationToken)
        {
            if (metadataInvalid.GetValueOrDefault(true)) {
                // unknown metadata status should not force the issue
                await router.RefreshTopicMetadataAsync(topicName, metadataInvalid.GetValueOrDefault(), cancellationToken).ConfigureAwait(false);
            }
            return false;
        }

        internal static void ThrowExtractedException<T>(this RoutedTopicRequest<T>[] routedTopicRequests) where T : class, IResponse
        {
            throw routedTopicRequests.Select(_ => _.ResponseException).FlattenAggregates();
        }

        internal static void MetadataRetry<T>(this IEnumerable<RoutedTopicRequest<T>> brokeredRequests, Exception exception, out bool? shouldRetry) where T : class, IResponse
        {
            shouldRetry = null;
            foreach (var brokeredRequest in brokeredRequests) {
                brokeredRequest.OnRetry(exception, out bool? requestRetry);
                if (requestRetry.HasValue) {
                    shouldRetry = requestRetry;
                }
            }
        }

        internal static bool IsPotentiallyRecoverableByMetadataRefresh(this Exception exception)
        {
            return exception is FetchOutOfRangeException
                || exception is TimeoutException
                || exception is ConnectionException
                || exception is RoutingException;
        }

        /// <summary>
        /// Given a collection of server connections, query for the topic metadata.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="request">Metadata request to make</param>
        /// <param name="cancellationToken"></param>
        /// <remarks>
        /// Used by <see cref="Router"/> internally. Broken out for better testability, but not intended to be used separately.
        /// </remarks>
        /// <returns>MetadataResponse validated to be complete.</returns>
        internal static async Task<MetadataResponse> GetMetadataAsync(this IRouter router, MetadataRequest request, CancellationToken cancellationToken)
        {
            return await router.Configuration.RefreshRetry.TryAsync(
                async (retryAttempt, elapsed) => {
                    var connections = router.Connections.ToList();
                    var connection = connections[retryAttempt % connections.Count];
                    var response = await connection.SendAsync(request, cancellationToken).ConfigureAwait(false);
                    if (response == null) return new RetryAttempt<MetadataResponse>(null);

                    var results = response.Brokers
                        .Select(ValidateServer)
                        .Union(response.TopicMetadata.Select(ValidateTopic))
                        .Where(r => !r.IsValid.GetValueOrDefault())
                        .ToList();

                    var exceptions = results.Select(r => r.ToException(connection.Endpoint)).Where(e => e != null).ToList();
                    if (exceptions.Count == 1) throw exceptions.Single();
                    if (exceptions.Count > 1) throw new AggregateException(exceptions);

                    if (results.Count == 0) return new RetryAttempt<MetadataResponse>(response);
                    foreach (var result in results.Where(r => !string.IsNullOrEmpty(r.Message))) {
                        router.Log.Warn(() => LogEvent.Create(result.Message));
                    }

                    return new RetryAttempt<MetadataResponse>(response, false);
                },
                (ex, retryAttempt, retryDelay) => router.Log.Warn(() => LogEvent.Create(ex, $"Failed metadata request on attempt {retryAttempt}: Will retry in {retryDelay}")),
                null, // return the failed response above, resulting in the final response
                cancellationToken).ConfigureAwait(false);
        }

        private class MetadataResult
        {
            public bool? IsValid { get; }
            public string Message { get; }
            private readonly ErrorCode _errorCode;

            public Exception ToException(Endpoint endpoint)
            {
                if (IsValid.GetValueOrDefault(true)) return null;

                if (_errorCode.IsSuccess()) return new ConnectionException(Message);
                return new RequestException(ApiKey.Metadata, _errorCode, endpoint, Message);
            }

            public MetadataResult(ErrorCode errorCode = ErrorCode.NONE, bool? isValid = null, string message = null)
            {
                Message = message ?? "";
                _errorCode = errorCode;
                IsValid = isValid;
            }
        }

        private static MetadataResult ValidateServer(Server server)
        {
            if (server.Id == -1)                   return new MetadataResult(ErrorCode.UNKNOWN);
            if (string.IsNullOrEmpty(server.Host)) return new MetadataResult(ErrorCode.NONE, false, "Broker missing host information.");
            if (server.Port <= 0)                  return new MetadataResult(ErrorCode.NONE, false, "Broker missing port information.");
            return new MetadataResult(isValid: true);
        }

        private static MetadataResult ValidateTopic(MetadataResponse.Topic topic)
        {
            var errorCode = topic.TopicError;
            if (errorCode.IsSuccess())   return new MetadataResult(isValid: true);
            if (errorCode.IsRetryable()) return new MetadataResult(errorCode, null, $"topic {topic.TopicName} returned error code of {errorCode}: Retrying");
            return new MetadataResult(errorCode, false, $"topic {topic.TopicName} returned an error of {errorCode}");
        }

        #endregion
    }
}
