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

        public static IKafkaWriter Write(this IKafkaWriter writer, IEnumerable<Message> messages)
        {
            foreach (var message in messages) {
                writer.Write(0L);
                using (writer.MarkForLength()) {
                    message.WriteTo(writer);
                }
            }
            return writer;
        }

        public static int Write(this IKafkaWriter writer, IEnumerable<Message> messages, MessageCodec codec)
        {
            if (codec == MessageCodec.None) {
                using (writer.MarkForLength()) {
                    writer.Write(messages);
                }
                return 0;
            }
            using (var messageWriter = new KafkaWriter()) {
                messageWriter.Write(messages);
                var messageSet = messageWriter.ToSegment(false);

                using (writer.MarkForLength()) { // messageset
                    writer.Write(0L); // offset
                    using (writer.MarkForLength()) { // message
                        using (writer.MarkForCrc()) {
                            writer.Write((byte)0) // message version
                                    .Write((byte)codec) // attribute
                                    .Write(-1); // key  -- null, so -1 length
                            using (writer.MarkForLength()) { // value
                                var initialPosition = writer.Position;
                                writer.WriteCompressed(messageSet, codec);
                                var compressedMessageLength = writer.Position - initialPosition;
                                return messageSet.Count - compressedMessageLength;
                            }
                        }
                    }
                }
            }
        }

        #endregion

        #region Decoding

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
                || code == ErrorCode.NOT_CONTROLLER;
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
