using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetFetch Response => *throttle_time_ms [responses] *error_code 
    /// </summary>
    /// <remarks>
    /// OffsetFetch Response => *throttle_time_ms [responses] *error_code 
    ///   throttle_time_ms => INT32
    ///   responses => topic [partition_responses] 
    ///     topic => STRING
    ///     partition_responses => partition offset metadata error_code 
    ///       partition => INT32
    ///       offset => INT64
    ///       metadata => NULLABLE_STRING
    ///       error_code => INT16
    ///   error_code => INT16
    /// 
    /// Version 2+: error_code
    /// Version 3+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_OffsetFetch
    /// </remarks>
    public class OffsetFetchResponse : ThrottledResponse, IResponse<OffsetFetchResponse.Topic>, IEquatable<OffsetFetchResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},responses:[{Responses.ToStrings()}]}}";

        public static OffsetFetchResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 3);
                var topics = new List<Topic>();
                var topicCount = reader.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var offset = reader.ReadInt64();
                        var metadata = reader.ReadString();
                        var errorCode = (ErrorCode) reader.ReadInt16();

                        topics.Add(new Topic(topicName, partitionId, errorCode, offset, metadata));
                    }
                }
                ErrorCode? error = null;
                if (context.ApiVersion >= 2) {
                    error = reader.ReadErrorCode();
                }

                return new OffsetFetchResponse(topics, error, throttleTime);
            }            
        }

        public OffsetFetchResponse(IEnumerable<Topic> topics = null, ErrorCode? errorCode = null, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Responses = topics.ToSafeImmutableList();
            Errors = ImmutableList<ErrorCode>.Empty;
            if (errorCode.HasValue) {
                Error = errorCode;
                Errors = Errors.Add(errorCode.Value);
            }
            Errors = Errors.AddRange(Responses.Select(t => t.Error));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Responses { get; }

        /// <summary>
        /// Error response code.
        /// Version: 2+
        /// </summary>
        public ErrorCode? Error { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetFetchResponse);
        }

        /// <inheritdoc />
        public bool Equals(OffsetFetchResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Responses.HasEqualElementsInOrder(other.Responses)
                && Error == other.Error;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Error?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Responses?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicOffset, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},offset:{Offset},metadata:{Metadata},error_code:{Error}}}";

            public Topic(string topic, int partitionId, ErrorCode errorCode, long offset, string metadata) 
                : base(topic, partitionId, offset)
            {
                Error = errorCode;
                Metadata = metadata;
            }

            /// <summary>
            /// Error response code.
            /// </summary>
            public ErrorCode Error { get; }

            /// <summary>
            /// Any arbitrary metadata stored during a CommitRequest.
            /// </summary>
            public string Metadata { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                       && Error == other.Error 
                       && string.Equals(Metadata, other.Metadata);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Error.GetHashCode();
                    hashCode = (hashCode*397) ^ (Metadata?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }        

            #endregion
        }
    }
}