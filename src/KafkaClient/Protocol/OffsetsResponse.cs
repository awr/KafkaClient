using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Offsets Response => *throttle_time_ms [responses] 
    /// </summary>
    /// <remarks>
    /// Offsets Response => *throttle_time_ms [responses] 
    ///   throttle_time_ms => INT32
    ///   responses => topic [partition_responses] 
    ///     topic => STRING
    ///     partition_responses => partition error_code *timestamp *offset *[offsets] 
    ///       partition => INT32
    ///       error_code => INT16
    ///       timestamp => INT64
    ///       offset => INT64
    ///       offsets => INT64
    /// 
    /// Version 0 only: offsets
    /// Version 1+: timestamp
    /// Version 1+: offset
    /// Version 2+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_Offsets
    /// </remarks>
    public class OffsetsResponse : IResponse<OffsetsResponse.Topic>, IEquatable<OffsetsResponse>
    {
        public override string ToString() => $"{{responses:[{Responses.ToStrings()}]}}";

        public static OffsetsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 2);
                var topics = new List<Topic>();
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
                                topics.Add(new Topic(topicName, partitionId, errorCode, offset));
                            }
                        } else {
                            var timestamp = reader.ReadInt64();
                            var offset = reader.ReadInt64();
                            topics.Add(new Topic(topicName, partitionId, errorCode, offset, DateTimeOffset.FromUnixTimeMilliseconds(timestamp)));
                        }
                    }
                }
                return new OffsetsResponse(topics, throttleTime);
            }            
        }

        public OffsetsResponse(Topic topic)
            : this(new[] {topic})
        {
        }

        public OffsetsResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
        {
            Responses = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Responses.Select(t => t.Error));
            ThrottleTime = throttleTime;
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Responses { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not violate any quota.)
        /// Version: 2+
        /// </summary>
        public TimeSpan? ThrottleTime { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetsResponse);
        }

        /// <inheritdoc />
        public bool Equals(OffsetsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Responses.HasEqualElementsInOrder(other.Responses)
                && ThrottleTime == other.ThrottleTime;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = ThrottleTime?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Responses?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicOffset, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},offset:{Offset},error_code:{Error}}}";

            public Topic(string topic, int partitionId, ErrorCode errorCode = ErrorCode.NONE, long offset = -1, DateTimeOffset? timestamp = null) 
                : base(topic, partitionId, offset)
            {
                this.Timestamp = timestamp;
                Error = errorCode;
            }

            /// <summary>
            /// Error response code.
            /// </summary>
            public ErrorCode Error { get; }

            /// <summary>
            /// The timestamp associated with the returned offset.
            /// Version: 1+
            /// </summary>
            public DateTimeOffset? Timestamp { get; }

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
                    && Timestamp?.ToUnixTimeMilliseconds() == other.Timestamp?.ToUnixTimeMilliseconds()
                    && Error == other.Error;
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ (Timestamp?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ Error.GetHashCode();
                    return hashCode;
                }
            }
        
            #endregion
        }
    }
}