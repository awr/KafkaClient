using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Produce Response => [responses] *throttle_time_ms 
    /// </summary>
    /// <remarks>
    /// Produce Response => [responses] *throttle_time_ms 
    ///   responses => topic [partition_responses] 
    ///     topic => STRING
    ///     partition_responses => partition error_code base_offset *log_append_time 
    ///       partition => INT32
    ///       error_code => INT16
    ///       base_offset => INT64
    ///       log_append_time => INT64
    ///   throttle_time_ms => INT32
    /// 
    /// Version 1+: throttle_time_ms
    /// Version 2+: log_append_time
    /// From http://kafka.apache.org/protocol.html#The_Messages_Produce
    /// </remarks>
    public class ProduceResponse : ThrottledResponse, IResponse<ProduceResponse.Topic>, IEquatable<ProduceResponse>
    {
        public override string ToString() => $"{{responses:[{Responses.ToStrings()}],throttle_time_ms:{ThrottleTime}}}";

        public static ProduceResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var topics = new List<Topic>();
                var topicCount = reader.ReadInt32();
                for (var i = 0; i < topicCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();
                        var offset = reader.ReadInt64();
                        DateTimeOffset? timestamp = null;

                        if (context.ApiVersion >= 2) {
                            var milliseconds = reader.ReadInt64();
                            if (milliseconds >= 0) {
                                timestamp = DateTimeOffset.FromUnixTimeMilliseconds(milliseconds);
                            }
                        }

                        topics.Add(new Topic(topicName, partitionId, errorCode, offset, timestamp));
                    }
                }

                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 1);
                return new ProduceResponse(topics, throttleTime);
            }
        }

        public ProduceResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Responses = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Responses.Select(t => t.Error));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Responses { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ProduceResponse);
        }

        /// <inheritdoc />
        public bool Equals(ProduceResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Responses.HasEqualElementsInOrder(other.Responses);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (Responses?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},error_code:{Error},base_offset:{BaseOffset},timestamp:{Timestamp}}}";

            public Topic(string topic, int partitionId, ErrorCode errorCode, long offset, DateTimeOffset? timestamp = null)
                : base(topic, partitionId, errorCode)
            {
                BaseOffset = offset;
                this.Timestamp = timestamp.HasValue && timestamp.Value.ToUnixTimeMilliseconds() >= 0 ? timestamp : null;
            }

            /// <summary>
            /// The offset number to commit as completed.
            /// </summary>
            public long BaseOffset { get; }

            /// <summary>
            /// If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
            /// All the messages in the message set have the same timestamp.
            /// If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
            /// produce request has been accepted by the broker if there is no error code returned.
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
                    && BaseOffset == other.BaseOffset 
                    && Timestamp.Equals(other.Timestamp);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ BaseOffset.GetHashCode();
                    hashCode = (hashCode*397) ^ Timestamp.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }
    }
}