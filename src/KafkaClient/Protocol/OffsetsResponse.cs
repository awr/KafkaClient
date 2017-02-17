using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Offsets Response => [responses]
    ///  response => topic [partition_responses]
    ///   topic => STRING  -- The name of the topic.
    /// 
    ///   partition_response => partition_id error_code *timestamp *offset *[offset]
    ///    *timestamp, *offset only applies to version 1 (Kafka 0.10.1 and higher)
    ///    *[offset] only applies to version 0 (Kafka 0.10.0.1 and below)
    ///    partition_id => INT32  -- The id of the partition the fetch is for.
    ///    error_code => INT16    -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
    ///                              be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
    ///    timestamp => INT64     -- The timestamp associated with the returned offset
    ///    offset => INT64        -- offset found
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
    /// </summary>
    public class OffsetsResponse : IResponse<OffsetsResponse.Topic>, IEquatable<OffsetsResponse>
    {
        public override string ToString() => $"{{responses:[{responses.ToStrings()}]}}";

        public static OffsetsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
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
                return new OffsetsResponse(topics);
            }            
        }

        public OffsetsResponse(Topic topic)
            : this(new[] {topic})
        {
        }

        public OffsetsResponse(IEnumerable<Topic> topics = null)
        {
            responses = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(responses.Select(t => t.error_code));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> responses { get; }

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
            return responses.HasEqualElementsInOrder(other.responses);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return responses?.Count.GetHashCode() ?? 0;
        }

        #endregion

        public class Topic : TopicOffset, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{topic},partition_id:{partition_id},offset:{offset},error_code:{error_code}}}";

            public Topic(string topic, int partitionId, ErrorCode errorCode = ErrorCode.NONE, long offset = -1, DateTimeOffset? timestamp = null) 
                : base(topic, partitionId, offset)
            {
                this.timestamp = timestamp;
                error_code = errorCode;
            }

            /// <summary>
            /// Error response code.
            /// </summary>
            public ErrorCode error_code { get; }

            /// <summary>
            /// The timestamp associated with the returned offset.
            /// This only applies to version 1 and above.
            /// </summary>
            public DateTimeOffset? timestamp { get; }

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
                    && timestamp?.ToUnixTimeMilliseconds() == other.timestamp?.ToUnixTimeMilliseconds()
                    && error_code == other.error_code;
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ (timestamp?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ error_code.GetHashCode();
                    return hashCode;
                }
            }
        
            #endregion
        }
    }
}