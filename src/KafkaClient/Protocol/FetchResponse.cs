using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Fetch Response => *throttle_time_ms [responses]
    ///  *throttle_time_ms is only version 1 (0.9.0) and above
    ///  throttle_time_ms => int32 -- Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
    ///                            violate any quota.)
    /// 
    ///  responses => topic [partition_responses]
    ///   topic => string          -- The topic this response entry corresponds to.
    /// 
    ///   partition_responses => partition_id error_code high_watermark record_set 
    ///    partition_id => int32   -- The partition this response entry corresponds to.
    ///    error_code => int16     -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
    ///                               be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
    ///    high_watermark => int64 -- The offset at the end of the log for this partition. This can be used by the client to determine how many messages 
    ///                               behind the end of the log they are.
    ///    record_set => BYTES     -- The size (and bytes) of the message set that follows.
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchResponse
    /// </summary>
    public class FetchResponse : IResponse<FetchResponse.Topic>, IEquatable<FetchResponse>
    {
        public override string ToString() => $"{{throttle_time_ms:{throttle_time_ms},responses:[{responses.ToStrings()}]}}";

        public static FetchResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                TimeSpan? throttleTime = null;

                if (context.ApiVersion >= 1) {
                    throttleTime = TimeSpan.FromMilliseconds(reader.ReadInt32());
                }

                var topics = new List<Topic>();
                var topicCount = reader.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();
                        var highWaterMarkOffset = reader.ReadInt64();
                        var messages = reader.ReadMessages();

                        topics.Add(new Topic(topicName, partitionId, highWaterMarkOffset, errorCode, messages));
                    }
                }
                return new FetchResponse(topics, throttleTime);
            }
        }

        public FetchResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
        {
            responses = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(responses.Select(t => t.error_code));
            throttle_time_ms = throttleTime;
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> responses { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        /// violate any quota.) Only version 1 and above (0.9.0)
        /// </summary>
        public TimeSpan? throttle_time_ms { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as FetchResponse);
        }

        /// <inheritdoc />
        public bool Equals(FetchResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return responses.HasEqualElementsInOrder(other.responses) 
                && throttle_time_ms.Equals(other.throttle_time_ms);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((responses?.Count.GetHashCode() ?? 0)*397) ^ throttle_time_ms.GetHashCode();
            }
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{topic},partition_id:{partition_id},error_code:{error_code},high_watermark:{high_watermark},Messages:{Messages.Count}}}";

            public Topic(string topic, int partitionId, long highWatermark, ErrorCode errorCode = ErrorCode.NONE, IEnumerable<Message> messages = null)
                : base(topic, partitionId, errorCode)
            {
                high_watermark = highWatermark;
                Messages = ImmutableList<Message>.Empty.AddNotNullRange(messages);
            }

            /// <summary>
            /// The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
            /// </summary>
            public long high_watermark { get; }

            public IImmutableList<Message> Messages { get; }

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
                    && high_watermark == other.high_watermark 
                    && Messages.HasEqualElementsInOrder(other.Messages);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ high_watermark.GetHashCode();
                    hashCode = (hashCode*397) ^ (Messages?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }
    }
}