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
    ///   partition_responses => partition_id error_code high_watermark *last_stable_offset *[aborted_transactions] record_set 
    ///    *last_stable_offset is only version 4 and above 
    ///    *aborted_transactions is only version 4 and above
    ///    partition_id => int32       -- The partition this response entry corresponds to.
    ///    error_code => int16         -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may 
    ///                                   be unavailable or maintained on a different host, while others may have successfully accepted the produce request.
    ///    high_watermark => int64     -- The offset at the end of the log for this partition. This can be used by the client to determine how many messages 
    ///                                   behind the end of the log they are.
    ///    last_stable_offset => INT64 -- The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional 
    ///                                   records prior to this offset have been decided (ABORTED or COMMITTED).
    /// 
    ///    aborted_transactions => producer_id first_offset
    ///     producer_id => INT64       -- The producer id associated with the aborted transactions.
    ///     first_offset => INT64      -- The first offset in the aborted transaction.
    ///    record_set => BYTES         -- The size (and bytes) of the message set that follows.
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchResponse
    /// </summary>
    public class FetchResponse : IResponse<FetchResponse.Topic>, IEquatable<FetchResponse>
    {
        public override string ToString() => $"{{throttle_time_ms:{throttle_time_ms},responses:[{Responses.ToStrings()}]}}";

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

                        long? lastStableOffset = null;
                        var transactions = new List<AbortedTransaction>();
                        if (context.ApiVersion >= 4) {
                            lastStableOffset = reader.ReadInt64();
                            var count = reader.ReadInt32();
                            for (var a = 0; a < count; a++) {
                                var producerId = reader.ReadInt64();
                                var firstOffset = reader.ReadInt64();
                                transactions.Add(new AbortedTransaction(producerId, firstOffset));
                            }
                        }
                        var messages = reader.ReadMessages();

                        topics.Add(new Topic(topicName, partitionId, highWaterMarkOffset, errorCode, lastStableOffset, messages));
                    }
                }
                return new FetchResponse(topics, throttleTime);
            }
        }

        public FetchResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
        {
            Responses = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Responses.Select(t => t.Error));
            throttle_time_ms = throttleTime;
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Responses { get; }

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
            return Responses.HasEqualElementsInOrder(other.Responses) 
                && throttle_time_ms.Equals(other.throttle_time_ms);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((Responses?.Count.GetHashCode() ?? 0)*397) ^ throttle_time_ms.GetHashCode();
            }
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},error_code:{Error},high_watermark:{high_watermark},Messages:{Messages.Count}}}";

            public Topic(string topic, int partitionId, long highWatermark, ErrorCode errorCode = ErrorCode.NONE, long? lastStableOffset = null, IEnumerable<Message> messages = null)
                : base(topic, partitionId, errorCode)
            {
                high_watermark = highWatermark;
                last_stable_offset = lastStableOffset;
                Messages = ImmutableList<Message>.Empty.AddNotNullRange(messages);
            }

            /// <summary>
            /// The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
            /// </summary>
            public long high_watermark { get; }

            /// <summary>
            /// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional 
            /// records prior to this offset have been decided (ABORTED or COMMITTED)
            /// </summary>
            public long? last_stable_offset { get; }

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
                    && last_stable_offset == other.last_stable_offset
                    && Messages.HasEqualElementsInOrder(other.Messages);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode * 397) ^ high_watermark.GetHashCode();
                    hashCode = (hashCode * 397) ^ (last_stable_offset?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Messages?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }

        public class AbortedTransaction : IEquatable<AbortedTransaction>
        {
            public AbortedTransaction(long producerId, long firstOffset)
            {
                producer_id = producerId;
                first_offset = firstOffset;
            }

            /// <summary>
            /// The producer id associated with the aborted transactions.
            /// </summary>
            public long producer_id { get; }

            /// <summary>
            /// The first offset in the aborted transaction.
            /// </summary>
            public long first_offset { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as AbortedTransaction);
            }

            public bool Equals(AbortedTransaction other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return producer_id == other.producer_id
                    && first_offset == other.first_offset;
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode * 397) ^ producer_id.GetHashCode();
                    hashCode = (hashCode * 397) ^ first_offset.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }
    }
}