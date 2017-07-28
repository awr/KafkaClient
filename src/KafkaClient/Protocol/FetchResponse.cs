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
    /// </summary>
    /// <remarks>
    /// Fetch Response => *throttle_time_ms [responses] 
    ///   throttle_time_ms => INT32
    ///   responses => topic [partition_responses] 
    ///     topic => STRING
    ///     partition_responses => partition_header record_set 
    ///       partition_header => partition error_code high_watermark *last_stable_offset *log_start_offset *[aborted_transactions] 
    ///         partition => INT32
    ///         error_code => INT16
    ///         high_watermark => INT64
    ///         last_stable_offset => INT64
    ///         log_start_offset => INT64
    ///         aborted_transactions => producer_id first_offset 
    ///           producer_id => INT64
    ///           first_offset => INT64
    ///       record_set => RECORDS
    /// 
    /// Version 1+: throttle_time_ms
    /// Version 4+: last_stable_offset, aborted_transactions
    /// Version 5+: log_start_offset
    /// From http://kafka.apache.org/protocol.html#The_Messages_Fetch
    /// </remarks>
    public class FetchResponse : IResponse<FetchResponse.Topic>, IEquatable<FetchResponse>
    {
        public override string ToString() => $"{{throttle_time_ms:{ThrottleTime},responses:[{Responses.ToStrings()}]}}";

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
                        long? logStartOffset = null;
                        var transactions = new List<AbortedTransaction>();
                        if (context.ApiVersion >= 4) {
                            lastStableOffset = reader.ReadInt64();
                            if (context.ApiVersion >= 5) {
                                logStartOffset = reader.ReadInt64();
                            }
                            var count = reader.ReadInt32();
                            for (var a = 0; a < count; a++) {
                                var producerId = reader.ReadInt64();
                                var firstOffset = reader.ReadInt64();
                                transactions.Add(new AbortedTransaction(producerId, firstOffset));
                            }
                        }
                        var messages = reader.ReadMessages();

                        topics.Add(new Topic(topicName, partitionId, highWaterMarkOffset, errorCode, lastStableOffset, logStartOffset, messages, transactions));
                    }
                }
                return new FetchResponse(topics, throttleTime);
            }
        }

        public FetchResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
        {
            Responses = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Responses.Select(t => t.Error));
            ThrottleTime = throttleTime;
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Responses { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        /// violate any quota.) Only version 1 and above (0.9.0)
        /// </summary>
        public TimeSpan? ThrottleTime { get; }

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
                && ThrottleTime.Equals(other.ThrottleTime);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((Responses?.Count.GetHashCode() ?? 0)*397) ^ ThrottleTime.GetHashCode();
            }
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},error_code:{Error},high_watermark:{HighWatermark},last_stable_offset:{LastStableOffset},log_start_offset:{LogStartOffset},Messages:{Messages.Count},aborted_transactions:{AbortedTransactions.ToStrings()}}}";

            public Topic(string topic, int partitionId, long highWatermark, ErrorCode errorCode = ErrorCode.NONE, long? lastStableOffset = null, long? logStartOffset = null, IEnumerable<Message> messages = null, IEnumerable<AbortedTransaction> abortedTransactions = null)
                : base(topic, partitionId, errorCode)
            {
                HighWatermark = highWatermark;
                LastStableOffset = lastStableOffset;
                LogStartOffset = logStartOffset;
                Messages = ImmutableList<Message>.Empty.AddNotNullRange(messages);
                AbortedTransactions = ImmutableList<AbortedTransaction>.Empty.AddNotNullRange(abortedTransactions);
            }

            /// <summary>
            /// The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
            /// </summary>
            public long HighWatermark { get; }

            /// <summary>
            /// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional 
            /// records prior to this offset have been decided (ABORTED or COMMITTED)
            /// Version: 4+
            /// </summary>
            public long? LastStableOffset { get; }

            /// <summary>
            /// Earliest available offset.
            /// Version: 5+
            /// </summary>
            public long? LogStartOffset { get; }

            public IImmutableList<Message> Messages { get; }

            /// <summary>
            /// Version: 4+
            /// </summary>
            public IImmutableList<AbortedTransaction> AbortedTransactions { get; }

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
                    && HighWatermark == other.HighWatermark
                    && LastStableOffset == other.LastStableOffset
                    && LogStartOffset == other.LogStartOffset
                    && Messages.HasEqualElementsInOrder(other.Messages)
                    && AbortedTransactions.HasEqualElementsInOrder(other.AbortedTransactions);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode * 397) ^ HighWatermark.GetHashCode();
                    hashCode = (hashCode * 397) ^ (LastStableOffset?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ (LogStartOffset?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ (Messages?.Count.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ (AbortedTransactions?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }

        public class AbortedTransaction : IEquatable<AbortedTransaction>
        {
            public override string ToString() => $"{{producer_id:{ProducerId},first_offset:{FirstOffset}}}";

            public AbortedTransaction(long producerId, long firstOffset)
            {
                ProducerId = producerId;
                FirstOffset = firstOffset;
            }

            /// <summary>
            /// The producer id associated with the aborted transactions.
            /// </summary>
            public long ProducerId { get; }

            /// <summary>
            /// The first offset in the aborted transaction.
            /// </summary>
            public long FirstOffset { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as AbortedTransaction);
            }

            public bool Equals(AbortedTransaction other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return ProducerId == other.ProducerId
                    && FirstOffset == other.FirstOffset;
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode * 397) ^ ProducerId.GetHashCode();
                    hashCode = (hashCode * 397) ^ FirstOffset.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }
    }
}