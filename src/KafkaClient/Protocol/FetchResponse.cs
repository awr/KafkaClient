using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

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
    public class FetchResponse : ThrottledResponse, IResponse<FetchResponse.Topic>, IEquatable<FetchResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},responses:[{Responses.ToStrings()}]}}";

        public static FetchResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 1);
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

                        var messageBatch = reader.ReadMessages();
                        topics.Add(new Topic(topicName, partitionId, highWaterMarkOffset, errorCode, lastStableOffset, logStartOffset, messageBatch.Messages, transactions));
                    }
                }
                return new FetchResponse(topics, throttleTime);
            }
        }

        public FetchResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Responses = topics.ToSafeImmutableList();
            Errors = Responses.Select(t => t.Error).ToImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Responses { get; }

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
            return base.Equals(other)
                && Responses.HasEqualElementsInOrder(other.Responses);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Responses?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{{this.PartitionToString()},error_code:{Error},high_watermark:{HighWatermark},last_stable_offset:{LastStableOffset},log_start_offset:{LogStartOffset},Messages:{Messages.Count},aborted_transactions:{AbortedTransactions.ToStrings()}}}";

            public Topic(string topic, int partitionId, long highWatermark, ErrorCode errorCode = ErrorCode.NONE, long? lastStableOffset = null, long? logStartOffset = null, IEnumerable<Message> messages = null, IEnumerable<AbortedTransaction> abortedTransactions = null)
                : base(topic, partitionId, errorCode)
            {
                HighWatermark = highWatermark;
                LastStableOffset = lastStableOffset;
                LogStartOffset = logStartOffset;
                Messages = messages.ToSafeImmutableList();
                AbortedTransactions = abortedTransactions.ToSafeImmutableList();
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
                    int hashCode = ProducerId.GetHashCode();
                    hashCode = (hashCode * 397) ^ FirstOffset.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }
    }
}