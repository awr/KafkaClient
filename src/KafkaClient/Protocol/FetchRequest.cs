using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Fetch Request => replica_id max_wait_time min_bytes *max_bytes *isolation_level [topics] 
    /// </summary>
    /// <remarks>
    /// Fetch Request => replica_id max_wait_time min_bytes *max_bytes *isolation_level [topics] 
    ///   replica_id => INT32
    ///   max_wait_time => INT32
    ///   min_bytes => INT32
    ///   max_bytes => INT32
    ///   isolation_level => INT8
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => partition fetch_offset *log_start_offset max_bytes 
    ///       partition => INT32
    ///       fetch_offset => INT64
    ///       log_start_offset => INT64
    ///       max_bytes => INT32
    /// 
    /// Version 3+: max_bytes
    /// Version 4+: isolation_level
    /// Version 5+: log_start_offset
    /// From http://kafka.apache.org/protocol.html#The_Messages_Fetch
    /// </remarks>
    public class FetchRequest : Request, IRequest<FetchResponse>, IEquatable<FetchRequest>
    {
        public override string ToString() => $"{{{this.RequestToString()},max_wait_time:{MaxWaitTime},min_bytes:{MinBytes},max_bytes:{MaxBytes},isolation_level:{IsolationLevel},topics:[{Topics.ToStrings()}]}}";

        public override string ShortString() => Topics.Count == 1 ? $"{ApiKey} {Topics[0].TopicName}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(-1) // replica_id -- see above
                    .WriteMilliseconds(MaxWaitTime)
                    .Write(MinBytes);

            if (context.ApiVersion >= 3) {
                writer.Write(MaxBytes);
                if (context.ApiVersion >= 4) {
                    writer.Write(IsolationLevel);
                }
            }

            writer.WriteGroupedTopics(
                Topics,
                partition => {
                    writer.Write(partition.PartitionId)
                          .Write(partition.FetchOffset);
                    if (context.ApiVersion >= 5) {
                        writer.Write(partition.LogStartOffset);
                    }
                    writer.Write(partition.MaxBytes);
                });
        }

        public FetchResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => FetchResponse.FromBytes(context, bytes);

        public FetchRequest(Topic topic, TimeSpan? maxWaitTime = null, int? minBytes = null, int? maxBytes = null, byte? isolationLevel = null) 
            : this (new []{ topic }, maxWaitTime, minBytes, maxBytes, isolationLevel)
        {
        }

        public FetchRequest(IEnumerable<Topic> fetches = null, TimeSpan? maxWaitTime = null, int? minBytes = null, int? maxBytes = null, byte? isolationLevel = null) 
            : base(ApiKey.Fetch)
        {
            MaxWaitTime = maxWaitTime ?? TimeSpan.FromMilliseconds(DefaultMaxBlockingWaitTime);
            MinBytes = minBytes.GetValueOrDefault(DefaultMinBlockingByteBufferSize);
            MaxBytes = maxBytes.GetValueOrDefault(MinBytes);
            IsolationLevel = isolationLevel.GetValueOrDefault();
            Topics = fetches.ToSafeImmutableList();
        }

        internal const int DefaultMinBlockingByteBufferSize = 4096;
        internal const int DefaultBufferSize = DefaultMinBlockingByteBufferSize * 8;
        internal const int DefaultMaxBlockingWaitTime = 5000;

        /// <summary>
        /// The max wait time is the maximum amount of time to block waiting if insufficient data is available at the time the request is issued.
        /// </summary>
        public TimeSpan MaxWaitTime { get; }

        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response.
        /// If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets.
        /// If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs.
        /// By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data
        /// (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
        /// </summary>
        public int MinBytes { get; }

        /// <summary>
        /// Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, if the first message in the first non-empty partition of the fetch is larger than 
        /// this value, the message will still be returned to ensure that progress can be made.
        /// Version: 3+
        /// </summary>
        public int MaxBytes { get; }

        /// <summary>
        /// This setting controls the visibility of transactional records. Using isolation_level = 0 (<see cref="IsolationLevels.ReadUnCommitted"/>) makes all records visible. 
        /// With isolation_level = 1 (<see cref="IsolationLevels.ReadCommitted"/>), non-transactional and COMMITTED transactional records are visible. To be more concrete, 
        /// READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list 
        /// of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
        /// Version: 4+
        /// </summary>
        public byte IsolationLevel { get; }

        public IImmutableList<Topic> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as FetchRequest);
        }

        /// <inheritdoc />
        public bool Equals(FetchRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return MaxWaitTime.Equals(other.MaxWaitTime) 
                && MinBytes == other.MinBytes
                && MaxBytes == other.MaxBytes
                && IsolationLevel == other.IsolationLevel
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = MaxWaitTime.GetHashCode();
                hashCode = (hashCode*397) ^ MinBytes;
                hashCode = (hashCode * 397) ^ MaxBytes;
                hashCode = (hashCode * 397) ^ IsolationLevel;
                hashCode = (hashCode*397) ^ (Topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{{this.PartitionToString()},fetch_offset:{FetchOffset},log_start_offset:{LogStartOffset},max_bytes:{MaxBytes}}}";

            public Topic(string topicName, int partitionId, long offset, long? logStartOffset = null, int? maxBytes = null)
                : base(topicName, partitionId)
            {
                FetchOffset = offset;
                LogStartOffset = logStartOffset.GetValueOrDefault();
                MaxBytes = maxBytes.GetValueOrDefault(DefaultMinBlockingByteBufferSize * 8);
            }

            /// <summary>
            /// The offset to begin this fetch from.
            /// </summary>
            public long FetchOffset { get; }

            /// <summary>
            /// Earliest available offset of the follower replica. The field is only used when request is sent by follower.
            /// Version: 5+
            /// </summary>
            public long LogStartOffset { get; }

            /// <summary>
            /// The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
            /// </summary>
            public int MaxBytes { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Equals((TopicPartition) other)
                    && FetchOffset == other.FetchOffset
                    && LogStartOffset == other.LogStartOffset
                    && MaxBytes == other.MaxBytes;
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ FetchOffset.GetHashCode();
                    hashCode = (hashCode * 397) ^ LogStartOffset.GetHashCode();
                    hashCode = (hashCode * 397) ^ MaxBytes;
                    return hashCode;
                }
            }

            #endregion
        }
    }
}