using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Offsets Request => replica_id *isolation_level [topics] 
    /// </summary>
    /// <remarks>
    /// Offsets Request => replica_id *isolation_level [topics] 
    ///   replica_id => INT32
    ///   isolation_level => INT8
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => partition timestamp *max_num_offsets
    ///       partition => INT32
    ///       timestamp => INT64
    ///       max_num_offsets => INT32
    /// 
    /// Version 1 only: max_num_offsets
    /// Version 2+: isolation_level
    /// From http://kafka.apache.org/protocol.html#The_Messages_Offsets
    /// 
    /// replica_id: Broker id of the follower. For normal consumers, use -1. As a result, this value is hardcoded.
    /// </remarks>
    public class OffsetsRequest : Request, IRequest<OffsetsResponse>, IEquatable<OffsetsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},topics:[{Topics.ToStrings()}]}}";

        public override string ShortString() => Topics.Count == 1 ? $"{ApiKey} {Topics[0].TopicName}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(-1); // replica_id -- see above for rationale
            if (context.ApiVersion >= 2) {
                writer.Write(IsolationLevel);
            }
            writer.WriteGroupedTopics(
                Topics,
                partition => {
                    writer.Write(partition.PartitionId)
                          .Write(partition.Timestamp);

                    if (context.ApiVersion == 0) {
                        writer.Write(partition.MaxNumOffsets);
                    }
                });
        }

        public OffsetsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => OffsetsResponse.FromBytes(context, bytes);

        public OffsetsRequest(params Topic[] topics)
            : this((IEnumerable<Topic>)topics)
        {
        }

        public OffsetsRequest(IEnumerable<Topic> offsets, byte? isolationLevel = null) 
            : base(ApiKey.Offsets)
        {
            Topics = offsets.ToSafeImmutableList();
            IsolationLevel = isolationLevel.GetValueOrDefault();
        }

        public IImmutableList<Topic> Topics { get; }

        /// <summary>
        /// This setting controls the visibility of transactional records. Using isolation_level = 0 (<see cref="IsolationLevels.ReadUnCommitted"/>) makes all records visible. 
        /// With isolation_level = 1 (<see cref="IsolationLevels.ReadCommitted"/>), non-transactional and COMMITTED transactional records are visible. To be more concrete, 
        /// READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list 
        /// of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
        /// Version: 2+
        /// </summary>
        public byte IsolationLevel { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetsRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Topics.HasEqualElementsInOrder(other.Topics)
                && IsolationLevel == other.IsolationLevel;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ IsolationLevel.GetHashCode();
                hashCode = (hashCode * 397) ^ (Topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},timestamp:{Timestamp},max_num_offsets:{MaxNumOffsets}}}";

            public Topic(string topicName, int partitionId, long timestamp = LatestTime, int maxOffsets = DefaultMaxOffsets) : base(topicName, partitionId)
            {
                Timestamp = timestamp;
                MaxNumOffsets = maxOffsets;
            }

            /// <summary>
            /// Used to ask for all messages before a certain time (ms). There are two special values.
            /// Specify -1 (<see cref="LatestTime"/>) to receive the latest offsets and -2 (<see cref="EarliestTime"/>) to receive the earliest available offset.
            /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
            /// </summary>
            public long Timestamp { get; }

            /// <summary>
            /// Maximum offsets to return. Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
            /// Version: 0 only
            /// </summary>
            public int MaxNumOffsets { get; }

            public const long LatestTime = -1L;
            public const long EarliestTime = -2L;
            public const int DefaultMaxOffsets = 1;

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
                    && Timestamp == other.Timestamp 
                    && MaxNumOffsets == other.MaxNumOffsets;
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Timestamp.GetHashCode();
                    hashCode = (hashCode*397) ^ MaxNumOffsets;
                    return hashCode;
                }
            }

            #endregion
        }
    }
}