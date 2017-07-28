using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Offsets Request => replica_id [topics]
    ///  replica_id => INT32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
    ///                           specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
    ///                           to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
    /// 
    ///  topics => topic [partitions]
    ///   topic => STRING        -- The name of the topic.
    /// 
    ///   partitions => partition_id timestamp *max_num_offsets
    ///    *max_num_offsets is only version 0 (0.10.0.1)
    ///    partition_id => INT32 -- The id of the partition the fetch is for.
    ///    timestamp => INT64    -- Used to ask for all messages before a certain time (ms). There are two special values. Specify -1 to receive the 
    ///                             latest offset (i.e. the offset of the next coming message) and -2 to receive the earliest available offset. Note 
    ///                             that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
    ///    max_num_offsets => INT32 
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition
    /// </summary>
    public class OffsetsRequest : Request, IRequest<OffsetsResponse>, IEquatable<OffsetsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},topics:[{topics.ToStrings()}]}}";

        public override string ShortString() => topics.Count == 1 ? $"{ApiKey} {topics[0].TopicName}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            var topicGroups = topics.GroupBy(x => x.TopicName).ToList();
            writer.Write(-1) // replica_id -- see above for rationale
                  .Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups) {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                writer.Write(topicGroup.Key)
                      .Write(partitions.Count);

                foreach (var partition in partitions) {
                    foreach (var offset in partition) {
                        writer.Write(partition.Key)
                              .Write(offset.timestamp);

                        if (context.ApiVersion == 0) {
                            writer.Write(offset.max_num_offsets);
                        }
                    }
                }
            }
        }

        public OffsetsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => OffsetsResponse.FromBytes(context, bytes);

        public OffsetsRequest(params Topic[] topics)
            : this((IEnumerable<Topic>)topics)
        {
        }

        public OffsetsRequest(IEnumerable<Topic> offsets) 
            : base(ApiKey.Offsets)
        {
            topics = ImmutableList<Topic>.Empty.AddNotNullRange(offsets);
        }

        public IImmutableList<Topic> topics { get; }

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
            return topics.HasEqualElementsInOrder(other.topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return topics?.Count.GetHashCode() ?? 0;
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},timestamp:{timestamp},max_num_offsets:{max_num_offsets}}}";

            public Topic(string topicName, int partitionId, long timestamp = LatestTime, int maxOffsets = DefaultMaxOffsets) : base(topicName, partitionId)
            {
                this.timestamp = timestamp;
                max_num_offsets = maxOffsets;
            }

            /// <summary>
            /// Used to ask for all messages before a certain time (ms). There are two special values.
            /// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset.
            /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
            /// </summary>
            public long timestamp { get; }

            /// <summary>
            /// Only applies to version 0 (Kafka 0.10.1 and below)
            /// </summary>
            public int max_num_offsets { get; }

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
                    && timestamp == other.timestamp 
                    && max_num_offsets == other.max_num_offsets;
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ timestamp.GetHashCode();
                    hashCode = (hashCode*397) ^ max_num_offsets;
                    return hashCode;
                }
            }

            #endregion
        }
    }
}