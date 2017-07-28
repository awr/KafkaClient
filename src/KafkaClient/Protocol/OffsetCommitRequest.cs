using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetCommit Request => group_id *generation_id *member_id *retention_time [topics]
    ///  *generation_id, member_id is only version 1 (0.8.2) and above
    ///  *retention_time is only version 2 (0.9.0) and above
    ///  group_id => STRING                 -- The consumer group id.
    ///  generation_id => INT32             -- The generation of the consumer group.
    ///  member_id => STRING                -- The consumer id assigned by the group coordinator.
    ///  retention_time => INT64            -- Time period in ms to retain the offset.
    /// 
    ///  topics => TopicName [partitions]
    ///   topic => STRING                   -- The topic to commit.
    /// 
    ///   partitions => partition_id offset *timestamp metadata
    ///    *TimeStamp is only version 1 (0.8.2)
    ///    partition_id => INT32            -- The partition id.
    ///    offset => INT64                  -- message offset to be committed.
    ///    timestamp => INT64               -- Commit timestamp.
    ///    metadata => NULLABLE_STRING      -- Any associated metadata the client wants to keep
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// Class that represents the api call to commit a specific set of offsets for a given topic.  The offset is saved under the
    /// arbitrary ConsumerGroup name provided by the call.
    /// </summary>
    public class OffsetCommitRequest : GroupRequest, IRequest<OffsetCommitResponse>, IEquatable<OffsetCommitRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{group_id},member_id:{member_id},generation_id:{generation_id},topics:[{topics.ToStrings()}],retention_time:{retention_time}}}";

        public override string ShortString() => $"{ApiKey} {group_id} {member_id}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(group_id);
            if (context.ApiVersion >= 1) {
                writer.Write(generation_id)
                        .Write(member_id);
            }
            if (context.ApiVersion >= 2) {
                if (retention_time.HasValue) {
                    writer.Write((long) retention_time.Value.TotalMilliseconds);
                } else {
                    writer.Write(-1L);
                }
            }

            var topicGroups = topics.GroupBy(x => x.TopicName).ToList();
            writer.Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups) {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                writer.Write(topicGroup.Key)
                        .Write(partitions.Count);

                foreach (var partition in partitions) {
                    foreach (var commit in partition) {
                        writer.Write(partition.Key)
                                .Write(commit.offset);
                        if (context.ApiVersion == 1) {
                            writer.Write(commit.timeStamp.GetValueOrDefault(-1));
                        }
                        writer.Write(commit.metadata);
                    }
                }
            }
        }

        public OffsetCommitResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => OffsetCommitResponse.FromBytes(context, bytes);

        public OffsetCommitRequest(string groupId, IEnumerable<Topic> offsetCommits, string memberId = null, int generationId = -1, TimeSpan? retentionTime = null) 
            : base(ApiKey.OffsetCommit, groupId, memberId ?? "", generationId)
        {
            retention_time = retentionTime;
            topics = ImmutableList<Topic>.Empty.AddNotNullRange(offsetCommits);
        }

        /// <summary>
        /// Time period to retain the offset.
        /// </summary>
        public TimeSpan? retention_time { get; }

        public IImmutableList<Topic> topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetCommitRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetCommitRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && retention_time.Equals(other.retention_time) 
                && topics.HasEqualElementsInOrder(other.topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ retention_time.GetHashCode();
                hashCode = (hashCode*397) ^ (topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},timeStamp:{timeStamp},offset:{offset},metadata:{metadata}}}";

            public Topic(string topicName, int partitionId, long offset, string metadata = null, long? timeStamp = null) 
                : base(topicName, partitionId)
            {
                if (offset < -1L) throw new ArgumentOutOfRangeException(nameof(offset), offset, "value must be >= -1");

                this.offset = offset;
                this.timeStamp = timeStamp;
                this.metadata = metadata;
            }

            /// <summary>
            /// The offset number to commit as completed.
            /// </summary>
            public long offset { get; }

            /// <summary>
            /// If the time stamp field is set to -1, then the broker sets the time stamp to the receive time before committing the offset.
            /// Only version 1 (0.8.2)
            /// </summary>
            public long? timeStamp { get; }

            /// <summary>
            /// Descriptive metadata about this commit.
            /// </summary>
            public string metadata { get; }

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                    && offset == other.offset 
                    && string.Equals(metadata, other.metadata);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ offset.GetHashCode();
                    hashCode = (hashCode*397) ^ (metadata?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }
        }
    }
}