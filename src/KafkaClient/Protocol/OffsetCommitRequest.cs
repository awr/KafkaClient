using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetCommit Request => group_id *group_generation_id *member_id *retention_time [topics] 
    /// </summary>
    /// <remarks>
    /// OffsetCommit Request => group_id *group_generation_id *member_id *retention_time [topics] 
    ///   group_id => STRING
    ///   group_generation_id => INT32
    ///   member_id => STRING
    ///   retention_time => INT64
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => partition offset *timestamp metadata 
    ///       partition => INT32
    ///       offset => INT64
    ///       metadata => NULLABLE_STRING
    /// 
    /// Version 1+: group_generation_id
    /// Version 1+: member_id
    /// Version 1 only: timestamp => INT64
    /// Version 2+: retention_time
    /// From http://kafka.apache.org/protocol.html#The_Messages_OffsetCommit
    /// </remarks>
    public class OffsetCommitRequest : GroupRequest, IRequest<OffsetCommitResponse>, IEquatable<OffsetCommitRequest>
    {
        public override string ToString() => $"{{{this.RequestToString()},group_id:{GroupId},generation_id:{GenerationId},member_id:{MemberId},retention_time:{RetentionTime?.TotalMilliseconds:F0},topics:[{Topics.ToStrings()}]}}";

        public override string ShortString() => $"{ApiKey} {GroupId} {MemberId}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(GroupId);
            if (context.ApiVersion >= 1) {
                writer.Write(GenerationId)
                        .Write(MemberId);
            }
            if (context.ApiVersion >= 2) {
                if (RetentionTime.HasValue) {
                    writer.Write((long) RetentionTime.Value.TotalMilliseconds);
                } else {
                    writer.Write(-1L);
                }
            }

            writer.WriteGroupedTopics(Topics,
                partition => {
                    writer.Write(partition.PartitionId)
                          .Write(partition.Offset);
                    if (context.ApiVersion == 1) {
                        writer.Write(partition.TimeStamp.GetValueOrDefault(-1));
                    }
                    writer.Write(partition.Metadata);
                });
        }

        public OffsetCommitResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => OffsetCommitResponse.FromBytes(context, bytes);

        public OffsetCommitRequest(string groupId, IEnumerable<Topic> offsetCommits, string memberId = null, int generationId = -1, TimeSpan? retentionTime = null) 
            : base(ApiKey.OffsetCommit, groupId, memberId ?? "", generationId)
        {
            RetentionTime = retentionTime;
            Topics = offsetCommits.ToSafeImmutableList();
        }

        /// <summary>
        /// ime period in ms to retain the offset. Brokers will always retain offsets until its commit time stamp + user specified retention 
        /// time in the commit request. If the retention time is not set (-1), the broker offset retention time will be used as default.
        /// Version: 2+
        /// </summary>
        public TimeSpan? RetentionTime { get; }

        public IImmutableList<Topic> Topics { get; }

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
                && RetentionTime.Equals(other.RetentionTime) 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ RetentionTime.GetHashCode();
                hashCode = (hashCode*397) ^ (Topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{{this.PartitionToString()},offset:{Offset},timeStamp:{TimeStamp},metadata:{Metadata}}}";

            public Topic(string topicName, int partitionId, long offset, string metadata = null, long? timeStamp = null) 
                : base(topicName, partitionId)
            {
                if (offset < -1L) throw new ArgumentOutOfRangeException(nameof(offset), offset, "value must be >= -1");

                Offset = offset;
                TimeStamp = timeStamp;
                Metadata = metadata;
            }

            /// <summary>
            /// The offset number to commit as completed.
            /// </summary>
            public long Offset { get; }

            /// <summary>
            /// If the time stamp field is set to -1, then the broker sets the time stamp to the receive time before committing the offset.
            /// Version: 1 only
            /// </summary>
            public long? TimeStamp { get; }

            /// <summary>
            /// Descriptive metadata about this commit.
            /// </summary>
            public string Metadata { get; }

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other)
                    && Offset == other.Offset
                    && TimeStamp == other.TimeStamp
                    && string.Equals(Metadata, other.Metadata);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Offset.GetHashCode();
                    hashCode = (hashCode * 397) ^ (TimeStamp?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ (Metadata?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }
        }
    }
}