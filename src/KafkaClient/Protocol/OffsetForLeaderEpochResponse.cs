using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetForLeaderEpoch Response => [topics] 
    /// </summary>
    /// <remarks>
    /// OffsetForLeaderEpoch Response => [topics] 
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => error_code partition_id end_offset 
    ///       error_code => INT16
    ///       partition_id => INT32
    ///       end_offset => INT64
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_OffsetForLeaderEpoch
    /// </remarks>
    public class OffsetForLeaderEpochResponse : IResponse, IEquatable<OffsetForLeaderEpochResponse>
    {
        public override string ToString() => $"{{topics:{Topics.ToStrings()}}}";

        public static OffsetForLeaderEpochResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var topicCount = reader.ReadInt32();
                reader.AssertMaxArraySize(topicCount);
                var topics = new List<Topic>();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();
                    var partitionCount = reader.ReadInt32();
                    reader.AssertMaxArraySize(partitionCount);
                    for (var p = 0; p < partitionCount; p++) {
                        var errorCode = reader.ReadErrorCode();
                        var partitionId = reader.ReadInt32();
                        var endOffset = reader.ReadInt64();
                        topics.Add(new Topic(topicName, errorCode, partitionId, endOffset));
                    }
                }
                return new OffsetForLeaderEpochResponse(topics);
            }
        }

        public OffsetForLeaderEpochResponse(IEnumerable<Topic> topics = null)
        {
            Topics = topics.ToSafeImmutableList();
            Errors = Topics.Select(t => t.Error).ToImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetForLeaderEpochResponse);
        }

        /// <inheritdoc />
        public bool Equals(OffsetForLeaderEpochResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Topics.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},error_code:{Error},partition_id:{PartitionId},end_offset:{EndOffset}}}";

            public Topic(string topic, ErrorCode errorCode, int partitionId, long endOffset)
                : base(topic, partitionId, errorCode)
            {
                EndOffset = endOffset;
            }

            /// <summary>
            /// Smallest available offset of all live replicas.
            /// </summary>
            public long EndOffset { get; }

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
                    && EndOffset == other.EndOffset;
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ EndOffset.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }
    }
}