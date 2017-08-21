using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetForLeaderEpoch Request => [topics] 
    /// </summary>
    /// <remarks>
    /// OffsetForLeaderEpoch Request => [topics] 
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => partition_id leader_epoch 
    ///       partition_id => INT32
    ///       leader_epoch => INT32
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_OffsetForLeaderEpoch
    /// </remarks>
    public class OffsetForLeaderEpochRequest : Request, IRequest<OffsetForLeaderEpochResponse>, IEquatable<OffsetForLeaderEpochRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},topics:[{Topics.ToStrings()}]}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.WriteGroupedTopics(
                Topics,
                partition => {
                    writer.Write(partition.PartitionId)
                          .Write(partition.LeaderEpoch);
                });
        }

        public OffsetForLeaderEpochResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => OffsetForLeaderEpochResponse.FromBytes(context, bytes);

        public OffsetForLeaderEpochRequest(IEnumerable<Topic> topics = null) 
            : base(ApiKey.OffsetForLeaderEpoch)
        {
            Topics = topics.ToSafeImmutableList();
        }

        /// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
        public IImmutableList<Topic> Topics { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetForLeaderEpochRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetForLeaderEpochRequest other)
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

        /// <summary>
        /// An array of topics to get epochs for.
        /// Included in <see cref="DeleteRecordsRequest"/>
        /// </summary>
        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},leader_epoch:{LeaderEpoch}}}";

            public Topic(string topicName, int partitionId, int leaderEpoch = 0) 
                : base(topicName, partitionId)
            {
                LeaderEpoch = leaderEpoch;
            }

            /// <summary>
            /// The epoch
            /// </summary>
            public int LeaderEpoch { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            /// <inheritdoc />
            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                    && LeaderEpoch == other.LeaderEpoch;
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ LeaderEpoch.GetHashCode();
                    return hashCode;
                }
            }

            #endregion

        }
    }
}