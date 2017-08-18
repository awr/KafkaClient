using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// TxnOffsetCommit Request => transactional_id consumer_group_id producer_id producer_epoch [topics] 
    /// </summary>
    /// <remarks>
    /// TxnOffsetCommit Request => transactional_id consumer_group_id producer_id producer_epoch [topics] 
    ///   transactional_id => STRING
    ///   consumer_group_id => STRING
    ///   producer_id => INT64
    ///   producer_epoch => INT16
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => partition offset metadata 
    ///       partition => INT32
    ///       offset => INT64
    ///       metadata => NULLABLE_STRING
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_TxnOffsetCommit
    /// </remarks>
    public class TxnOffsetCommitRequest : TransactionRequest, IRequest<TxnOffsetCommitResponse>, IEquatable<TxnOffsetCommitRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},transactional_id:{TransactionId},producer_id:{ProducerId},producer_epoch:{ProducerEpoch},consumer_group_id:{GroupId},topics:[{Topics.ToStrings()}]}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(TransactionId)
                  .Write(GroupId)
                  .Write(ProducerId)
                  .Write(ProducerEpoch)
                  .WriteGroupedTopics(Topics,
                      partition => {
                          writer.Write(partition.PartitionId)
                                .Write(partition.Offset)
                                .Write(partition.Metadata);
                      });
        }

        public TxnOffsetCommitResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => TxnOffsetCommitResponse.FromBytes(context, bytes);

        public TxnOffsetCommitRequest(string transactionId, long producerId, short producerEpoch, string consumerGroupId, IEnumerable<Topic> topics = null) 
            : base(ApiKey.TxnOffsetCommit, transactionId, producerId, producerEpoch)
        {
            GroupId = consumerGroupId;
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
        }

        /// <summary>
        /// Id of the associated consumer group to commit offsets for.
        /// </summary>
        public string GroupId { get; }

        /// <summary>
        /// The transaction markers to be written.
        /// </summary>
        public IImmutableList<Topic> Topics { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as TxnOffsetCommitRequest);
        }

        /// <inheritdoc />
        public bool Equals(TxnOffsetCommitRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals((TransactionRequest)other)
                && GroupId == other.GroupId
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (GroupId?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ Topics.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},offset:{Offset},metadata:{Metadata}}}";

            public Topic(string topicName, int partitionId, long offset, string metadata = null) 
                : base(topicName, partitionId)
            {
                if (offset < -1L) throw new ArgumentOutOfRangeException(nameof(offset), offset, "value must be >= -1");

                Offset = offset;
                Metadata = metadata;
            }

            /// <summary>
            /// The offset number to commit as completed.
            /// </summary>
            public long Offset { get; }

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
                    && string.Equals(Metadata, other.Metadata);
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Offset.GetHashCode();
                    hashCode = (hashCode * 397) ^ (Metadata?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }
        }
    }
}