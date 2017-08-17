using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// AddPartitionsToTxn Request => transactional_id producer_id producer_epoch [topics] 
    /// </summary>
    /// <remarks>
    /// AddPartitionsToTxn Request => transactional_id producer_id producer_epoch [topics] 
    ///   transactional_id => STRING
    ///   producer_id => INT64
    ///   producer_epoch => INT16
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => INT32
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_AddPartitionsToTxn
    /// </remarks>
    public class AddPartitionsToTxnRequest : TransactionRequest, IRequest<AddPartitionsToTxnResponse>, IEquatable<AddPartitionsToTxnRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},transactional_id:{TransactionId},producer_id:{ProducerId},producer_epoch:{ProducerEpoch},topics:[{Topics.ToStrings()}]}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(TransactionId)
                  .Write(ProducerId)
                  .Write(ProducerEpoch)
                  .WriteGroupedTopics(Topics);
        }

        public AddPartitionsToTxnResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => AddPartitionsToTxnResponse.FromBytes(context, bytes);

        public AddPartitionsToTxnRequest(string transactionId, long producerId, short producerEpoch, IEnumerable<TopicPartition> topics = null) 
            : base(ApiKey.AddPartitionsToTxn, transactionId, producerId, producerEpoch)
        {
            Topics = ImmutableList<TopicPartition>.Empty.AddNotNullRange(topics);
        }

        /// <summary>
        /// The partitions to add to the transaction.
        /// </summary>
        public IImmutableList<TopicPartition> Topics { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as AddPartitionsToTxnRequest);
        }

        /// <inheritdoc />
        public bool Equals(AddPartitionsToTxnRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals((TransactionRequest)other)
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ Topics.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

    }
}