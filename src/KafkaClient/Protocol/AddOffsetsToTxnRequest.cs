using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// AddOffsetsToTxn Request => transactional_id producer_id producer_epoch consumer_group_id 
    /// </summary>
    /// <remarks>
    /// AddOffsetsToTxn Request => transactional_id producer_id producer_epoch consumer_group_id 
    ///   transactional_id => STRING
    ///   producer_id => INT64
    ///   producer_epoch => INT16
    ///   consumer_group_id => STRING
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_AddOffsetsToTxn
    /// </remarks>
    public class AddOffsetsToTxnRequest : Request, IRequest<AddOffsetsToTxnResponse>, IEquatable<AddOffsetsToTxnRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},transactional_id:{TransactionId},producer_id:{ProducerId},producer_epoch:{ProducerEpoch},consumer_group_id:{GroupId}}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(TransactionId)
                  .Write(ProducerId)
                  .Write(ProducerEpoch)
                  .Write(GroupId);
        }

        public AddOffsetsToTxnResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => AddOffsetsToTxnResponse.FromBytes(context, bytes);

        public AddOffsetsToTxnRequest(string transactionId, long producerId, short producerEpoch, string consumerGroupId = null) 
            : base(ApiKey.AddOffsetsToTxn)
        {
            TransactionId = transactionId;
            ProducerId = producerId;
            ProducerEpoch = producerEpoch;
            GroupId = consumerGroupId;
        }

        /// <summary>
        /// The transactional id corresponding to the transaction.
        /// </summary>
        public string TransactionId { get; }

        /// <summary>
        /// Current producer id in use by the transactional id.
        /// </summary>
        public long ProducerId { get; }

        /// <summary>
        /// Current epoch associated with the producer id.
        /// </summary>
        public short ProducerEpoch { get; }

        /// <summary>
        /// Consumer group id whose offsets should be included in the transaction.
        /// </summary>
        public string GroupId { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as AddOffsetsToTxnRequest);
        }

        /// <inheritdoc />
        public bool Equals(AddOffsetsToTxnRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return TransactionId == other.TransactionId
                && ProducerId == other.ProducerId
                && ProducerEpoch == other.ProducerEpoch
                && GroupId == other.GroupId;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = TransactionId?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ ProducerId.GetHashCode(); 
                hashCode = (hashCode * 397) ^ ProducerEpoch.GetHashCode();
                hashCode = (hashCode * 397) ^ (GroupId?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

    }
}