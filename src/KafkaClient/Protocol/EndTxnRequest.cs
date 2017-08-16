using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// EndTxn Request => transactional_id producer_id producer_epoch transaction_result 
    /// </summary>
    /// <remarks>
    /// EndTxn Request => transactional_id producer_id producer_epoch transaction_result 
    ///   transactional_id => STRING
    ///   producer_id => INT64
    ///   producer_epoch => INT16
    ///   transaction_result => BOOLEAN
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_EndTxn
    /// </remarks>
    public class EndTxnRequest : TransactionRequest, IRequest<EndTxnResponse>, IEquatable<EndTxnRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},transactional_id:{TransactionId},producer_id:{ProducerId},producer_epoch:{ProducerEpoch},transaction_result:{TransactionResult}}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(TransactionId)
                  .Write(ProducerId)
                  .Write(ProducerEpoch)
                  .Write(TransactionResult);
        }

        public EndTxnResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => EndTxnResponse.FromBytes(context, bytes);

        public EndTxnRequest(string transactionId, long producerId, short producerEpoch, bool transactionResult) 
            : base(ApiKey.EndTxn, transactionId, producerId, producerEpoch)
        {
            TransactionResult = transactionResult;
        }

        /// <summary>
        /// The result of the transaction (0 = ABORT, 1 = COMMIT)
        /// </summary>
        public bool TransactionResult { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as EndTxnRequest);
        }

        /// <inheritdoc />
        public bool Equals(EndTxnRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals((TransactionRequest)other)
                && TransactionResult == other.TransactionResult;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ TransactionResult.GetHashCode();
                return hashCode;
            }
        }

        #endregion

    }
}