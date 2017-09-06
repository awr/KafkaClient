using System;

namespace KafkaClient.Protocol
{
    public abstract class TransactionRequest : Request, IEquatable<TransactionRequest>
    {
        public override string ToString() => $"{{{this.RequestToString()},transactional_id:{TransactionId},producer_id:{ProducerId},producer_epoch:{ProducerEpoch}}}";
 
        protected TransactionRequest(ApiKey apiKey, string transactionId, long producerId, short producerEpoch, bool expectResponse = true) 
            : base(apiKey, expectResponse)
        {
            TransactionId = transactionId;
            ProducerId = producerId;
            ProducerEpoch = producerEpoch;
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

        public bool Equals(TransactionRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(TransactionId, other.TransactionId) 
                && ProducerId == other.ProducerId 
                && ProducerEpoch == other.ProducerEpoch;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as TransactionRequest);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = TransactionId?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ ProducerId.GetHashCode();
                hashCode = (hashCode * 397) ^ ProducerEpoch.GetHashCode();
                return hashCode;
            }
        }
    }
}