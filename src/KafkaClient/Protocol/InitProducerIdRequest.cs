using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// InitProducerId Request => transactional_id transaction_timeout_ms 
    /// </summary>
    /// <remarks>
    /// InitProducerId Request => transactional_id transaction_timeout_ms 
    ///   transactional_id => NULLABLE_STRING
    ///   transaction_timeout_ms => INT32
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_InitProducerId
    /// </remarks>
    public class InitProducerIdRequest : Request, IRequest<InitProducerIdResponse>, IEquatable<InitProducerIdRequest>
    {
        public override string ToString() => $"{{{this.RequestToString()},transactional_id:{TransactionId},transaction_timeout_ms:{TransactionTimeout.TotalMilliseconds:##########} }}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(TransactionId);
            writer.WriteMilliseconds(TransactionTimeout);
        }

        public InitProducerIdResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => InitProducerIdResponse.FromBytes(context, bytes);

        public InitProducerIdRequest(string transactionId, TimeSpan? transactionTimeout = null) 
            : base(ApiKey.InitProducerId)
        {
            TransactionTimeout = transactionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(30));
            TransactionId = transactionId;
        }

        /// <summary>
        /// The transactional id whose producer id we want to retrieve or generate.
        /// </summary>
        public string TransactionId { get; }

        /// <summary>
        /// The time in ms to wait for before aborting idle transactions sent by this producer.
        /// </summary>
        public TimeSpan TransactionTimeout { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as InitProducerIdRequest);
        }

        /// <inheritdoc />
        public bool Equals(InitProducerIdRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return TransactionTimeout.Equals(other.TransactionTimeout) 
                && string.Equals(TransactionId, other.TransactionId);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = TransactionTimeout.GetHashCode();
                hashCode = (hashCode * 397) ^ (TransactionId?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

    }
}