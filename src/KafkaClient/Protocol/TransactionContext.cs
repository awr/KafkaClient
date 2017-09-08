namespace KafkaClient.Protocol
{
    public class TransactionContext : ITransactionContext
    {
        public TransactionContext(long? producerId = null, short? producerEpoch = null, int? firstSequence = null)
        {
            ProducerId = producerId;
            ProducerEpoch = producerEpoch;
            FirstSequence = firstSequence;
        }

        /// <summary>
        ///  The broker assigned producerId received by the 'InitProducerId' request. Clients which want to support idempotent message delivery and transactions must set this field.
        /// </summary>
        public long? ProducerId { get; }

        /// <summary>
        /// The broker assigned producerEpoch received by the 'InitProducerId' request. Clients which want to support idempotent message delivery and transactions must set this field.
        /// </summary>
        public short? ProducerEpoch { get; }

        /// <summary>
        /// The producer assigned sequence number which is used by the broker to deduplicate messages. Clients which want to support idempotent message delivery and transactions must set this field. 
        /// The sequence number for each Record in the RecordBatch is its OffsetDelta + FirstSequence.
        /// </summary>
        public int? FirstSequence { get; }
    }
}