using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// WriteTxnMarkers Request => [transaction_markers] 
    /// </summary>
    /// <remarks>
    /// WriteTxnMarkers Request => [transaction_markers] 
    ///   transaction_markers => producer_id producer_epoch transaction_result [topics] coordinator_epoch 
    ///     producer_id => INT64
    ///     producer_epoch => INT16
    ///     transaction_result => BOOLEAN
    ///     topics => topic [partitions] 
    ///       topic => STRING
    ///       partitions => INT32
    ///     coordinator_epoch => INT32
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_WriteTxnMarkers
    /// </remarks>
    public class WriteTxnMarkersRequest : Request, IRequest<WriteTxnMarkersResponse>, IEquatable<WriteTxnMarkersRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},transaction_markers:[{TransactionMarkers.ToStrings()}]}}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(TransactionMarkers.Count);
            foreach (var marker in TransactionMarkers) {
                writer.Write(marker.ProducerId)
                      .Write(marker.ProducerEpoch)
                      .Write(marker.TransactionResult)
                      .WriteGroupedTopics(marker.Topics)
                      .Write(marker.CoordinatorEpoch);
            }
        }

        public WriteTxnMarkersResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => WriteTxnMarkersResponse.FromBytes(context, bytes);

        public WriteTxnMarkersRequest(IEnumerable<TransactionMarker> transactionMarkers = null) 
            : base(ApiKey.WriteTxnMarkers)
        {
            TransactionMarkers = ImmutableList<TransactionMarker>.Empty.AddNotNullRange(transactionMarkers);
        }

        /// <summary>
        /// The transaction markers to be written.
        /// </summary>
        public IImmutableList<TransactionMarker> TransactionMarkers { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as WriteTxnMarkersRequest);
        }

        /// <inheritdoc />
        public bool Equals(WriteTxnMarkersRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return TransactionMarkers.HasEqualElementsInOrder(other.TransactionMarkers);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ TransactionMarkers.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class TransactionMarker : IEquatable<TransactionMarker>
        {
            public override string ToString() => $"{{producer_id:{ProducerId},producer_epoch:{ProducerEpoch},transaction_result:{TransactionResult},topics:[{Topics.ToStrings()}],coordinator_epoch:{CoordinatorEpoch}}}";

            public TransactionMarker(long producerId, short producerEpoch, bool transactionResult, IEnumerable<TopicPartition> topics, int coordinatorEpoch)
            {
                ProducerId = producerId;
                ProducerEpoch = producerEpoch;
                TransactionResult = transactionResult;
                Topics = ImmutableList<TopicPartition>.Empty.AddRange(topics);
                CoordinatorEpoch = coordinatorEpoch;
            }

            /// <summary>
            /// Current producer id in use by the transactional id.
            /// </summary>
            public long ProducerId { get; }

            /// <summary>
            /// Current epoch associated with the producer id.
            /// </summary>
            public short ProducerEpoch { get; }

            /// <summary>
            /// The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
            /// </summary>
            public bool TransactionResult { get; }

            /// <summary>
            /// The partitions to write markers for.
            /// </summary>
            public IImmutableList<TopicPartition> Topics { get; }

            /// <summary>
            /// Epoch associated with the transaction state partition hosted by this transaction coordinator.
            /// </summary>
            public int CoordinatorEpoch { get; }

            public bool Equals(TransactionMarker other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return ProducerId == other.ProducerId 
                    && ProducerEpoch == other.ProducerEpoch 
                    && TransactionResult == other.TransactionResult 
                    && Topics.HasEqualElementsInOrder(other.Topics) 
                    && CoordinatorEpoch == other.CoordinatorEpoch;
            }

            public override bool Equals(object obj)
            {
                return Equals(obj as TransactionMarker);
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = ProducerId.GetHashCode();
                    hashCode = (hashCode * 397) ^ ProducerEpoch.GetHashCode();
                    hashCode = (hashCode * 397) ^ TransactionResult.GetHashCode();
                    hashCode = (hashCode * 397) ^ (Topics?.Count.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ CoordinatorEpoch;
                    return hashCode;
                }
            }
        }
    }
}