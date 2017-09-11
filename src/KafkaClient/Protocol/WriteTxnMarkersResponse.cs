using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// WriteTxnMarkers Response => [transaction_markers] 
    /// </summary>
    /// <remarks>
    /// WriteTxnMarkers Response => [transaction_markers] 
    ///   transaction_markers => producer_id [topics] 
    ///     producer_id => INT64
    ///     topics => topic [partitions] 
    ///       topic => STRING
    ///       partitions => partition error_code 
    ///         partition => INT32
    ///         error_code => INT16
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_WriteTxnMarkers
    /// </remarks>
    public class WriteTxnMarkersResponse : IResponse, IEquatable<WriteTxnMarkersResponse>
    {
        public override string ToString() => $"{{transaction_markers:[{TransactionMarkers.ToStrings()}]}}";

        public static WriteTxnMarkersResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var markerCount = reader.ReadInt32();
                context.ThrowIfCountTooBig(markerCount);
                var markers = new TransactionMarker[markerCount];
                for (var m = 0; m < markerCount; m++) {
                    var producerId = reader.ReadInt64();
                    var topicCount = reader.ReadInt32();
                    context.ThrowIfCountTooBig(topicCount);
                    var topics = new List<TopicResponse>();
                    for (var t = 0; t < topicCount; t++) {
                        var topicName = reader.ReadString();

                        var partitionCount = reader.ReadInt32();
                        context.ThrowIfCountTooBig(partitionCount);
                        for (var j = 0; j < partitionCount; j++) {
                            var partitionId = reader.ReadInt32();
                            var errorCode = (ErrorCode) reader.ReadInt16();

                            topics.Add(new TopicResponse(topicName, partitionId, errorCode));
                        }
                    }
                    markers[m] = new TransactionMarker(producerId, topics);
                }

                return new WriteTxnMarkersResponse(markers);
            }
        }

        public WriteTxnMarkersResponse(IEnumerable<TransactionMarker> transactionMarkers = null)
        {
            TransactionMarkers = transactionMarkers.ToSafeImmutableList();
            Errors = TransactionMarkers.SelectMany(t => t.Topics.Select(p => p.Error)).ToImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }


        /// <summary>
        /// The transaction markers to be written.
        /// </summary>
        public IImmutableList<TransactionMarker> TransactionMarkers { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as WriteTxnMarkersResponse);
        }

        /// <inheritdoc />
        public bool Equals(WriteTxnMarkersResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return TransactionMarkers.HasEqualElementsInOrder(other.TransactionMarkers);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = TransactionMarkers.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class TransactionMarker : IEquatable<TransactionMarker>
        {
            public override string ToString() => $"{{producer_id:{ProducerId},topics:[{Topics.ToStrings()}]}}";

            public TransactionMarker(long producerId, IEnumerable<TopicResponse> topics)
            {
                ProducerId = producerId;
                Topics = topics.ToSafeImmutableList();
            }

            /// <summary>
            /// Current producer id in use by the transactional id.
            /// </summary>
            public long ProducerId { get; }

            /// <summary>
            /// The partitions to write markers for.
            /// </summary>
            public IImmutableList<TopicResponse> Topics { get; }

            public bool Equals(TransactionMarker other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return ProducerId == other.ProducerId 
                    && Topics.HasEqualElementsInOrder(other.Topics);
            }

            public override bool Equals(object obj)
            {
                return Equals(obj as TransactionMarker);
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = ProducerId.GetHashCode();
                    hashCode = (hashCode * 397) ^ (Topics?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }
        }
    }
}