using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// TxnOffsetCommit Response => throttle_time_ms [topics] 
    /// </summary>
    /// <remarks>
    /// TxnOffsetCommit Response => throttle_time_ms [topics] 
    ///   throttle_time_ms => INT32
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => partition error_code 
    ///       partition => INT32
    ///       error_code => INT16
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_TxnOffsetCommit
    /// </remarks>
    public class TxnOffsetCommitResponse : ThrottledResponse, IResponse, IEquatable<TxnOffsetCommitResponse>
    {
        public override string ToString() => $"{{throttle_time_ms:{ThrottleTime},topics:[{Topics.ToStrings()}]}}";

        public static TxnOffsetCommitResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();
                var topics = new List<TopicResponse>();
                var topicCount = reader.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();
                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = reader.ReadErrorCode();
                        topics.Add(new TopicResponse(topicName, partitionId, errorCode));
                    }
                }
                return new TxnOffsetCommitResponse(throttleTime.Value, topics);
            }
        }

        public TxnOffsetCommitResponse(TimeSpan throttleTime, IEnumerable<TopicResponse> topics = null)
            : base(throttleTime)
        {
            Topics = topics.ToSafeImmutableList();
            Errors = Topics.Select(t => t.Error).ToImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <summary>
        /// Errors per partition from writing markers.
        /// </summary>
        public IImmutableList<TopicResponse> Topics { get; }

        #region Equality 

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as TxnOffsetCommitResponse);
        }

        /// <inheritdoc />
        public bool Equals(TxnOffsetCommitResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals((ThrottledResponse)other) 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Topics.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}