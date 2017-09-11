using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// AddPartitionsToTxn Response => throttle_time_ms [errors] 
    /// </summary>
    /// <remarks>
    /// AddPartitionsToTxn Response => throttle_time_ms [errors] 
    ///   throttle_time_ms => INT32
    ///   errors => topic [partition_errors] 
    ///     topic => STRING
    ///     partition_errors => partition error_code 
    ///       partition => INT32
    ///       error_code => INT16
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_AddPartitionsToTxn
    /// </remarks>
    public class AddPartitionsToTxnResponse : ThrottledResponse, IResponse, IEquatable<AddPartitionsToTxnResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},errors:[{Topics.ToStrings()}]}}";

        public static AddPartitionsToTxnResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();
                var topicCount = reader.ReadInt32();
                context.ThrowIfCountTooBig(topicCount);
                var topics = new List<TopicResponse>();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    context.ThrowIfCountTooBig(partitionCount);
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();

                        topics.Add(new TopicResponse(topicName, partitionId, errorCode));
                    }
                }

                return new AddPartitionsToTxnResponse(throttleTime.Value, topics);
            }
        }

        public AddPartitionsToTxnResponse(TimeSpan throttleTime, IEnumerable<TopicResponse> topics = null)
            : base(throttleTime)
        {
            Topics = topics.ToSafeImmutableList();
            Errors = Topics.Select(t => t.Error).ToImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<TopicResponse> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as AddPartitionsToTxnResponse);
        }

        /// <inheritdoc />
        public bool Equals(AddPartitionsToTxnResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
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