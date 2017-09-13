using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetCommit Response => *throttle_time_ms [responses] 
    /// </summary>
    /// <remarks>
    /// OffsetCommit Response => *throttle_time_ms [responses] 
    ///   throttle_time_ms => INT32
    ///   responses => topic [partition_responses] 
    ///     topic => STRING
    ///     partition_responses => partition error_code 
    ///       partition => INT32
    ///       error_code => INT16
    /// 
    /// Version 3+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_OffsetCommit
    /// </remarks>
    public class OffsetCommitResponse : ThrottledResponse, IResponse<TopicResponse>, IEquatable<OffsetCommitResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},responses:[{Responses.ToStrings()}]}}";

        public static OffsetCommitResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 3);
                var topicCount = reader.ReadInt32();
                reader.AssertMaxArraySize(topicCount);
                var topics = new List<TopicResponse>();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    reader.AssertMaxArraySize(partitionCount);
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();

                        topics.Add(new TopicResponse(topicName, partitionId, errorCode));
                    }
                }

                return new OffsetCommitResponse(topics, throttleTime);
            }
        }

        public OffsetCommitResponse(IEnumerable<TopicResponse> topics = null, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Responses = topics.ToSafeImmutableList();
            Errors = Responses.Select(t => t.Error).ToImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<TopicResponse> Responses { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetCommitResponse);
        }

        /// <inheritdoc />
        public bool Equals(OffsetCommitResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Responses.HasEqualElementsInOrder(other.Responses);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Responses?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion
    }
}