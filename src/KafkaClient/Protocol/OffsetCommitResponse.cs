using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetCommit Response => [responses]
    ///  responses => topic [partition_responses] 
    ///   topic => STRING       -- The topic name.
    ///   partition_response => partition_id error_code 
    ///   partition_id => INT32 -- The id of the partition.
    ///   error_code => INT16   -- The error code for the partition, if any.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class OffsetCommitResponse : IResponse<TopicResponse>, IEquatable<OffsetCommitResponse>
    {
        public override string ToString() => $"{{responses:[{Responses.ToStrings()}]}}";

        public static OffsetCommitResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var topics = new List<TopicResponse>();
                var topicCount = reader.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var errorCode = (ErrorCode) reader.ReadInt16();

                        topics.Add(new TopicResponse(topicName, partitionId, errorCode));
                    }
                }

                return new OffsetCommitResponse(topics);
            }
        }

        public OffsetCommitResponse(IEnumerable<TopicResponse> topics = null)
        {
            Responses = ImmutableList<TopicResponse>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Responses.Select(t => t.Error));
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
            return Responses.HasEqualElementsInOrder(other.Responses);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Responses?.Count.GetHashCode() ?? 0;
        }

        #endregion
    }
}