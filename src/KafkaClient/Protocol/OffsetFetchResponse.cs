using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetFetch Response => [responses]
    ///  response => topic [partition_responses] 
    ///   topic => STRING              -- The name of the topic.
    ///   partition_response => partition_id offset metadata error_code
    ///    partition_id => INT32       -- The id of the partition.
    ///    offset => INT64             -- The offset, or -1 if none exists.
    ///    metadata => NULLABLE_STRING -- The metadata associated with the topic and partition.
    ///    error_code => INT16         -- The error code for the partition, if any.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class OffsetFetchResponse : IResponse<OffsetFetchResponse.Topic>, IEquatable<OffsetFetchResponse>
    {
        public override string ToString() => $"{{responses:[{responses.ToStrings()}]}}";

        public static OffsetFetchResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var topics = new List<Topic>();
                var topicCount = reader.ReadInt32();
                for (var t = 0; t < topicCount; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var offset = reader.ReadInt64();
                        var metadata = reader.ReadString();
                        var errorCode = (ErrorCode) reader.ReadInt16();

                        topics.Add(new Topic(topicName, partitionId, errorCode, offset, metadata));
                    }
                }

                return new OffsetFetchResponse(topics);
            }            
        }

        public OffsetFetchResponse(IEnumerable<Topic> topics = null)
        {
            responses = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(responses.Select(t => t.error_code));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> responses { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetFetchResponse);
        }

        /// <inheritdoc />
        public bool Equals(OffsetFetchResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return responses.HasEqualElementsInOrder(other.responses);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return responses?.Count.GetHashCode() ?? 0;
        }

        #endregion

        public class Topic : TopicOffset, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{topic},partition_id:{partition_id},offset:{offset},metadata:{metadata},error_code:{error_code}}}";

            public Topic(string topic, int partitionId, ErrorCode errorCode, long offset, string metadata) 
                : base(topic, partitionId, offset)
            {
                error_code = errorCode;
                this.metadata = metadata;
            }

            /// <summary>
            /// Error response code.
            /// </summary>
            public ErrorCode error_code { get; }

            /// <summary>
            /// Any arbitrary metadata stored during a CommitRequest.
            /// </summary>
            public string metadata { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                       && error_code == other.error_code 
                       && string.Equals(metadata, other.metadata);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ error_code.GetHashCode();
                    hashCode = (hashCode*397) ^ (metadata?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }        

            #endregion
        }
    }
}