using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DeleteRecords Response => throttle_time_ms [topics] 
    /// </summary>
    /// <remarks>
    /// DeleteRecords Response => throttle_time_ms [topics] 
    ///   throttle_time_ms => INT32
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => partition low_watermark error_code 
    ///       partition => INT32
    ///       low_watermark => INT64
    ///       error_code => INT16
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_DeleteRecords
    /// </remarks>
    public class DeleteRecordsResponse : ThrottledResponse, IResponse, IEquatable<DeleteRecordsResponse>
    {
        public override string ToString() => $"{{topics:[{Topics.ToStrings()}],throttle_time_ms:{ThrottleTime}}}";

        public static DeleteRecordsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();

                var topics = new List<Topic>();
                var topicCount = reader.ReadInt32();
                for (var i = 0; i < topicCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var lowWatermark = reader.ReadInt64();
                        var errorCode = (ErrorCode) reader.ReadInt16();

                        topics.Add(new Topic(topicName, partitionId, lowWatermark, errorCode));
                    }
                }

                return new DeleteRecordsResponse(topics, throttleTime);
            }
        }

        public DeleteRecordsResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Topics.Select(t => t.Error));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Topic> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DeleteRecordsResponse);
        }

        /// <inheritdoc />
        public bool Equals(DeleteRecordsResponse other)
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
                hashCode = (hashCode*397) ^ (Topics?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},low_watermark:{LowWatermark},error_code:{Error}}}";

            public Topic(string topic, int partitionId, long lowWatermark, ErrorCode errorCode)
                : base(topic, partitionId, errorCode)
            {
                LowWatermark = lowWatermark;
            }

            /// <summary>
            /// Smallest available offset of all live replicas.
            /// </summary>
            public long LowWatermark { get; }

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
                    && LowWatermark == other.LowWatermark;
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ LowWatermark.GetHashCode();
                    return hashCode;
                }
            }

            #endregion
        }
    }
}