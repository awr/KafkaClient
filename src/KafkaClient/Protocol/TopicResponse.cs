using System;

namespace KafkaClient.Protocol
{
    public class TopicResponse : TopicPartition, IEquatable<TopicResponse>
    {
        public TopicResponse(string topicName, int partitionId, ErrorCode errorCode)
            : base(topicName, partitionId)
        {
            Error = errorCode;
        }

        /// <summary>
        /// Error response code.
        /// </summary>
        public ErrorCode Error { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as TopicResponse);
        }

        public bool Equals(TopicResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other)
                && Error == other.Error;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (int) Error;
                return hashCode;
            }
        }

        #endregion

        public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},error_code:{Error}}}";
    }
}