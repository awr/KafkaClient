using System;

namespace KafkaClient.Protocol
{
    public class TopicOffset : TopicPartition, IEquatable<TopicOffset>
    {
        public override string ToString() => $"{{topic:{TopicName},partition_id:{PartitionId},offset:{Offset}}}";

        public TopicOffset(string topic, int partitionId, long offset = -1) 
            : base(topic, partitionId)
        {
            Offset = offset;
        }

        /// <summary>
        /// The offset found on the server.
        /// </summary>
        public long Offset { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as TopicOffset);
        }

        public bool Equals(TopicOffset other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Offset == other.Offset;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Offset.GetHashCode();
                return hashCode;
            }
        }
        
        #endregion
    }
}