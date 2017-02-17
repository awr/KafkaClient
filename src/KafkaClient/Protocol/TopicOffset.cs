using System;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    public class TopicOffset : TopicPartition, IEquatable<TopicOffset>
    {
        public override string ToString() => $"{{topic:{topic},partition_id:{partition_id},offset:{offset}}}";

        public TopicOffset(string topic, int partitionId, long offset = -1) 
            : base(topic, partitionId)
        {
            this.offset = offset;
        }

        /// <summary>
        /// The offset found on the server.
        /// </summary>
        public long offset { get; }

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
                && offset == other.offset;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ offset.GetHashCode();
                return hashCode;
            }
        }
        
        #endregion
    }
}