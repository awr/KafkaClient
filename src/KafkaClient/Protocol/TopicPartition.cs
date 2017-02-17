using System;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    public class TopicPartition : IEquatable<TopicPartition>
    {
        public TopicPartition(string topicName, int partitionId)
        {
            topic = topicName;
            partition_id = partitionId;
        }

        /// <summary>
        /// The topic name.
        /// </summary>
        public string topic { get; }

        /// <summary>
        /// The partition id.
        /// </summary>
        public int partition_id { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as TopicPartition);
        }

        public bool Equals(TopicPartition other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(topic, other.topic) 
                   && partition_id == other.partition_id;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = topic?.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ partition_id;
                return hashCode;
            }
        }

        #endregion

        public override string ToString() => $"{{topic:{topic},partition_id:{partition_id}}}";
    }
}