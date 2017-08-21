using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    /// <summary>
    ///  The format of the MemberAssignment field for consumer groups is included below:
    /// MemberAssignment => Version PartitionAssignment
    ///   Version => int16
    ///   PartitionAssignment => [Topic [Partition]]
    ///     Topic => string
    ///     Partition => int32
    ///   UserData => bytes
    /// All client implementations using the "consumer" protocol type should support this schema.
    /// 
    /// from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public class ConsumerMemberAssignment : IMemberAssignment, IEquatable<ConsumerMemberAssignment>
    {
        public override string ToString() => $"{{Version:{Version},Assignments:[{PartitionAssignments.ToStrings()}],UserData:{UserData.Count}}}";

        private static readonly ArraySegment<byte> EmptySegment = new ArraySegment<byte>(new byte[0]);

        public ConsumerMemberAssignment(IEnumerable<TopicPartition> partitionAssignments = null, short version = 0)
            : this(partitionAssignments, EmptySegment, version)
        {
        }

        public ConsumerMemberAssignment(IEnumerable<TopicPartition> partitionAssignments, ArraySegment<byte> userData, short version = 0)
        {
            Version = version;
            PartitionAssignments = partitionAssignments.ToSafeImmutableList();
            UserData = userData;
        }

        public short Version { get; }
        public IImmutableList<TopicPartition> PartitionAssignments { get; }

        public ArraySegment<byte> UserData { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ConsumerMemberAssignment);
        }

        /// <inheritdoc />
        public bool Equals(ConsumerMemberAssignment other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Version == other.Version 
                && PartitionAssignments.HasEqualElementsInOrder(other.PartitionAssignments)
                && UserData.HasEqualElementsInOrder(other.UserData);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Version.GetHashCode();
                hashCode = (hashCode*397) ^ (PartitionAssignments?.Count.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ UserData.Count.GetHashCode();
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ConsumerMemberAssignment left, ConsumerMemberAssignment right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ConsumerMemberAssignment left, ConsumerMemberAssignment right)
        {
            return !Equals(left, right);
        }
        
        #endregion
    }
}