using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Assignment
{
    /// <summary>
    /// ProtocolType => "consumer"
    ///  
    /// ProtocolName => AssignmentStrategy
    ///   AssignmentStrategy => string
    ///  
    /// ProtocolMetadata => Version Subscription UserData
    ///   Version => int16
    ///   Subscription => [Topic]
    ///     Topic => string
    ///   UserData => bytes    
    /// see http://kafka.apache.org/protocol.html#protocol_messages for details
    /// 
    /// The UserData field can be used by custom partition assignment strategies. For example, in a sticky partitioning implementation, this 
    /// field can contain the assignment from the previous generation. In a resource-based assignment strategy, it could include the number of 
    /// cpus on the machine hosting each consumer instance.
    /// 
    /// Kafka Connect uses the "connect" protocol type and its protocol details are internal to the Connect implementation.
    /// </summary>
    public class ConsumerProtocolMetadata : IMemberMetadata, IEquatable<ConsumerProtocolMetadata>
    {
        public override string ToString() => $"{{Version:{Version},Subscriptions:[{Subscriptions.ToStrings()}],UserData:{UserData.Count}}}";

        private static readonly ArraySegment<byte> EmptySegment = new ArraySegment<byte>(new byte[0]);

        public ConsumerProtocolMetadata(string topicName, string assignmentStrategy = SimpleAssignor.Strategy, short version = 0)
            : this(new []{ topicName }, assignmentStrategy, EmptySegment, version)
        {
        }

        public ConsumerProtocolMetadata(IEnumerable<string> topicNames, string assignmentStrategy = SimpleAssignor.Strategy, short version = 0)
            : this(topicNames, assignmentStrategy, EmptySegment, version)
        {
        }

        public ConsumerProtocolMetadata(IEnumerable<string> topicNames, string assignmentStrategy, ArraySegment<byte> userData, short version = 0)
        {
            AssignmentStrategy = assignmentStrategy;
            Version = version;
            Subscriptions = topicNames.ToSafeImmutableList();
            UserData = userData;
        }

        public short Version { get; }

        /// <summary>
        /// The topics subscribed to by this consumer group.
        /// </summary>
        public IImmutableList<string> Subscriptions { get; }

        public ArraySegment<byte> UserData { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ConsumerProtocolMetadata);
        }

        /// <inheritdoc />
        public bool Equals(ConsumerProtocolMetadata other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Version == other.Version 
                && Subscriptions.HasEqualElementsInOrder(other.Subscriptions)
                && UserData.HasEqualElementsInOrder(other.UserData);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Version.GetHashCode();
                hashCode = (hashCode*397) ^ (Subscriptions?.Count.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ UserData.Count.GetHashCode();
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ConsumerProtocolMetadata left, ConsumerProtocolMetadata right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ConsumerProtocolMetadata left, ConsumerProtocolMetadata right)
        {
            return !Equals(left, right);
        }

        public string AssignmentStrategy { get; }
    }
}