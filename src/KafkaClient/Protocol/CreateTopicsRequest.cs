using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// CreateTopics Request => [create_topic_requests] timeout *validate_only 
    /// </summary>
    /// <remarks>
    /// CreateTopics Request => [create_topic_requests] timeout *validate_only 
    ///   create_topic_requests => topic num_partitions replication_factor [replica_assignment] [config_entries] 
    ///     topic => STRING
    ///     num_partitions => INT32
    ///     replication_factor => INT16
    ///     replica_assignment => partition_id [replicas] 
    ///       partition_id => INT32
    ///       replicas => INT32
    ///     config_entries => config_name config_value 
    ///       config_name => STRING
    ///       config_value => NULLABLE_STRING
    ///   timeout => INT32
    ///   validate_only => BOOLEAN
    ///
    /// Version 1+: validate_only
    /// From http://kafka.apache.org/protocol.html#The_Messages_CreateTopics
    /// </remarks>
    public class CreateTopicsRequest : Request, IRequest<CreateTopicsResponse>, IEquatable<CreateTopicsRequest>
    {
        public override string ToString() => $"{{{this.RequestToString()},create_topic_requests:[{Topics.ToStrings()}],timeout:{Timeout},validate_only:{ValidateOnly}}}";

        public override string ShortString() => Topics.Count == 1 ? $"{ApiKey} {Topics[0].TopicName}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(Topics.Count);
            foreach (var topic in Topics) {
                writer.Write(topic.TopicName)
                        .Write(topic.NumPartitions)
                        .Write(topic.ReplicationFactor)
                        .Write(topic.ReplicaAssignments.Count);
                foreach (var assignment in topic.ReplicaAssignments) {
                    writer.Write(assignment.PartitionId)
                          .Write(assignment.Replicas);
                }
                writer.Write(topic.Configs.Count);
                foreach (var config in topic.Configs) {
                    writer.Write(config.ConfigName)
                          .Write(config.ConfigValue);
                }
            }
            writer.WriteMilliseconds(Timeout);
            if (context.ApiVersion >= 1) {
                writer.Write(ValidateOnly.GetValueOrDefault());
            }
        }

        public CreateTopicsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => CreateTopicsResponse.FromBytes(context, bytes);

        public CreateTopicsRequest(IEnumerable<Topic> topics = null, TimeSpan? timeout = null, bool? validateOnly = null)
            : base(ApiKey.CreateTopics)
        {
            Topics = topics.ToSafeImmutableList();
            Timeout = timeout ?? TimeSpan.Zero;
            ValidateOnly = validateOnly;
        }

        public IImmutableList<Topic> Topics { get; }

        /// <summary>
        /// The time in ms to wait for a topic to be completely created on the controller node. Values &lt;= 0 will trigger 
        /// topic creation and return immediately
        /// </summary>
        public TimeSpan Timeout { get; }

        /// <summary>
        /// If this is true, the request will be validated, but the topic won't be created.
        /// Version: 1+
        /// </summary>
        public bool? ValidateOnly { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as CreateTopicsRequest);
        }

        public bool Equals(CreateTopicsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics)
                && Timeout.Equals(other.Timeout)
                && ValidateOnly == other.ValidateOnly;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Topics?.Count.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ Timeout.GetHashCode();
                hashCode = (hashCode*397) ^ (ValidateOnly?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Topic : IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{TopicName},num_partitions:{NumPartitions},replication_factor:{ReplicationFactor},replica_assignments:[{ReplicaAssignments.ToStrings()}],configs:[{Configs.ToStrings()}]}}";

            public Topic(string topicName, int numberOfPartitions, short replicationFactor, IEnumerable<ConfigEntry> configs = null)
                : this (topicName, configs)
            {
                NumPartitions = numberOfPartitions;
                ReplicationFactor = replicationFactor;
                ReplicaAssignments = ImmutableList<ReplicaAssignment>.Empty;
            }

            public Topic(string topicName, IEnumerable<ReplicaAssignment> replicaAssignments, IEnumerable<ConfigEntry> configs = null)
                : this (topicName, configs)
            {
                NumPartitions = -1;
                ReplicationFactor = -1;
                ReplicaAssignments = replicaAssignments.ToSafeImmutableList();
            }

            private Topic(string topicName, IEnumerable<ConfigEntry> configs)
            {
                TopicName = topicName;
                Configs = configs.ToSafeImmutableList();
            }

            /// <summary>
            /// Name for newly created topic.
            /// </summary>
            public string TopicName { get; }

            /// <summary>
            /// Number of partitions to be created. -1 indicates unset.
            /// </summary>
            public int NumPartitions { get; }

            /// <summary>
            /// Replication factor for the topic. -1 indicates unset.
            /// </summary>
            public short ReplicationFactor { get; }

            /// <summary>
            /// Replica assignment among kafka brokers for this topic partitions. 
            /// If this is set num_partitions and replication_factor must be unset.
            /// </summary>
            public IImmutableList<ReplicaAssignment> ReplicaAssignments { get; }

            /// <summary>
            /// Topic level configuration for topic to be set.
            /// </summary>
            public IImmutableList<ConfigEntry> Configs { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(TopicName, other.TopicName) 
                    && NumPartitions == other.NumPartitions 
                    && ReplicationFactor == other.ReplicationFactor 
                    && ReplicaAssignments.HasEqualElementsInOrder(other.ReplicaAssignments) 
                    && Configs.HasEqualElementsInOrder(other.Configs);
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = TopicName?.GetHashCode() ?? 0;
                    hashCode = (hashCode * 397) ^ NumPartitions;
                    hashCode = (hashCode * 397) ^ ReplicationFactor.GetHashCode();
                    hashCode = (hashCode * 397) ^ (ReplicaAssignments?.Count.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ (Configs?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }

        public class ReplicaAssignment : IEquatable<ReplicaAssignment>
        {
            public override string ToString() => $"{{partition_id:{PartitionId},replicas:[{Replicas.ToStrings()}]}}";

            public ReplicaAssignment(int partitionId, IEnumerable<int> replicas = null)
            {
                PartitionId = partitionId;
                Replicas = replicas.ToSafeImmutableList();
            }

            public int PartitionId { get; }

            /// <summary>
            /// The set of all nodes that should host this partition. 
            /// The first replica in the list is the preferred leader.
            /// </summary>
            public IImmutableList<int> Replicas { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as ReplicaAssignment);
            }

            public bool Equals(ReplicaAssignment other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return PartitionId == other.PartitionId 
                    && Replicas.HasEqualElementsInOrder(other.Replicas);
            }

            public override int GetHashCode()
            {
                unchecked {
                    return (PartitionId * 397) ^ (Replicas?.Count.GetHashCode() ?? 0);
                }
            }

            #endregion
        }
    }
}