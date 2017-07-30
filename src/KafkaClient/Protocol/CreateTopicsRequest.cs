using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// CreateTopics Request => [create_topic_requests] timeout 
    ///  create_topic_requests => topic num_partitions replication_factor [replica_assignment] [configs] 
    ///    topic => STRING             -- Name for newly created topic.
    ///    num_partitions => INT32     -- Number of partitions to be created. -1 indicates unset.
    ///    replication_factor => INT16 -- Replication factor for the topic. -1 indicates unset.
    ///    replica_assignment => partition_id [replicas] 
    ///      partition_id => INT32
    ///      replica => INT32          -- The set of all nodes that should host this partition. The first replica in the list is the preferred leader.
    ///    configs => config_key config_value 
    ///      config_key => STRING      -- Configuration key name
    ///      config_value => STRING    -- Configuration value
    ///  timeout => INT32              -- The time in ms to wait for a topic to be completely created on the controller node. Values &lt;= 0 will trigger topic creation and return immediately
    /// </summary>
    public class CreateTopicsRequest : Request, IRequest<CreateTopicsResponse>, IEquatable<CreateTopicsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},create_topic_requests:[{create_topic_requests.ToStrings()}],timeout:{timeout}}}";

        public override string ShortString() => create_topic_requests.Count == 1 ? $"{ApiKey} {create_topic_requests[0].topic}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(create_topic_requests.Count);
            foreach (var topic in create_topic_requests) {
                writer.Write(topic.topic)
                        .Write(topic.num_partitions)
                        .Write(topic.replication_factor)
                        .Write(topic.replica_assignments.Count);
                foreach (var assignment in topic.replica_assignments) {
                    writer.Write(assignment.partition_id)
                          .Write(assignment.replicas);
                }
                writer.Write(topic.configs.Count);
                foreach (var config in topic.configs) {
                    writer.Write(config.Key)
                          .Write(config.Value);
                }
            }
            writer.Write((int)timeout.TotalMilliseconds);
        }

        public CreateTopicsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => CreateTopicsResponse.FromBytes(context, bytes);

        public CreateTopicsRequest(params Topic[] topics)
            : this(topics, null)
        {
        }

        public CreateTopicsRequest(IEnumerable<Topic> topics = null, TimeSpan? timeout = null)
            : base(ApiKey.CreateTopics)
        {
            create_topic_requests = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            this.timeout = timeout ?? TimeSpan.Zero;
        }

        public IImmutableList<Topic> create_topic_requests { get; }

        /// <summary>
        /// The time in ms to wait for a topic to be completely created on the controller node. Values &lt;= 0 will trigger 
        /// topic creation and return immediately
        /// </summary>
        public TimeSpan timeout { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as CreateTopicsRequest);
        }

        public bool Equals(CreateTopicsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return create_topic_requests.HasEqualElementsInOrder(other.create_topic_requests)
                && timeout.Equals(other.timeout);
        }

        public override int GetHashCode()
        {
            unchecked {
                return ((create_topic_requests?.Count.GetHashCode() ?? 0) * 397) ^ timeout.GetHashCode();
            }
        }

        #endregion

        public class Topic : IEquatable<Topic>
        {
            public override string ToString() => $"{{topic:{topic},num_partitions:{num_partitions},replication_factor:{replication_factor},replica_assignments:[{replica_assignments.ToStrings()}],configs:{{{string.Join(",",configs.Select(pair => $"{pair.Key}:{pair.Value}"))}}}}}";

            public Topic(string topic, int numberOfPartitions, short replicationFactor, IEnumerable<KeyValuePair<string, string>> configs = null)
                : this (topic, configs)
            {
                num_partitions = numberOfPartitions;
                replication_factor = replicationFactor;
                replica_assignments = ImmutableList<ReplicaAssignment>.Empty;
            }

            public Topic(string topic, IEnumerable<ReplicaAssignment> replicaAssignments, IEnumerable<KeyValuePair<string, string>> configs = null)
                : this (topic, configs)
            {
                num_partitions = -1;
                replication_factor = -1;
                replica_assignments = ImmutableList<ReplicaAssignment>.Empty.AddNotNullRange(replicaAssignments);
            }

            private Topic(string topic, IEnumerable<KeyValuePair<string, string>> configs)
            {
                this.topic = topic;
                this.configs = ImmutableDictionary<string, string>.Empty.AddNotNullRange(configs);
            }

            /// <summary>
            /// Name for newly created topic.
            /// </summary>
            public string topic { get; }

            /// <summary>
            /// Number of partitions to be created. -1 indicates unset.
            /// </summary>
            public int num_partitions { get; }

            /// <summary>
            /// Replication factor for the topic. -1 indicates unset.
            /// </summary>
            public short replication_factor { get; }

            /// <summary>
            /// Replica assignment among kafka brokers for this topic partitions. 
            /// If this is set num_partitions and replication_factor must be unset.
            /// </summary>
            public IImmutableList<ReplicaAssignment> replica_assignments { get; }

            /// <summary>
            /// Topic level configuration for topic to be set.
            /// </summary>
            public IImmutableDictionary<string, string> configs { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return String.Equals(topic, other.topic) 
                    && num_partitions == other.num_partitions 
                    && replication_factor == other.replication_factor 
                    && replica_assignments.HasEqualElementsInOrder(other.replica_assignments) 
                    && configs.HasEqualElementsInOrder(other.configs);
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = topic?.GetHashCode() ?? 0;
                    hashCode = (hashCode * 397) ^ num_partitions;
                    hashCode = (hashCode * 397) ^ replication_factor.GetHashCode();
                    hashCode = (hashCode * 397) ^ (replica_assignments?.Count.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ (configs?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }

        public class ReplicaAssignment : IEquatable<ReplicaAssignment>
        {
            public override string ToString() => $"{{partition_id:{partition_id},replicas:[{replicas.ToStrings()}]}}";

            public ReplicaAssignment(int partitionId, IEnumerable<int> replicas = null)
            {
                partition_id = partitionId;
                this.replicas = ImmutableList<int>.Empty.AddNotNullRange(replicas);
            }

            public int partition_id { get; }

            /// <summary>
            /// The set of all nodes that should host this partition. 
            /// The first replica in the list is the preferred leader.
            /// </summary>
            public IImmutableList<int> replicas { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as ReplicaAssignment);
            }

            public bool Equals(ReplicaAssignment other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return partition_id == other.partition_id 
                    && replicas.HasEqualElementsInOrder(other.replicas);
            }

            public override int GetHashCode()
            {
                unchecked {
                    return (partition_id * 397) ^ (replicas?.Count.GetHashCode() ?? 0);
                }
            }

            #endregion
        }
    }
}