using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Metadata Response => *throttle_time_ms [brokers] *cluster_id *controller_id [topic_metadata] 
    /// </summary>
    /// <remarks>
    /// Metadata Response => *throttle_time_ms [brokers] *cluster_id *controller_id [topic_metadata] 
    ///   throttle_time_ms => INT32
    ///   brokers => node_id host port *rack 
    ///     node_id => INT32
    ///     host => STRING
    ///     port => INT32
    ///     rack => NULLABLE_STRING
    ///   cluster_id => NULLABLE_STRING
    ///   controller_id => INT32
    ///   topic_metadata => topic_error_code topic *is_internal [partition_metadata] 
    ///     topic_error_code => INT16
    ///     topic => STRING
    ///     is_internal => BOOLEAN
    ///     partition_metadata => partition_error_code partition_id leader [replicas] [isr] 
    ///       partition_error_code => INT16
    ///       partition_id => INT32
    ///       leader => INT32
    ///       replicas => INT32
    ///       isr => INT32
    ///
    /// Version 1+: controller_id
    /// Version 1+: is_internal
    /// Version 1+: rack
    /// Version 2+: cluster_id
    /// Version 3+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_Metadata
    /// </remarks>
    public class MetadataResponse : ThrottledResponse, IResponse, IEquatable<MetadataResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},brokers:[{Brokers.ToStrings()}],cluster_id:{ClusterId},controller_id:{ControllerId},topic_metadata:[{TopicMetadata.ToStrings()}]}}";

        public static MetadataResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 3);
                var brokers = new Server[reader.ReadInt32()];
                for (var b = 0; b < brokers.Length; b++) {
                    var brokerId = reader.ReadInt32();
                    var host = reader.ReadString();
                    var port = reader.ReadInt32();
                    string rack = null;
                    if (context.ApiVersion >= 1) {
                        rack = reader.ReadString();
                    }

                    brokers[b] = new Server(brokerId, host, port, rack);
                }

                string clusterId = null;
                if (context.ApiVersion >= 2) {
                    clusterId = reader.ReadString();
                }

                int? controllerId = null;
                if (context.ApiVersion >= 1) {
                    controllerId = reader.ReadInt32();
                }

                var topics = new Topic[reader.ReadInt32()];
                for (var t = 0; t < topics.Length; t++) {
                    var topicError = (ErrorCode) reader.ReadInt16();
                    var topicName = reader.ReadString();
                    bool? isInternal = null;
                    if (context.ApiVersion >= 1) {
                        isInternal = reader.ReadBoolean();
                    }

                    var partitions = new Partition[reader.ReadInt32()];
                    for (var p = 0; p < partitions.Length; p++) {
                        var partitionError = (ErrorCode) reader.ReadInt16();
                        var partitionId = reader.ReadInt32();
                        var leaderId = reader.ReadInt32();

                        var replicaCount = reader.ReadInt32();
                        var replicas = replicaCount.Repeat(() => reader.ReadInt32()).ToArray();

                        var isrCount = reader.ReadInt32();
                        var isrs = isrCount.Repeat(() => reader.ReadInt32()).ToArray();

                        partitions[p] = new Partition(partitionId, leaderId, partitionError, replicas, isrs);

                    }
                    topics[t] = new Topic(topicName, topicError, partitions, isInternal);
                }

                return new MetadataResponse(brokers, topics, controllerId, clusterId, throttleTime);
            }            
        }

        public MetadataResponse(IEnumerable<Server> brokers = null, IEnumerable<Topic> topics = null, int? controllerId = null, string clusterId = null, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Brokers = brokers.ToSafeImmutableList();
            TopicMetadata = topics.ToSafeImmutableList();
            ControllerId = controllerId;
            ClusterId = clusterId;
            Errors = TopicMetadata.Select(t => t.TopicError).ToImmutableList();
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Server> Brokers { get; }

        /// <summary>
        /// The broker id of the controller broker.
        /// Version: 1+
        /// </summary>
        public int? ControllerId { get; }

        /// <summary>
        /// The cluster id that this broker belongs to.
        /// Version: 2+
        /// </summary>
        public string ClusterId { get; }

        public IImmutableList<Topic> TopicMetadata { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as MetadataResponse);
        }

        /// <inheritdoc />
        public bool Equals(MetadataResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Brokers.HasEqualElementsInOrder(other.Brokers) 
                && ControllerId == other.ControllerId
                && ClusterId == other.ClusterId
                && TopicMetadata.HasEqualElementsInOrder(other.TopicMetadata);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (ControllerId?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (ClusterId?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Brokers?.Count.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (TopicMetadata?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        /// <summary>
        /// Metadata for each topic requested
        /// </summary>
        public class Topic : IEquatable<Topic>
        {
            public override string ToString() => $"{{topic_error_code:{TopicError},topic:{TopicName},is_internal:{IsInternal},partition_metadata:[{PartitionMetadata.ToStrings()}]}}";

            public Topic(string topicName, ErrorCode errorCode = ErrorCode.NONE, IEnumerable<Partition> partitions = null, bool? isInternal = null)
            {
                TopicError = errorCode;
                TopicName = topicName;
                IsInternal = isInternal;
                PartitionMetadata = partitions.ToSafeImmutableList();
            }

            /// <summary>
            /// The error code for the partition, if any.
            /// </summary>
            public ErrorCode TopicError { get; }

            /// <summary>
            /// The name of the topic.
            /// </summary>
            public string TopicName { get; }

            /// <summary>
            /// Indicates if the topic is considered a Kafka internal topic.
            /// Version: 1+
            /// </summary>
            public bool? IsInternal { get; }

            public IImmutableList<Partition> PartitionMetadata { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            /// <inheritdoc />
            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return TopicError == other.TopicError 
                    && string.Equals(TopicName, other.TopicName) 
                    && IsInternal == other.IsInternal
                    && PartitionMetadata.HasEqualElementsInOrder(other.PartitionMetadata);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) TopicError;
                    hashCode = (hashCode*397) ^ (TopicName?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (IsInternal?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (PartitionMetadata?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }

        public class Partition : IEquatable<Partition>
        {
            public override string ToString() => $"{{partition_error_code:{PartitionError},partition_id:{PartitionId},leader:{Leader},replicas:[{Replicas.ToStrings()}],isr:[{Isr.ToStrings()}]}}";

            public Partition(int partitionId, int leaderId, ErrorCode errorCode = ErrorCode.NONE, IEnumerable<int> replicas = null, IEnumerable<int> isrs = null)
            {
                PartitionError = errorCode;
                PartitionId = partitionId;
                Leader = leaderId;
                Replicas = replicas.ToSafeImmutableList();
                Isr = isrs.ToSafeImmutableList();
            }

            /// <summary>
            /// Error code.
            /// </summary>
            public ErrorCode PartitionError { get; }

            /// <summary>
            /// The Id of the partition that this metadata describes.
            /// </summary>
            public int PartitionId { get; }

            /// <summary>
            /// The node id for the kafka broker currently acting as leader for this partition. If no leader exists because we are in the middle of a leader election this id will be -1.
            /// </summary>
            public int Leader { get; }

            public bool IsElectingLeader => Leader == -1;

            /// <summary>
            /// The set of alive nodes that currently acts as slaves for the leader for this partition.
            /// </summary>
            public IImmutableList<int> Replicas { get; }

            /// <summary>
            /// The set subset of the replicas that are "caught up" to the leader
            /// </summary>
            public IImmutableList<int> Isr { get; }

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Partition);
            }

            /// <inheritdoc />
            public bool Equals(Partition other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return PartitionError == other.PartitionError 
                    && PartitionId == other.PartitionId 
                    && Leader == other.Leader 
                    && Replicas.HasEqualElementsInOrder(other.Replicas) 
                    && Isr.HasEqualElementsInOrder(other.Isr);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) PartitionError;
                    hashCode = (hashCode*397) ^ PartitionId;
                    hashCode = (hashCode*397) ^ Leader;
                    hashCode = (hashCode*397) ^ (Replicas?.Count.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Isr?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }
        }
    }
}