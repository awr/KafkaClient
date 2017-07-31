using System;
using System.Collections.Generic;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Testing
{
    /// <summary>
    /// Only used by testing and benchmarking code
    /// </summary>
    public static class KafkaDecoder
    {
        public static IRequestContext DecodeHeader(ArraySegment<byte> bytes)
        {
            IRequestContext context;
            using (ReadHeader(bytes, out context)) {
                return context;
            }            
        }

        public static Tuple<IRequestContext, ApiKey> DecodeFullHeader(ArraySegment<byte> bytes)
        {
            ApiKey apiKey;
            IRequestContext context;
            using (ReadHeader(bytes, out apiKey, out context))
            {
                return new Tuple<IRequestContext, ApiKey>(context, apiKey);
            }
        }

        public static T Decode<T>(ArraySegment<byte> bytes, IRequestContext context = null) where T : class, IRequest
        {
            var protocolType = context?.ProtocolType;
            var encoders = context?.Encoders;
            using (ReadHeader(bytes, out context)) { }

            return Decode<T>(new RequestContext(context.CorrelationId, context.ApiVersion, context.ClientId, encoders, protocolType), bytes);
        }

        public static T Decode<T>(IRequestContext context, ArraySegment<byte> bytes) where T : class, IRequest
        {
            if (typeof(T) == typeof(ProduceRequest)) return (T)ProduceRequest(context, bytes);
            if (typeof(T) == typeof(FetchRequest)) return (T)FetchRequest(context, bytes);
            if (typeof(T) == typeof(OffsetsRequest)) return (T)OffsetRequest(context, bytes);
            if (typeof(T) == typeof(MetadataRequest)) return (T)MetadataRequest(context, bytes);
            if (typeof(T) == typeof(OffsetCommitRequest)) return (T)OffsetCommitRequest(context, bytes);
            if (typeof(T) == typeof(OffsetFetchRequest)) return (T)OffsetFetchRequest(context, bytes);
            if (typeof(T) == typeof(FindCoordinatorRequest)) return (T)FindCoordinatorRequest(context, bytes);
            if (typeof(T) == typeof(JoinGroupRequest)) return (T)JoinGroupRequest(context, bytes);
            if (typeof(T) == typeof(HeartbeatRequest)) return (T)HeartbeatRequest(context, bytes);
            if (typeof(T) == typeof(LeaveGroupRequest)) return (T)LeaveGroupRequest(context, bytes);
            if (typeof(T) == typeof(SyncGroupRequest)) return (T)SyncGroupRequest(context, bytes);
            if (typeof(T) == typeof(DescribeGroupsRequest)) return (T)DescribeGroupsRequest(context, bytes);
            if (typeof(T) == typeof(ListGroupsRequest)) return (T)ListGroupsRequest(context, bytes);
            if (typeof(T) == typeof(SaslHandshakeRequest)) return (T)SaslHandshakeRequest(context, bytes);
            if (typeof(T) == typeof(ApiVersionsRequest)) return (T)ApiVersionsRequest(context, bytes);
            if (typeof(T) == typeof(CreateTopicsRequest)) return (T)CreateTopicsRequest(context, bytes);
            if (typeof(T) == typeof(DeleteTopicsRequest)) return (T)DeleteTopicsRequest(context, bytes);
            return default(T);
        }

        public static ArraySegment<byte> EncodeResponseBytes<T>(IRequestContext context, T response) where T : IResponse
        {
            using (var writer = new KafkaWriter()) {
                // From http://kafka.apache.org/protocol.html#protocol_messages
                // 
                // Response Header => correlation_id 
                //  correlation_id => INT32  -- The user-supplied value passed in with the request
                writer.Write(context.CorrelationId);

                // ReSharper disable once UnusedVariable
                var isEncoded = 
                   TryEncodeResponse(writer, context, response as ProduceResponse)
                || TryEncodeResponse(writer, context, response as FetchResponse)
                || TryEncodeResponse(writer, context, response as OffsetsResponse)
                || TryEncodeResponse(writer, context, response as MetadataResponse)
                || TryEncodeResponse(writer, context, response as OffsetCommitResponse)
                || TryEncodeResponse(writer, context, response as OffsetFetchResponse)
                || TryEncodeResponse(writer, context, response as FindCoordinatorResponse)
                || TryEncodeResponse(writer, context, response as JoinGroupResponse)
                || TryEncodeResponse(writer, context, response as HeartbeatResponse)
                || TryEncodeResponse(writer, context, response as LeaveGroupResponse)
                || TryEncodeResponse(writer, context, response as SyncGroupResponse)
                || TryEncodeResponse(writer, context, response as DescribeGroupsResponse)
                || TryEncodeResponse(writer, context, response as ListGroupsResponse)
                || TryEncodeResponse(writer, context, response as SaslHandshakeResponse)
                || TryEncodeResponse(writer, context, response as ApiVersionsResponse)
                || TryEncodeResponse(writer, context, response as CreateTopicsResponse)
                || TryEncodeResponse(writer, context, response as DeleteTopicsResponse);

                return writer.ToSegment();
            }
        }

        #region Decode

        private static IRequest ProduceRequest(IRequestContext context, ArraySegment<byte> data)
        {
            using (var reader = ReadHeader(data)) {
                string transaction_id = null;
                if (context.ApiVersion >= 3) {
                    transaction_id = reader.ReadString();
                }
                var acks = reader.ReadInt16();
                var timeout = reader.ReadInt32();

                var payloads = new List<ProduceRequest.Topic>();
                var payloadCount = reader.ReadInt32();
                for (var i = 0; i < payloadCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var messages = reader.ReadMessages();

                        payloads.Add(new ProduceRequest.Topic(topicName, partitionId, messages));
                    }
                }
                return new ProduceRequest(payloads, TimeSpan.FromMilliseconds(timeout), acks, transaction_id);
            }
        }

        private static IRequest FetchRequest(IRequestContext context, ArraySegment<byte> data)
        {
            using (var reader = ReadHeader(data)) {
                // ReSharper disable once UnusedVariable
                var replicaId = reader.ReadInt32(); // expect -1
                var maxWaitTime = reader.ReadInt32();
                var minBytes = reader.ReadInt32();

                var totalMaxBytes = 0;
                byte? isolationLevel = null;
                if (context.ApiVersion >= 3) {
                    totalMaxBytes = reader.ReadInt32();
                    if (context.ApiVersion >= 4) {
                        isolationLevel = reader.ReadByte();
                    }
                }

                var topics = new List<FetchRequest.Topic>();
                var topicCount = reader.ReadInt32();
                for (var i = 0; i < topicCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var offset = reader.ReadInt64();
                        long? log_start_offset = null;
                        if (context.ApiVersion >= 5) {
                            log_start_offset = reader.ReadInt64();
                        }
                        var maxBytes = reader.ReadInt32();

                        topics.Add(new FetchRequest.Topic(topicName, partitionId, offset, log_start_offset, maxBytes));
                    }
                }
                return new FetchRequest(topics, TimeSpan.FromMilliseconds(maxWaitTime), minBytes, totalMaxBytes, isolationLevel);
            }
        }

        private static IRequest OffsetRequest(IRequestContext context, ArraySegment<byte> data)
        {
            using (var reader = ReadHeader(data)) {
                // ReSharper disable once UnusedVariable
                var replicaId = reader.ReadInt32(); // expect -1
                byte isolationLevel = 0;
                if (context.ApiVersion >= 2) {
                    isolationLevel = reader.ReadByte();
                }

                var topics = new List<OffsetsRequest.Topic>();
                var offsetCount = reader.ReadInt32();
                for (var i = 0; i < offsetCount; i++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var j = 0; j < partitionCount; j++) {
                        var partitionId = reader.ReadInt32();
                        var time = reader.ReadInt64();
                        var maxOffsets = 1;
                        if (context.ApiVersion == 0) {
                            maxOffsets = reader.ReadInt32();
                        }

                        topics.Add(new OffsetsRequest.Topic(topicName, partitionId, time, maxOffsets));
                    }
                }
                return new OffsetsRequest(topics, isolationLevel);
            }
        }

        private static IRequest MetadataRequest(IRequestContext context, ArraySegment<byte> data)
        {
            using (var reader = ReadHeader(data)) {
                var topicNames = new string[reader.ReadInt32()];
                for (var t = 0; t < topicNames.Length; t++) {
                    topicNames[t] = reader.ReadString();
                }
                bool? allowAutoTopicCreation = null;
                if (context.ApiVersion >= 4) {
                    allowAutoTopicCreation = reader.ReadBoolean();
                }

                return new MetadataRequest(topicNames, allowAutoTopicCreation);
            }
        }
        
        private static IRequest OffsetCommitRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var generationId = 0;
                string memberId = null; 
                if (context.ApiVersion >= 1) {
                    generationId = reader.ReadInt32();
                    memberId = reader.ReadString();
                }
                TimeSpan? offsetRetention = null;
                if (context.ApiVersion >= 2) {
                    var retentionTime = reader.ReadInt64();
                    if (retentionTime >= 0) {
                        offsetRetention = TimeSpan.FromMilliseconds(retentionTime);
                    }
                }

                var offsetCommits = new List<OffsetCommitRequest.Topic>();
                var count = reader.ReadInt32();
                for (var o = 0; o < count; o++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var offset = reader.ReadInt64();
                        long? timestamp = null;
                        if (context.ApiVersion == 1) {
                            timestamp = reader.ReadInt64();
                        }
                        var metadata = reader.ReadString();

                        offsetCommits.Add(new OffsetCommitRequest.Topic(topicName, partitionId, offset, metadata, timestamp));
                    }
                }

                return new OffsetCommitRequest(groupId, offsetCommits, memberId, generationId, offsetRetention);
            }
        }

        private static IRequest OffsetFetchRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();

                var topics = new List<TopicPartition>();
                var count = reader.ReadInt32();
                for (var t = 0; t < count; t++) {
                    var topicName = reader.ReadString();

                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();

                        topics.Add(new TopicPartition(topicName, partitionId));
                    }
                }

                return new OffsetFetchRequest(groupId, topics);
            }
        }
        
        private static IRequest FindCoordinatorRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var coordinatorType = CoordinatorType.Group;
                if (context.ApiVersion >= 1) {
                    coordinatorType = (CoordinatorType)reader.ReadByte();
                }

                return new FindCoordinatorRequest(groupId, coordinatorType);
            }
        }

        private static IRequest JoinGroupRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var sessionTimeout = TimeSpan.FromMilliseconds(reader.ReadInt32());
                TimeSpan? rebalanceTimeout = null;
                if (context.ApiVersion >= 1) {
                    rebalanceTimeout = TimeSpan.FromMilliseconds(reader.ReadInt32());
                }
                var memberId = reader.ReadString();
                var protocolType = reader.ReadString();
                var groupProtocols = new JoinGroupRequest.GroupProtocol[reader.ReadInt32()];

                var encoder = context.GetEncoder(protocolType);
                for (var g = 0; g < groupProtocols.Length; g++) {
                    var protocolName = reader.ReadString();
                    var metadata = encoder.DecodeMetadata(protocolName, reader);
                    groupProtocols[g] = new JoinGroupRequest.GroupProtocol(metadata);
                }

                return new JoinGroupRequest(groupId, sessionTimeout, memberId, protocolType, groupProtocols, rebalanceTimeout);
            }
        }

        private static IRequest HeartbeatRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var generationId = reader.ReadInt32();
                var memberId = reader.ReadString();

                return new HeartbeatRequest(groupId, generationId, memberId);
            }
        }

        private static IRequest LeaveGroupRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var memberId = reader.ReadString();

                return new LeaveGroupRequest(groupId, memberId);
            }
        }

        private static IRequest SyncGroupRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupId = reader.ReadString();
                var generationId = reader.ReadInt32();
                var memberId = reader.ReadString();

                var encoder = context.GetEncoder();
                var groupAssignments = new SyncGroupRequest.GroupAssignment[reader.ReadInt32()];
                for (var a = 0; a < groupAssignments.Length; a++) {
                    var groupMemberId = reader.ReadString();
                    var assignment = encoder.DecodeAssignment(reader);

                    groupAssignments[a] = new SyncGroupRequest.GroupAssignment(groupMemberId, assignment);
                }

                return new SyncGroupRequest(groupId, generationId, memberId, groupAssignments);
            }
        }

        private static IRequest DescribeGroupsRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupIds = new string[reader.ReadInt32()];
                for (var i = 0; i < groupIds.Length; i++) {
                    groupIds[i] = reader.ReadString();
                }

                return new DescribeGroupsRequest(groupIds);
            }
        }

        private static IRequest ListGroupsRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (ReadHeader(payload)) {
                return new ListGroupsRequest();
            }
        }

        private static IRequest SaslHandshakeRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var mechanism = reader.ReadString();
                return new SaslHandshakeRequest(mechanism);
            }
        }

        private static IRequest ApiVersionsRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (ReadHeader(payload)) {
                return new ApiVersionsRequest();
            }
        }
        
        private static IRequest CreateTopicsRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var topics = new CreateTopicsRequest.Topic[reader.ReadInt32()];
                for (var t = 0; t < topics.Length; t++) {
                    var topicName = reader.ReadString();
                    var numPartitions = reader.ReadInt32();
                    var replicationFactor = reader.ReadInt16();

                    var assignments = new CreateTopicsRequest.ReplicaAssignment[reader.ReadInt32()];
                    for (var a = 0; a < assignments.Length; a++) {
                        var partitionId = reader.ReadInt32();
                        var replicaCount = reader.ReadInt32();
                        var replicas = replicaCount.Repeat(reader.ReadInt32).ToArray();
                        assignments[a] = new CreateTopicsRequest.ReplicaAssignment(partitionId, replicas);
                    }

                    var configs = new KeyValuePair<string, string>[reader.ReadInt32()];
                    for (var c = 0; c < configs.Length; c++) {
                        var key = reader.ReadString();
                        var value = reader.ReadString();
                        configs[c] = new KeyValuePair<string, string>(key, value);
                    }

                    topics[t] = assignments.Length > 0
                        ? new CreateTopicsRequest.Topic(topicName, assignments, configs)
                        : new CreateTopicsRequest.Topic(topicName, numPartitions, replicationFactor, configs);
                }
                var timeout = reader.ReadInt32();

                return new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeout));
            }
        }
        
        private static IRequest DeleteTopicsRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var topics = new string[reader.ReadInt32()];
                for (var t = 0; t < topics.Length; t++) {
                    topics[t] = reader.ReadString();
                }
                var timeout = reader.ReadInt32();

                return new DeleteTopicsRequest(topics, TimeSpan.FromMilliseconds(timeout));
            }
        }
        
        private static IKafkaReader ReadHeader(ArraySegment<byte> data)
        {
            IRequestContext context;
            return ReadHeader(data, out context);
        }

        private static IKafkaReader ReadHeader(ArraySegment<byte> data, out IRequestContext context)
        {
            ApiKey apikey;
            return ReadHeader(data, out apikey, out context);
        }

        private static IKafkaReader ReadHeader(ArraySegment<byte> data, out ApiKey apiKey, out IRequestContext context)
        {
            var reader = new KafkaReader(data);
            try {
                apiKey = (ApiKey)reader.ReadInt16();
                var version = reader.ReadInt16();
                var correlationId = reader.ReadInt32();
                var clientId = reader.ReadString();

                context = new RequestContext(correlationId, version, clientId);
            } catch {
                apiKey = 0;
                context = null;
                reader.Dispose();
                reader = null;
            }
            return reader;
        }

        #endregion

        #region Encode

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, ProduceResponse response)
        {
            if (response == null) return false;

            var groupedTopics = response.Responses.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();

                writer.Write(topic.Key)
                    .Write(partitions.Count);
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.Error)
                        .Write(partition.BaseOffset);
                    if (context.ApiVersion >= 2) {
                        writer.Write(partition.Timestamp?.ToUnixTimeMilliseconds() ?? -1L);
                    }
                }
            }
            if (context.ApiVersion >= 1) {
                writer.Write((int?)response.ThrottleTime?.TotalMilliseconds ?? 0);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, FetchResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.Write((int?)response.ThrottleTime?.TotalMilliseconds ?? 0);
            }
            var groupedTopics = response.Responses.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();

                writer.Write(topic.Key)
                    .Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.Error)
                        .Write(partition.HighWatermark);

                    if (context.ApiVersion >= 4) {
                        writer.Write(partition.LastStableOffset.GetValueOrDefault());
                        if (context.ApiVersion >= 5) {
                            writer.Write(partition.LogStartOffset.GetValueOrDefault());
                        }
                        writer.Write(partition.AbortedTransactions.Count);
                        foreach (var abortedTransaction in partition.AbortedTransactions) {
                            writer.Write(abortedTransaction.ProducerId);
                            writer.Write(abortedTransaction.FirstOffset);
                        }
                    }

                    if (partition.Messages.Count > 0) {
                        // assume all are the same codec
                        var codec = (MessageCodec) (partition.Messages[0].Attribute & Message.CodecMask);
                        writer.Write(partition.Messages, codec);
                    } else {
                        using (writer.MarkForLength()) {
                            writer.Write(partition.Messages);
                        }
                    }
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, OffsetsResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 2) {
                writer.Write((int)response.ThrottleTime.GetValueOrDefault().TotalMilliseconds);
            }
            var groupedTopics = response.Responses.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();

                writer.Write(topic.Key)
                    .Write(partitions.Count);
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.Error);
                    if (context.ApiVersion == 0) {
                        writer.Write(1)
                              .Write(partition.Offset);
                    } else {
                        writer.Write(partition.Timestamp?.ToUnixTimeMilliseconds() ?? 0L)
                            .Write(partition.Offset);
                    }
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, MetadataResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 3) {
                writer.Write((int)response.ThrottleTime.GetValueOrDefault().TotalMilliseconds);
            }
            writer.Write(response.Brokers.Count);
            foreach (var broker in response.Brokers) {
                writer.Write(broker.Id)
                    .Write(broker.Host)
                    .Write(broker.Port);
                if (context.ApiVersion >= 1) {
                    writer.Write(broker.Rack);
                }
            }

            if (context.ApiVersion >= 2) {
                writer.Write(response.ClusterId);
            }
            if (context.ApiVersion >= 1) {
                writer.Write(response.ControllerId.GetValueOrDefault());
            }

            var groupedTopics = response.TopicMetadata.GroupBy(t => new { TopicName = t.TopicName, ErrorCode = t.TopicError, IsInternal = t.IsInternal }).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.SelectMany(_ => _.PartitionMetadata).ToList();

                writer.Write(topic.Key.ErrorCode)
                    .Write(topic.Key.TopicName);
                if (context.ApiVersion >= 1) {
                    writer.Write(topic.Key.IsInternal.GetValueOrDefault());
                }
                writer.Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionError)
                        .Write(partition.PartitionId)
                        .Write(partition.Leader)
                        .Write(partition.Replicas)
                        .Write(partition.Isr);
                }
            }
            return true;
        }
        
        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, OffsetCommitResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 3) {
                writer.Write((int)response.ThrottleTime.GetValueOrDefault().TotalMilliseconds);
            }
            var groupedTopics = response.Responses.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();
                writer.Write(topic.Key)
                    .Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.Error);
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, OffsetFetchResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 3) {
                writer.Write((int)response.ThrottleTime.GetValueOrDefault().TotalMilliseconds);
            }
            var groupedTopics = response.Responses.GroupBy(t => t.TopicName).ToList();
            writer.Write(groupedTopics.Count);
            foreach (var topic in groupedTopics) {
                var partitions = topic.ToList();
                writer.Write(topic.Key)
                    .Write(partitions.Count); // partitionsPerTopic
                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId)
                        .Write(partition.Offset)
                        .Write(partition.Metadata)
                        .Write(partition.Error);
                }
            }
            if (context.ApiVersion >= 2) {
                writer.Write(response.Error.GetValueOrDefault());
            }
            return true;
        }
        
        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, FindCoordinatorResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.Write((int)response.ThrottleTime.GetValueOrDefault().TotalMilliseconds);
            }
            writer.Write(response.Error);
            if (context.ApiVersion >= 1) {
                writer.Write(response.ErrorMessage);
            }
            writer.Write(response.Id)
                .Write(response.Host)
                .Write(response.Port);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, JoinGroupResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 2) {
                writer.Write((int)response.ThrottleTime.GetValueOrDefault().TotalMilliseconds);
            }
            writer.Write(response.Error)
                .Write(response.GenerationId)
                .Write(response.GroupProtocol)
                .Write(response.LeaderId)
                .Write(response.MemberId)
                .Write(response.Members.Count);

            var encoder = context.GetEncoder();
            foreach (var member in response.Members) {
                writer.Write(member.MemberId)
                      .Write(member.MemberMetadata, encoder);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, HeartbeatResponse response)
        {
            if (response == null) return false;

            writer.Write(response.error_code);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, LeaveGroupResponse response)
        {
            if (response == null) return false;

            writer.Write(response.error_code);
            writer.Write(response.error_code);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, SyncGroupResponse response)
        {
            if (response == null) return false;

            var encoder = context.GetEncoder(context.ProtocolType);
            writer.Write(response.error_code)
                   .Write(response.member_assignment, encoder);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, DescribeGroupsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.groups.Count);
            foreach (var group in response.groups) {
                writer.Write(group.error_code)
                    .Write(group.group_id)
                    .Write(group.state)
                    .Write(group.protocol_type)
                    .Write(group.protocol);

                var encoder = context.GetEncoder(group.protocol_type);
                writer.Write(group.members.Count);
                foreach (var member in group.members) {
                    writer.Write(member.member_id)
                        .Write(member.client_id)
                        .Write(member.client_host)
                        .Write(member.member_metadata, encoder)
                        .Write(member.member_assignment, encoder);
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, ListGroupsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.error_code)
                .Write(response.groups.Count);
            foreach (var group in response.groups) {
                writer.Write(group.group_id)
                    .Write(group.protocol_type);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, SaslHandshakeResponse response)
        {
            if (response == null) return false;

            writer.Write(response.error_code)
                .Write(response.enabled_mechanisms.Count);
            foreach (var mechanism in response.enabled_mechanisms) {
                writer.Write(mechanism);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, ApiVersionsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.error_code)
                .Write(response.api_versions.Count);
            foreach (var versionSupport in response.api_versions) {
                writer.Write((short)versionSupport.api_key)
                    .Write(versionSupport.min_version)
                    .Write(versionSupport.max_version);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, CreateTopicsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.Topics.Count);
            foreach (var topic in response.Topics) {
                writer.Write(topic.TopicName)
                    .Write(topic.ErrorCode);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, DeleteTopicsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.Topics.Count);
            foreach (var topic in response.Topics) {
                writer.Write(topic.TopicName)
                    .Write(topic.ErrorCode);
            }
            return true;
        }

        #endregion
    }
}