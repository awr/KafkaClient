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
            if (typeof(T) == typeof(DeleteRecordsRequest)) return (T)DeleteRecordsRequest(context, bytes);
            if (typeof(T) == typeof(InitProducerIdRequest)) return (T)InitProducerIdRequest(context, bytes);
            if (typeof(T) == typeof(OffsetForLeaderEpochRequest)) return (T)OffsetForLeaderEpochRequest(context, bytes);
            if (typeof(T) == typeof(AddPartitionsToTxnRequest)) return (T)AddPartitionsToTxnRequest(context, bytes);
            if (typeof(T) == typeof(AddOffsetsToTxnRequest)) return (T)AddOffsetsToTxnRequest(context, bytes);
            if (typeof(T) == typeof(EndTxnRequest)) return (T)EndTxnRequest(context, bytes);
            if (typeof(T) == typeof(WriteTxnMarkersRequest)) return (T)WriteTxnMarkersRequest(context, bytes);
            if (typeof(T) == typeof(TxnOffsetCommitRequest)) return (T)TxnOffsetCommitRequest(context, bytes);
            if (typeof(T) == typeof(DescribeAclsRequest)) return (T)DescribeAclsRequest(context, bytes);
            if (typeof(T) == typeof(CreateAclsRequest)) return (T)CreateAclsRequest(context, bytes);
            if (typeof(T) == typeof(DeleteAclsRequest)) return (T)DeleteAclsRequest(context, bytes);
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
                || TryEncodeResponse(writer, context, response as DeleteTopicsResponse)
                || TryEncodeResponse(writer, context, response as DeleteRecordsResponse)
                || TryEncodeResponse(writer, context, response as InitProducerIdResponse)
                || TryEncodeResponse(writer, context, response as OffsetForLeaderEpochResponse)
                || TryEncodeResponse(writer, context, response as AddPartitionsToTxnResponse)
                || TryEncodeResponse(writer, context, response as AddOffsetsToTxnResponse)
                || TryEncodeResponse(writer, context, response as EndTxnResponse)
                || TryEncodeResponse(writer, context, response as WriteTxnMarkersResponse)
                || TryEncodeResponse(writer, context, response as TxnOffsetCommitResponse)
                || TryEncodeResponse(writer, context, response as DescribeAclsResponse)
                || TryEncodeResponse(writer, context, response as CreateAclsResponse)
                || TryEncodeResponse(writer, context, response as DeleteAclsResponse);

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
                bool? validateOnly = null;
                if (context.ApiVersion >= 1) {
                    validateOnly = reader.ReadBoolean();
                }
                return new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeout), validateOnly);
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
        
        private static IRequest DeleteRecordsRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupedTopics = reader.ReadInt32();
                var topics = new List<DeleteRecordsRequest.Topic>();
                for (var t = 0; t < groupedTopics; t++) {
                    var topicName = reader.ReadString();
                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var offset = reader.ReadInt64();
                        topics.Add(new DeleteRecordsRequest.Topic(topicName, partitionId, offset));
                    }
                }
                var timeout = reader.ReadInt32();

                return new DeleteRecordsRequest(topics, TimeSpan.FromMilliseconds(timeout));
            }
        }
        
        private static IRequest InitProducerIdRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var transactionalId = reader.ReadString();
                var transactionTimeout = reader.ReadInt32();

                return new InitProducerIdRequest(transactionalId, TimeSpan.FromMilliseconds(transactionTimeout));
            }
        }
        
        private static IRequest OffsetForLeaderEpochRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var groupedTopics = reader.ReadInt32();
                var topics = new List<OffsetForLeaderEpochRequest.Topic>();
                for (var t = 0; t < groupedTopics; t++) {
                    var topicName = reader.ReadString();
                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var leaderEpoch = reader.ReadInt32();
                        topics.Add(new OffsetForLeaderEpochRequest.Topic(topicName, partitionId, leaderEpoch));
                    }
                }

                return new OffsetForLeaderEpochRequest(topics);
            }
        }
        
        private static IRequest AddPartitionsToTxnRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var transactionalId = reader.ReadString();
                var producerId = reader.ReadInt64();
                var producerEpoch = reader.ReadInt16();

                var groupedTopics = reader.ReadInt32();
                var topics = new List<TopicPartition>();
                for (var t = 0; t < groupedTopics; t++) {
                    var topicName = reader.ReadString();
                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        topics.Add(new TopicPartition(topicName, partitionId));
                    }
                }

                return new AddPartitionsToTxnRequest(transactionalId, producerId, producerEpoch, topics);
            }
        }
        
        private static IRequest AddOffsetsToTxnRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var transactionalId = reader.ReadString();
                var producerId = reader.ReadInt64();
                var producerEpoch = reader.ReadInt16();
                var consumerGroupId = reader.ReadString();

                return new AddOffsetsToTxnRequest(transactionalId, producerId, producerEpoch, consumerGroupId);
            }
        }

        private static IRequest EndTxnRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var transactionalId = reader.ReadString();
                var producerId = reader.ReadInt64();
                var producerEpoch = reader.ReadInt16();
                var transactionResult = reader.ReadBoolean();

                return new EndTxnRequest(transactionalId, producerId, producerEpoch, transactionResult);
            }
        }

        private static IRequest WriteTxnMarkersRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var markers = new WriteTxnMarkersRequest.TransactionMarker[reader.ReadInt32()];
                for (var m = 0; m < markers.Length; m++) {
                    var producerId = reader.ReadInt64();
                    var producerEpoch = reader.ReadInt16();
                    var transactionResult = reader.ReadBoolean();

                    var groupedTopics = reader.ReadInt32();
                    var topics = new List<TopicPartition>();
                    for (var t = 0; t < groupedTopics; t++) {
                        var topicName = reader.ReadString();
                        var partitionCount = reader.ReadInt32();
                        for (var p = 0; p < partitionCount; p++) {
                            var partitionId = reader.ReadInt32();
                            topics.Add(new TopicPartition(topicName, partitionId));
                        }
                    }
                    var coordinatorEpoch = reader.ReadInt32();

                    markers[m] = new WriteTxnMarkersRequest.TransactionMarker(producerId, producerEpoch, transactionResult, topics, coordinatorEpoch);
                }

                return new WriteTxnMarkersRequest(markers);
            }
        }

        private static IRequest TxnOffsetCommitRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var transactionalId = reader.ReadString();
                var consumerGroupId = reader.ReadString();
                var producerId = reader.ReadInt64();
                var producerEpoch = reader.ReadInt16();

                var groupedTopics = reader.ReadInt32();
                var topics = new List<TxnOffsetCommitRequest.Topic>();
                for (var t = 0; t < groupedTopics; t++) {
                    var topicName = reader.ReadString();
                    var partitionCount = reader.ReadInt32();
                    for (var p = 0; p < partitionCount; p++) {
                        var partitionId = reader.ReadInt32();
                        var offset = reader.ReadInt64();
                        var metadata = reader.ReadString();
                        topics.Add(new TxnOffsetCommitRequest.Topic(topicName, partitionId, offset, metadata));
                    }
                }

                return new TxnOffsetCommitRequest(transactionalId, producerId, producerEpoch, consumerGroupId, topics);
            }
        }

        private static IRequest DescribeAclsRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var resourceType = reader.ReadByte();
                var resourceName = reader.ReadString();
                var principal = reader.ReadString();
                var host = reader.ReadString();
                var operation = reader.ReadByte();
                var permissionType = reader.ReadByte();

                return new DescribeAclsRequest(resourceType, resourceName, principal, host, operation, permissionType);
            }
        }

        private static IRequest CreateAclsRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var resources = new AclResource[reader.ReadInt32()];
                for (var r = 0; r < resources.Length; r++) {
                    var resourceType = reader.ReadByte();
                    var resourceName = reader.ReadString();
                    var principal = reader.ReadString();
                    var host = reader.ReadString();
                    var operation = reader.ReadByte();
                    var permissionType = reader.ReadByte();

                    resources[r] = new AclResource(resourceType, resourceName, principal, host, operation, permissionType);
                }

                return new CreateAclsRequest(resources);
            }
        }

        private static IRequest DeleteAclsRequest(IRequestContext context, ArraySegment<byte> payload)
        {
            using (var reader = ReadHeader(payload)) {
                var filters = new AclResource[reader.ReadInt32()];
                for (var f = 0; f < filters.Length; f++) {
                    var resourceType = reader.ReadByte();
                    var resourceName = reader.ReadString();
                    var principal = reader.ReadString();
                    var host = reader.ReadString();
                    var operation = reader.ReadByte();
                    var permissionType = reader.ReadByte();

                    filters[f] = new AclResource(resourceType, resourceName, principal, host, operation, permissionType);
                }

                return new DeleteAclsRequest(filters);
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
                writer.WriteMilliseconds(response.ThrottleTime);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, FetchResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.WriteMilliseconds(response.ThrottleTime);
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
                writer.WriteMilliseconds(response.ThrottleTime);
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
                writer.WriteMilliseconds(response.ThrottleTime);
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
                writer.WriteMilliseconds(response.ThrottleTime);
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
                writer.WriteMilliseconds(response.ThrottleTime);
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
                writer.WriteMilliseconds(response.ThrottleTime);
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
                writer.WriteMilliseconds(response.ThrottleTime);
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

            if (context.ApiVersion >= 1) {
                writer.WriteMilliseconds(response.ThrottleTime);
            }
            writer.Write(response.Error);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, LeaveGroupResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.WriteMilliseconds(response.ThrottleTime);
            }
            writer.Write(response.Error);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, SyncGroupResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.WriteMilliseconds(response.ThrottleTime);
            }
            var encoder = context.GetEncoder(context.ProtocolType);
            writer.Write(response.Error)
                   .Write(response.MemberAssignment, encoder);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, DescribeGroupsResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.WriteMilliseconds(response.ThrottleTime);
            }
            writer.Write(response.Groups.Count);
            foreach (var group in response.Groups) {
                writer.Write(group.Error)
                    .Write(group.GroupId)
                    .Write(group.State)
                    .Write(group.ProtocolType)
                    .Write(group.Protocol);

                var encoder = context.GetEncoder(group.ProtocolType);
                writer.Write(group.Members.Count);
                foreach (var member in group.Members) {
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

            if (context.ApiVersion >= 1) {
                writer.WriteMilliseconds(response.ThrottleTime);
            }
            writer.Write(response.Error)
                .Write(response.Groups.Count);
            foreach (var group in response.Groups) {
                writer.Write(group.GroupId)
                    .Write(group.ProtocolType);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, SaslHandshakeResponse response)
        {
            if (response == null) return false;

            writer.Write(response.Error)
                .Write(response.EnabledMechanisms.Count);
            foreach (var mechanism in response.EnabledMechanisms) {
                writer.Write(mechanism);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, ApiVersionsResponse response)
        {
            if (response == null) return false;

            writer.Write(response.Error)
                .Write(response.ApiVersions.Count);
            foreach (var versionSupport in response.ApiVersions) {
                writer.Write((short)versionSupport.ApiKey)
                    .Write(versionSupport.MinVersion)
                    .Write(versionSupport.MaxVersion);
            }
            if (context.ApiVersion >= 1) {
                writer.WriteMilliseconds(response.ThrottleTime);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, CreateTopicsResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 2) {
                writer.WriteMilliseconds(response.ThrottleTime);
            }
            writer.Write(response.Topics.Count);
            foreach (var topic in response.Topics) {
                writer.Write(topic.TopicName)
                    .Write(topic.ErrorCode);
                if (context.ApiVersion >= 1) {
                    writer.Write(topic.ErrorMessage);
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, DeleteTopicsResponse response)
        {
            if (response == null) return false;

            if (context.ApiVersion >= 1) {
                writer.WriteMilliseconds(response.ThrottleTime);
            }
            writer.Write(response.Topics.Count);
            foreach (var topic in response.Topics) {
                writer.Write(topic.TopicName)
                      .Write(topic.ErrorCode);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, DeleteRecordsResponse response)
        {
            if (response == null) return false;

            writer.WriteMilliseconds(response.ThrottleTime)
                  .WriteGroupedTopics(response.Topics,
                      partition => {
                          writer.Write(partition.PartitionId)
                                .Write(partition.LowWatermark)
                                .Write(partition.Error);
                      });
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, InitProducerIdResponse response)
        {
            if (response == null) return false;

            writer.WriteMilliseconds(response.ThrottleTime);
            writer.Write(response.Error);
            writer.Write(response.ProducerId);
            writer.Write(response.ProducerEpoch);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, OffsetForLeaderEpochResponse response)
        {
            if (response == null) return false;

            writer.WriteGroupedTopics(
                response.Topics,
                partition => {
                    writer.Write(partition.Error)
                          .Write(partition.PartitionId)
                          .Write(partition.EndOffset);
                });
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, AddPartitionsToTxnResponse response)
        {
            if (response == null) return false;

            writer.WriteMilliseconds(response.ThrottleTime)
                  .WriteGroupedTopics(
                      response.Topics,
                      partition => {
                          writer.Write(partition.PartitionId)
                                .Write(partition.Error);
                      });
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, AddOffsetsToTxnResponse response)
        {
            if (response == null) return false;

            writer.WriteMilliseconds(response.ThrottleTime)
                  .Write(response.Error);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, EndTxnResponse response)
        {
            if (response == null) return false;

            writer.WriteMilliseconds(response.ThrottleTime)
                  .Write(response.Error);
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, WriteTxnMarkersResponse response)
        {
            if (response == null) return false;

            writer.Write(response.TransactionMarkers.Count);
            foreach (var marker in response.TransactionMarkers) {
                writer.Write(marker.ProducerId)
                      .WriteGroupedTopics(marker.Topics,
                          partition => {
                              writer.Write(partition.PartitionId)
                                    .Write(partition.Error);
                          });
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, TxnOffsetCommitResponse response)
        {
            if (response == null) return false;

            writer.WriteMilliseconds(response.ThrottleTime)
                  .WriteGroupedTopics(response.Topics,
                      partition => {
                          writer.Write(partition.PartitionId)
                                .Write(partition.Error);
                      });
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, DescribeAclsResponse response)
        {
            if (response == null) return false;

            writer.WriteMilliseconds(response.ThrottleTime)
                  .Write(response.Error)
                  .Write(response.ErrorMessage);
            var grouped = response.Resources.GroupBy(r => new {r.ResourceType, r.ResourceName}).ToList();
            writer.Write(grouped.Count);
            foreach (var resource in grouped) {
                var acls = resource.ToList();
                writer.Write(resource.Key.ResourceType)
                      .Write(resource.Key.ResourceName)
                      .Write(acls.Count);
                foreach (var acl in acls) {
                    writer.Write(acl.Principal)
                          .Write(acl.Host)
                          .Write(acl.Operation)
                          .Write(acl.PermissionType);
                }
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, CreateAclsResponse response)
        {
            if (response == null) return false;

            writer.WriteMilliseconds(response.ThrottleTime)
                  .Write(response.Responses.Count);
            foreach (var error in response.Responses) {
                writer.Write(error.Error)
                      .Write(error.ErrorMessage);
            }
            return true;
        }

        private static bool TryEncodeResponse(IKafkaWriter writer, IRequestContext context, DeleteAclsResponse response)
        {
            if (response == null) return false;

            writer.WriteMilliseconds(response.ThrottleTime)
                  .Write(response.FilterResponses.Count);

            foreach (var filter in response.FilterResponses) {
                writer.Write(filter.Error)
                      .Write(filter.ErrorMessage)
                      .Write(filter.MatchingAcls.Count);
                foreach (var acl in filter.MatchingAcls) {
                    writer.Write(acl.Error)
                          .Write(acl.ErrorMessage)
                          .Write(acl.ResourceType)
                          .Write(acl.ResourceName)
                          .Write(acl.Principal)
                          .Write(acl.Host)
                          .Write(acl.Operation)
                          .Write(acl.PermissionType);
                }
            }
            return true;
        }

        #endregion
    }
}