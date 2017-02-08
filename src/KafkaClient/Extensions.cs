using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using KafkaClient.Telemetry;

namespace KafkaClient
{
    public static class Extensions
    {
        #region Configuration

        public static IVersionSupport Dynamic(this VersionSupport versionSupport)
        {
            return new DynamicVersionSupport(versionSupport);
        }

        public static IConnectionConfiguration ToConfiguration(this ITrackEvents tracker)
        {
            if (tracker == null) return ConnectionConfiguration.Default;

            return new ConnectionConfiguration(
                onDisconnected: tracker.Disconnected,
                onConnecting: tracker.Connecting,
                onConnected: tracker.Connected,
                onWriting: tracker.Writing,
                onWritingBytes: tracker.WritingBytes,
                onWroteBytes: tracker.WroteBytes,
                onWritten: tracker.Written,
                onWriteFailed: tracker.WriteFailed,
                onReading: tracker.Reading,
                onReadingBytes: tracker.ReadingBytes,
                onReadBytes: tracker.ReadBytes,
                onRead: tracker.Read,
                onReadFailed: tracker.ReadFailed,
                onProduceRequestMessages: tracker.ProduceRequestMessages);
        }

        public static IConnectionConfiguration CopyWith(
            this IConnectionConfiguration configuration,
            IRetry connectionRetry = null,
            IVersionSupport versionSupport = null,
            TimeSpan? requestTimeout = null,
            int? readBufferSize = null,
            int? writeBufferSize = null,
            bool? isTcpKeepalive = null,
            IEnumerable<IMembershipEncoder> encoders = null,
            ISslConfiguration sslConfiguration = null,
            ConnectError onDisconnected = null,
            Connecting onConnecting = null,
            Connecting onConnected = null,
            Writing onWriting = null,
            StartingBytes onWritingBytes = null,
            FinishedBytes onWroteBytes = null,
            WriteSuccess onWritten = null,
            WriteError onWriteFailed = null,
            Reading onReading = null,
            StartingBytes onReadingBytes = null,
            FinishedBytes onReadBytes = null,
            ReadSuccess onRead = null,
            ReadError onReadFailed = null,
            ProduceRequestMessages onProduceRequestMessages = null)
        {
            return new ConnectionConfiguration(
                connectionRetry ?? configuration.ConnectionRetry,
                versionSupport ?? configuration.VersionSupport,
                requestTimeout ?? configuration.RequestTimeout,
                readBufferSize ?? configuration.ReadBufferSize,
                writeBufferSize ?? configuration.WriteBufferSize,
                isTcpKeepalive ?? configuration.IsTcpKeepalive,
                encoders ?? configuration.Encoders.Values,
                sslConfiguration ?? configuration.SslConfiguration,
                onDisconnected ?? configuration.OnDisconnected,
                onConnecting ?? configuration.OnConnecting,
                onConnected ?? configuration.OnConnected,
                onWriting ?? configuration.OnWriting,
                onWritingBytes ?? configuration.OnWritingBytes,
                onWroteBytes ?? configuration.OnWroteBytes,
                onWritten ?? configuration.OnWritten,
                onWriteFailed ?? configuration.OnWriteFailed,
                onReading ?? configuration.OnReading,
                onReadingBytes ?? configuration.OnReadingBytes,
                onReadBytes ?? configuration.OnReadBytes,
                onRead ?? configuration.OnRead,
                onReadFailed ?? configuration.OnReadFailed,
                onProduceRequestMessages ?? configuration.OnProduceRequestMessages);
        }

        #endregion

        #region KafkaOptions

        public static async Task<IConnection> CreateConnectionAsync(this KafkaOptions options)
        {
            var endpoint = await Endpoint.ResolveAsync(options.ServerUris.First(), options.Log);
            return options.CreateConnection(endpoint);
        }

        public static IConnection CreateConnection(this KafkaOptions options, Endpoint endpoint)
        {
            return options.ConnectionFactory.Create(endpoint, options.ConnectionConfiguration, options.Log);
        }

        public static async Task<IConsumer> CreateConsumerAsync(this KafkaOptions options, string topicName, int partitionId)
        {
            return new Consumer(topicName, partitionId, await options.CreateRouterAsync(), options.ConsumerConfiguration, false);
        }

        public static async Task<IProducer> CreateProducerAsync(this KafkaOptions options)
        {
            return new Producer(await options.CreateRouterAsync(), options.ProducerConfiguration, false);
        }

        public static Task<Router> CreateRouterAsync(this KafkaOptions options)
        {
            return Router.CreateAsync(
                options.ServerUris,
                options.ConnectionFactory,
                options.ConnectionConfiguration,
                options.RouterConfiguration,
                options.Log);
        }

        #endregion

        #region Producing

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendAsync(this IProducer producer, IEnumerable<Message> messages, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return producer.SendAsync(messages, topicName, partitionId, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<IEnumerable<ProduceResponse.Topic>> SendAsync(this IProducer producer, IEnumerable<Message> messages, string topicName, CancellationToken cancellationToken)
        {
            return producer.SendAsync(messages, topicName, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendAsync(this IProducer producer, Message message, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return producer.SendAsync(message, topicName, partitionId, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<IEnumerable<ProduceResponse.Topic>> SendAsync(this IProducer producer, Message message, string topicName, CancellationToken cancellationToken)
        {
            return producer.SendAsync(message, topicName, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to.</param>
        /// <param name="configuration">The configuration for sending the messages (ie acks, ack Timeout and codec)</param>
        /// <param name="cancellationToken">The token for cancellation</param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendAsync(this IProducer producer, Message message, string topicName, int partitionId, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            return producer.SendAsync(new[] { message }, topicName, partitionId, configuration, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="configuration">The configuration for sending the messages (ie acks, ack Timeout and codec)</param>
        /// <param name="cancellationToken">The token for cancellation</param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<IEnumerable<ProduceResponse.Topic>> SendAsync(this IProducer producer, Message message, string topicName, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            return producer.SendAsync(new[] { message }, topicName, configuration, cancellationToken);
        }

        #endregion

        #region Simple Consuming

        public static Task FetchAsync(this IConsumer consumer, Action<Message> onMessage, CancellationToken cancellationToken, int? batchSize = null)
        {
            return consumer.FetchAsync(batch => {
                foreach (var message in batch.Messages) {
                    onMessage(message);
                }
            }, cancellationToken, batchSize);
        }

        public static Task FetchAsync(this IConsumer consumer, Action<IMessageBatch> onMessages, CancellationToken cancellationToken, int? batchSize = null)
        {
            return consumer.FetchAsync(
                (batch, token) => {
                    onMessages(batch);
                    return Task.FromResult(0);
                }, cancellationToken, batchSize);
        }

        public static Task FetchAsync(this IConsumer consumer, Func<Message, CancellationToken, Task> onMessageAsync, CancellationToken cancellationToken, int? batchSize = null)
        {
            return consumer.FetchAsync(async (batch, token) => {
                foreach (var message in batch.Messages) {
                    await onMessageAsync(message, token).ConfigureAwait(false);
                }
            }, cancellationToken, batchSize);
        }

        public static async Task FetchAsync(this IConsumer consumer, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, CancellationToken cancellationToken, int? batchSize = null)
        {
            await MessageBatch.FetchAsync(
                () => consumer.FetchBatchAsync(cancellationToken, batchSize),
                onMessagesAsync, consumer.Router.Log, cancellationToken).ConfigureAwait(false);
        }

        #endregion

        #region Consuming Group

        public static Task<IGroupConsumer> JoinConsumerGroupAsync(this IRouter router, string groupId, ConsumerProtocolMetadata metadata, CancellationToken cancellationToken)
        {
            return router.JoinConsumerGroupAsync(groupId, ConsumerEncoder.Protocol, metadata, ConsumerConfiguration.Default, ConnectionConfiguration.Defaults.Encoders(), cancellationToken);
        }

        public static Task<IGroupConsumer> JoinConsumerGroupAsync(this IRouter router, string groupId, ConsumerProtocolMetadata metadata, IConsumerConfiguration configuration, IImmutableDictionary<string, IMembershipEncoder> encoders, CancellationToken cancellationToken)
        {
            return router.JoinConsumerGroupAsync(groupId, ConsumerEncoder.Protocol, metadata, configuration, encoders, cancellationToken);
        }

        public static Task<IGroupConsumer> JoinConsumerGroupAsync(this IRouter router, string groupId, string protocolType, IMemberMetadata metadata, IConsumerConfiguration configuration, IImmutableDictionary<string, IMembershipEncoder> encoders, CancellationToken cancellationToken)
        {
            return router.JoinConsumerGroupAsync(groupId, protocolType, new[] { metadata }, configuration, encoders, cancellationToken);
        }

        public static async Task<IGroupConsumer> JoinConsumerGroupAsync(this IRouter router, string groupId, string protocolType, IEnumerable<IMemberMetadata> metadata, IConsumerConfiguration configuration, IImmutableDictionary<string, IMembershipEncoder> encoders, CancellationToken cancellationToken)
        {
            if (!encoders.ContainsKey(protocolType ?? "")) throw new ArgumentOutOfRangeException(nameof(protocolType), $"ProtocolType {protocolType} is unknown");

            var response = await router.JoinGroupAsync(groupId, protocolType, metadata, configuration, cancellationToken);
            return new GroupConsumer(router, groupId, protocolType, response, configuration, encoders);
        }

        public static async Task<IImmutableList<IMessageBatch>> FetchBatchesAsync(this IGroupConsumer consumer, CancellationToken cancellationToken, int? batchSize = null)
        {
            var batches = new List<IMessageBatch>();
            IMessageBatch batch;
            while (!(batch = await consumer.FetchBatchAsync(cancellationToken, batchSize).ConfigureAwait(false)).IsEmpty()) {
                batches.Add(batch);
            }
            return batches.ToImmutableList();
        }

        public static async Task FetchUntilDisposedAsync(this IGroupConsumer consumer, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, CancellationToken cancellationToken, int? batchSize = null)
        {
            try {
                await consumer.FetchAsync(onMessagesAsync, cancellationToken, batchSize);
            } catch (ObjectDisposedException) {
                // ignore
            }
        }

        public static async Task FetchAsync(this IGroupConsumer consumer, Func<IMessageBatch, CancellationToken, Task> onMessagesAsync, CancellationToken cancellationToken, int? batchSize = null)
        {
            var tasks = new List<Task>();
            while (!cancellationToken.IsCancellationRequested) {
                var batches = await consumer.FetchBatchesAsync(cancellationToken, batchSize).ConfigureAwait(false);
                tasks.AddRange(batches.Select(batch => MessageBatch.FetchAsync(batch, onMessagesAsync, consumer.Router.Log, cancellationToken)));
                if (tasks.Count == 0) break;
                await Task.WhenAny(tasks).ConfigureAwait(false);
                tasks = tasks.Where(t => !t.IsCompleted).ToList();
            }
        }

        #endregion

        #region MessageBatch

        public static Task CommitAsync(this IMessageBatch batch, CancellationToken cancellationToken)
        {
            if (batch.Messages.Count == 0) return Task.FromResult(0);

            return batch.CommitAsync(batch.Messages[batch.Messages.Count - 1], cancellationToken);
        }

        public static Task CommitAsync(this IMessageBatch batch, Message message, CancellationToken cancellationToken)
        {
            batch.MarkSuccessful(message);
            return batch.CommitMarkedAsync(cancellationToken);
        }

        public static bool IsEmpty(this IMessageBatch batch)
        {
            return batch?.Messages?.Count == 0;
        }
        
        public static async Task<long> CommitMarkedIgnoringDisposedAsync(this IMessageBatch batch, CancellationToken cancellationToken)
        {
            try {
                return await batch.CommitMarkedAsync(cancellationToken);
            } catch (ObjectDisposedException) {
                // ignore
                return 0;
            }
        }

        #endregion

        #region Router

        /// <exception cref="RoutingException">Thrown if the cached metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="FetchOutOfRangeException">Thrown if the fetch request is not valid.</exception>
        /// <exception cref="TimeoutException">Thrown if there request times out</exception>
        /// <exception cref="ConnectionException">Thrown in case of network error contacting broker (after retries), or if none of the default brokers can be contacted.</exception>
        /// <exception cref="RequestException">Thrown in case of an unexpected error in the request</exception>
        public static async Task<T> SendAsync<T>(this IRouter router, IRequest<T> request, string topicName, int partitionId, CancellationToken cancellationToken, IRequestContext context = null, IRetry retryPolicy = null) where T : class, IResponse
        {
            bool? metadataInvalid = false;
            var routedRequest = new RoutedTopicRequest<T>(request, topicName, partitionId, router.Log);

            return await (retryPolicy ?? router.Configuration.SendRetry).TryAsync(
                async (retryAttempt, elapsed) => {
                    metadataInvalid = await router.RefreshTopicMetadataIfInvalidAsync(topicName, metadataInvalid, cancellationToken).ConfigureAwait(false);
                    await routedRequest.SendAsync(router, cancellationToken, context).ConfigureAwait(false);
                    return routedRequest.MetadataRetryResponse(retryAttempt, out metadataInvalid);
                },
                (ex, retryAttempt, retryDelay) => routedRequest.OnRetry(ex, out metadataInvalid),
                routedRequest.ThrowExtractedException,
                cancellationToken).ConfigureAwait(false);
        }

        /// <exception cref="RoutingException">Thrown if the cached metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="FetchOutOfRangeException">Thrown if the fetch request is not valid.</exception>
        /// <exception cref="TimeoutException">Thrown if there request times out</exception>
        /// <exception cref="ConnectionException">Thrown in case of network error contacting broker (after retries), or if none of the default brokers can be contacted.</exception>
        /// <exception cref="RequestException">Thrown in case of an unexpected error in the request</exception>
        public static async Task<T> SendAsync<T>(this IRouter router, IRequest<T> request, string groupId, CancellationToken cancellationToken, IRequestContext context = null, IRetry retryPolicy = null) where T : class, IResponse
        {
            bool? metadataInvalid = false;
            var routedRequest = new RoutedGroupRequest<T>(request, groupId, router.Log);

            return await (retryPolicy ?? router.Configuration.SendRetry).TryAsync(
                async (retryAttempt, elapsed) => {
                    routedRequest.LogAttempt(retryAttempt);
                    metadataInvalid = await router.RefreshGroupMetadataIfInvalidAsync(groupId, metadataInvalid, cancellationToken).ConfigureAwait(false);
                    await routedRequest.SendAsync(router, cancellationToken, context).ConfigureAwait(false);
                    return routedRequest.MetadataRetryResponse(retryAttempt, out metadataInvalid);
                },
                (ex, retryAttempt, retryDelay) => routedRequest.OnRetry(ex, out metadataInvalid),
                routedRequest.ThrowExtractedException,
                cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">These are best described by <see cref="OffsetsRequest.Topic.timestamp"/></param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetsResponse.Topic>> GetOffsetsAsync(this IRouter router, string topicName, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            return router.GetOffsetsAsync<OffsetsRequest, OffsetsResponse, OffsetsResponse.Topic>(
                topicName,
                partitions =>
                    new OffsetsRequest(
                        partitions.Select(
                            _ => new OffsetsRequest.Topic(topicName, _.partition_id, offsetTime, maxOffsets))),
                cancellationToken);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetsResponse.Topic>> GetOffsetsAsync(this IRouter router, string topicName, CancellationToken cancellationToken)
        {
            return router.GetOffsetsAsync(topicName, OffsetsRequest.Topic.DefaultMaxOffsets, OffsetsRequest.Topic.LatestTime, cancellationToken);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="partitionId">The partition to get offsets for.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">These are best described by <see cref="OffsetsRequest.Topic.timestamp"/></param>
        /// <param name="cancellationToken"></param>
        public static async Task<OffsetsResponse.Topic> GetOffsetAsync(this IRouter router, string topicName, int partitionId, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            var request = new OffsetsRequest(new OffsetsRequest.Topic(topicName, partitionId, offsetTime, maxOffsets));
            var response = await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            return response.responses.SingleOrDefault(t => t.topic == topicName && t.partition_id == partitionId);
        }

        /// <summary>
        /// Get offsets for a single partitions of a given topic.
        /// </summary>
        public static Task<OffsetsResponse.Topic> GetOffsetAsync(this IRouter router, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return router.GetOffsetAsync(topicName, partitionId, OffsetsRequest.Topic.DefaultMaxOffsets, OffsetsRequest.Topic.LatestTime, cancellationToken);
        }

        /// <summary>
        /// Get group offsets for a single partition of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="partitionId">The partition to get offsets for.</param>
        /// <param name="groupId">The id of the consumer group</param>
        /// <param name="cancellationToken"></param>
        public static async Task<OffsetFetchResponse.Topic> GetOffsetAsync(this IRouter router, string groupId, string topicName, int partitionId, CancellationToken cancellationToken)
        {
            var request = new OffsetFetchRequest(groupId, new TopicPartition(topicName, partitionId));
            var response = await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            return response.responses.SingleOrDefault(t => t.topic == topicName && t.partition_id == partitionId);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        /// <param name="router">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="groupId">The id of the consumer group</param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetFetchResponse.Topic>> GetOffsetsAsync(this IRouter router, string groupId, string topicName, CancellationToken cancellationToken)
        {
            return router.GetOffsetsAsync<OffsetFetchRequest, OffsetFetchResponse, OffsetFetchResponse.Topic>(
                topicName,
                partitions =>
                    new OffsetFetchRequest(
                        groupId, partitions.Select(_ => new OffsetsRequest.Topic(topicName, _.partition_id))),
                cancellationToken);
        }

        /// <summary>
        /// Get offsets for all partitions of a given topic.
        /// </summary>
        private static async Task<IImmutableList<TTopicResponse>> GetOffsetsAsync<TRequest, TResponse, TTopicResponse>(
            this IRouter router, 
            string topicName, 
            Func<IGrouping<int, MetadataResponse.Partition>, TRequest> requestFunc, 
            CancellationToken cancellationToken
            )
            where TRequest : class, IRequest<TResponse>
            where TResponse : class, IResponse<TTopicResponse>
            where TTopicResponse : TopicOffset
        {
            bool? metadataInvalid = false;
            var offsets = new Dictionary<int, TTopicResponse>();
            RoutedTopicRequest<TResponse>[] routedTopicRequests = null;

            return await router.Configuration.SendRetry.TryAsync(
                async (retryAttempt, elapsed) => {
                    metadataInvalid = await router.RefreshTopicMetadataIfInvalidAsync(topicName, metadataInvalid, cancellationToken).ConfigureAwait(false);

                    var topicMetadata = await router.GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false);
                    routedTopicRequests = topicMetadata
                        .partition_metadata
                        .Where(_ => !offsets.ContainsKey(_.partition_id)) // skip partitions already successfully retrieved
                        .GroupBy(x => x.leader)
                        .Select(partitions => 
                            new RoutedTopicRequest<TResponse>(requestFunc(partitions),
                                topicName, 
                                partitions.Select(_ => _.partition_id).First(), 
                                router.Log))
                        .ToArray();

                    await Task.WhenAll(routedTopicRequests.Select(_ => _.SendAsync(router, cancellationToken))).ConfigureAwait(false);
                    var responses = routedTopicRequests.Select(_ => _.MetadataRetryResponse(retryAttempt, out metadataInvalid)).ToArray();
                    foreach (var response in responses.Where(_ => _.IsSuccessful)) {
                        foreach (var offsetTopic in response.Value.responses) {
                            offsets[offsetTopic.partition_id] = offsetTopic;
                        }
                    }

                    return responses.All(_ => _.IsSuccessful) 
                        ? new RetryAttempt<IImmutableList<TTopicResponse>>(offsets.Values.ToImmutableList()) 
                        : RetryAttempt<IImmutableList<TTopicResponse>>.Retry;
                },
                (ex, retryAttempt, retryDelay) => routedTopicRequests.MetadataRetry(ex, out metadataInvalid),
                routedTopicRequests.ThrowExtractedException, 
                cancellationToken).ConfigureAwait(false);
        }

        public static async Task<JoinGroupResponse> JoinGroupAsync(this IRouter router, string groupId, string protocolType, IEnumerable<IMemberMetadata> metadata, IConsumerConfiguration configuration, CancellationToken cancellationToken)
        {
            var protocols = metadata?.Select(m => new JoinGroupRequest.GroupProtocol(m));
            var request = new JoinGroupRequest(groupId, configuration.GroupHeartbeat, null, protocolType, protocols, configuration.GroupRebalanceTimeout);
            var response = await router.SendAsync(request, request.group_id, cancellationToken, new RequestContext(protocolType: request.protocol_type), configuration.GroupCoordinationRetry).ConfigureAwait(false);
            if (response == null || !response.error_code.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }
            return response;
        }

        internal static async Task<ImmutableList<Message>> FetchMessagesAsync(this IRouter router, ImmutableList<Message> existingMessages, string topicName, int partitionId, long offset, IConsumerConfiguration configuration, CancellationToken cancellationToken, int? count = null)
        {
            var extracted = ExtractMessages(existingMessages, offset);
            var fetchOffset = extracted == ImmutableList<Message>.Empty
                ? offset
                : extracted[extracted.Count - 1].Offset + 1;
            var fetched = extracted.Count < count.GetValueOrDefault(configuration.BatchSize)
                ? await router.FetchMessagesAsync(topicName, partitionId, fetchOffset, configuration, cancellationToken).ConfigureAwait(false)
                : ImmutableList<Message>.Empty;

            if (extracted == ImmutableList<Message>.Empty) return fetched;
            if (fetched == ImmutableList<Message>.Empty) return extracted;
            return extracted.AddRange(fetched);
        }

        internal static async Task<ImmutableList<Message>> FetchMessagesAsync(this IRouter router, string topicName, int partitionId, long offset, IConsumerConfiguration configuration, CancellationToken cancellationToken)
        {
            var topic = new FetchRequest.Topic(topicName, partitionId, offset, configuration.MaxPartitionFetchBytes);
            FetchResponse response = null;
            for (var attempt = 1; response == null && attempt <= 12; attempt++) { // at a (minimum) multiplier of 2, this results in a total factor of 256
                var request = new FetchRequest(topic, configuration.MaxFetchServerWait, configuration.MinFetchBytes, configuration.MaxFetchBytes);
                try {
                    response = await router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
                } catch (BufferUnderRunException ex) {
                    if (configuration.FetchByteMultiplier <= 1) throw;
                    var maxBytes = topic.max_bytes * configuration.FetchByteMultiplier;
                    router.Log.Warn(() => LogEvent.Create(ex, $"Retrying Fetch Request with multiplier {Math.Pow(configuration.FetchByteMultiplier, attempt)}, {topic.max_bytes} -> {maxBytes}"));
                    topic = new FetchRequest.Topic(topic.topic, topic.partition_id, topic.fetch_offset, maxBytes);
                }
            }
            return response?.responses?.SingleOrDefault()?.Messages?.ToImmutableList() ?? ImmutableList<Message>.Empty;
        }

        private static ImmutableList<Message> ExtractMessages(ImmutableList<Message> existingMessages, long offset)
        {
            var localIndex = existingMessages.FindIndex(m => m.Offset == offset);
            if (localIndex == 0) return existingMessages;
            if (0 < localIndex) {
                return existingMessages.GetRange(localIndex, existingMessages.Count - (localIndex + 1));
            }
            return ImmutableList<Message>.Empty;
        }

        #endregion
    }
}