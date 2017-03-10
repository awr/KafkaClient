using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using Nito.AsyncEx;

namespace KafkaClient
{
    /// <summary>
    /// Provides a simplified high level API for producing messages on a topic.
    /// </summary>
    public class Producer : IProducer
    {
        private readonly bool _leaveRouterOpen;

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount; // = 0;
        private readonly TaskCompletionSource<bool> _disposePromise = new TaskCompletionSource<bool>();

        private readonly AsyncProducerConsumerQueue<ProduceTask> _produceMessageQueue;
        private readonly SemaphoreSlim _produceRequestSemaphore;
        private readonly Task _sendTask;

        private int _sendingMessageCount;
        private ImmutableList<ProduceTask> _batch = ImmutableList<ProduceTask>.Empty;

        /// <summary>
        /// Get the number of messages sitting in the buffer waiting to be sent.
        /// </summary>
        public int BufferedMessageCount => _sendingMessageCount - InFlightMessageCount;

        /// <summary>
        /// Get the number of messages staged for Async Request.
        /// </summary>
        public int InFlightMessageCount => _batch.Sum(p => p.Messages.Count);

        /// <summary>
        /// Get the number of active async threads sending messages.
        /// </summary>
        public int ActiveSenders => Configuration.RequestParallelization - _produceRequestSemaphore.CurrentCount;

        /// <inheritdoc />
        public IRouter Router { get; }

        public IProducerConfiguration Configuration { get; }

        ///  <summary>
        ///  Construct a Producer class.
        ///  </summary>
        ///  <param name="router">The router used to direct produced messages to the correct partition.</param>
        ///  <param name="configuration">The configuration parameters.</param>
        /// <param name="leaveRouterOpen">Whether to dispose the router when the producer is disposed.</param>
        /// <remarks>
        ///  The <see cref="IProducerConfiguration.RequestParallelization"/> parameter provides a mechanism for minimizing the amount of 
        ///  async requests in flight at any one time by blocking the caller requesting the async call. This effectively puts an upper 
        ///  limit on the amount of times a caller can call SendMessagesAsync before the caller is blocked.
        /// 
        ///  The <see cref="IProducerConfiguration.BatchSize"/> parameter provides a way to limit the max amount of memory the driver uses 
        ///  should the send pipeline get overwhelmed and the buffer starts to fill up.  This is an inaccurate limiting memory use as the 
        ///  amount of memory actually used is dependant on the general message size being buffered.
        /// 
        ///  A message will start its timeout countdown as soon as it is added to the producer async queue. If there are a large number of
        ///  messages sitting in the async queue then a message may spend its entire timeout cycle waiting in this queue and never getting
        ///  attempted to send to Kafka before a timeout exception is thrown.
        ///  </remarks>
        public Producer(IRouter router, IProducerConfiguration configuration = null, bool leaveRouterOpen = true)
        {
            _leaveRouterOpen = leaveRouterOpen;
            Router = router;
            Configuration = configuration ?? ProducerConfiguration.Default;
            _produceMessageQueue = new AsyncProducerConsumerQueue<ProduceTask>();
            _produceRequestSemaphore = new SemaphoreSlim(Configuration.RequestParallelization, Configuration.RequestParallelization);
            _sendTask = Task.Run(DedicatedSendAsync, _disposeToken.Token);
        }

        /// <inheritdoc />
        public async Task<ProduceResponse.Topic> SendAsync(IEnumerable<Message> messages, string topicName, int partitionId, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            var produceTopicTask = new ProduceTask(topicName, partitionId, messages, configuration ?? Configuration.SendDefaults, cancellationToken);
            Interlocked.Add(ref _sendingMessageCount, produceTopicTask.Messages.Count);
            try {
                await _produceMessageQueue.EnqueueAsync(produceTopicTask, cancellationToken).ConfigureAwait(false);
                return await produceTopicTask.Tcs.Task.ConfigureAwait(false);
            } catch (InvalidOperationException ex) {
                throw new ObjectDisposedException("Cannot send messages after Stopped or Disposed", ex);
            } finally {
                Interlocked.Add(ref _sendingMessageCount, -produceTopicTask.Messages.Count);
                produceTopicTask.Dispose();
            }
        }

        /// <inheritdoc />
        public async Task<IEnumerable<ProduceResponse.Topic>> SendAsync(IEnumerable<Message> messages, string topicName, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException("Cannot send messages after Stopped or Disposed");

            var topic = await Router.GetTopicMetadataAsync(topicName, cancellationToken);
            var partitionedMessages =
                from message in messages
                group message by Configuration.PartitionSelector.Select(topic, message.Key).partition_id
                into partition
                select new { PartitionId = partition.Key, Messages = partition };

            var sendPartitions = partitionedMessages.Select(p => SendAsync(p.Messages, topicName, p.PartitionId, configuration, cancellationToken)).ToArray();
            await Task.WhenAll(sendPartitions);
            return sendPartitions.Select(p => p.Result);
        }

        private int _activeSenderCount;

        private async Task DedicatedSendAsync()
        {
            // only allow one sender to execute, dump out all other requests
            if (Interlocked.Increment(ref _activeSenderCount) != 1) return;

            Router.Log.Info(() => LogEvent.Create("Started sending from Producer"));
            try {
                while (!_disposeToken.IsCancellationRequested) {
                    _batch = ImmutableList<ProduceTask>.Empty;
                    try {
                        _batch = await GetNextBatchAsync().ConfigureAwait(false);
                        if (_batch.IsEmpty) {
                            if (_disposeCount > 0) {
                                break;
                            }
                        } else {
                            foreach (var codec in new[] {MessageCodec.None, MessageCodec.Gzip, MessageCodec.Snappy}) {
                                var filteredBatch = _batch.Where(_ => _.Codec == codec).ToImmutableList();
                                if (filteredBatch.IsEmpty) continue;

                                Router.Log.Debug(() => LogEvent.Create($"Producer compiled batch({filteredBatch.Count}) on codec {codec}{(_disposeCount == 0 ? "" : ": producer is stopping")}"));
                                var batchToken = filteredBatch[0].CancellationToken;
                                if (filteredBatch.TrueForAll(p => p.CancellationToken == batchToken)) {
                                    using (var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, batchToken)) {
                                        await SendBatchWithCodecAsync(filteredBatch, codec, cancellation.Token).ConfigureAwait(false);
                                    }
                                } else { // they have different cancellations, so we ignore anything other than the main token
                                    await SendBatchWithCodecAsync(filteredBatch, codec, _disposeToken.Token).ConfigureAwait(false);
                                }
                                _batch = _batch.Where(_ => _.Codec != codec).ToImmutableList(); // so the below catch doesn't update tasks that are already completed
                            }
                        }
                    } catch (Exception ex) {
                        Router.Log.Info(() => LogEvent.Create(ex));
                        _batch.ForEach(x => x.Tcs.TrySetException(ex));
                    }
                }
            } catch(Exception ex) { 
                Router.Log.Warn(() => LogEvent.Create(ex, "Error during producer send"));
            } finally {
                await DisposeAsync().ConfigureAwait(false);
                Router.Log.Info(() => LogEvent.Create("Stopped sending from Producer"));
            }
        }

        private async Task<ImmutableList<ProduceTask>> GetNextBatchAsync()
        {
            var batch = ImmutableList<ProduceTask>.Empty;
            try {
                if (await _produceMessageQueue.OutputAvailableAsync(_disposeToken.Token).ConfigureAwait(false)) {
                    using (var cancellation = new TimedCancellation(_disposeToken.Token, Configuration.BatchMaxDelay)) {
                        var messageCount = 0;
                        while (messageCount < Configuration.BatchSize && !cancellation.Token.IsCancellationRequested) {
                            // Try rather than simply Take (in case the collection has been closed and is not empty)
                            var result = await _produceMessageQueue.DequeueAsync(cancellation.Token).ConfigureAwait(false);

                            if (result.CancellationToken.IsCancellationRequested) {
                                result.Tcs.SetCanceled();
                            } else {
                                batch = batch.Add(result);
                                messageCount += result.Messages.Count;
                            }
                        }
                    }
                }
            } catch (InvalidOperationException) { // From DequeueAsync
            } catch (OperationCanceledException) { // cancellation token fired while attempting to get tasks: normal behavior
            }
            return batch;
        }

        private async Task SendBatchWithCodecAsync(IImmutableList<ProduceTask> produceTasks, MessageCodec codec, CancellationToken cancellationToken)
        {
            await Router.GetTopicMetadataAsync(produceTasks.Select(m => m.Partition.topic), cancellationToken).ConfigureAwait(false);

            // we must send a different produce request for each ack level and timeout combination.
            // we must also send requests to the correct server
            var endpointGroups = produceTasks.Select(
                produceTask => new {
                    ProduceTask = produceTask,
                    TopicConnection = Router.GetTopicConnection(produceTask.Partition.topic, produceTask.Partition.partition_id)
                })
                .GroupBy(_ => new {_.ProduceTask.Acks, _.ProduceTask.AckTimeout, _.TopicConnection.Connection.Endpoint});

            var sendBatches = new List<ProduceTaskBatch>();
            foreach (var endpointGroup in endpointGroups) {
                var produceTasksByTopic = endpointGroup
                    .GroupBy(_ => new TopicPartition(_.TopicConnection.TopicName, _.TopicConnection.PartitionId))
                    .ToImmutableDictionary(g => g.Key, g => g.Select(_ => _.ProduceTask).ToImmutableList());
                var messageCount = produceTasksByTopic.Values.Sum(_ => _.Count);
                var payloads = produceTasksByTopic.Select(p => new ProduceRequest.Topic(p.Key.topic, p.Key.partition_id, p.Value.SelectMany(_ => _.Messages), codec));
                var request = new ProduceRequest(payloads, endpointGroup.Key.AckTimeout, endpointGroup.Key.Acks);
                Router.Log.Debug(() => LogEvent.Create($"Produce request for topics{request.topics.Aggregate("", (buffer, p) => $"{buffer} {p}")} with {messageCount} messages"));

                var connection = endpointGroup.Select(_ => _.TopicConnection).First().Connection; // they'll all be the same since they're grouped by this
                // TODO: what about retryability like for the router?? Need this to be robust to node failures
                var sendGroupTask = _produceRequestSemaphore.LockAsync(() => connection.SendAsync(request, cancellationToken), cancellationToken);
                sendBatches.Add(new ProduceTaskBatch(connection.Endpoint, endpointGroup.Key.Acks, sendGroupTask, produceTasksByTopic));
            }

            try {
                await Task.WhenAll(sendBatches.Select(batch => batch.ReceiveTask)).ConfigureAwait(false);
            } catch (Exception ex) {
                Router.Log.Error(LogEvent.Create(ex));
            }
            await SetResult(sendBatches).ConfigureAwait(false);
        }

        private async Task SetResult(IEnumerable<ProduceTaskBatch> batches)
        {
            foreach (var batch in batches) {
                try {
                    var batchResult = await batch.ReceiveTask.ConfigureAwait(false); // await triggers exceptions correctly
                    if (batch.Acks == 0 && batchResult == null) {
                        foreach (var topic in batch.ProduceTasksByTopic.Keys) {
                            foreach (var produceTask in batch.ProduceTasksByTopic[topic]) {
                                produceTask.Tcs.SetResult(new ProduceResponse.Topic(topic.topic, topic.partition_id, ErrorCode.NONE, -1));
                            }
                        }
                        return;
                    }
                    var resultTopics = batchResult.responses.ToImmutableDictionary(t => new TopicPartition(t.topic, t.partition_id));
                    Router.Log.Debug(() => LogEvent.Create($"Produce response for topics{resultTopics.Keys.Aggregate("", (buffer, p) => $"{buffer} {p}")}"));

                    foreach (var topic in batch.ProduceTasksByTopic.Keys.Except(resultTopics.Keys)) {
                        Router.Log.Warn(() => LogEvent.Create($"No response included for produce batch topic/{topic.topic}/partition/{topic.partition_id} on {batch.Endpoint}"));
                        foreach (var produceTask in batch.ProduceTasksByTopic[topic]) {
                            produceTask.Tcs.SetResult(null);
                        }
                    }

                    foreach (var pair in resultTopics) {
                        var topic = pair.Value;
                        if (!batch.ProduceTasksByTopic.TryGetValue(pair.Key, out ImmutableList<ProduceTask> produceTasks)) {
                            Router.Log.Error(LogEvent.Create($"Extra response to produce batch topic/{topic.topic}/partition/{topic.partition_id} on {batch.Endpoint}"));
                            continue;
                        }

                        var messageCount = produceTasks.Sum(t => t.Messages.Count);
                        foreach (var produceTask in produceTasks) {
                            produceTask.Tcs.SetResult(batch.Acks == 0
                                ? new ProduceResponse.Topic(topic.topic, topic.partition_id, ErrorCode.NONE, -1)
                                : new ProduceResponse.Topic(topic.topic, topic.partition_id, topic.error_code, topic.base_offset + messageCount - 1, topic.timestamp));
                        }
                    }
                } catch (Exception ex) {
                    Router.Log.Error(LogEvent.Create(ex, $"failed to send batch to {batch.Endpoint} with acks {batch.Acks}"));
                    foreach (var produceTask in batch.ProduceTasksByTopic.Values.SelectMany(_ => _)) {
                        produceTask.Tcs.TrySetException(ex);
                    }
                }
            }
        }

        public async Task DisposeAsync()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) {
                await _disposePromise.Task;
                return;
            }

            try {
                Router.Log.Debug(() => LogEvent.Create("Disposing Producer"));
                _produceMessageQueue.CompleteAdding(); // block incoming data
                await Task.WhenAny(_sendTask, Task.Delay(Configuration.StopTimeout)).ConfigureAwait(false);
                _disposeToken.Cancel();

                using (_disposeToken) {
                    if (!_leaveRouterOpen) {
                        using (Router) {
                        }
                    }
                    _produceRequestSemaphore.Dispose();
                }
            } finally {
                _disposePromise.TrySetResult(true);
            }
        }

        /// <summary>
        /// Stops the producer from accepting new messages, allows in-flight messages to be sent before returning.
        /// </summary>
        public void Dispose()
        {
#pragma warning disable 4014
            // trigger, and set the promise appropriately
            DisposeAsync();
#pragma warning restore 4014
        }

        private class ProduceTask : CancellableTask<ProduceResponse.Topic>
        {
            public ProduceTask(string topicName, int partitionId, IEnumerable<Message> messages, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
                : base(cancellationToken)
            {
                Partition = new TopicPartition(topicName, partitionId);
                Messages = messages as IList<Message> ?? messages.ToList();
                Codec = configuration.Codec;
                Acks = configuration.Acks;
                AckTimeout = configuration.AckTimeout;
            }

            // where
            public TopicPartition Partition { get; }

            // what
            public IList<Message> Messages { get; }
            public MessageCodec Codec { get; }

            // confirmation
            public short Acks { get; }
            public TimeSpan AckTimeout { get; }
        }

        private class ProduceTaskBatch
        {
            public ProduceTaskBatch(Endpoint endpoint, short acks, Task<ProduceResponse> receiveTask, IImmutableDictionary<TopicPartition, ImmutableList<ProduceTask>> produceTasksByTopic)
            {
                Endpoint = endpoint;
                Acks = acks;
                ReceiveTask = receiveTask;
                ProduceTasksByTopic = produceTasksByTopic;
            }

            public Endpoint Endpoint { get; }
            public short Acks { get; }
            public Task<ProduceResponse> ReceiveTask { get; }
            public IImmutableDictionary<TopicPartition, ImmutableList<ProduceTask>> ProduceTasksByTopic { get; }
        }
    }
}