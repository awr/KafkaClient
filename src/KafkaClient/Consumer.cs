using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class Consumer : IConsumer
    {
        private int _disposeCount; // = 0;
        private readonly TaskCompletionSource<bool> _disposePromise = new TaskCompletionSource<bool>();
        private readonly IImmutableList<TopicPartition> _topicPartitions;
        private readonly bool _leaveRouterOpen;
        private int _fetchCount; // = 0;

        public Consumer(string topicName, int partitionId, IRouter router, IConsumerConfiguration configuration = null, bool? leaveRouterOpen = null, bool? autoConsume = null)
            : this(new TopicPartition(topicName, partitionId), router, configuration, leaveRouterOpen, autoConsume)
        {
        }

        public Consumer(TopicPartition topicPartition, IRouter router, IConsumerConfiguration configuration = null, bool? leaveRouterOpen = null, bool? autoConsume = null)
            : this(new []{ topicPartition }, router, configuration, leaveRouterOpen, autoConsume)
        {
        }

        public Consumer(IEnumerable<TopicPartition> partitions, IRouter router, IConsumerConfiguration configuration = null, bool? leaveRouterOpen = null, bool? autoConsume = null)
        {
            Router = router;
            _topicPartitions = ImmutableList<TopicPartition>.Empty.AddNotNullRange(partitions);
            _leaveRouterOpen = leaveRouterOpen.GetValueOrDefault(true);
            AutoConsume = autoConsume.GetValueOrDefault(true);
            Configuration = configuration ?? ConsumerConfiguration.Default;
        }

        public IConsumerConfiguration Configuration { get; }

        public IRouter Router { get; }

        public bool AutoConsume { get; }

        /// <inheritdoc />
        public async Task<IMessageBatch> FetchAsync(CancellationToken cancellationToken, int? batchSize = null)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Consumer));
            var index = Interlocked.Increment(ref _fetchCount) - 1;
            if (index >= _topicPartitions.Count) return MessageBatch.Empty;

            var topicPartition = _topicPartitions[index];
            var currentOffset = topicPartition as TopicOffset 
                ?? await Router.GetOffsetsAsync(topicPartition.topic, topicPartition.partition_id, cancellationToken);
            var offset = currentOffset.offset;
            var messages = await Router.FetchMessagesAsync(ImmutableList<Message>.Empty, topicPartition.topic, topicPartition.partition_id, offset, Configuration, cancellationToken, batchSize).ConfigureAwait(false);
            return new MessageBatch(messages, topicPartition, offset, Router, Configuration, AutoConsume, batchSize);
        }

        public async Task DisposeAsync()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) {
                await _disposePromise.Task;
                return;
            }

            try {
                Router.Log.Debug(() => LogEvent.Create("Disposing Consumer"));
                if (_leaveRouterOpen) return;
                await Router.DisposeAsync();
            } finally {
                _disposePromise.TrySetResult(true);
            }
        }

        public void Dispose()
        {
#pragma warning disable 4014
            // trigger, and set the promise appropriately
            DisposeAsync();
#pragma warning restore 4014
        }
    }
}