using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class Consumer : IConsumer
    {
        private int _disposeCount; // = 0;
        private readonly TaskCompletionSource<bool> _disposePromise = new TaskCompletionSource<bool>();
        private readonly bool _leaveRouterOpen;

        public Consumer(IRouter router, IConsumerConfiguration configuration = null, IImmutableDictionary<string, IMembershipEncoder> encoders = null, bool leaveRouterOpen = true)
        {
            Router = router;
            _leaveRouterOpen = leaveRouterOpen;
            Configuration = configuration ?? new ConsumerConfiguration();
            Encoders = encoders ?? ConnectionConfiguration.Defaults.Encoders();
        }

        public IImmutableDictionary<string, IMembershipEncoder> Encoders { get; }

        public IConsumerConfiguration Configuration { get; }

        public IRouter Router { get; }

        /// <inheritdoc />
        public async Task<IMessageBatch> FetchBatchAsync(string topicName, int partitionId, long offset, CancellationToken cancellationToken, int? batchSize = null)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Consumer));
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            var messages = await Router.FetchBatchAsync(ImmutableList<Message>.Empty, topicName, partitionId, offset, Configuration, cancellationToken, batchSize).ConfigureAwait(false);
            return new MessageBatch(messages, new TopicPartition(topicName, partitionId), offset, Router, Configuration, batchSize);
        }

        /// <inheritdoc />
        public async Task<IMessageBatch> FetchBatchAsync(string groupId, string memberId, int generationId, string topicName, int partitionId, CancellationToken cancellationToken, int? batchSize = null)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Consumer));

            var currentOffset = await Router.GetOffsetAsync(groupId, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            var offset = currentOffset.offset + 1;
            var messages = await Router.FetchBatchAsync(ImmutableList<Message>.Empty, topicName, partitionId, offset, Configuration, cancellationToken, batchSize).ConfigureAwait(false);
            return new MessageBatch(messages, new TopicPartition(topicName, partitionId), offset, Router, Configuration, batchSize, groupId, memberId, generationId);
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

        public async Task<IConsumerMember> JoinGroupAsync(string groupId, string protocolType, IEnumerable<IMemberMetadata> metadata, CancellationToken cancellationToken)
        {
            if (_disposeCount > 0) throw new ObjectDisposedException(nameof(Consumer));
            if (!Encoders.ContainsKey(protocolType ?? "")) throw new ArgumentOutOfRangeException(nameof(metadata), $"ProtocolType {protocolType} is unknown");

            var response = await Router.JoinGroupAsync(groupId, protocolType, metadata, Configuration, cancellationToken);
            return new ConsumerMember(Router, groupId, protocolType, response, Configuration, Encoders);
        }
    }
}