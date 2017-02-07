using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient
{
    public interface IConsumer : IAsyncDisposable
    {
        /// <summary>
        /// Explicit fetch for topic/partition. This does not use consumer groups.
        /// </summary>
        Task<IMessageBatch> FetchBatchAsync(string topicName, int partitionId, long offset, CancellationToken cancellationToken, int? batchSize = null);

        /// <summary>
        /// The configuration for various limits and for consume defaults
        /// </summary>
        IConsumerConfiguration Configuration { get; }

        IImmutableDictionary<string, IMembershipEncoder> Encoders { get; }

        IRouter Router { get; }
    }
}