using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient
{
    /// <summary>
    /// Provides a mechanism for fetching batches of messages, possibly from many topics and partitions. 
    /// There are many extensions available through <see cref="Extensions"/>, making consumption simpler.
    /// </summary>
    public interface IConsumer : IAsyncDisposable
    {
        Task<IMessageBatch> FetchAsync(CancellationToken cancellationToken, int? batchSize = null);

        /// <summary>
        /// The configuration for various limits and for consume defaults
        /// </summary>
        IConsumerConfiguration Configuration { get; }

        IRouter Router { get; }

        bool AutoConsume { get; }
    }
}