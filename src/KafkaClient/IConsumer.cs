using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient
{
    public interface IConsumer : IAsyncDisposable
    {
        Task<IMessageBatch> FetchAsync(CancellationToken cancellationToken, int? batchSize = null);

        /// <summary>
        /// The configuration for various limits and for consume defaults
        /// </summary>
        IConsumerConfiguration Configuration { get; }

        IRouter Router { get; }
    }
}