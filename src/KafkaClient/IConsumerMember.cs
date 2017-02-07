using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient
{
    public interface IConsumerMember : IAsyncDisposable
    {
        string GroupId { get; }
        string MemberId { get; }
        int GenerationId { get; }
        bool IsLeader { get; }
        string ProtocolType { get; }

        ILog Log { get; }

        /// <summary>
        /// Fetch messages for this consumer group's current assignment.
        /// Messages are collected together in a batch per assignment. Each batch can be used to get available messages, 
        /// commit offsets and get subsequent batches on the given topic/partition. Once the topic/partition is reassigned, the batch will be disposed.
        /// 
        /// Subsequent calls to this function will result in new batches for each assignment. Once all active assignments have been given,
        /// the <see cref="MessageBatch.Empty"/> result will be used as an indication of nothing being currently available.
        /// </summary>
        Task<IMessageBatch> FetchBatchAsync(CancellationToken cancellationToken, int? batchSize = null);
    }
}