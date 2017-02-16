using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// Provides a high level abstraction for sending messages (batches or otherwise) to a Kafka cluster.
    /// There are several extensions available through <see cref="Extensions"/>, making production simpler. In particular, it is possible 
    /// to select the partition based on the topic metadata and message key by way of a <see cref="IPartitionSelector"/>.
    /// </summary>
    public interface IProducer : IAsyncDisposable
    {
        /// <summary>
        /// Send messages to the given topic.
        /// </summary>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partitionId">The partition to send messages to.</param>
        /// <param name="configuration">The configuration for sending the messages (ie acks, ack Timeout and codec)</param>
        /// <param name="cancellationToken">The token for cancellation</param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        Task<ProduceResponse.Topic> SendAsync(IEnumerable<Message> messages, string topicName, int partitionId, ISendMessageConfiguration configuration, CancellationToken cancellationToken);

        /// <summary>
        /// Send messages to the given topic.
        /// </summary>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="configuration">The configuration for sending the messages (ie acks, ack Timeout and codec)</param>
        /// <param name="cancellationToken">The token for cancellation</param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        Task<IEnumerable<ProduceResponse.Topic>> SendAsync(IEnumerable<Message> messages, string topicName, ISendMessageConfiguration configuration, CancellationToken cancellationToken);

        /// <summary>
        /// The configuration for various limits and for send defaults
        /// </summary>
        IProducerConfiguration Configuration { get; }
    }
}