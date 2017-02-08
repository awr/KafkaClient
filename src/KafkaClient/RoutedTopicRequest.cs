using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    internal class RoutedTopicRequest<T> : RoutedRequest<T> 
        where T : class, IResponse
    {
        public RoutedTopicRequest(IRequest<T> request, string topicName, int partitionId, ILog log)
            : base(request, log)
        {
            _topicName = topicName;
            _partitionId = partitionId;
        }

        private readonly string _topicName;
        private readonly int _partitionId;

        protected override async Task<IConnection> GetConnectionAsync(IRouter router, CancellationToken cancellationToken)
        {
            var topicConnection = await router.GetTopicConnectionAsync(_topicName, _partitionId, cancellationToken).ConfigureAwait(false);
            return topicConnection?.Connection;
        }
    }
}