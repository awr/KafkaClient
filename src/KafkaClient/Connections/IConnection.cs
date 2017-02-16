using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Connections
{
    /// <summary>
    /// Provides async methods to send data to a kafka server. It uses a persistent connection, and interleaves requests and responses. 
    /// 
    /// The send method internally uses the <see cref="ITransport"/> abstraction to allow for either direct tcp socket access, or ssl 
    /// stream access (when ssl is configured). Tcp reconnection is coordinated between the <see cref="IConnection"/> and the 
    /// <see cref="ITransport"/>, based on the configuration settings for ssl.
    /// </summary>
    public interface IConnection : IAsyncDisposable
    {
        /// <summary>
        /// The unique ip/port endpoint of this connection.
        /// </summary>
        Endpoint Endpoint { get; }

        /// <summary>
        /// Visibility of state, so a container can manage whether to keep in the pool
        /// </summary>
        bool IsDisposed { get; }

        /// <summary>
        /// Send a specific IRequest to the connected endpoint.
        /// </summary>
        /// <typeparam name="T">The type of the KafkaResponse expected from the request being sent.</typeparam>
        /// <param name="request">The Request to send to the connected endpoint.</param>
        /// <param name="cancellationToken">The token for cancelling the send request.</param>
        /// <param name="context">The context for the request.</param>
        /// <returns>Task representing the future responses from the sent request.</returns>
        Task<T> SendAsync<T>(IRequest<T> request, CancellationToken cancellationToken, IRequestContext context = null) where T : class, IResponse;
    }
}