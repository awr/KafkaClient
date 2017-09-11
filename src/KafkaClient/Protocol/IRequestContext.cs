using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Telemetry;

namespace KafkaClient.Protocol
{
    public interface IRequestContext
    {
        /// <summary>
        /// An identifier for the client making the request. Used to log errors, calculate aggregates for monitoring, etc.
        /// </summary>
        string ClientId { get; }

        /// <summary>
        /// Id which will be echoed back by Kafka unchanged to correlate responses to this request.
        /// </summary>
        int CorrelationId { get; }

        /// <summary>
        /// This is a numeric version number for the api request. It allows the server to properly interpret the request as the protocol evolves. Responses will always be in the format corresponding to the request version.
        /// </summary>
        short? ApiVersion { get; }

        /// <summary>
        /// Custom Encoding support for different protocol types
        /// </summary>
        IImmutableDictionary<string, IMembershipEncoder> Encoders { get; }

        /// <summary>
        /// The protocol type, used for custom <see cref="IMembershipEncoder"/>
        /// </summary>
        string ProtocolType { get; }

        /// <summary>
        /// Triggered when encoding ProduceRequest messages.
        /// </summary>
        ProduceRequestMessages OnProduceRequestMessages { get; }

        /// <summary>
        /// Check for scenarios where the count from the server is too big and may cause a memory issue.
        /// </summary>
        void ThrowIfCountTooBig(int size, bool byteSize = false);
    }
}