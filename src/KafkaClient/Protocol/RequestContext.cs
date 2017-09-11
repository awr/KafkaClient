using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Telemetry;

namespace KafkaClient.Protocol
{
    public class RequestContext : IRequestContext
    {
        public static string DefaultClientId = "KC"; // This gets sent on every request, so may as well shorten "KafkaClient" to "KC"

        public static RequestContext Copy(IRequestContext original, int correlationId, short? version = null, string clientId = null, IImmutableDictionary<string, IMembershipEncoder> encoders = null, string protocolType = null, ProduceRequestMessages onProduceRequestMessages = null)
        {
            return new RequestContext(correlationId, original?.ApiVersion ?? version, original?.ClientId ?? clientId, original?.Encoders ?? encoders, original?.ProtocolType ?? protocolType, original?.OnProduceRequestMessages ?? onProduceRequestMessages);
        }

        public RequestContext(int? correlationId = null, short? version = null, string clientId = null, IImmutableDictionary<string, IMembershipEncoder> encoders = null, string protocolType = null, ProduceRequestMessages onProduceRequestMessages = null)
        {
            CorrelationId = correlationId.GetValueOrDefault(1);
            ApiVersion = version;
            ClientId = clientId ?? DefaultClientId;
            Encoders = encoders;
            ProtocolType = protocolType;
            OnProduceRequestMessages = onProduceRequestMessages;
        }

        /// <summary>
        /// Descriptive name of the source of the messages sent to kafka
        /// </summary>
        public string ClientId { get; }

        /// <summary>
        /// Value supplied will be passed back in the response by the server unmodified.
        /// It is useful for matching request and response between the client and server.
        /// </summary>
        public int CorrelationId { get; }

        /// <summary>
        /// This is a numeric version number for the api request. It allows the server to 
        /// properly interpret the request as the protocol evolves. Responses will always 
        /// be in the format corresponding to the request version.
        /// </summary>
        public short? ApiVersion { get; }

        /// <inheritdoc />
        public IImmutableDictionary<string, IMembershipEncoder> Encoders { get; }

        /// <inheritdoc />
        public string ProtocolType { get; }

        /// <inheritdoc />
        public ProduceRequestMessages OnProduceRequestMessages { get; }

        public void ThrowIfCountTooBig(int size, bool byteSize = false)
        {
            if (byteSize) {
                if (size > MaxByteSize) throw new BufferUnderRunException($"Cannot allocate an array of size {size} bytes (> {MaxByteSize}). Configure this through {nameof(RequestContext)}.{nameof(MaxByteSize)}.");
                return;
            }
            if (size > MaxArraySize) throw new BufferUnderRunException($"Cannot allocate an array of size {size} (> {MaxArraySize}). Configure this through {nameof(RequestContext)}.{nameof(MaxArraySize)}.");
        }

        public static int MaxArraySize = 1000;
        public static int MaxByteSize = 1000000;

        public override string ToString() => $"{{id:{CorrelationId},version:{ApiVersion},client:{ClientId}}}";
    }
}