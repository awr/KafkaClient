using System;

namespace KafkaClient.Protocol
{
    public abstract class Request : IRequest, IEquatable<Request>
    {
        protected Request(ApiKey apiKey, bool expectResponse = true)
        {
            ApiKey = apiKey;
            ExpectResponse = expectResponse;
        }

        /// <inheritdoc />
        public ApiKey ApiKey { get; }

        /// <inheritdoc />
        public bool ExpectResponse { get; }

        public override string ToString() => $"{{{this.RequestToString()}}}";

        public virtual string ShortString() => ApiKey.ToString();

        public ArraySegment<byte> ToBytes(IRequestContext context)
        {
            using (var writer = EncodeHeader(context, this)) {
                EncodeBody(writer, context);
                return writer.ToSegment();
            }
        }

        public const int IntegerByteSize = 4;
        public const int CorrelationSize = IntegerByteSize;

        /// <summary>
        /// Encode the common header for kafka request.
        /// see http://kafka.apache.org/protocol.html#protocol_messages
        /// </summary>
        /// <remarks>
        /// Request Header => api_key api_version correlation_id client_id 
        ///  api_key => INT16             -- The id of the request type.
        ///  api_version => INT16         -- The version of the API.
        ///  correlation_id => INT32      -- A user-supplied integer value that will be passed back with the response.
        ///  client_id => NULLABLE_STRING -- A user specified identifier for the client making the request.
        /// </remarks>
        private static IKafkaWriter EncodeHeader(IRequestContext context, IRequest request)
        {
            return new KafkaWriter()
                .Write((short)request.ApiKey)
                .Write(context.ApiVersion.GetValueOrDefault())
                .Write(context.CorrelationId)
                .Write(context.ClientId);
        }

        protected virtual void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
        }

        #region Equals

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as Request);
        }

        /// <inheritdoc />
        public bool Equals(Request other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ApiKey == other.ApiKey;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return ApiKey.GetHashCode();
        }

        #endregion
    }
}