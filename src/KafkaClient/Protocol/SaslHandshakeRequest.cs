using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SaslHandshake Request (Version: 0) => mechanism 
    ///   mechanism => STRING
    /// </summary>
    public class SaslHandshakeRequest : Request, IRequest<SaslHandshakeResponse>, IEquatable<SaslHandshakeRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},mechanism:{mechanism}}}";

        public SaslHandshakeResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => SaslHandshakeResponse.FromBytes(context, bytes);

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(mechanism);
        }

        public SaslHandshakeRequest(string mechanism)
            : base(ApiKey.SaslHandshake)
        {
            this.mechanism = mechanism;
        }

        /// <summary>
        /// SASL Mechanism chosen by the client.
        /// </summary>
        public string mechanism { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SaslHandshakeRequest);
        }

        /// <inheritdoc />
        public bool Equals(SaslHandshakeRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) && string.Equals(mechanism, other.mechanism);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (mechanism?.GetHashCode() ?? 0);
            }
        }
        
        #endregion
    }
}