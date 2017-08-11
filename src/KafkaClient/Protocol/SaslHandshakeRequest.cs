using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SaslHandshake Request => mechanism 
    /// </summary>
    /// <remarks>
    /// SaslHandshake Request => mechanism 
    ///   mechanism => STRING
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_SaslHandshake
    /// </remarks>
    public class SaslHandshakeRequest : Request, IRequest<SaslHandshakeResponse>, IEquatable<SaslHandshakeRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},mechanism:{Mechanism}}}";

        public SaslHandshakeResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => SaslHandshakeResponse.FromBytes(context, bytes);

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(Mechanism);
        }

        public SaslHandshakeRequest(string mechanism)
            : base(ApiKey.SaslHandshake)
        {
            Mechanism = mechanism;
        }

        /// <summary>
        /// SASL Mechanism chosen by the client.
        /// </summary>
        public string Mechanism { get; }

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
            return base.Equals(other) && string.Equals(Mechanism, other.Mechanism);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (Mechanism?.GetHashCode() ?? 0);
            }
        }
        
        #endregion
    }
}