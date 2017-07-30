using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Heartbeat Response => error_code 
    ///   error_code => INT16
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class HeartbeatResponse : IResponse, IEquatable<HeartbeatResponse>
    {
        public override string ToString() => $"{{error_code:{error_code}}}";

        public static HeartbeatResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var errorCode = (ErrorCode)reader.ReadInt16();
                return new HeartbeatResponse(errorCode);
            }            
        }

        public HeartbeatResponse(ErrorCode errorCode)
        {
            error_code = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(error_code);
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode error_code { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as HeartbeatResponse);
        }

        /// <inheritdoc />
        public bool Equals(HeartbeatResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return error_code == other.error_code;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return (int) error_code;
        }

        #endregion
    }
}