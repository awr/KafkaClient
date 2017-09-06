using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// EndTxn Response => throttle_time_ms error_code 
    /// </summary>
    /// <remarks>
    /// EndTxn Response => throttle_time_ms error_code 
    ///   throttle_time_ms => INT32
    ///   error_code => INT16
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_EndTxn
    /// </remarks>
    public class EndTxnResponse : ThrottledResponse, IResponse, IEquatable<EndTxnResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},error_code:{Error}}}";

        public static EndTxnResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();
                var errorCode = (ErrorCode) reader.ReadInt16();

                return new EndTxnResponse(throttleTime.Value, errorCode);
            }
        }

        public EndTxnResponse(TimeSpan throttleTime, ErrorCode errorCode)
            : base(throttleTime)
        {
            Error = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(Error);
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode Error { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as EndTxnResponse);
        }

        /// <inheritdoc />
        public bool Equals(EndTxnResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Error == other.Error;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Error.GetHashCode();
                return hashCode;
            }
        }

        #endregion
    }
}