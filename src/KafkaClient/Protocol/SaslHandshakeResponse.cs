using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SaslHandshake Response => error_code [enabled_mechanisms] 
    /// </summary>
    /// <remarks>
    /// SaslHandshake Response => error_code [enabled_mechanisms] 
    ///   error_code => INT16
    ///   enabled_mechanisms => STRING
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_SaslHandshake
    /// </remarks>
    public class SaslHandshakeResponse : IResponse, IEquatable<SaslHandshakeResponse>
    {
        public override string ToString() => $"{{error_code:{Error},enabled_mechanisms:[{EnabledMechanisms.ToStrings()}]}}";

        public static SaslHandshakeResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var enabledMechanisms = new string[reader.ReadInt32()];
                for (var m = 0; m < enabledMechanisms.Length; m++) {
                    enabledMechanisms[m] = reader.ReadString();
                }

                return new SaslHandshakeResponse(errorCode, enabledMechanisms);
            }
        }

        public SaslHandshakeResponse(ErrorCode errorCode, IEnumerable<string> enabledMechanisms = null)
        {
            Error = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(Error);
            EnabledMechanisms = enabledMechanisms.ToSafeImmutableList();
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode Error { get; }

        /// <summary>
        /// Array of mechanisms enabled in the server.
        /// </summary>
        public IImmutableList<string> EnabledMechanisms { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SaslHandshakeResponse);
        }

        /// <inheritdoc />
        public bool Equals(SaslHandshakeResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Error == other.Error 
                   && EnabledMechanisms.HasEqualElementsInOrder(other.EnabledMechanisms);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) Error*397) ^ (EnabledMechanisms?.Count.GetHashCode() ?? 0);
            }
        }
        
        #endregion
    }
}