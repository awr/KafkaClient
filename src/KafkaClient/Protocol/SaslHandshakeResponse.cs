using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SaslHandshake Response (Version: 0) => error_code [enabled_mechanisms] 
    ///   error_code => INT16
    ///   enabled_mechanisms => STRING -- Array of mechanisms enabled in the server.
    /// </summary>
    public class SaslHandshakeResponse : IResponse, IEquatable<SaslHandshakeResponse>
    {
        public override string ToString() => $"{{error_code:{error_code},enabled_mechanisms:[{enabled_mechanisms.ToStrings()}]}}";

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
            error_code = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(error_code);
            enabled_mechanisms = ImmutableList<string>.Empty.AddNotNullRange(enabledMechanisms);
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode error_code { get; }

        /// <summary>
        /// Array of mechanisms enabled in the server.
        /// </summary>
        public IImmutableList<string> enabled_mechanisms { get; }

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
            return error_code == other.error_code 
                   && enabled_mechanisms.HasEqualElementsInOrder(other.enabled_mechanisms);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) error_code*397) ^ (enabled_mechanisms?.Count.GetHashCode() ?? 0);
            }
        }
        
        #endregion
    }
}