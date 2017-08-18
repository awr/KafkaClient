using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// CreateAcls Response => throttle_time_ms [creation_responses] 
    /// </summary>
    /// <remarks>
    /// CreateAcls Response => throttle_time_ms [creation_responses] 
    ///   throttle_time_ms => INT32
    ///   creation_responses => error_code error_message 
    ///     error_code => INT16
    ///     error_message => NULLABLE_STRING
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_CreateAcls
    /// </remarks>
    public class CreateAclsResponse : ThrottledResponse, IResponse, IEquatable<CreateAclsResponse>
    {
        public override string ToString() => $"{{throttle_time_ms:{ThrottleTime},responses:{Responses.ToStrings()}}}";

        public static CreateAclsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime();

                var responses = new ErrorResponse[reader.ReadInt32()];
                for (var r = 0; r < responses.Length; r++ ) {
                    var errorCode = (ErrorCode) reader.ReadInt16();
                    var errorMessage = reader.ReadString();
                    responses[r] = new ErrorResponse(errorCode, errorMessage);
                }
                return new CreateAclsResponse(throttleTime.Value, responses);
            }
        }

        public CreateAclsResponse(TimeSpan throttleTime, IEnumerable<ErrorResponse> responses = null)
            : base(throttleTime)
        {
            Responses = ImmutableList<ErrorResponse>.Empty.AddNotNullRange(responses);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Responses.Select(r => r.Error));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<ErrorResponse> Responses { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as CreateAclsResponse);
        }

        /// <inheritdoc />
        public bool Equals(CreateAclsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Responses.HasEqualElementsInOrder(other.Responses);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ Responses.Count.GetHashCode();
                return hashCode;
            }
        }

        #endregion

        public class ErrorResponse : IEquatable<ErrorResponse>
        {
            public ErrorResponse(ErrorCode errorCode, string errorMessage)
            {
                Error = errorCode;
                ErrorMessage = errorMessage;
            }

            /// <summary>
            /// The error code.
            /// </summary>
            public ErrorCode Error { get; }

            /// <summary>
            /// The error message.
            /// </summary>
            public string ErrorMessage { get; }
            
            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as ErrorResponse);
            }

            /// <inheritdoc />
            public bool Equals(ErrorResponse other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Error == other.Error
                    && string.Equals(ErrorMessage, other.ErrorMessage);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = Error.GetHashCode();
                    hashCode = (hashCode*397) ^ (ErrorMessage?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }
    }
}