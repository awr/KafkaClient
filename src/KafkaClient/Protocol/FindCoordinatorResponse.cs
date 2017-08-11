using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// FindCoordinator Response => *throttle_time_ms error_code *error_message coordinator 
    /// </summary>
    /// <remarks>
    /// FindCoordinator Response => *throttle_time_ms error_code *error_message coordinator 
    ///   throttle_time_ms => INT32
    ///   error_code => INT16
    ///   error_message => NULLABLE_STRING
    ///   coordinator => node_id host port 
    ///     node_id => INT32
    ///     host => STRING
    ///     port => INT32
    /// 
    /// Version 1+: throttle_time_ms
    /// Version 1+: error_message
    /// From http://kafka.apache.org/protocol.html#The_Messages_FindCoordinator
    /// </remarks>
    public class FindCoordinatorResponse : Server, IResponse, IEquatable<FindCoordinatorResponse>
    {
        public override string ToString() => $"{{error_code:{Error},node_id:{Id},host:'{Host}',port:{Port}}}";

        public static FindCoordinatorResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 1);
                var errorCode = (ErrorCode)reader.ReadInt16();
                string errorMessage = null;
                if (context.ApiVersion >= 1) {
                    errorMessage = reader.ReadString();
                }
                var coordinatorId = reader.ReadInt32();
                var coordinatorHost = reader.ReadString();
                var coordinatorPort = reader.ReadInt32();

                return new FindCoordinatorResponse(errorCode, coordinatorId, coordinatorHost, coordinatorPort, throttleTime, errorMessage);
            }
        }

        public FindCoordinatorResponse(ErrorCode errorCode, int coordinatorId, string host, int port, TimeSpan? throttleTime = null, string errorMessage = null)
            : base(coordinatorId, host, port)
        {
            Error = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(Error);
            ErrorMessage = errorMessage;
            ThrottleTime = throttleTime;
        }

        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public ErrorCode Error { get; }

        /// <summary>
        /// Details about the error.
        /// Version: 1+
        /// </summary>
        public string ErrorMessage { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        /// violate any quota.) 
        /// Version: 3+
        /// </summary>
        public TimeSpan? ThrottleTime { get; }

        public IImmutableList<ErrorCode> Errors { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as FindCoordinatorResponse);
        }

        /// <inheritdoc />
        public bool Equals(FindCoordinatorResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Error == other.Error
                && ErrorMessage == other.ErrorMessage
                && ThrottleTime == other.ThrottleTime;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ Error.GetHashCode();
                hashCode = (hashCode * 397) ^ (ErrorMessage?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (ThrottleTime?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
    }
}