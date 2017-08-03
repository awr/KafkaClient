using System;
using System.Collections.Immutable;
using KafkaClient.Assignment;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SyncGroup Response => *throttle_time_ms error_code member_assignment 
    /// </summary>
    /// <remarks>
    /// SyncGroup Response => *throttle_time_ms error_code member_assignment 
    ///   throttle_time_ms => INT32
    ///   error_code => INT16
    ///   member_assignment => BYTES
    /// 
    /// Version 1+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_SyncGroup
    /// </remarks>
    public class SyncGroupResponse : IResponse, IEquatable<SyncGroupResponse>
    {
        public override string ToString() => $"{{error_code:{Error},member_assignment:{MemberAssignment}}}";

        public static SyncGroupResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 1);
                var errorCode = (ErrorCode)reader.ReadInt16();

                var encoder = context.GetEncoder();
                var memberAssignment = encoder.DecodeAssignment(reader);
                return new SyncGroupResponse(errorCode, memberAssignment, throttleTime);
            }
        }

        public SyncGroupResponse(ErrorCode errorCode, IMemberAssignment memberAssignment, TimeSpan? throttleTime = null)
        {
            Error = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(Error);
            MemberAssignment = memberAssignment;
            ThrottleTime = throttleTime;
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode Error { get; }
        public IMemberAssignment MemberAssignment { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. (Zero if the request did not 
        /// violate any quota.) 
        /// Version: 1+
        /// </summary>
        public TimeSpan? ThrottleTime { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SyncGroupResponse);
        }

        /// <inheritdoc />
        public bool Equals(SyncGroupResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Error == other.Error
                && ThrottleTime == other.ThrottleTime
                && Equals(MemberAssignment, other.MemberAssignment);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Error.GetHashCode();
                hashCode = (hashCode * 397) ^ (ThrottleTime?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (MemberAssignment?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
        
        #endregion
    }
}