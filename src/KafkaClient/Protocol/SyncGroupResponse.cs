using System;
using System.Collections.Immutable;
using KafkaClient.Assignment;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SyncGroup Response => error_code member_assignment
    ///   error_code => INT16
    ///   member_assignment => BYTES
    /// 
    /// see https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public class SyncGroupResponse : IResponse, IEquatable<SyncGroupResponse>
    {
        public override string ToString() => $"{{error_code:{error_code},member_assignment:{member_assignment}}}";

        public static SyncGroupResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var errorCode = (ErrorCode)reader.ReadInt16();

                var encoder = context.GetEncoder();
                var memberAssignment = encoder.DecodeAssignment(reader);
                return new SyncGroupResponse(errorCode, memberAssignment);
            }
        }

        public SyncGroupResponse(ErrorCode errorCode, IMemberAssignment memberAssignment)
        {
            error_code = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(error_code);
            member_assignment = memberAssignment;
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode error_code { get; }
        public IMemberAssignment member_assignment { get; }

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
            return error_code == other.error_code 
                && Equals(member_assignment, other.member_assignment);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) error_code*397) ^ (member_assignment?.GetHashCode() ?? 0);
            }
        }
        
        #endregion
    }
}