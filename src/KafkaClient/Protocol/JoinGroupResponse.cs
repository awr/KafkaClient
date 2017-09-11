using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// JoinGroup Response => *throttle_time_ms error_code generation_id group_protocol leader_id member_id [members] 
    /// 
    /// After receiving join group requests from all members in the group, the coordinator will select one member to be the group leader 
    /// and a protocol which is supported by all members. The leader will receive the full list of members along with the associated metadata 
    /// for the protocol chosen. Other members, followers, will receive an empty array of members. It is the responsibility of the leader to 
    /// inspect the metadata of each member and assign state using SyncGroup request below.
    /// 
    /// Upon every completion of the join group phase, the coordinator increments a GenerationId for the group. This is returned as a field in 
    /// the response to each member, and is sent in heartbeats and offset commit requests. When the coordinator rebalances a group, the 
    /// coordinator will send an error code indicating that the member needs to rejoin. If the member does not rejoin before a rebalance 
    /// completes, then it will have an old generationId, which will cause ILLEGAL_GENERATION errors when included in new requests.
    /// </summary>
    /// <remarks>
    /// JoinGroup Response => *throttle_time_ms error_code generation_id group_protocol leader_id member_id [members] 
    ///   throttle_time_ms => INT32
    ///   error_code => INT16
    ///   generation_id => INT32
    ///   group_protocol => STRING
    ///   leader_id => STRING
    ///   member_id => STRING
    ///   members => member_id member_metadata 
    ///     member_id => STRING
    ///     member_metadata => BYTES
    /// 
    /// Version 2+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_JoinGroup
    /// </remarks>
    public class JoinGroupResponse : ThrottledResponse, IResponse, IEquatable<JoinGroupResponse>
    {
        public override string ToString() => $"{{{this.ThrottleToString()},error_code:{Error},generation_id:{GenerationId},group_protocol:{GroupProtocol},leader_id:{LeaderId},member_id:{MemberId},members:[{Members.ToStrings()}]}}";

        public static JoinGroupResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 2);
                var errorCode = (ErrorCode)reader.ReadInt16();
                var generationId = reader.ReadInt32();
                var groupProtocol = reader.ReadString();
                var leaderId = reader.ReadString();
                var memberId = reader.ReadString();

                var encoder = context.GetEncoder(context.ProtocolType);
                var memberCount = reader.ReadInt32();
                context.ThrowIfCountTooBig(memberCount);
                var members = new Member[memberCount];
                for (var m = 0; m < members.Length; m++) {
                    var id = reader.ReadString();
                    var metadata = encoder.DecodeMetadata(groupProtocol, context, reader);
                    members[m] = new Member(id, metadata);
                }

                return new JoinGroupResponse(errorCode, generationId, groupProtocol, leaderId, memberId, members, throttleTime);
            }
        }

        public JoinGroupResponse(ErrorCode errorCode, int generationId, string groupProtocol, string leaderId, string memberId, IEnumerable<Member> members, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Error = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(Error);
            GenerationId = generationId;
            GroupProtocol = groupProtocol;
            LeaderId = leaderId;
            MemberId = memberId;
            Members = members.ToSafeImmutableList();
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode Error { get; }

        /// <summary>
        /// The generation of the consumer group. It is incremented the completion of the join group phase. 
        /// Members need to send this id in heartbeats and offset commit requests.
        /// </summary>
        public int GenerationId { get; }

        /// <summary>
        /// The group protocol selected by the coordinator.
        /// </summary>
        public string GroupProtocol { get; }

        /// <summary>
        /// The leader of the group.
        /// </summary>
        public string LeaderId { get; }

        /// <summary>
        /// The consumer id assigned by the group coordinator.
        /// </summary>
        public string MemberId { get; }

        /// <summary>
        /// The leader will receive the full list of members along with the associated metadata for the protocol chosen. 
        /// Other members, followers, will receive an empty array of members.
        /// </summary>
        public IImmutableList<Member> Members { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as JoinGroupResponse);
        }

        /// <inheritdoc />
        public bool Equals(JoinGroupResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Error == other.Error 
                && GenerationId == other.GenerationId 
                && string.Equals(GroupProtocol, other.GroupProtocol) 
                && string.Equals(LeaderId, other.LeaderId) 
                && string.Equals(MemberId, other.MemberId)
                && Members.HasEqualElementsInOrder(other.Members);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ GenerationId;
                hashCode = (hashCode*397) ^ (GroupProtocol?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (LeaderId?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (MemberId?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (int) Error;
                hashCode = (hashCode*397) ^ (Members?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Member : IEquatable<Member>
        {
            public override string ToString() => $"{{member_id:{MemberId},member_metadata:{MemberMetadata}}}";

            public Member(string memberId, IMemberMetadata metadata)
            {
                MemberId = memberId;
                MemberMetadata = metadata;
            }

            /// <summary>
            /// The consumer id assigned to this particular member.
            /// </summary>
            public string MemberId { get; }

            /// <summary>
            /// The metadata supplied in this member's join group request.
            /// </summary>
            public IMemberMetadata MemberMetadata { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Member);
            }

            /// <inheritdoc />
            public bool Equals(Member other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(MemberId, other.MemberId) 
                    && Equals(MemberMetadata, other.MemberMetadata);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((MemberId?.GetHashCode() ?? 0)*397) ^ (MemberMetadata?.GetHashCode() ?? 0);
                }
            }

            #endregion
        }
    }
}