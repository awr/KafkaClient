using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// JoinGroup Response => error_code generation_id group_protocol leader_id member_id [members] 
    ///   error_code => INT16
    ///   generation_id => INT32   -- The generation of the consumer group.
    ///   group_protocol => STRING -- The group protocol selected by the coordinator (ie AssignmentStrategy)
    ///   leader_id => STRING      -- The leader of the group
    ///   member_id => STRING      -- The consumer id assigned by the group coordinator.
    ///   member => member_id member_metadata 
    ///     member_id => STRING
    ///     member_metadata => BYTES
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages for details
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
    public class JoinGroupResponse : IResponse, IEquatable<JoinGroupResponse>
    {
        public override string ToString() => $"{{error_code:{error_code},generation_id:{generation_id},group_protocol:{group_protocol},leader_id:{leader_id},member_id:{member_id},members:[{members.ToStrings()}]}}";

        public static JoinGroupResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var errorCode = (ErrorCode)reader.ReadInt16();
                var generationId = reader.ReadInt32();
                var groupProtocol = reader.ReadString();
                var leaderId = reader.ReadString();
                var memberId = reader.ReadString();

                var encoder = context.GetEncoder(context.ProtocolType);
                var members = new Member[reader.ReadInt32()];
                for (var m = 0; m < members.Length; m++) {
                    var id = reader.ReadString();
                    var metadata = encoder.DecodeMetadata(groupProtocol, reader);
                    members[m] = new Member(id, metadata);
                }

                return new JoinGroupResponse(errorCode, generationId, groupProtocol, leaderId, memberId, members);
            }
        }

        public JoinGroupResponse(ErrorCode errorCode, int generationId, string groupProtocol, string leaderId, string memberId, IEnumerable<Member> members)
        {
            error_code = errorCode;
            Errors = ImmutableList<ErrorCode>.Empty.Add(error_code);
            generation_id = generationId;
            group_protocol = groupProtocol;
            leader_id = leaderId;
            member_id = memberId;
            this.members = ImmutableList<Member>.Empty.AddNotNullRange(members);
        }

        /// <inheritdoc />
        public IImmutableList<ErrorCode> Errors { get; }

        public ErrorCode error_code { get; }

        /// <summary>
        /// The generation counter for completion of the join group phase.
        /// </summary>
        public int generation_id { get; }

        /// <summary>
        /// The group protocol selected by the coordinator. Is this the name or the type??
        /// </summary>
        public string group_protocol { get; }

        /// <summary>
        /// The leader of the group.
        /// </summary>
        public string leader_id { get; }

        /// <summary>
        /// The consumer id assigned by the group coordinator.
        /// </summary>
        public string member_id { get; }

        public IImmutableList<Member> members { get; }

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
            return error_code == other.error_code 
                && generation_id == other.generation_id 
                && string.Equals(group_protocol, other.group_protocol) 
                && string.Equals(leader_id, other.leader_id) 
                && string.Equals(member_id, other.member_id) 
                && members.HasEqualElementsInOrder(other.members);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = (int) error_code;
                hashCode = (hashCode*397) ^ generation_id;
                hashCode = (hashCode*397) ^ (group_protocol?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (leader_id?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (member_id?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (members?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Member : IEquatable<Member>
        {
            public override string ToString() => $"{{member_id:{member_id},member_metadata:{member_metadata}}}";

            public Member(string memberId, IMemberMetadata metadata)
            {
                member_id = memberId;
                member_metadata = metadata;
            }

            public string member_id { get; }
            public IMemberMetadata member_metadata { get; }

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
                return string.Equals(member_id, other.member_id) 
                    && Equals(member_metadata, other.member_metadata);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((member_id?.GetHashCode() ?? 0)*397) ^ (member_metadata?.GetHashCode() ?? 0);
                }
            }

            #endregion
        }
    }
}