using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// JoinGroup Request => group_id session_timeout *rebalance_timeout member_id protocol_type [GroupProtocol] 
    ///  *rebalance_timeout only applies to version 1 (Kafka 0.10.1) and above
    ///   group_id => STRING           -- The group id.
    ///   session_timeout => INT32     -- The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms.
    ///   rebalance_timeout => INT32   -- The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group
    ///   member_id => STRING          -- The assigned consumer id or an empty string for a new consumer.
    ///   protocol_type => STRING      -- Unique name for class of protocols implemented by group (ie "consumer")
    ///   GroupProtocol => ProtocolName ProtocolMetadata
    ///     protocol_name => STRING    -- ie AssignmentStrategy for "consumer" type. protocol_name != protocol_type. It's a subtype of sorts.
    ///     protocol_metadata => BYTES -- <see cref="ConsumerProtocolMetadata"/>
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages for details
    /// 
    /// The join group request is used by a client to become a member of a group. 
    /// When new members join an existing group, all previous members are required to rejoin by sending a new join group request. 
    /// When a member first joins the group, the memberId will be empty (i.e. ""), but a rejoining member should use the same memberId 
    /// from the previous generation. 
    /// 
    /// The SessionTimeout field is used to indicate client liveness. If the coordinator does not receive at least one heartbeat (see below) 
    /// before expiration of the session timeout, then the member will be removed from the group. Prior to version 0.10.1, the session timeout 
    /// was also used as the timeout to complete a needed rebalance. Once the coordinator begins rebalancing, each member in the group has up 
    /// to the session timeout in order to send a new JoinGroup request. If they fail to do so, they will be removed from the group. In 0.10.1, 
    /// a new version of the JoinGroup request was created with a separate RebalanceTimeout field. Once a rebalance begins, each client has up 
    /// to this duration to rejoin, but note that if the session timeout is lower than the rebalance timeout, the client must still continue 
    /// to send heartbeats.
    /// 
    /// The ProtocolType field defines the embedded protocol that the group implements. The group coordinator ensures that all members in 
    /// the group support the same protocol type. The meaning of the protocol name and metadata contained in the GroupProtocols field depends 
    /// on the protocol type. Note that the join group request allows for multiple protocol/metadata pairs. This enables rolling upgrades 
    /// without downtime. The coordinator chooses a single protocol which all members support. The upgraded member includes both the new 
    /// version and the old version of the protocol. Once all members have upgraded, the coordinator will choose whichever protocol is listed 
    /// first in the GroupProtocols array.
    /// </summary>
    public class JoinGroupRequest : Request, IRequest<JoinGroupResponse>, IGroupMember, IEquatable<JoinGroupRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{GroupId},member_id:{MemberId},session_timeout:{session_timeout},rebalance_timeout:{rebalance_timeout},protocol_type:{protocol_type},group_protocols:[{group_protocols.ToStrings()}]}}";

        public override string ShortString() => $"{ApiKey} {GroupId} {MemberId}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(GroupId)
                    .Write((int)session_timeout.TotalMilliseconds);

            if (context.ApiVersion >= 1) {
                writer.Write((int) rebalance_timeout.TotalMilliseconds);
            }
            writer.Write(MemberId)
                    .Write(protocol_type)
                    .Write(group_protocols.Count);

            var encoder = context.GetEncoder(protocol_type);
            foreach (var protocol in group_protocols) {
                writer.Write(protocol.protocol_name)
                        .Write(protocol.protocol_metadata, encoder);
            }
        }

        public JoinGroupResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => JoinGroupResponse.FromBytes(context, bytes);

        public JoinGroupRequest(string groupId, TimeSpan sessionTimeout, string memberId, string protocolType, IEnumerable<GroupProtocol> groupProtocols, TimeSpan? rebalanceTimeout = null) 
            : base(ApiKey.JoinGroup)
        {
            GroupId = groupId;
            session_timeout = sessionTimeout;
            rebalance_timeout = rebalanceTimeout ?? session_timeout;
            MemberId = memberId ?? "";
            protocol_type = protocolType;
            group_protocols = ImmutableList<GroupProtocol>.Empty.AddNotNullRange(groupProtocols);
        }

        /// <summary>
        /// The SessionTimeout field is used to indicate client liveness. If the coordinator does not receive at least one heartbeat (see below) 
        /// before expiration of the session timeout, then the member will be removed from the group. Prior to version 0.10.1, the session timeout 
        /// was also used as the timeout to complete a needed rebalance. Once the coordinator begins rebalancing, each member in the group has up 
        /// to the session timeout in order to send a new JoinGroup request. If they fail to do so, they will be removed from the group. In 0.10.1, 
        /// a new version of the JoinGroup request was created with a separate RebalanceTimeout field. Once a rebalance begins, each client has up 
        /// to this duration to rejoin, but note that if the session timeout is lower than the rebalance timeout, the client must still continue 
        /// to send heartbeats.
        /// </summary>
        public TimeSpan session_timeout { get; }

        /// <summary>
        /// Once a rebalance begins, each client has up to this duration to rejoin, but note that if the session timeout is lower than the rebalance 
        /// timeout, the client must still continue to send heartbeats.
        /// </summary>
        public TimeSpan rebalance_timeout { get; }

        public IImmutableList<GroupProtocol> group_protocols { get; }
        /// <inheritdoc />
        public string GroupId { get; }

        /// <inheritdoc />
        public string MemberId { get; }

        /// <inheritdoc />
        public string protocol_type { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as JoinGroupRequest);
        }

        /// <inheritdoc />
        public bool Equals(JoinGroupRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && session_timeout.Equals(other.session_timeout) 
                && rebalance_timeout.Equals(other.rebalance_timeout) 
                && string.Equals(GroupId, other.GroupId) 
                && string.Equals(MemberId, other.MemberId)
                && group_protocols.HasEqualElementsInOrder(other.group_protocols);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ session_timeout.GetHashCode();
                hashCode = (hashCode*397) ^ rebalance_timeout.GetHashCode();
                hashCode = (hashCode*397) ^ (group_protocols?.Count.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (GroupId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (MemberId?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class GroupProtocol : IEquatable<GroupProtocol>
        {
            public override string ToString() => $"{{protocol_name:{protocol_name},protocol_metadata:{protocol_metadata}}}";

            public GroupProtocol(IMemberMetadata metadata)
            {
                protocol_metadata = metadata;
            }

            public string protocol_name => protocol_metadata.AssignmentStrategy;
            public IMemberMetadata protocol_metadata { get; }

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as GroupProtocol);
            }

            /// <inheritdoc />
            public bool Equals(GroupProtocol other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(protocol_name, other.protocol_name) 
                    && Equals(protocol_metadata, other.protocol_metadata);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((protocol_name?.GetHashCode() ?? 0)*397) ^ (protocol_metadata?.GetHashCode() ?? 0);
                }
            }
        }

    }
}