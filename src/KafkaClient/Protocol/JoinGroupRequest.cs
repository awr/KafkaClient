using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// JoinGroup Request => group_id session_timeout *rebalance_timeout member_id protocol_type [group_protocols] 
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
    /// <remarks>
    /// JoinGroup Request => group_id session_timeout *rebalance_timeout member_id protocol_type [group_protocols] 
    ///   group_id => STRING
    ///   session_timeout => INT32
    ///   rebalance_timeout => INT32
    ///   member_id => STRING
    ///   protocol_type => STRING
    ///   group_protocols => protocol_name protocol_metadata 
    ///     protocol_name => STRING
    ///     protocol_metadata => BYTES
    /// 
    /// Version 1+: rebalance_timeout
    /// From http://kafka.apache.org/protocol.html#The_Messages_JoinGroup
    /// </remarks>
    public class JoinGroupRequest : Request, IRequest<JoinGroupResponse>, IGroupMember, IEquatable<JoinGroupRequest>
    {
        public override string ToString() => $"{{{this.RequestToString()},group_id:{GroupId},session_timeout:{SessionTimeout},rebalance_timeout:{RebalanceTimeout},member_id:{MemberId},protocol_type:{ProtocolType},group_protocols:[{GroupProtocols.ToStrings()}]}}";

        public override string ShortString() => $"{ApiKey} {GroupId} {MemberId}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(GroupId)
                    .WriteMilliseconds(SessionTimeout);

            if (context.ApiVersion >= 1) {
                writer.WriteMilliseconds(RebalanceTimeout);
            }
            writer.Write(MemberId)
                    .Write(ProtocolType)
                    .Write(GroupProtocols.Count);

            var encoder = context.GetEncoder(ProtocolType);
            foreach (var protocol in GroupProtocols) {
                writer.Write(protocol.ProtocolName)
                        .Write(protocol.ProtocolMetadata, encoder);
            }
        }

        public JoinGroupResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => JoinGroupResponse.FromBytes(context, bytes);

        public JoinGroupRequest(string groupId, TimeSpan sessionTimeout, string memberId, string protocolType, IEnumerable<GroupProtocol> groupProtocols, TimeSpan? rebalanceTimeout = null) 
            : base(ApiKey.JoinGroup)
        {
            GroupId = groupId;
            SessionTimeout = sessionTimeout;
            RebalanceTimeout = rebalanceTimeout ?? SessionTimeout;
            MemberId = memberId ?? "";
            ProtocolType = protocolType;
            GroupProtocols = groupProtocols.ToSafeImmutableList();
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
        public TimeSpan SessionTimeout { get; }

        /// <summary>
        /// Once a rebalance begins, each client has up to this duration to rejoin, but note that if the session timeout is lower than the rebalance 
        /// timeout, the client must still continue to send heartbeats.
        /// Version: 1+
        /// </summary>
        public TimeSpan RebalanceTimeout { get; }

        /// <summary>
        /// List of protocols that the member supports. The coordinator chooses a single protocol which all members support. This enables e.g. rolling upgrades without downtime.
        /// </summary>
        public IImmutableList<GroupProtocol> GroupProtocols { get; }

        /// <inheritdoc />
        public string GroupId { get; }

        /// <inheritdoc />
        public string MemberId { get; }

        /// <summary>
        /// Unique name for class of protocols implemented by group.
        /// </summary>
        public string ProtocolType { get; }

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
                && SessionTimeout.Equals(other.SessionTimeout) 
                && RebalanceTimeout.Equals(other.RebalanceTimeout) 
                && string.Equals(GroupId, other.GroupId)
                && string.Equals(MemberId, other.MemberId)
                && string.Equals(ProtocolType, other.ProtocolType)
                && GroupProtocols.HasEqualElementsInOrder(other.GroupProtocols);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ SessionTimeout.GetHashCode();
                hashCode = (hashCode*397) ^ RebalanceTimeout.GetHashCode();
                hashCode = (hashCode*397) ^ (GroupProtocols?.Count.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (GroupId?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (MemberId?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (ProtocolType?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class GroupProtocol : IEquatable<GroupProtocol>
        {
            public override string ToString() => $"{{protocol_name:{ProtocolName},protocol_metadata:{ProtocolMetadata}}}";

            public GroupProtocol(IMemberMetadata metadata)
            {
                ProtocolMetadata = metadata;
            }

            /// <summary>
            /// ie AssignmentStrategy for "consumer" type. protocol_name != protocol_type. It's a subtype of sorts.
            /// </summary>
            public string ProtocolName => ProtocolMetadata.AssignmentStrategy;

            /// <summary>
            /// For an example: <see cref="ConsumerProtocolMetadata"/>
            /// </summary>
            public IMemberMetadata ProtocolMetadata { get; }

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
                return string.Equals(ProtocolName, other.ProtocolName) 
                    && Equals(ProtocolMetadata, other.ProtocolMetadata);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((ProtocolName?.GetHashCode() ?? 0)*397) ^ (ProtocolMetadata?.GetHashCode() ?? 0);
                }
            }
        }

    }
}