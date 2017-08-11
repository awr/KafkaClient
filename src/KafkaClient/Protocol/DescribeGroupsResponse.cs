using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeGroups Response => *throttle_time_ms [groups] 
    /// </summary>
    /// <remarks>
    /// DescribeGroups Response => *throttle_time_ms [groups] 
    ///   throttle_time_ms => INT32
    ///   groups => error_code group_id state protocol_type protocol [members] 
    ///     error_code => INT16
    ///     group_id => STRING
    ///     state => STRING
    ///     protocol_type => STRING
    ///     protocol => STRING
    ///     members => member_id client_id client_host member_metadata member_assignment 
    ///       member_id => STRING
    ///       client_id => STRING
    ///       client_host => STRING
    ///       member_metadata => BYTES
    ///       member_assignment => BYTES
    /// 
    /// Version 1+: throttle_time_ms
    /// From http://kafka.apache.org/protocol.html#The_Messages_DescribeGroups
    /// </remarks>
    public class DescribeGroupsResponse : ThrottledResponse, IResponse, IEquatable<DescribeGroupsResponse>
    {
        public override string ToString() => $"{{groups:[{Groups.ToStrings()}]}}";

        public static DescribeGroupsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
                var throttleTime = reader.ReadThrottleTime(context.ApiVersion >= 1);
                var groups = new Group[reader.ReadInt32()];
                for (var g = 0; g < groups.Length; g++) {
                    var errorCode = (ErrorCode)reader.ReadInt16();
                    var groupId = reader.ReadString();
                    var state = reader.ReadString();
                    var protocolType = reader.ReadString();
                    var protocol = reader.ReadString();

                    IMembershipEncoder encoder = null;
                    var members = new Member[reader.ReadInt32()];
                    for (var m = 0; m < members.Length; m++) {
                        encoder = encoder ?? context.GetEncoder(protocolType);
                        var memberId = reader.ReadString();
                        var clientId = reader.ReadString();
                        var clientHost = reader.ReadString();
                        var memberMetadata = encoder.DecodeMetadata(protocol, reader);
                        var memberAssignment = encoder.DecodeAssignment(reader);
                        members[m] = new Member(memberId, clientId, clientHost, memberMetadata, memberAssignment);
                    }
                    groups[g] = new Group(errorCode, groupId, state, protocolType, protocol, members);
                }

                return new DescribeGroupsResponse(groups, throttleTime);
            }
        }

        public DescribeGroupsResponse(IEnumerable<Group> groups, TimeSpan? throttleTime = null)
            : base(throttleTime)
        {
            Groups = ImmutableList<Group>.Empty.AddNotNullRange(groups);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(Groups.Select(g => g.Error));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Group> Groups { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeGroupsResponse);
        }

        /// <inheritdoc />
        public bool Equals(DescribeGroupsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other)
                && Groups.HasEqualElementsInOrder(other.Groups);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Groups?.Count.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        #endregion

        public class Group : IEquatable<Group>
        {
            public override string ToString() => $"{{error_code:{Error},group_id:{GroupId},state:{State},protocol_type:{ProtocolType},protocol:{Protocol},members:[{Members.ToStrings()}]}}";

            public Group(ErrorCode errorCode, string groupId, string state, string protocolType, string protocol, IEnumerable<Member> members)
            {
                Error = errorCode;
                GroupId = groupId;
                State = state;
                ProtocolType = protocolType;
                Protocol = protocol;
                Members = ImmutableList<Member>.Empty.AddNotNullRange(members);
            }

            public ErrorCode Error { get; }
            public string GroupId { get; }

            /// <summary>
            /// State machine for Coordinator
            /// 
            /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
            /// </summary>
            /// <remarks>
            ///               +----------------------------------+
            ///               |             [Down]               |
            ///           +---> There are no active members and  |
            ///           |   | group state has been cleaned up. |
            ///           |   +----------------+-----------------+
            ///  Timeout  |                    |
            ///  expires  |                    | JoinGroup/Heartbeat
            ///  with     |                    | received
            ///  no       |                    v
            ///  group    |   +----------------+-----------------+
            ///  activity |   |           [Initialize]           | 
            ///           |   | The coordinator reads group data |
            ///           |   | in order to transition groups    +---v JoinGroup/Heartbeat  
            ///           |   | from failed coordinators. Any    |   | return               
            ///           |   | heartbeat or join group requests |   | coordinator not ready
            ///           |   | are returned with an error       +---v                      
            ///           |   | indicating that the coordinator  |    
            ///           |   | is not ready yet.                |
            ///           |   +----------------+-----------------+
            ///           |                    |
            ///           |                    | After reading
            ///           |                    | group state
            ///           |                    v
            ///           |   +----------------+-----------------+
            ///           |   |             [Stable]             |
            ///           |   | The coordinator either has an    |
            ///           +---+ active generation or has no      +---v 
            ///               | members and is awaiting the first|   | Heartbeat/SyncGroup
            ///               | JoinGroup. Heartbeats are        |   | from
            ///               | accepted from members in this    |   | active generation
            ///           +---> state and are used to keep group +---v
            ///           |   | members active or to indicate    |
            ///           |   | that they need to join the group |
            ///           |   +----------------+-----------------+
            ///           |                    |
            ///           |                    | JoinGroup
            ///           |                    | received
            ///           |                    v
            ///           |   +----------------+-----------------+
            ///           |   |            [Joining]             |
            ///           |   | The coordinator has received a   |
            ///           |   | JoinGroup request from at least  |
            ///           |   | one member and is awaiting       |
            ///           |   | JoinGroup requests from the rest |
            ///           |   | of the group. Heartbeats or      |
            ///           |   | SyncGroup requests in this state |
            ///           |   | return an error indicating that  |
            ///           |   | a rebalance is in progress.      |
            /// Leader    |   +----------------+-----------------+
            /// SyncGroup |                    |
            /// or        |                    | JoinGroup received
            /// session   |                    | from all members
            /// timeout   |                    v
            ///           |   +----------------+-----------------+
            ///           |   |            [AwaitSync]           |
            ///           |   | The join group phase is complete |
            ///           |   | (all expected group members have |
            ///           |   | sent JoinGroup requests) and the |
            ///           +---+ coordinator is awaiting group    |
            ///               | state from the leader. Unexpected|
            ///               | coordinator requests return an   |
            ///               | error indicating that a rebalance|
            ///               | is in progress.                  |
            ///               +----------------------------------+
            /// </remarks>
            public static class States
            {
                public const string Dead = "Dead";
                public const string Stable = "Stable";
                public const string AwaitingSync = "AwaitingSync";
                public const string PreparingRebalance = "PreparingRebalance";
                public const string NoActiveGroup = "";
            }

            /// <summary>
            /// The current state of the group (one of: Dead, Stable, AwaitingSync, or PreparingRebalance, or empty if there is no active group)
            /// </summary>
            public string State { get; }

            /// <summary>
            /// The current group protocol type (will be empty if there is no active group)
            /// </summary>
            public string ProtocolType { get; }

            /// <summary>
            /// The current group protocol (only provided if the group is Stable)
            /// </summary>
            public string Protocol { get; }

            /// <summary>
            /// Current group members (only provided if the group is not Dead)
            /// </summary>
            public IImmutableList<Member> Members { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Group);
            }

            /// <inheritdoc />
            public bool Equals(Group other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Error == other.Error 
                       && string.Equals(GroupId, other.GroupId) 
                       && string.Equals(State, other.State) 
                       && string.Equals(ProtocolType, other.ProtocolType) 
                       && string.Equals(Protocol, other.Protocol) 
                       && Members.HasEqualElementsInOrder(other.Members);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) Error;
                    hashCode = (hashCode*397) ^ (GroupId?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (State?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (ProtocolType?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Protocol?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Members?.Count.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }

        public class Member : IEquatable<Member>
        {
            public override string ToString() => $"{{member_id:{member_id},client_id:{client_id},client_host:{client_host},member_metadata:{member_metadata},member_assignment:{member_assignment}}}";

            public Member(string memberId, string clientId, string clientHost, IMemberMetadata memberMetadata, IMemberAssignment memberAssignment)
            {
                member_id = memberId;
                client_id = clientId;
                client_host = clientHost;
                member_metadata = memberMetadata;
                member_assignment = memberAssignment;
            }

            /// <summary>
            /// The memberId assigned by the coordinator
            /// </summary>
            public string member_id { get; }

            /// <summary>
            /// The client id used in the member's latest join group request
            /// </summary>
            public string client_id { get; }

            /// <summary>
            /// The client host used in the request session corresponding to the member's join group.
            /// </summary>
            public string client_host { get; }

            /// <summary>
            /// The metadata corresponding to the current group protocol in use (will only be present if the group is stable).
            /// </summary>
            public IMemberMetadata member_metadata { get; }

            /// <summary>
            /// The current assignment provided by the group leader (will only be present if the group is stable).
            /// </summary>
            public IMemberAssignment member_assignment { get; }

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
                    && string.Equals(client_id, other.client_id) 
                    && string.Equals(client_host, other.client_host) 
                    && Equals(member_metadata, other.member_metadata) 
                    && Equals(member_assignment, other.member_assignment);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = member_id?.GetHashCode() ?? 0;
                    hashCode = (hashCode*397) ^ (client_id?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (client_host?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (member_metadata?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (member_assignment?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            #endregion
        }
    }
}