using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeGroups Response => [groups]
    ///  group => error_code group_id state protocol_type protocol [members] 
    ///   error_code => INT16
    ///   group_id => STRING
    ///   state => STRING              -- The current state of the group (one of: Dead, Stable, AwaitingSync, or PreparingRebalance, or empty if there is no active group)
    ///   protocol_type => STRING      -- The current group protocol type (will be empty if there is no active group)
    ///   protocol => STRING           -- 	The current group protocol (only provided if the group is Stable)
    ///   member => member_id client_id client_host member_metadata member_assignment
    ///     member_id => STRING        -- The memberId assigned by the coordinator
    ///     client_id => STRING        -- The client id used in the member's latest join group request
    ///     client_host => STRING      -- The client host used in the request session corresponding to the member's join group.
    ///     member_metadata => BYTES   -- The metadata corresponding to the current group protocol in use (will only be present if the group is stable).
    ///     member_assignment => BYTES -- The current assignment provided by the group leader (will only be present if the group is stable).
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class DescribeGroupsResponse : IResponse, IEquatable<DescribeGroupsResponse>
    {
        public override string ToString() => $"{{groups:[{groups.ToStrings()}]}}";

        public static DescribeGroupsResponse FromBytes(IRequestContext context, ArraySegment<byte> bytes)
        {
            using (var reader = new KafkaReader(bytes)) {
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

                return new DescribeGroupsResponse(groups);
            }
        }

        public DescribeGroupsResponse(IEnumerable<Group> groups)
        {
            this.groups = ImmutableList<Group>.Empty.AddNotNullRange(groups);
            Errors = ImmutableList<ErrorCode>.Empty.AddRange(this.groups.Select(g => g.error_code));
        }

        public IImmutableList<ErrorCode> Errors { get; }

        public IImmutableList<Group> groups { get; }

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
            return groups.HasEqualElementsInOrder(other.groups);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return groups?.Count.GetHashCode() ?? 0;
        }

        #endregion

        public class Group : IEquatable<Group>
        {
            public override string ToString() => $"{{error_code:{error_code},group_id:{group_id},state:{state},protocol_type:{protocol_type},protocol:{protocol},members:[{members.ToStrings()}]}}";

            public Group(ErrorCode errorCode, string groupId, string state, string protocolType, string protocol, IEnumerable<Member> members)
            {
                error_code = errorCode;
                group_id = groupId;
                this.state = state;
                protocol_type = protocolType;
                this.protocol = protocol;
                this.members = ImmutableList<Member>.Empty.AddNotNullRange(members);
            }

            public ErrorCode error_code { get; }
            public string group_id { get; }

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
            public string state { get; }

            /// <summary>
            /// The current group protocol type (will be empty if there is no active group)
            /// </summary>
            public string protocol_type { get; }

            /// <summary>
            /// The current group protocol (only provided if the group is Stable)
            /// </summary>
            public string protocol { get; }

            /// <summary>
            /// Current group members (only provided if the group is not Dead)
            /// </summary>
            public IImmutableList<Member> members { get; }

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
                return error_code == other.error_code 
                       && String.Equals(group_id, other.group_id) 
                       && String.Equals(state, other.state) 
                       && String.Equals(protocol_type, other.protocol_type) 
                       && String.Equals(protocol, other.protocol) 
                       && members.HasEqualElementsInOrder(other.members);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) error_code;
                    hashCode = (hashCode*397) ^ (group_id?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (state?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (protocol_type?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (protocol?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (members?.Count.GetHashCode() ?? 0);
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