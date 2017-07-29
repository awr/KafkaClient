using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SyncGroupRequest => group_id generation_id member_id [group_assignment]
    ///   group_id => STRING
    ///   generation_id => INT32
    ///   member_id => STRING
    ///   group_assignment => member_id MemberAssignment -- only the leader should specify group assignments
    ///     member_id => STRING
    ///     member_assignment => BYTES
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class SyncGroupRequest : GroupRequest, IRequest<SyncGroupResponse>, IEquatable<SyncGroupRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{GroupId},member_id:{MemberId},generation_id:{GenerationId},group_assignments:[{group_assignments.ToStrings()}]}}";

        public override string ShortString() => $"{ApiKey} {GroupId} {MemberId}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(GroupId)
                  .Write(GenerationId)
                  .Write(MemberId)
                  .Write(group_assignments.Count);

            var encoder = context.GetEncoder(context.ProtocolType);
            foreach (var assignment in group_assignments) {
                writer.Write(assignment.member_id)
                        .Write(assignment.member_assignment, encoder);
            }
        }

        public SyncGroupResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => SyncGroupResponse.FromBytes(context, bytes);

        /// <inheritdoc />
        public SyncGroupRequest(string groupId, int generationId, string memberId, IEnumerable<GroupAssignment> groupAssignments = null) 
            : base(ApiKey.SyncGroup, groupId, memberId, generationId)
        {
            group_assignments = ImmutableList<GroupAssignment>.Empty.AddNotNullRange(groupAssignments);
        }

        public IImmutableList<GroupAssignment> group_assignments { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SyncGroupRequest);
        }

        /// <inheritdoc />
        public bool Equals(SyncGroupRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && group_assignments.HasEqualElementsInOrder(other.group_assignments);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (group_assignments?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion

        public class GroupAssignment : IEquatable<GroupAssignment>
        {
            public override string ToString() => $"{{member_id:{member_id},member_assignment:{member_assignment}}}";

            public GroupAssignment(string memberId, IMemberAssignment memberAssignment)
            {
                member_id = memberId;
                member_assignment = memberAssignment;
            }

            public string member_id { get; }
            public IMemberAssignment member_assignment { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as GroupAssignment);
            }

            /// <inheritdoc />
            public bool Equals(GroupAssignment other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(member_id, other.member_id) 
                    && Equals(member_assignment, other.member_assignment);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((member_id?.GetHashCode() ?? 0)*397) ^ (member_assignment?.GetHashCode() ?? 0);
                }
            }

            #endregion
        }
    }
}