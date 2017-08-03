using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SyncGroup Request => group_id generation_id member_id [group_assignment] 
    /// </summary>
    /// <remarks>
    /// SyncGroup Request => group_id generation_id member_id [group_assignment] 
    ///   group_id => STRING
    ///   generation_id => INT32
    ///   member_id => STRING
    ///   group_assignment => member_id member_assignment 
    ///     member_id => STRING
    ///     member_assignment => BYTES
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_SyncGroup
    /// </remarks>
    public class SyncGroupRequest : GroupRequest, IRequest<SyncGroupResponse>, IEquatable<SyncGroupRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{GroupId},member_id:{MemberId},generation_id:{GenerationId},group_assignments:[{GroupAssignments.ToStrings()}]}}";

        public override string ShortString() => $"{ApiKey} {GroupId} {MemberId}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(GroupId)
                  .Write(GenerationId)
                  .Write(MemberId)
                  .Write(GroupAssignments.Count);

            var encoder = context.GetEncoder(context.ProtocolType);
            foreach (var assignment in GroupAssignments) {
                writer.Write(assignment.MemberId)
                        .Write(assignment.MemberAssignment, encoder);
            }
        }

        public SyncGroupResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => SyncGroupResponse.FromBytes(context, bytes);

        /// <inheritdoc />
        public SyncGroupRequest(string groupId, int generationId, string memberId, IEnumerable<GroupAssignment> groupAssignments = null) 
            : base(ApiKey.SyncGroup, groupId, memberId, generationId)
        {
            GroupAssignments = ImmutableList<GroupAssignment>.Empty.AddNotNullRange(groupAssignments);
        }

        public IImmutableList<GroupAssignment> GroupAssignments { get; }

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
                && GroupAssignments.HasEqualElementsInOrder(other.GroupAssignments);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (GroupAssignments?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion

        public class GroupAssignment : IEquatable<GroupAssignment>
        {
            public override string ToString() => $"{{member_id:{MemberId},member_assignment:{MemberAssignment}}}";

            public GroupAssignment(string memberId, IMemberAssignment memberAssignment)
            {
                MemberId = memberId;
                MemberAssignment = memberAssignment;
            }

            /// <summary>
            /// The member id assigned by the group coordinator (ie one of the kafka servers).
            /// </summary>
            public string MemberId { get; }

            public IMemberAssignment MemberAssignment { get; }

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
                return string.Equals(MemberId, other.MemberId) 
                    && Equals(MemberAssignment, other.MemberAssignment);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((MemberId?.GetHashCode() ?? 0)*397) ^ (MemberAssignment?.GetHashCode() ?? 0);
                }
            }

            #endregion
        }
    }
}