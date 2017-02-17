using System;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    public abstract class GroupRequest : Request, IGroupMember, IEquatable<GroupRequest>
    {
        /// <inheritdoc />
        protected GroupRequest(ApiKey apiKey, string groupId, string memberId, int generationId = 0, bool expectResponse = true) : base(apiKey, expectResponse)
        {
            if (string.IsNullOrEmpty(groupId)) throw new ArgumentNullException(nameof(groupId));

            group_id = groupId;
            member_id = memberId;
            generation_id = generationId;
        }

        /// <inheritdoc />
        public string group_id { get; }

        /// <inheritdoc />
        public string member_id { get; }

        /// <summary>
        /// The generation of the group.
        /// 
        /// Upon every completion of the join group phase, the coordinator increments a GenerationId for the group. This is returned as a field in the 
        /// response to each member, and is sent in <see cref="HeartbeatRequest"/> and <see cref="OffsetCommitRequest"/>s. When the coordinator rebalances 
        /// a group, the coordinator will send an error code indicating that the member needs to rejoin. If the member does not rejoin before a rebalance 
        /// completes, then it will have an old generationId, which will cause <see cref="ErrorCode.ILLEGAL_GENERATION"/> errors when included in 
        /// new requests.
        /// </summary>
        public int generation_id { get; }


        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupRequest);
        }

        /// <inheritdoc />
        public bool Equals(GroupRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && string.Equals(group_id, other.group_id) 
                && string.Equals(member_id, other.member_id) 
                && generation_id == other.generation_id;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (group_id?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (member_id?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ generation_id;
                return hashCode;
            }
        }
    }
}