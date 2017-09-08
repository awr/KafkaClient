using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeGroups Request => [group_ids] 
    /// 
    /// This API can be used to find the current groups managed by a broker. To get a list of all groups in the cluster, 
    /// you must send ListGroup to all brokers.
    /// </summary>
    /// <remarks>
    /// DescribeGroups Request => [group_ids] 
    ///   group_ids => STRING
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_DescribeGroups
    /// </remarks>
    public class DescribeGroupsRequest : Request, IRequest<DescribeGroupsResponse>, IEquatable<DescribeGroupsRequest>
    {
        public override string ToString() => $"{{{this.RequestToString()},group_ids:[{GroupIds.ToStrings()}]}}";

        public override string ShortString() => GroupIds.Count == 1 ? $"{ApiKey} {GroupIds[0]}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(GroupIds, true);
        }

        public DescribeGroupsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => DescribeGroupsResponse.FromBytes(context, bytes);

        public DescribeGroupsRequest(params string[] groupIds) 
            : this((IEnumerable<string>) groupIds)
        {
        }

        public DescribeGroupsRequest(IEnumerable<string> groupIds) 
            : base(ApiKey.DescribeGroups)
        {
            GroupIds = groupIds.ToSafeImmutableList();
        }

        public IImmutableList<string> GroupIds { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeGroupsRequest);
        }

        /// <inheritdoc />
        public bool Equals(DescribeGroupsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                   && GroupIds.HasEqualElementsInOrder(other.GroupIds);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (GroupIds?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion
    }
}