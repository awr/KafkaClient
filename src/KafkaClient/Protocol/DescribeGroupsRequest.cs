using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeGroupsRequest => [group_id]
    ///   group_id => STRING
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// 
    /// This API can be used to find the current groups managed by a broker. To get a list of all groups in the cluster, 
    /// you must send ListGroup to all brokers.
    /// </summary>
    public class DescribeGroupsRequest : Request, IRequest<DescribeGroupsResponse>, IEquatable<DescribeGroupsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_ids:[{group_ids.ToStrings()}]}}";

        public override string ShortString() => group_ids.Count == 1 ? $"{ApiKey} {group_ids[0]}" : ApiKey.ToString();

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(group_ids, true);
        }

        public DescribeGroupsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => DescribeGroupsResponse.FromBytes(context, bytes);

        public DescribeGroupsRequest(params string[] groupIds) 
            : this((IEnumerable<string>) groupIds)
        {
        }

        public DescribeGroupsRequest(IEnumerable<string> groupIds) 
            : base(ApiKey.DescribeGroups)
        {
            group_ids = ImmutableList<string>.Empty.AddNotNullRange(groupIds);
        }

        public IImmutableList<string> group_ids { get; }

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
                   && group_ids.HasEqualElementsInOrder(other.group_ids);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (group_ids?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion
    }
}