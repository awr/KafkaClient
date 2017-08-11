using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ListGroups Request => 
    /// 
    /// This API can be used to find the current groups managed by a broker. To get a list of all groups in the cluster, 
    /// you must send ListGroup to all brokers.
    /// </summary>
    /// <remarks>
    /// ListGroups Request => 
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_ListGroups
    /// </remarks>
    public class ListGroupsRequest : Request, IRequest<ListGroupsResponse>, IEquatable<ListGroupsRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey}}}";

        public ListGroupsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => ListGroupsResponse.FromBytes(context, bytes);

        public ListGroupsRequest() 
            : base(ApiKey.ListGroups)
        {
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ListGroupsRequest);
        }

        public bool Equals(ListGroupsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}