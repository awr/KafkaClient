using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ListGroupsRequest => 
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// 
    /// This API can be used to find the current groups managed by a broker. To get a list of all groups in the cluster, 
    /// you must send ListGroup to all brokers.
    /// </summary>
    public class ListGroupsRequest : Request, IRequest<ListGroupsResponse>
    {
        public override string ToString() => $"{{Api:{ApiKey}}}";

        public ListGroupsResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => ListGroupsResponse.FromBytes(context, bytes);

        public ListGroupsRequest() 
            : base(ApiKey.ListGroups)
        {
        }
    }
}