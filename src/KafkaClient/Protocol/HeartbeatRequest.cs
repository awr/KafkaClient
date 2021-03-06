using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Heartbeat Request => group_id group_generation_id member_id 
    /// 
    /// Once a member has joined and synced, it will begin sending periodic heartbeats to keep itself in the group. If a heartbeat has *not* been 
    /// received by the coordinator with the configured session timeout, the member will be kicked out of the group.
    /// </summary>
    /// <remarks>
    /// Heartbeat Request => group_id group_generation_id member_id 
    ///   group_id => STRING
    ///   group_generation_id => INT32
    ///   member_id => STRING
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_Heartbeat
    /// </remarks>
    public class HeartbeatRequest : GroupRequest, IRequest<HeartbeatResponse>
    {
        public override string ToString() => $"{{{this.RequestToString()},group_id:{GroupId},member_id:{MemberId},generation_id:{GenerationId}}}";

        public override string ShortString() => $"{ApiKey} {GroupId} {MemberId}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(GroupId)
                  .Write(GenerationId)
                  .Write(MemberId);
        }

        public HeartbeatResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => HeartbeatResponse.FromBytes(context, bytes);

        /// <inheritdoc />
        public HeartbeatRequest(string groupId, int generationId, string memberId) 
            : base(ApiKey.Heartbeat, groupId, memberId, generationId)
        {
        }
    }
}