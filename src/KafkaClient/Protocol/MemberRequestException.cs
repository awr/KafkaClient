using KafkaClient.Connections;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request for groups (JoinGroup, SyncGroup, etc)
    /// </summary>
    public class MemberRequestException : RequestException
    {
        public MemberRequestException(IGroupMember member, ApiKey apiKey, ErrorCode errorCode, Endpoint endpoint)
            : base(apiKey, errorCode, endpoint, member != null ? $"{{GroupId:{member.GroupId},MemberId:{member.MemberId}}}" : null)
        {
            _member = member;
        }

        // ReSharper disable once NotAccessedField.Local -- for debugging
        private readonly IGroupMember _member;
    }
}