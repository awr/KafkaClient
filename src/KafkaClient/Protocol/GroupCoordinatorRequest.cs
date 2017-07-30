using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// GroupCoordinatorRequest => group_id
    ///  group_id => STRING -- The consumer group id.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// The offsets for a given consumer group is maintained by a specific broker called the offset coordinator. i.e., a consumer needs
    /// to issue its offset commit and fetch requests to this specific broker. It can discover the current offset coordinator by issuing a consumer metadata request.
    /// </summary>
    public class GroupCoordinatorRequest : Request, IRequest<GroupCoordinatorResponse>, IEquatable<GroupCoordinatorRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{group_id}}}";

        public override string ShortString() => $"{ApiKey} {group_id}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(group_id);
        }

        public GroupCoordinatorResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => GroupCoordinatorResponse.FromBytes(context, bytes);

        public GroupCoordinatorRequest(string groupId) 
            : base(ApiKey.GroupCoordinator)
        {
            if (string.IsNullOrEmpty(groupId)) throw new ArgumentNullException(nameof(groupId));

            group_id = groupId;
        }

        public string group_id { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupCoordinatorRequest);
        }

        /// <inheritdoc />
        public bool Equals(GroupCoordinatorRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(group_id, other.group_id);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return group_id?.GetHashCode() ?? 0;
        }

        #endregion
    }
}