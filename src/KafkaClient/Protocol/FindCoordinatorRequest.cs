using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// FindCoordinator Request => coordinator_key *coordinator_type 
    ///
    /// The offsets for a given consumer group is maintained by a specific broker called the offset coordinator. i.e., a consumer needs
    /// to issue its offset commit and fetch requests to this specific broker. It can discover the current offset coordinator by issuing a consumer metadata request.
    /// </summary>
    /// <remarks>
    /// FindCoordinator Request => coordinator_key *coordinator_type 
    ///   coordinator_key => STRING
    ///   coordinator_type => INT8
    /// 
    /// Version 0 only: coordinator_key can only be group_id
    /// Version 1+: coordinator_type
    /// From http://kafka.apache.org/protocol.html#The_Messages_FindCoordinator
    /// </remarks>
    public class FindCoordinatorRequest : Request, IRequest<FindCoordinatorResponse>, IEquatable<FindCoordinatorRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{CoordinatorId}}}";

        public override string ShortString() => $"{ApiKey} {CoordinatorId}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(CoordinatorId);
            if (context.ApiVersion >= 1) {
                writer.Write((byte) CoordinatorType);
            }
        }

        public FindCoordinatorResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => FindCoordinatorResponse.FromBytes(context, bytes);

        public FindCoordinatorRequest(string coordinatorId, CoordinatorType? coordinatorType = null) 
            : base(ApiKey.FindCoordinator)
        {
            if (string.IsNullOrEmpty(coordinatorId)) throw new ArgumentNullException(nameof(coordinatorId));

            CoordinatorId = coordinatorId;
            CoordinatorType = coordinatorType.GetValueOrDefault(CoordinatorType.Group);
        }

        /// <summary>
        /// Id to use for finding the coordinator (for groups, this is the groupId, for transactional producers, this is the transactional id).
        /// Version 1: This is the group_id
        /// Version 2+: This is the coordinator_key
        /// </summary>
        public string CoordinatorId { get; }

        /// <summary>
        /// The type of coordinator to find (0 = group, 1 = transaction).
        /// </summary>
        public CoordinatorType CoordinatorType { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as FindCoordinatorRequest);
        }

        /// <inheritdoc />
        public bool Equals(FindCoordinatorRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(CoordinatorId, other.CoordinatorId)
                && CoordinatorType == other.CoordinatorType;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((CoordinatorId?.GetHashCode() ?? 0) * 397) ^ CoordinatorType.GetHashCode();
            }
        }

        #endregion
    }
}