using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetFetch Request => group_id [topics] 
    /// </summary>
    /// <remarks>
    /// OffsetFetch Request => group_id [topics] 
    ///   group_id => STRING
    ///   topics => topic [partitions] 
    ///     topic => STRING
    ///     partitions => partition 
    ///       partition => INT32
    /// 
    /// From http://kafka.apache.org/protocol.html#The_Messages_OffsetFetch
    /// </remarks>
    public class OffsetFetchRequest : Request, IRequest<OffsetFetchResponse>, IEquatable<OffsetFetchRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{GroupId},topics:[{Topics.ToStrings()}]}}";

        public override string ShortString() => Topics.Count == 1 ? $"{ApiKey} {GroupId} {Topics[0].TopicName}" : $"{ApiKey} {GroupId}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            writer.Write(GroupId)
                  .WriteGroupedTopics(Topics);
        }

        public OffsetFetchResponse ToResponse(IRequestContext context, ArraySegment<byte> bytes) => OffsetFetchResponse.FromBytes(context, bytes);

        public OffsetFetchRequest(string groupId, params TopicPartition[] topics) 
            : this(groupId, (IEnumerable<TopicPartition>)topics)
        {
        }

        public OffsetFetchRequest(string groupId, IEnumerable<TopicPartition> topics) 
            : base(ApiKey.OffsetFetch)
        {
            if (string.IsNullOrEmpty(groupId)) throw new ArgumentNullException(nameof(groupId));

            GroupId = groupId;
            Topics = ImmutableList<TopicPartition>.Empty.AddNotNullRange(topics);
        }

        /// <summary>
        /// The consumer group id.
        /// </summary>
        public string GroupId { get; }

        public IImmutableList<TopicPartition> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetFetchRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetFetchRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(GroupId, other.GroupId) 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((GroupId?.GetHashCode() ?? 0)*397) ^ (Topics?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion
    }
}