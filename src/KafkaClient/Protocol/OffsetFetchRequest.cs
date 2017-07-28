using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
// ReSharper disable InconsistentNaming

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetFetch Request => group_id [topics]
    ///  group_id => STRING     -- The consumer group id.
    ///  topics => topic [partition] 
    ///   topic => STRING       -- The topic to commit.
    ///   partition_id => INT32    -- The partition id.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// Class that represents both the request and the response from a kafka server of requesting a stored offset value
    /// for a given consumer group.  Essentially this part of the api allows a user to save/load a given offset position
    /// under any abritrary name.
    /// </summary>
    public class OffsetFetchRequest : Request, IRequest<OffsetFetchResponse>, IEquatable<OffsetFetchRequest>
    {
        public override string ToString() => $"{{Api:{ApiKey},group_id:{group_id},topics:[{topics.ToStrings()}]}}";

        public override string ShortString() => topics.Count == 1 ? $"{ApiKey} {group_id} {topics[0].TopicName}" : $"{ApiKey} {group_id}";

        protected override void EncodeBody(IKafkaWriter writer, IRequestContext context)
        {
            var topicGroups = topics.GroupBy(x => x.TopicName).ToList();

            writer.Write(group_id)
                  .Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups) {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                writer.Write(topicGroup.Key)
                      .Write(partitions.Count);

                foreach (var partition in partitions) {
                    foreach (var offset in partition) {
                        writer.Write(offset.PartitionId);
                    }
                }
            }
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

            group_id = groupId;
            this.topics = ImmutableList<TopicPartition>.Empty.AddNotNullRange(topics);
        }

        public string group_id { get; }

        public IImmutableList<TopicPartition> topics { get; }

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
            return string.Equals(group_id, other.group_id) 
                && topics.HasEqualElementsInOrder(other.topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((group_id?.GetHashCode() ?? 0)*397) ^ (topics?.Count.GetHashCode() ?? 0);
            }
        }

        #endregion
    }
}