using System;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class PartitionSelector : IPartitionSelector
    {
        public MetadataResponse.Partition Select(MetadataResponse.Topic topic, ArraySegment<byte> key)
        {
            if (topic == null) throw new ArgumentNullException(nameof(topic));
            if (topic.PartitionMetadata.Count == 0) throw new RoutingException($"No partitions to choose on {topic}.");

            if (key.Count == 0) {
                return RoundRobinPartitionSelector.Singleton.Select(topic, key);
            }

            // use key hash
            var partitionId = Crc32.Compute(key) % topic.PartitionMetadata.Count;
            var partition = topic.PartitionMetadata.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition != null) return partition;

            throw new RoutingException($"Hash function return partition {partitionId}, but the available partitions are {string.Join(",", topic.PartitionMetadata.Select(x => x.PartitionId))}");
        }
    }
}