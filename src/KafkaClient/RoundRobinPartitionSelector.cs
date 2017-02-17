using System;
using System.Collections.Concurrent;
using KafkaClient.Protocol;

namespace KafkaClient
{
    // ReSharper disable once ClassNeverInstantiated.Global -- see LazySingleton below
    public class RoundRobinPartitionSelector : IPartitionSelector
    {
        private static readonly Lazy<RoundRobinPartitionSelector> LazySingleton = new Lazy<RoundRobinPartitionSelector>();
        public static RoundRobinPartitionSelector Singleton => LazySingleton.Value;

        // for use in tests only
        internal void Reset() => _tracker.Clear();

        private readonly ConcurrentDictionary<string, int> _tracker = new ConcurrentDictionary<string, int>();

        public MetadataResponse.Partition Select(MetadataResponse.Topic topic, ArraySegment<byte> key)
        {
            if (topic == null) throw new ArgumentNullException(nameof(topic));
            if (topic.partition_metadata.Count == 0) throw new RoutingException($"No partitions to choose on {topic}.");

            var paritionIndex = _tracker.AddOrUpdate(topic.topic, p => 0, (s, i) => (i + 1) % topic.partition_metadata.Count);
            return topic.partition_metadata[paritionIndex];
        }
    }
}