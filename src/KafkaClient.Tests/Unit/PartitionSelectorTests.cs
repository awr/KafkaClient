using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using Xunit;

namespace KafkaClient.Tests.Unit
{
    public class PartitionSelectorTests
    {
        private MetadataResponse.Topic _topicA;
        private MetadataResponse.Topic _topicB;

        [SetUp]
        public void Setup()
        {
            _topicA = new MetadataResponse.Topic("a", ErrorCode.NONE, new [] {
                                            new MetadataResponse.Partition(0, 0),
                                            new MetadataResponse.Partition(1, 1),
                                        });
            _topicB = new MetadataResponse.Topic("b", ErrorCode.NONE, new [] {
                                            new MetadataResponse.Partition(0, 0),
                                            new MetadataResponse.Partition(1, 1),
                                        });
        }

        [Fact]
        public void RoundRobinShouldRollOver()
        {
            RoundRobinPartitionSelector.Singleton.Reset();
            var selector = RoundRobinPartitionSelector.Singleton;

            var first = selector.Select(_topicA, new ArraySegment<byte>());
            var second = selector.Select(_topicA, new ArraySegment<byte>());
            var third = selector.Select(_topicA, new ArraySegment<byte>());

            Assert.That(first.partition_id, Is.EqualTo(0));
            Assert.That(second.partition_id, Is.EqualTo(1));
            Assert.That(third.partition_id, Is.EqualTo(0));
        }

        [Fact]
        public void RoundRobinShouldHandleMultiThreadedRollOver()
        {
            RoundRobinPartitionSelector.Singleton.Reset();
            var bag = new ConcurrentBag<MetadataResponse.Partition>();

            Parallel.For(0, 100, x => bag.Add(RoundRobinPartitionSelector.Singleton.Select(_topicA, new ArraySegment<byte>())));

            Assert.That(bag.Count(x => x.partition_id == 0), Is.EqualTo(50));
            Assert.That(bag.Count(x => x.partition_id == 1), Is.EqualTo(50));
        }

        [Fact]
        public void RoundRobinShouldTrackEachTopicSeparately()
        {
            RoundRobinPartitionSelector.Singleton.Reset();
            var selector = RoundRobinPartitionSelector.Singleton;

            var a1 = selector.Select(_topicA, new ArraySegment<byte>());
            var b1 = selector.Select(_topicB, new ArraySegment<byte>());
            var a2 = selector.Select(_topicA, new ArraySegment<byte>());
            var b2 = selector.Select(_topicB, new ArraySegment<byte>());

            Assert.That(a1.partition_id, Is.EqualTo(0));
            Assert.That(a2.partition_id, Is.EqualTo(1));

            Assert.That(b1.partition_id, Is.EqualTo(0));
            Assert.That(b2.partition_id, Is.EqualTo(1));
        }

        [Fact]
        public void RoundRobinShouldEvenlyDistributeAcrossManyPartitions()
        {
            RoundRobinPartitionSelector.Singleton.Reset();
            const int TotalPartitions = 100;
            var partitions = new List<MetadataResponse.Partition>();
            for (int i = 0; i < TotalPartitions; i++)
            {
                partitions.Add(new MetadataResponse.Partition(i, i));
            }
            var topic = new MetadataResponse.Topic("a", partitions: partitions);

            var bag = new ConcurrentBag<MetadataResponse.Partition>();
            Parallel.For(0, TotalPartitions * 3, x => bag.Add(RoundRobinPartitionSelector.Singleton.Select(topic, new ArraySegment<byte>())));

            var eachPartitionHasThree = bag.GroupBy(x => x.partition_id).Count();

            Assert.That(eachPartitionHasThree, Is.EqualTo(TotalPartitions), "Each partition should have received three selections.");
        }

        [Fact]
        public void KeyHashShouldSelectEachPartitionType()
        {
            var selector = new PartitionSelector();

            var first = selector.Select(_topicA, CreateKeyForPartition(0));
            var second = selector.Select(_topicA, CreateKeyForPartition(1));

            Assert.That(first.partition_id, Is.EqualTo(0));
            Assert.That(second.partition_id, Is.EqualTo(1));
        }

        private ArraySegment<byte> CreateKeyForPartition(int partitionId)
        {
            while (true)
            {
                var key = new ArraySegment<byte>(Guid.NewGuid().ToString().ToIntSizedBytes());
                if ((Crc32.Compute(key) % 2) == partitionId)
                    return key;
            }
        }

        [Fact]
        public void KeyHashShouldThrowExceptionWhenChoosesAPartitionIdThatDoesNotExist()
        {
            var selector = new PartitionSelector();
            var topic = new MetadataResponse.Topic("badPartition", partitions: new [] {
                                              new MetadataResponse.Partition(0, 0),
                                              new MetadataResponse.Partition(999, 1) 
                                          });

            Assert.Throws<RoutingException>(() => selector.Select(topic, CreateKeyForPartition(1)));
        }

        [Fact]
        public void PartitionSelectionOnEmptyKeyHashShouldNotFail()
        {
            var selector = new PartitionSelector();
            var topic = new MetadataResponse.Topic("badPartition", partitions: new [] {
                                              new MetadataResponse.Partition(0, 0),
                                              new MetadataResponse.Partition(999, 1) 
                                          });

            Assert.That(selector.Select(topic, new ArraySegment<byte>()), Is.Not.Null);
        }

        [Fact]
        public void SelectorShouldThrowExceptionWhenPartitionsAreEmpty()
        {
            var selector = new PartitionSelector();
            var topic = new MetadataResponse.Topic("emptyPartition");
            Assert.Throws<RoutingException>(() => selector.Select(topic, CreateKeyForPartition(1)));
        }

        [Fact]
        public void RoundRobinShouldThrowExceptionWhenPartitionsAreEmpty()
        {
            var topic = new MetadataResponse.Topic("emptyPartition");
            Assert.Throws<RoutingException>(() => RoundRobinPartitionSelector.Singleton.Select(topic, CreateKeyForPartition(1)));
        }
    }
}