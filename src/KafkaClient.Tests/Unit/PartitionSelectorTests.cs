﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("CI")]
    public class PartitionSelectorTests
    {
        private readonly MetadataResponse.Topic _topicA;
        private readonly MetadataResponse.Topic _topicB;

        public PartitionSelectorTests()
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

        [Test]
        public void RoundRobinShouldRollOver()
        {
            RoundRobinPartitionSelector.Singleton.Reset();
            var selector = RoundRobinPartitionSelector.Singleton;

            var first = selector.Select(_topicA, new ArraySegment<byte>());
            var second = selector.Select(_topicA, new ArraySegment<byte>());
            var third = selector.Select(_topicA, new ArraySegment<byte>());

            Assert.AreEqual(0, first.PartitionId);
            Assert.AreEqual(1, second.PartitionId);
            Assert.AreEqual(0, third.PartitionId);
        }

        [Test]
        public void RoundRobinShouldHandleMultiThreadedRollOver()
        {
            RoundRobinPartitionSelector.Singleton.Reset();
            var bag = new ConcurrentBag<MetadataResponse.Partition>();

            Parallel.For(0, 100, x => bag.Add(RoundRobinPartitionSelector.Singleton.Select(_topicA, new ArraySegment<byte>())));

            Assert.AreEqual(50, bag.Count(x => x.PartitionId == 0));
            Assert.AreEqual(50, bag.Count(x => x.PartitionId == 1));
        }

        [Test]
        public void RoundRobinShouldTrackEachTopicSeparately()
        {
            RoundRobinPartitionSelector.Singleton.Reset();
            var selector = RoundRobinPartitionSelector.Singleton;

            var a1 = selector.Select(_topicA, new ArraySegment<byte>());
            var b1 = selector.Select(_topicB, new ArraySegment<byte>());
            var a2 = selector.Select(_topicA, new ArraySegment<byte>());
            var b2 = selector.Select(_topicB, new ArraySegment<byte>());

            Assert.AreEqual(0, a1.PartitionId);
            Assert.AreEqual(1, a2.PartitionId);

            Assert.AreEqual(0, b1.PartitionId);
            Assert.AreEqual(1, b2.PartitionId);
        }

        [Test]
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

            var eachPartitionHasThree = bag.GroupBy(x => x.PartitionId).Count();

            Assert.AreEqual(TotalPartitions, eachPartitionHasThree); // Each partition should have received three selections.
        }

        [Test]
        public void KeyHashShouldSelectEachPartitionType()
        {
            var selector = new PartitionSelector();

            var first = selector.Select(_topicA, CreateKeyForPartition(0));
            var second = selector.Select(_topicA, CreateKeyForPartition(1));

            Assert.AreEqual(0, first.PartitionId);
            Assert.AreEqual(1, second.PartitionId);
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

        [Test]
        public void KeyHashShouldThrowExceptionWhenChoosesAPartitionIdThatDoesNotExist()
        {
            var selector = new PartitionSelector();
            var topic = new MetadataResponse.Topic("badPartition", partitions: new [] {
                                              new MetadataResponse.Partition(0, 0),
                                              new MetadataResponse.Partition(999, 1) 
                                          });

            Assert.Throws<RoutingException>(() => selector.Select(topic, CreateKeyForPartition(1)));
        }

        [Test]
        public void PartitionSelectionOnEmptyKeyHashShouldNotFail()
        {
            var selector = new PartitionSelector();
            var topic = new MetadataResponse.Topic("badPartition", partitions: new [] {
                                              new MetadataResponse.Partition(0, 0),
                                              new MetadataResponse.Partition(999, 1) 
                                          });

            Assert.NotNull(selector.Select(topic, new ArraySegment<byte>()));
        }

        [Test]
        public void SelectorShouldThrowExceptionWhenPartitionsAreEmpty()
        {
            var selector = new PartitionSelector();
            var topic = new MetadataResponse.Topic("emptyPartition");
            Assert.Throws<RoutingException>(() => selector.Select(topic, CreateKeyForPartition(1)));
        }

        [Test]
        public void RoundRobinShouldThrowExceptionWhenPartitionsAreEmpty()
        {
            var topic = new MetadataResponse.Topic("emptyPartition");
            Assert.Throws<RoutingException>(() => RoundRobinPartitionSelector.Singleton.Select(topic, CreateKeyForPartition(1)));
        }
    }
}