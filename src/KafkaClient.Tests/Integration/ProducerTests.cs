using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using NSubstitute;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [Category("Integration")]
    public class ProducerTests
    {
        [Test]
        public async Task ProducerShouldNotExpectResponseWhenAckIsZero()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var sendTask = producer.SendAsync(
                            new Message(Guid.NewGuid().ToString()), TestConfig.TopicName(), 0,
                            new SendMessageConfiguration(acks: 0), CancellationToken.None);

                        await Task.WhenAny(sendTask, Task.Delay(TimeSpan.FromMinutes(2)));

                        Assert.AreEqual(sendTask.Status, TaskStatus.RanToCompletion);
                    }
                });
            }
        }

        [Test]
        public async Task SendAsyncShouldGetOneResultForMessage()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var result = await producer.SendAsync(new[] { new Message(Guid.NewGuid().ToString()) }, topicName, 0, CancellationToken.None);

                        Assert.AreEqual(result.TopicName, topicName);
                    }
                });
            }
        }

        [Test]
        public async Task SendAsyncShouldGetOneResultForEachPartitionThroughBatching()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var tasks = new[] {
                            producer.SendAsync(new Message("1"), TestConfig.TopicName(), 0, CancellationToken.None),
                            producer.SendAsync(new Message("2"), TestConfig.TopicName(), 1, CancellationToken.None),
                            producer.SendAsync(new Message("3"), TestConfig.TopicName(), 2, CancellationToken.None),
                        };

                        await Task.WhenAll(tasks);

                        var result = tasks.Select(x => x.Result).Distinct().ToList();
                        Assert.AreEqual(result.Count, tasks.Length);
                    }
                }, 3);
            }
        }

        [Test]
        public async Task ProducerAckLevel()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel0 = await producer.SendAsync(new Message("Ack Level 0"), topicName, 0, new SendMessageConfiguration(acks: 0), CancellationToken.None);
                        Assert.AreEqual(responseAckLevel0.BaseOffset, -1);
                        var responseAckLevel1 = await producer.SendAsync(new Message("Ack Level 1"), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        Assert.True(responseAckLevel1.BaseOffset > -1);
                    }
                });
            }
        }

        [Test]
        public async Task ProducerAckLevel1ResponseOffsetShouldBeEqualToLastOffset()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 = await producer.SendAsync(new Message("Ack Level 1"), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        var offsetResponse = await producer.Router.GetOffsetsAsync(topicName, CancellationToken.None);
                        var maxOffset = offsetResponse.First(x => x.PartitionId == 0);
                        Assert.AreEqual(responseAckLevel1.BaseOffset, maxOffset.Offset - 1);
                    }
                });
            }
        }

        [Test]
        public async Task ProducerLastResposeOffsetAckLevel1ShouldBeEqualToLastOffset()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 = await producer.SendAsync(new[] { new Message("Ack Level 1"), new Message("Ack Level 1") }, topicName, 0,new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        var offsetResponse = await router.GetOffsetsAsync(topicName, CancellationToken.None);
                        var maxOffset = offsetResponse.First(x => x.PartitionId == 0);

                        Assert.AreEqual(responseAckLevel1.BaseOffset, maxOffset.Offset - 1);
                    }
                });
            }
        }

        [Test]
        public async Task ProducerShouldUsePartitionIdInsteadOfMessageKeyToChoosePartition()
        {
            var partitionSelector = Substitute.For<IPartitionSelector>();
            partitionSelector.Select(null, new ArraySegment<byte>())
                             .ReturnsForAnyArgs(_ => _.Arg<MetadataResponse.Topic>().PartitionMetadata.Single(p => p.PartitionId == 1));

            using (var router = await new KafkaOptions(TestConfig.IntegrationUri).CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                    using (var producer = new Producer(router, new ProducerConfiguration(partitionSelector: partitionSelector))) {
                        //message should send to PartitionId and not use the key to Select Broker Route !!
                        for (var i = 0; i < 20; i++) {
                            await producer.SendAsync(new Message(i.ToString(), "key"), offset.TopicName, offset.PartitionId, CancellationToken.None);
                        }
                    }

                    using (var consumer = new Consumer(offset, router)) {
                        using (var source = new CancellationTokenSource()) {
                            var i = 0;
                            await consumer.ConsumeAsync(
                                (message, token) => {
                                    Assert.AreEqual(message.Value.ToUtf8String(), i++.ToString());
                                    if (i >= 20) {
                                        source.Cancel();
                                    }
                                    return Task.FromResult(0);
                                }, source.Token, 20);
                        }
                    }
                });
            }
        }
    }
}