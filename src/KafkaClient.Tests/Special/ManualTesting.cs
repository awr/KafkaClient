using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Special
{
    [TestFixture]
    [Category("Manual")]
    internal class ManualTesting
    {
        /// <summary>
        /// These tests are for manual run. You need to stop the partition leader and then start it again and let it became the leader.        
        /// </summary>

        [Test]
        public async Task NewlyCreatedTopicShouldRetryUntilBrokerIsAssigned()
        {
            // Disable auto topic create in our server
            var expectedTopic = Guid.NewGuid().ToString();
            var router = await TestConfig.IntegrationOptions.CreateRouterAsync();
            var response = router.GetMetadataAsync(new MetadataRequest(expectedTopic), CancellationToken.None);
            var topic = (await response).topic_metadata.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.topic, Is.EqualTo(expectedTopic));
            Assert.That(topic.topic_error_code, Is.EqualTo((int)ErrorCode.NONE));
        }

        [Test]
        public async Task ManualConsumerFailure()
        {
            var topicName = "TestTopicIssue13-3R-1P";
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                var consumer = new Consumer(await TestConfig.IntegrationOptions.CreateRouterAsync(), new ConsumerConfiguration(maxPartitionFetchBytes: 10000));
                var offset = await router.GetOffsetAsync(topicName, 0, CancellationToken.None);

                var producer = new Producer(router);
                var send = SandMessageForever(producer, offset.topic, offset.partition_id);
                var read = ReadMessageForever(consumer, offset.topic, offset.partition_id, offset.offset);
                await Task.WhenAll(send, read);
            }
        }

        private async Task SandMessageForever(IProducer producer, string topicName, int partitionId)
        {
            for (var id = 0;;) {
                try {
                    await producer.SendAsync(new Message((++id).ToString()), topicName, partitionId, CancellationToken.None);
                    await Task.Delay(100);
                } catch (Exception ex) {
                    TestConfig.Log.Info(() => LogEvent.Create(ex, "can't send:"));
                }
            }
        }

        private  async Task ReadMessageForever(IConsumer consumer, string topicName, int partitionId, long offset)
        {
            while (true) {
                try {
                    var batch = await consumer.FetchBatchAsync(topicName, partitionId, offset, CancellationToken.None, 100);

                    if (batch.Messages.Any()) {
                        foreach (var message in batch.Messages) {
                            TestConfig.Log.Info(() => LogEvent.Create($"Offset{message.Offset}"));
                        }
                        offset = batch.Messages.Max(x => x.Offset) + 3;
                    } else {
                        await Task.Delay(100);
                    }
                } catch (Exception ex) {
                    TestConfig.Log.Info(() => LogEvent.Create(ex, "can't read:"));
                }
            }
        }
    }
}