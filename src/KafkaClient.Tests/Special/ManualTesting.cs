using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Special
{
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
                var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                var consumer = new Consumer(offset, await TestConfig.IntegrationOptions.CreateRouterAsync(), new ConsumerConfiguration(maxPartitionFetchBytes: 10000));

                var producer = new Producer(router);
                var send = SandMessageForever(producer, offset.topic, offset.partition_id);
                var read = consumer.ConsumeAsync(
                    message => TestConfig.Log.Info(() => LogEvent.Create($"Offset{message.Offset}")), 
                    CancellationToken.None);
                await Task.WhenAll(send, read);
            }
        }

        private async Task SandMessageForever(IProducer producer, string topicName, int partitionId)
        {
            var id = 0;
            while (true) {
                try {
                    await producer.SendAsync(new Message((++id).ToString()), topicName, partitionId, CancellationToken.None);
                    await Task.Delay(100);
                } catch (Exception ex) {
                    TestConfig.Log.Info(() => LogEvent.Create(ex, "can't send:"));
                }
            }
        }
    }
}