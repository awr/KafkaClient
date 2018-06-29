using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [Category("Integration")]
    public class CompressionTests
    {
        [TestCase(TestConfig.Kafka10ConnectionString)]
        [TestCase(TestConfig.Kafka11ConnectionString)]
        public async Task GzipCanCompressMessageAndSend(string uri)
        {
            using (var router = await TestConfig.IntegrationOptions.WithUri(uri).CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    TestConfig.Log.Info(() => LogEvent.Create(">> Start EnsureGzipCompressedMessageCanSend"));
                    var endpoint = await Endpoint.ResolveAsync(TestConfig.IntegrationOptions.ServerUris.First(), TestConfig.IntegrationOptions.Log);
                    using (var conn1 = TestConfig.IntegrationOptions.CreateConnection(endpoint)) {
                        await conn1.SendAsync(new MetadataRequest(topicName), CancellationToken.None);
                    }

                    TestConfig.Log.Info(() => LogEvent.Create(">> Start GetTopicMetadataAsync"));
                    await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    TestConfig.Log.Info(() => LogEvent.Create(">> End GetTopicMetadataAsync"));
                    var conn = router.GetTopicConnection(topicName, 0);

                    var request = new ProduceRequest(new ProduceRequest.Topic(topicName, 0, new [] {
                                        new Message("0", "1"),
                                        new Message("1", "1"),
                                        new Message("2", "1")
                                    }, MessageCodec.Gzip));
                    TestConfig.Log.Info(() => LogEvent.Create(">> start SendAsync"));
                    var response = await conn.Connection.SendAsync(request, CancellationToken.None);
                    TestConfig.Log.Info(() => LogEvent.Create("end SendAsync"));
                    Assert.False(response.Errors.Any(e => e != ErrorCode.NONE));
                    TestConfig.Log.Info(() => LogEvent.Create("start dispose"));
                    TestConfig.Log.Info(() => LogEvent.Create(">> End EnsureGzipCompressedMessageCanSend"));
                });
            }
        }

        [TestCase(TestConfig.Kafka10ConnectionString)]
        [TestCase(TestConfig.Kafka11ConnectionString)]
        public async Task GzipCanDecompressMessageFromKafka(string uri)
        {
            const int numberOfMessages = 3;
            const int partitionId = 0;

            TestConfig.Log.Info(() => LogEvent.Create(">> Start EnsureGzipCanDecompressMessageFromKafka"));
            using (var router = await TestConfig.IntegrationOptions.WithUri(uri).CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    OffsetsResponse.Topic offset;
                    var messages = new List<Message>();
                    using (var producer = new Producer(router, new ProducerConfiguration(batchSize: numberOfMessages))) {
                        offset = await producer.Router.GetOffsetsAsync(topicName, 0, CancellationToken.None) ?? new OffsetsResponse.Topic(topicName, partitionId, offset: 0);
                        for (var i = 0; i < numberOfMessages; i++) {
                            messages.Add(new Message(i.ToString()));
                        }
                        TestConfig.Log.Info(() => LogEvent.Create(">> Start Produce"));
                        await producer.SendAsync(messages, topicName, partitionId, new SendMessageConfiguration(codec: MessageCodec.Gzip), CancellationToken.None);
                        TestConfig.Log.Info(() => LogEvent.Create(">> End Produce"));
                    }
                    TestConfig.Log.Info(() => LogEvent.Create(">> Start Consume"));
                    using (var consumer = new Consumer(offset, router)) {
                        var results = await consumer.FetchAsync(CancellationToken.None, messages.Count);
                        TestConfig.Log.Info(() => LogEvent.Create(">> End Consume"));
                        Assert.NotNull(results);
                        Assert.AreEqual(results.Messages.Count, messages.Count);
                        for (var i = 0; i < messages.Count; i++) {
                            Assert.AreEqual(results.Messages[i].Value.ToUtf8String(), i.ToString());
                        }
                    }
                    TestConfig.Log.Info(() => LogEvent.Create(">> End EnsureGzipCanDecompressMessageFromKafka"));
                });
            }
        }

        [TestCase(TestConfig.Kafka10ConnectionString)]
        [TestCase(TestConfig.Kafka11ConnectionString)]
        public async Task SnappyCanCompressMessageAndSend(string uri)
        {
            using (var router = await TestConfig.IntegrationOptions.WithUri(uri).CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    TestConfig.Log.Info(() => LogEvent.Create(">> Start EnsureGzipCompressedMessageCanSend"));
                    var endpoint = await Endpoint.ResolveAsync(TestConfig.IntegrationOptions.ServerUris.First(), TestConfig.IntegrationOptions.Log);
                    using (var conn1 = TestConfig.IntegrationOptions.CreateConnection(endpoint)) {
                        await conn1.SendAsync(new MetadataRequest(topicName), CancellationToken.None);
                    }

                    TestConfig.Log.Info(() => LogEvent.Create(">> Start GetTopicMetadataAsync"));
                    await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    TestConfig.Log.Info(() => LogEvent.Create(">> End GetTopicMetadataAsync"));
                    var conn = router.GetTopicConnection(topicName, 0);

                    var request = new ProduceRequest(new ProduceRequest.Topic(topicName, 0, new [] {
                                        new Message("0", "1"),
                                        new Message("1", "1"),
                                        new Message("2", "1")
                                    }, MessageCodec.Snappy));
                    TestConfig.Log.Info(() => LogEvent.Create(">> start SendAsync"));
                    var response = await conn.Connection.SendAsync(request, CancellationToken.None);
                    TestConfig.Log.Info(() => LogEvent.Create("end SendAsync"));
                    Assert.False(response.Errors.Any(e => e != ErrorCode.NONE));
                    TestConfig.Log.Info(() => LogEvent.Create("start dispose"));
                    TestConfig.Log.Info(() => LogEvent.Create(">> End EnsureGzipCompressedMessageCanSend"));
                });
            }
        }

        [TestCase(TestConfig.Kafka10ConnectionString)]
        [TestCase(TestConfig.Kafka11ConnectionString)]
        public async Task SnappyCanDecompressMessageFromKafka(string uri)
        {
            const int numberOfMessages = 3;
            const int partitionId = 0;

            TestConfig.Log.Info(() => LogEvent.Create(">> Start EnsureGzipCanDecompressMessageFromKafka"));
            using (var router = await TestConfig.IntegrationOptions.WithUri(uri).CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    OffsetsResponse.Topic offset;
                    var messages = new List<Message>();
                    using (var producer = new Producer(router, new ProducerConfiguration(batchSize: numberOfMessages))) {
                        offset = await producer.Router.GetOffsetsAsync(topicName, 0, CancellationToken.None) ?? new OffsetsResponse.Topic(topicName, partitionId, offset: 0);
                        for (var i = 0; i < numberOfMessages; i++) {
                            messages.Add(new Message(i.ToString()));
                        }
                        TestConfig.Log.Info(() => LogEvent.Create(">> Start Produce"));
                        await producer.SendAsync(messages, topicName, partitionId, new SendMessageConfiguration(codec: MessageCodec.Snappy), CancellationToken.None);
                        TestConfig.Log.Info(() => LogEvent.Create(">> End Produce"));
                    }
                    TestConfig.Log.Info(() => LogEvent.Create(">> Start Consume"));
                    using (var consumer = new Consumer(offset, router)) {
                        var results = await consumer.FetchAsync(CancellationToken.None, messages.Count);
                        TestConfig.Log.Info(() => LogEvent.Create(">> End Consume"));
                        Assert.NotNull(results);
                        Assert.AreEqual(results.Messages.Count, messages.Count);
                        for (var i = 0; i < messages.Count; i++) {
                            Assert.AreEqual(results.Messages[i].Value.ToUtf8String(), i.ToString());
                        }
                    }
                    TestConfig.Log.Info(() => LogEvent.Create(">> End EnsureGzipCanDecompressMessageFromKafka"));
                });
            }
        }
    }
}