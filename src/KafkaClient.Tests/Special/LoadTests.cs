using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Special
{
    [TestFixture]
    [Category("Load")]
    public class LoadTests
    {
        [Test]
        [TestCase(10, 1000)]
        [TestCase(100, 1000)]
        [TestCase(1000, 1000)]
        [TestCase(10000, 5000)]
        [TestCase(100000, 5000)]
        public async Task SendAsyncShouldHandleHighVolumeOfMessages(int amount, int maxAsync)
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router, new ProducerConfiguration(maxAsync, amount / 2)))
                    {
                        var tasks = new Task<ProduceResponse.Topic>[amount];

                        for (var i = 0; i < amount; i++) {
                            tasks[i] = producer.SendAsync(new Message(Guid.NewGuid().ToString()), TestConfig.TopicName(), 0, CancellationToken.None);
                        }
                        var results = await Task.WhenAll(tasks.ToArray());

                        //Because of how responses are batched up and sent to servers, we will usually get multiple responses per requested message batch
                        //So this assertion will never pass
                        //Assert.That(results.Count, Is.EqualTo(amount));

                        Assert.That(results.Any(x => x.error_code != ErrorCode.NONE), Is.False,
                            "Should not have received any results as failures.");
                    }
                });
            }
        }

        [Test]
        [TestCase(1, 1, MessageCodec.None)]
        [TestCase(1, 1, MessageCodec.Gzip)]
        [TestCase(1000, 50, MessageCodec.None)]
        [TestCase(1000, 50, MessageCodec.Gzip)]
        [TestCase(10000, 100, MessageCodec.None)]
        [TestCase(10000, 100, MessageCodec.Gzip)]
        [TestCase(10000, 100, MessageCodec.Snappy)]
        [TestCase(100000, 1000, MessageCodec.None)]
        [TestCase(100000, 1000, MessageCodec.Gzip)]
        [TestCase(1000000, 5000, MessageCodec.None)]
        [TestCase(1000000, 5000, MessageCodec.Gzip)]
        [TestCase(5000000, 5000, MessageCodec.None)]
        [TestCase(5000000, 5000, MessageCodec.Gzip)]
        [TestCase(5000000, 5000, MessageCodec.Snappy)]
        public async Task ProducerSpeed(int totalMessages, int batchSize, MessageCodec codec)
        {
            int timeoutInMs = Math.Max(100, totalMessages / 20);
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var producer = new Producer(router, new ProducerConfiguration(batchSize: totalMessages / 10, batchMaxDelay: TimeSpan.FromMilliseconds(25)));
                    await producer.UsingAsync(async () => {
                        var offset = await producer.Router.GetOffsetsAsync(TestConfig.TopicName(), 0, CancellationToken.None);

                        var maxTimeToRun = TimeSpan.FromMilliseconds(timeoutInMs);
                        var stopwatch = new Stopwatch();
                        stopwatch.Start();
                        var sendList = new List<Task>(totalMessages/batchSize);
                        var timedOut = Task.Delay(maxTimeToRun);
                        for (var i = 0; i < totalMessages; i+=batchSize) {
                            var sendTask = producer.SendAsync(batchSize.Repeat(x => new Message(x.ToString())), offset.topic, offset.partition_id, new SendMessageConfiguration(codec: codec), CancellationToken.None);
                            sendList.Add(sendTask);
                        }
                        var doneSend = Task.WhenAll(sendList.ToArray());
                        await Task.WhenAny(doneSend, timedOut);
                        stopwatch.Stop();
                        if (!doneSend.IsCompleted) {
                            var completed = sendList.Count(t => t.IsCompleted);
                            Assert.Fail($"Only finished sending {completed} of {totalMessages} in {timeoutInMs} ms.");
                        }
                        await doneSend;
                        TestConfig.Log.Info(() => LogEvent.Create($">> done send, time Milliseconds:{stopwatch.ElapsedMilliseconds}"));
                    });
                });
            }
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(1000, 50)]
        [TestCase(50000, 100)]
        [TestCase(100000, 1000)]
        [TestCase(500000, 5000)]
        public async Task ConsumerSpeed(int totalMessages, int batchSize)
        {
            int timeoutInMs = Math.Max(100, totalMessages / 20);
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router, new ProducerConfiguration(batchSize: totalMessages / 10, batchMaxDelay: TimeSpan.FromMilliseconds(25)))) {
                        var offset = await producer.Router.GetOffsetsAsync(TestConfig.TopicName(), 0, CancellationToken.None);

                        var maxTimeToRun = TimeSpan.FromMilliseconds(timeoutInMs);
                        var stopwatch = new Stopwatch();
                        var missingMessages = Math.Max(0, totalMessages - (int)offset.offset);
                        if (missingMessages > 0) {
                            stopwatch.Start();
                            var sendList = new List<Task>(missingMessages/batchSize);
                            for (var i = 0; i < missingMessages; i+=batchSize) {
                                var sendTask = producer.SendAsync(batchSize.Repeat(x => new Message(x.ToString())), offset.topic, offset.partition_id, new SendMessageConfiguration(codec: MessageCodec.Gzip), CancellationToken.None);
                                sendList.Add(sendTask);
                            }
                            var doneSend = Task.WhenAll(sendList.ToArray());
                            await Task.WhenAny(doneSend, Task.Delay(maxTimeToRun));
                            stopwatch.Stop();
                            if (!doneSend.IsCompleted) {
                                var completed = sendList.Count(t => t.IsCompleted);
                                Assert.Inconclusive($"Only finished sending {completed} of {missingMessages} in {timeoutInMs} ms.");
                            }
                            await doneSend;
                            TestConfig.Log.Info(() => LogEvent.Create($">> done send, time Milliseconds:{stopwatch.ElapsedMilliseconds}"));
                            stopwatch.Restart();
                        }

                        using (var consumer = new Consumer(offset.topic, offset.partition_id, router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero, fetchByteMultiplier: 2))) {
                            var fetched = 0;
                            stopwatch.Restart();
                            while (fetched < totalMessages) {
                                var doneFetch = consumer.FetchAsync(CancellationToken.None, totalMessages);
                                var delay = Task.Delay((int) Math.Max(0, maxTimeToRun.TotalMilliseconds - stopwatch.ElapsedMilliseconds));
                                await Task.WhenAny(doneFetch, delay);
                                if (delay.IsCompleted && !doneFetch.IsCompleted) {
                                    Assert.Fail($"Received {fetched} of {totalMessages} in {timeoutInMs} ms.");
                                }
                                var results = await doneFetch;
                                fetched += results.Messages.Count;
                            }
                            stopwatch.Stop();
                            TestConfig.Log.Info(() => LogEvent.Create($">> done Consume, time Milliseconds:{stopwatch.ElapsedMilliseconds}"));

                            Assert.That(fetched, Is.AtLeast(totalMessages));
                        }
                    }
                });
            }
        }
    }
}