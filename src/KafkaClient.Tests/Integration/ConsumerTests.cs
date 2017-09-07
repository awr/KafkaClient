using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

#pragma warning disable 1998

namespace KafkaClient.Tests.Integration
{
    [Category("Integration")]
    public class ConsumerTests
    {
        [Test]
        public async Task CanFetch()
        {
            const int partitionId = 0;
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var messageValue = Guid.NewGuid().ToString();
                        var response = await producer.SendAsync(new Message(messageValue), topicName, partitionId, CancellationToken.None);
                        var offset = response.BaseOffset;

                        var fetch = new FetchRequest.Topic(topicName, partitionId, offset, 32000);

                        var fetchRequest = new FetchRequest(fetch, minBytes: 10);

                        var r = await router.SendAsync(fetchRequest, topicName, partitionId, CancellationToken.None);
                        Assert.AreEqual(r.Responses.First().Messages.First().Value.ToUtf8String(), messageValue);
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesSimpleTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                        using (var consumer = new Consumer(offset, router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {

                            // Produce 5 messages
                            var messages = CreateTestMessages(5, 1);
                            await producer.SendAsync(messages, topicName, 0, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchAsync(CancellationToken.None, 5);
                            CheckMessages(messages, result);
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesCacheContainsAllRequestTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                        using (var consumer = new Consumer(offset, router, TestConfig.IntegrationOptions.ConsumerConfiguration, autoConsume: false)) {

                            // Produce 5 messages
                            var messages = CreateTestMessages(10, 1);
                            await producer.SendAsync(messages, topicName, 0, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchAsync(CancellationToken.None, 5);
                            CheckMessages(messages.Take(5).ToList(), result);

                            // Now let's consume again
                            result = await result.FetchNextAsync(CancellationToken.None);
                            CheckMessages(messages.Skip(5).ToList(), result);
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesCacheContainsPartOfRequestTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                        using (var consumer = new Consumer(offset, router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {

                            // Produce messages
                            var messages = CreateTestMessages(10, 4096);
                            await producer.SendAsync(messages, topicName, 0, new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Consume messages, and check
                            var result = await consumer.FetchAsync(CancellationToken.None, 5);
                            CheckMessages(messages.Take(5).ToList(), result);

                            // Now let's consume again
                            result = await result.FetchNextAsync(CancellationToken.None);
                            CheckMessages(messages.Skip(5).ToList(), result);
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesNoNewMessagesInQueueTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                    using (var consumer = new Consumer(offset, router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {

                        // Now let's consume
                        var result = await consumer.FetchAsync(CancellationToken.None, 5);
                        Assert.AreEqual(0, result.Messages.Count); // Should not get any messages
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesOffsetBiggerThanLastOffsetInQueueTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                    using (var consumer = new Consumer(new TopicOffset(offset.TopicName, offset.PartitionId, offset.Offset + 1), router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {

                        await AssertAsync.Throws<FetchOutOfRangeException>(
                            () => consumer.FetchAsync(CancellationToken.None, 5),
                            ex => ex.Message.Contains($"{TestConfig.IntegrationUri.Host}:{TestConfig.IntegrationUri.Port} returned OFFSET_OUT_OF_RANGE for Fetch request"));
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesInvalidOffsetTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(new TopicOffset(topicName, 0, -1), router, TestConfig.IntegrationOptions.ConsumerConfiguration)) {

                        // Now let's consume
                        await AssertAsync.Throws<FetchOutOfRangeException>(() => consumer.FetchAsync(CancellationToken.None, 5));
                    }
                });
            }
        }

        [Category("Flaky")]
        [Test]
        public async Task FetchMessagesTopicDoesntExist([Values(true, false)] bool deleteFirst)
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                var topicName = TestConfig.TopicName();
                if (deleteFirst) {
                    topicName += "-deleted";
                    await router.DeleteTopicAsync(topicName);
                }
                using (var consumer = new Consumer(new TopicOffset(topicName, 0, 0), router, new ConsumerConfiguration(maxPartitionFetchBytes: TestConfig.IntegrationOptions.ConsumerConfiguration.MaxFetchBytes * 2))) {

                    await AssertAsync.Throws<RoutingException>(
                        () => consumer.FetchAsync(CancellationToken.None, 5),
                        ex => ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {0} defined."));
                }
            }
        }

        [Test]
        public async Task FetchMessagesPartitionDoesntExist()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var consumer = new Consumer(topicName, 100, router, new ConsumerConfiguration(maxPartitionFetchBytes: TestConfig.IntegrationOptions.ConsumerConfiguration.MaxFetchBytes * 2))) {
                        await AssertAsync.Throws<RoutingException>(
                            () => consumer.FetchAsync(CancellationToken.None, 5),
                            ex => ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {100} defined."));
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesBufferUnderRunNoMultiplier()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var smallMessageSet = 4096 / 2;

                    using (var producer = new Producer(router)) {
                        var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                        using (var consumer = new Consumer(offset, router, new ConsumerConfiguration(maxPartitionFetchBytes: smallMessageSet, fetchByteMultiplier: 1))) {

                            // Creating 5 messages
                            var messages = CreateTestMessages(10, 4096);

                            await producer.SendAsync(
                                messages, topicName, 0,
                                new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            await AssertAsync.Throws<BufferUnderRunException>(() => consumer.FetchAsync(CancellationToken.None, 5));
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchMessagesBufferUnderRunWithMultiplier()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var smallMessageSet = 4096 / 3;

                    using (var producer = new Producer(router)) {
                        var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                        using (var consumer = new Consumer(offset, router, new ConsumerConfiguration(maxPartitionFetchBytes: smallMessageSet, fetchByteMultiplier: 2))) {

                            // Creating 5 messages
                            var messages = CreateTestMessages(10, 4096);

                            await producer.SendAsync(
                                messages, topicName, 0,
                                new SendMessageConfiguration(ackTimeout: TimeSpan.FromSeconds(3)), CancellationToken.None);

                            // Now let's consume
                            await consumer.FetchAsync(CancellationToken.None);
                        }
                    }
                });
            }
        }

        [Test]
        public async Task FetchOffsetConsumerGroupDoesntExistTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var groupId = Guid.NewGuid().ToString();

                    await router.GetOffsetsAsync(groupId, topicName, partitionId, CancellationToken.None);
                });
            }
        }

        [Test]
        public async Task FetchOffsetPartitionDoesntExistTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;
                    var groupId = TestConfig.GroupId();

                    try {
                        await router.GetOffsetsAsync(groupId, topicName, partitionId, CancellationToken.None);
                        Assert.True(false, "should have thrown RoutingException");
                    } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {partitionId} defined.")) {
                        // expected
                    }
                });
            }
        }

        [Category("Flaky")]
        [Test]
        public async Task FetchOffsetTopicDoesntExistTest([Values(true, false)] bool deleteFirst)
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                var topicName = TestConfig.TopicName();
                if (deleteFirst) {
                    topicName += "-deleted";
                    await router.DeleteTopicAsync(topicName);
                }

                var groupId = TestConfig.GroupId();
                try {
                    await router.GetOffsetsAsync(groupId, topicName, 0, CancellationToken.None);
                    Assert.True(false, "should have thrown RoutingException");
                } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {0} defined.")) {
                    // expected
                }
            }
        }

        [Test]
        public async Task FetchOffsetConsumerGroupExistsTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var groupId = TestConfig.GroupId();

                    var offset = 5L;

                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offset, CancellationToken.None);
                    var res = await router.GetOffsetsAsync(groupId, topicName, 0, CancellationToken.None);

                    Assert.AreEqual(offset, res.Offset);
                });
            }
        }

        [TestCase(null)]
        [TestCase("")]
        public async Task FetchOffsetConsumerGroupArgumentNull(string group)
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var groupId = TestConfig.GroupId();

                    var offset = 5;

                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offset, CancellationToken.None);
                    Assert.ThrowsAsync<ArgumentNullException>(async () => await router.GetOffsetsAsync(group, topicName, partitionId, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetConsumerGroupExistsTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var groupId = TestConfig.GroupId();

                    var offest = 5;
                    var newOffset = 10;

                    await router.GetOffsetsAsync(topicName, partitionId, CancellationToken.None);
                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None);
                    var res = await router.GetOffsetsAsync(groupId, topicName, partitionId, CancellationToken.None);
                    Assert.AreEqual(offest, res.Offset);

                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, newOffset, CancellationToken.None);
                    res = await router.GetOffsetsAsync(groupId, topicName, partitionId, CancellationToken.None);

                    Assert.AreEqual(newOffset, res.Offset);
                });
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetPartitionDoesntExistTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;
                    var groupId = Guid.NewGuid().ToString();

                    var offest = 5;
                    try {
                        await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None);
                        Assert.True(false, "should have thrown RoutingException");
                    } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {partitionId} defined.")) {
                        // expected
                    }
                });
            }
        }

        [Category("Flaky")]
        [Test]
        public async Task UpdateOrCreateOffsetTopicDoesntExistTest([Values(true, false)] bool deleteFirst)
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                var topicName = TestConfig.TopicName();
                if (deleteFirst) {
                    topicName += "-deleted";
                    await router.DeleteTopicAsync(topicName);
                }

                var partitionId = 0;
                var groupId = TestConfig.GroupId();

                var offest = 5;
                try {
                    await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None);
                    Assert.True(false, "should have thrown RoutingException");
                } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {0} defined.")) {
                    // expected
                }
            }
        }

        [TestCase(null)]
        [TestCase("")]
        public async Task UpdateOrCreateOffsetConsumerGroupArgumentNull(string group)
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;

                    var offest = 5;

                    Assert.ThrowsAsync<ArgumentNullException>(async () => await router.CommitTopicOffsetAsync(topicName, partitionId, group, offest, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task UpdateOrCreateOffsetNegativeOffsetTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 0;
                    var groupId = TestConfig.GroupId();

                    var offest = -5;

                    Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await router.CommitTopicOffsetAsync(topicName, partitionId, groupId, offest, CancellationToken.None));
                });
            }
        }

        [Test]
        public async Task FetchLastOffsetSimpleTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var offset = await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);

                    Assert.AreNotEqual(-1, offset.Offset);
                });
            }
        }

        [Test]
        public async Task FetchLastOffsetPartitionDoesntExistTest()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var partitionId = 100;

                    try {
                        await router.GetOffsetsAsync(topicName, partitionId, CancellationToken.None);
                        Assert.True(false, "should have thrown RoutingException");
                    } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {partitionId} defined.")) {
                        // expected
                    }
                });
            }
        }

        [Category("Flaky")]
        [Test]
        public async Task FetchLastOffsetTopicDoesntExistTest([Values(true, false)] bool deleteFirst)
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                var topicName = TestConfig.TopicName();
                if (deleteFirst) {
                    topicName += "-deleted";
                    await router.DeleteTopicAsync(topicName);
                }

                try {
                    await router.GetOffsetsAsync(topicName, 0, CancellationToken.None);
                    Assert.True(false, "should have thrown RoutingException");
                } catch (RoutingException ex) when (ex.Message.StartsWith($"The topic ({topicName}) has no partitionId {0} defined.")) {
                    // expected
                }
            }
        }

        [Test]
        public async Task ConsumeByOffsetShouldGetSameMessageProducedAtSameOffset()
        {
            long offsetResponse;
            var messge = Guid.NewGuid();

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 = await producer.SendAsync(new Message(messge.ToString()), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        offsetResponse = responseAckLevel1.BaseOffset;
                    }
                    using (var consumer = new Consumer(new TopicOffset(topicName, 0, offsetResponse), router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                        var result = await consumer.FetchAsync(CancellationToken.None, 1);
                        Assert.AreEqual(messge.ToString(), result.Messages[0].Value.ToUtf8String());
                    }
                });
            }
        }

        [TestCase(20, 1)]
        [TestCase(20, 10)]
        public async Task ConsumerShouldBeAbleToSeekBackToEarlierOffset(int sends, int messagesPerSend)
        {
            var totalMessages = sends * messagesPerSend;

            var testId = Guid.NewGuid().ToString();

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var offset = await producer.Router.GetOffsetsAsync(topicName, 0, CancellationToken.None);

                        for (var i = 0; i < sends; i++) {
                            if (messagesPerSend == 1) {
                                await producer.SendAsync(new Message(i.ToString(), testId), topicName, 0, CancellationToken.None);
                            } else {
                                var current = i * messagesPerSend;
                                var messages = messagesPerSend.Repeat(_ => new Message((current + _).ToString(), testId)).ToList();
                                await producer.SendAsync(messages, topicName, 0, CancellationToken.None);
                            }
                        }

                        IMessageBatch results1, results2;
                        using (var consumer = new Consumer(offset, router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                            results1 = await consumer.FetchAsync(CancellationToken.None, totalMessages);
                            TestConfig.Log.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results1.Messages.Select(x => x.Value.ToUtf8String()).ToList())}"));
                        }
                        using (var consumer = new Consumer(offset, router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                            results2 = await consumer.FetchAsync(CancellationToken.None, totalMessages);
                            TestConfig.Log.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results2.Messages.Select(x => x.Value.ToUtf8String()).ToList())}"));
                        }

                        Assert.AreEqual(results1.Messages.Count, results2.Messages.Count);
                        Assert.AreEqual(results1.Messages.Count, totalMessages);
                        Assert.AreEqual(results1.Messages.Select(x => x.Value.ToUtf8String()).ToList(), results2.Messages.Select(x => x.Value.ToUtf8String()).ToList()); // Expected the message list in the correct order
                    }
                });
            }
        }

        [Test]
        public async Task ConsumerShouldBeAbleToGetCurrentOffsetInformation()
        {
            var totalMessages = 20;
            var expected = totalMessages.Repeat(i => i.ToString()).ToList();
            var testId = Guid.NewGuid().ToString();

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var offset = await producer.Router.GetOffsetsAsync(topicName, 0, CancellationToken.None);

                        for (var i = 0; i < totalMessages; i++) {
                            await producer.SendAsync(new Message(i.ToString(), testId), topicName, 0, CancellationToken.None);
                        }

                        using (var consumer = new Consumer(offset, router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                            var results = await consumer.FetchAsync(CancellationToken.None, totalMessages);
                            TestConfig.Log.Info(() => LogEvent.Create($"Message order:  {string.Join(", ", results.Messages.Select(x => x.Value.ToUtf8String()).ToList())}"));

                            Assert.AreEqual(results.Messages.Count, totalMessages);
                            Assert.AreEqual(results.Messages.Select(x => x.Value.ToUtf8String()).ToList(), expected); // Expected the message list in the correct order.

                            var newOffset = await producer.Router.GetOffsetsAsync(offset.TopicName, offset.PartitionId, CancellationToken.None);
                            Assert.AreEqual(newOffset.Offset - offset.Offset, totalMessages);
                        }
                    }
                });
            }
        }

        [TestCase(5, 200)]
        [TestCase(1000, 500)]
        public async Task ConsumerShouldConsumeInSameOrderAsProduced(int totalMessages, int timeoutInMs)
        {
            var expected = totalMessages.Repeat(i => i.ToString()).ToList();

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router, new ProducerConfiguration(batchSize: totalMessages / 10, batchMaxDelay: TimeSpan.FromMilliseconds(25)))) {
                        var offset = await producer.Router.GetOffsetsAsync(topicName, 0, CancellationToken.None);

                        var stopwatch = new Stopwatch();
                        stopwatch.Start();
                        var sendList = new List<Task>(totalMessages);
                        for (var i = 0; i < totalMessages; i++) {
                            var sendTask = producer.SendAsync(new Message(i.ToString()), offset.TopicName, offset.PartitionId, CancellationToken.None);
                            sendList.Add(sendTask);
                        }
                        var maxTimeToRun = TimeSpan.FromMilliseconds(timeoutInMs);
                        var doneSend = Task.WhenAll(sendList.ToArray());
                        await Task.WhenAny(doneSend, Task.Delay(maxTimeToRun));
                        stopwatch.Stop();
                        if (!doneSend.IsCompleted) {
                            var completed = sendList.Count(t => t.IsCompleted);
                            Assert.True(false, $"Only finished sending {completed} of {totalMessages} in {timeoutInMs} ms.");
                        }
                        await doneSend;
                        TestConfig.Log.Info(() => LogEvent.Create($">> done send, time Milliseconds:{stopwatch.ElapsedMilliseconds}"));
                        stopwatch.Restart();

                        using (var consumer = new Consumer(offset, router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                            var fetched = ImmutableList<Message>.Empty;
                            stopwatch.Restart();
                            while (fetched.Count < totalMessages) {
                                var doneFetch = consumer.FetchAsync(CancellationToken.None, totalMessages);
                                var delay = Task.Delay((int) Math.Max(0, maxTimeToRun.TotalMilliseconds - stopwatch.ElapsedMilliseconds));
                                await Task.WhenAny(doneFetch, delay);
                                if (delay.IsCompleted && !doneFetch.IsCompleted) {
                                    Assert.True(false, $"Received {fetched.Count} of {totalMessages} in {timeoutInMs} ms.");
                                }
                                var results = await doneFetch;
                                fetched = fetched.AddRange(results.Messages);
                            }
                            stopwatch.Stop();
                            TestConfig.Log.Info(() => LogEvent.Create($">> done Consume, time Milliseconds:{stopwatch.ElapsedMilliseconds}"));

                            Assert.AreEqual(fetched.Select(x => x.Value.ToUtf8String()).ToList(), expected); // Expected the message list in the correct order.
                            Assert.AreEqual(fetched.Count, totalMessages);
                        }
                    }
                });
            }
        }

        //[Test]
        //public async Task ConsumerShouldNotLoseMessageWhenBlocked()
        //{
        //    var testId = Guid.NewGuid().ToString();

        //    using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
        //        await router.TemporaryTopicAsync(async topicName => {
        //            using (var producer = new Producer(router)) {
        //                var offsets = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None);

        //                //create consumer with buffer size of 1 (should block upstream)
        //                using (var consumer = new OldConsumer(new ConsumerOptions(topicName, router) { ConsumerBufferSize = 1, MaxWaitTimeForMinimumBytes = TimeSpan.Zero },
        //                      offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offset)).ToArray()))
        //                {
        //                    for (var i = 0; i < 20; i++)
        //                    {
        //                        await producer.SendMessageAsync(new Message(i.ToString(), testId), topicName, CancellationToken.None);
        //                    }

        //                    for (var i = 0; i < 20; i++)
        //                    {
        //                        var result = consumer.Consume().Take(1).First();
        //                        Assert.AreEqual(result.Key.ToUtf8String(), testId);
        //                        Assert.AreEqual(result.Value.ToUtf8String(), i.ToString());
        //                    }
        //                }
        //            }
        //        });
        //    }
        //}

        //[Test]
        //public async Task ConsumerShouldMoveToNextAvailableOffsetWhenQueryingForNextMessage()
        //{
        //    const int expectedCount = 1000;
        //    var options = new KafkaOptions(TestConfig.IntegrationUri);

        //    using (var router = new Router(options)) {
        //        await router.TemporaryTopicAsync(async topicName => {
        //            using (var producer = new Producer(router)) {
        //                //get current offset and reset consumer to top of log
        //                var offsets = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None).ConfigureAwait(false);

        //                using (var consumerRouter = new Router(options))
        //                using (var consumer = new OldConsumer(new ConsumerOptions(topicName, consumerRouter) { MaxWaitTimeForMinimumBytes = TimeSpan.Zero },
        //                     offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offset)).ToArray()))
        //                {
        //                    Console.WriteLine("Sending {0} test messages", expectedCount);
        //                    var response = await producer.SendMessagesAsync(Enumerable.Range(0, expectedCount).Select(x => new Message(x.ToString())), topicName, CancellationToken.None);

        //                    Assert.False(response.Any(x => x.ErrorCode != (int)ErrorResponseCode.None), "Error occured sending test messages to server.");

        //                    var stream = consumer.Consume();

        //                    Console.WriteLine("Reading message back out from consumer.");
        //                    var data = stream.Take(expectedCount).ToList();

        //                    var consumerOffset = consumer.GetOffsetPosition().OrderBy(x => x.PartitionId).ToList();

        //                    var serverOffset = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None).ConfigureAwait(false);
        //                    var positionOffset = serverOffset.Select(x => new OffsetPosition(x.PartitionId, x.Offset))
        //                        .OrderBy(x => x.PartitionId)
        //                        .ToList();

        //                    Assert.AreEqual(consumerOffset, positionOffset, "The consumerOffset position should match the server offset position.");
        //                    Assert.AreEqual(data.Count, expectedCount, "We should have received 2000 messages from the server.");
        //                }
        //            }
        //        });
        //    }
        //}

        [Test]
        public async Task JoiningConsumerGroupOnMissingTopicFails()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var groupId = TestConfig.GroupId();

                    using (var consumer = await router.CreateGroupConsumerAsync(groupId, new ConsumerProtocolMetadata(topicName), TestConfig.IntegrationOptions.ConsumerConfiguration, TestConfig.IntegrationOptions.Encoders, CancellationToken.None)) {
                        Assert.AreEqual(consumer.GroupId, groupId);
                    }
                });
            }
        }

        [Test]
        public async Task ConsumerCanJoinConsumerGroup()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var groupId = TestConfig.GroupId();
                    using (var consumer = await router.CreateGroupConsumerAsync(groupId, new ConsumerProtocolMetadata(topicName), TestConfig.IntegrationOptions.ConsumerConfiguration, TestConfig.IntegrationOptions.Encoders, CancellationToken.None)) {
                        Assert.AreEqual(consumer.GroupId, groupId);
                        Assert.True(consumer.IsLeader);
                    }
                });
            }
        }

        private static async Task ProduceMessages(Router router, string topicName, string groupId, int totalMessages, IEnumerable<int> partitionIds = null)
        {
            var producer = new Producer(router, new ProducerConfiguration(batchSize: totalMessages / 10, batchMaxDelay: TimeSpan.FromMilliseconds(25), stopTimeout: TimeSpan.FromMilliseconds(50)));
            await producer.UsingAsync(async () => {
                var offsets = await router.GetOffsetsAsync(topicName, CancellationToken.None);
                var groupOffsets = await router.GetOffsetsAsync(groupId, topicName, CancellationToken.None);
                await router.GetGroupServerIdAsync(groupId, CancellationToken.None);
                foreach (var partitionId in partitionIds ?? new [] { 0 }) {
                    var offset = offsets.SingleOrDefault(o => o.PartitionId == partitionId);
                    var groupOffset = groupOffsets.SingleOrDefault(o => o.PartitionId == partitionId);
                    //await router.SendAsync(new GroupCoordinatorRequest(groupId), topicName, partitionId, CancellationToken.None).ConfigureAwait(false);
                    //var groupOffset = await router.GetOffsetAsync(groupId, topicName, partitionId, CancellationToken.None);

                    var missingMessages = Math.Max(0, totalMessages + groupOffset.Offset - offset.Offset + 1);
                    if (missingMessages > 0)
                    {
                        var messages = new List<Message>();
                        for (var i = 0; i < missingMessages; i++) {
                            messages.Add(new Message(i.ToString()));
                        }
                        await producer.SendAsync(messages, topicName, partitionId, CancellationToken.None);
                    }
                }
            });
        }

        [Category("Flaky")]
        [TestCase(1, 100)]
        [TestCase(2, 100)]
        [TestCase(10, 500)]
        public async Task CanConsumeFromGroup(int members, int batchSize)
        {
            var cancellation = new CancellationTokenSource();
            var totalMessages = 1000;
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName =>
                {
                    var groupId = TestConfig.GroupId();

                    await ProduceMessages(router, topicName, groupId, totalMessages, Enumerable.Range(0, members));

                    var fetched = 0;
                    var tasks = new List<Task>();
                    for (var index = 0; index < members; index++) {
                        tasks.Add(Task.Run(async () => {
                            var consumer = await router.CreateGroupConsumerAsync(groupId, new ConsumerProtocolMetadata(topicName), TestConfig.IntegrationOptions.ConsumerConfiguration, TestConfig.IntegrationOptions.Encoders, cancellation.Token);
                            await consumer.UsingAsync(async () => {
                                try {
                                    await consumer.FetchAsync(async (batch, token) => {
                                        router.Log.Info(() => LogEvent.Create($"Member {consumer.MemberId} starting batch of {batch.Messages.Count}"));
                                        foreach (var message in batch.Messages) {
                                            await Task.Delay(1); // do the work...
                                            batch.MarkSuccessful(message);
                                            if (Interlocked.Increment(ref fetched) >= totalMessages) {
                                                cancellation.Cancel();
                                                break;
                                            }
                                        }
                                        router.Log.Info(() => LogEvent.Create($"Member {consumer.MemberId} finished batch size {batch.Messages.Count} ({fetched} of {totalMessages})"));
                                    }, cancellation.Token, batchSize);
                                } catch (ObjectDisposedException) {
                                    // expected
                                }
                            });
                        }, CancellationToken.None));
                    }
//                    await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(TimeSpan.FromMinutes(1)));
                    await Task.WhenAll(tasks);
                    Assert.True(fetched >= totalMessages, $"fetched {fetched} of {totalMessages}");
                }, 10);
            }
        }
        
        [Category("Flaky")]
        [TestCase(2, 2)]
        [TestCase(3, 3)]
        public async Task CanConsumeFromMultipleGroups(int groups, int members)
        {
            using (var timed = new TimedCancellation(CancellationToken.None, TimeSpan.FromMinutes(1))) {
                var batchSize = 50;
                var totalMessages = 200;
                using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                    await router.TemporaryTopicAsync(async topicName =>
                    {
                        var allFetched = 0;
                        var tasks = new List<Task>();
                        var groupIdPrefix = TestConfig.GroupId();
                        for (var group = 0; group < groups; group++) {
                            var groupId = $"{groupIdPrefix}.{group}";
                            var groupFetched = 0;
                            await ProduceMessages(router, topicName, groupId, totalMessages, Enumerable.Range(0, members));

                            var cancellation = new CancellationTokenSource();
                            for (var index = 0; index < members; index++) {
                                tasks.Add(Task.Run(async () => {
                                    using (var merged = CancellationTokenSource.CreateLinkedTokenSource(timed.Token, cancellation.Token)) {
                                        var consumer = await router.CreateGroupConsumerAsync(groupId, new ConsumerProtocolMetadata(topicName), TestConfig.IntegrationOptions.ConsumerConfiguration, TestConfig.IntegrationOptions.Encoders, merged.Token);
                                        await consumer.UsingAsync(async () => {
                                            try {
                                                await consumer.FetchAsync(async (batch, token) => {
                                                    router.Log.Info(() => LogEvent.Create($"Member {consumer.MemberId} starting batch of {batch.Messages.Count}"));
                                                    foreach (var message in batch.Messages) {
                                                        await Task.Delay(1); // do the work...
                                                        batch.MarkSuccessful(message);
                                                        Interlocked.Increment(ref allFetched);
                                                        if (Interlocked.Increment(ref groupFetched) >= totalMessages) {
                                                            cancellation.Cancel();
                                                            break;
                                                        }
                                                    }
                                                    router.Log.Info(() => LogEvent.Create($"Member {consumer.MemberId} finished batch size {batch.Messages.Count} ({groupFetched} of {totalMessages})"));
                                                }, merged.Token, batchSize);
                                            } catch (ObjectDisposedException) {
                                                // ignore
                                            }
                                        });
                                    }
                                }, timed.Token));
                            }
                        }
                        await Task.WhenAll(tasks);
                        Assert.True(allFetched >= totalMessages * groups);
    //                    await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(TimeSpan.FromMinutes(1)));
                    }, 10);
                }
            }
        }

        #region helpers

        private void CheckMessages(List<Message> expected, IMessageBatch actual)
        {
            Assert.AreEqual(expected.Count(), actual.Messages.Count()); // Didn't get all messages

            foreach (var message in expected)
            {
                Assert.True(actual.Messages.Any(m => m.Value.SequenceEqual(message.Value)), "Didn't get the same messages");
            }
        }

        private List<Message> CreateTestMessages(int count, int messageSize)
        {
            var messages = new List<Message>();

            for (var i = 0; i < count; i++) {
                var value = new byte[messageSize];
                for (var j = 0; j < messageSize; j++) {
                    value[j] = 1;
                }

                messages.Add(new Message(new ArraySegment<byte>(value), new ArraySegment<byte>(new byte[0]), 0));
            }

            return messages;
        }

        #endregion

        // design unit TESTS to write:
        // dealing correctly with losing ownership
        // can read messages from assigned partition(s)
        // multiple partition assignment test
        // multiple member test
        // assignment priority is given to first assignor if multiple available
    }
}