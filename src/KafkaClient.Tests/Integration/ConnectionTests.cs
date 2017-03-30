using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    public class ConnectionTests
    {
        [Test]
        public async Task EnsureTwoRequestsCanCallOneAfterAnother()
        {
            await Async.Using(
                await TestConfig.IntegrationOptions.CreateConnectionAsync(),
                async connection => {
                    var result1 = await connection.SendAsync(new MetadataRequest(), CancellationToken.None);
                    var result2 = await connection.SendAsync(new MetadataRequest(), CancellationToken.None);
                    Assert.That(result1.Errors.Count(code => code != ErrorCode.NONE), Is.EqualTo(0));
                    Assert.That(result2.Errors.Count(code => code != ErrorCode.NONE), Is.EqualTo(0));
                });
        }

        [Test]
        public async Task EnsureAsyncRequestResponsesCorrelate()
        {
            await Async.Using(
                await TestConfig.IntegrationOptions.CreateConnectionAsync(),
                async connection => {
                    var result1 = connection.SendAsync(new MetadataRequest(), CancellationToken.None);
                    var result2 = connection.SendAsync(new MetadataRequest(), CancellationToken.None);
                    var result3 = connection.SendAsync(new MetadataRequest(), CancellationToken.None);

                    await Task.WhenAll(result1, result2, result3);

                    Assert.That(result1.Result.Errors.Count(code => code != ErrorCode.NONE), Is.EqualTo(0));
                    Assert.That(result2.Result.Errors.Count(code => code != ErrorCode.NONE), Is.EqualTo(0));
                    Assert.That(result3.Result.Errors.Count(code => code != ErrorCode.NONE), Is.EqualTo(0));
                });
        }

        [Test]
        public async Task EnsureMultipleAsyncRequestsCanReadResponses([Values(1, 5)] int senders, [Values(10, 50, 200)] int totalRequests)
        {
            var requestsSoFar = 0;
            var requestTasks = new ConcurrentBag<Task<MetadataResponse>>();
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var singleResult = await router.Connections.First().SendAsync(new MetadataRequest(TestConfig.TopicName()), CancellationToken.None);
                    Assert.That(singleResult.topic_metadata.Count, Is.GreaterThan(0));
                    Assert.That(singleResult.topic_metadata.First().partition_metadata.Count, Is.GreaterThan(0));

                    var senderTasks = new List<Task>();
                    for (var s = 0; s < senders; s++) {
                        senderTasks.Add(Task.Run(async () => {
                            while (true) {
                                await Task.Delay(1);
                                if (Interlocked.Increment(ref requestsSoFar) > totalRequests) break;
                                requestTasks.Add(router.Connections.First().SendAsync(new MetadataRequest(), CancellationToken.None));
                            }
                        }));
                    }

                    await Task.WhenAll(senderTasks);
                    var requests = requestTasks.ToArray();
                    await Task.WhenAll(requests);

                    var results = requests.Select(x => x.Result).ToList();
                    Assert.That(results.Count, Is.EqualTo(totalRequests));
                });
            }
        }

        [Test]
        public async Task EnsureDifferentTypesOfResponsesCanBeReadAsync()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(
                    async topicName => {
                        var result1 = router.Connections.First().SendAsync(RequestFactory.CreateProduceRequest(topicName, "test"), CancellationToken.None);
                        var result2 = router.Connections.First().SendAsync(new MetadataRequest(topicName), CancellationToken.None);
                        var result3 = router.Connections.First().SendAsync(RequestFactory.CreateOffsetRequest(topicName), CancellationToken.None);
                        var result4 = router.Connections.First().SendAsync(RequestFactory.CreateFetchRequest(topicName, 0), CancellationToken.None);

                        await Task.WhenAll(result1, result2, result3, result4);

                        Assert.That(result1.Result.responses.Count, Is.EqualTo(1));
                        Assert.That(result1.Result.responses.First().topic == topicName, Is.True, "ProduceRequest did not return expected topic.");

                        Assert.That(result2.Result.topic_metadata.Count, Is.GreaterThan(0));
                        Assert.That(result2.Result.topic_metadata.Any(x => x.topic == topicName), Is.True, "MetadataRequest did not return expected topic.");

                        Assert.That(result3.Result.responses.Count, Is.EqualTo(1));
                        Assert.That(result3.Result.responses.First().topic == topicName, Is.True, "OffsetRequest did not return expected topic.");

                        Assert.That(result4.Result.responses.Count, Is.EqualTo(1));
                        Assert.That(result4.Result.responses.First().topic == topicName, Is.True, "FetchRequest did not return expected topic.");
                    }
                );
            }
        }
    }
}