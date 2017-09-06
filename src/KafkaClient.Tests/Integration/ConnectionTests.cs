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
    [Category("Integration")]
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
                    Assert.AreEqual(result1.Errors.Count(code => code != ErrorCode.NONE), 0);
                    Assert.AreEqual(result2.Errors.Count(code => code != ErrorCode.NONE), 0);
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

                    Assert.AreEqual(result1.Result.Errors.Count(code => code != ErrorCode.NONE), 0);
                    Assert.AreEqual(result2.Result.Errors.Count(code => code != ErrorCode.NONE), 0);
                    Assert.AreEqual(result3.Result.Errors.Count(code => code != ErrorCode.NONE), 0);
                });
        }

        [TestCase(1, 10)]
        [TestCase(1, 50)]
        [TestCase(5, 10)]
        [TestCase(5, 10)]
        [TestCase(5, 200)]
        public async Task EnsureMultipleAsyncRequestsCanReadResponses(int senders, int totalRequests)
        {
            var requestsSoFar = 0;
            var requestTasks = new ConcurrentBag<Task<MetadataResponse>>();
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var singleResult = await router.Connections.First().SendAsync(new MetadataRequest(topicName), CancellationToken.None);
                    Assert.True(singleResult.TopicMetadata.Count > 0);
                    Assert.True(singleResult.TopicMetadata.First().PartitionMetadata.Count > 0);

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
                    Assert.AreEqual(results.Count, totalRequests);
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

                        Assert.AreEqual(result1.Result.Responses.Count, 1);
                        Assert.True(result1.Result.Responses.First().TopicName == topicName, "ProduceRequest did not return expected topic.");

                        Assert.True(result2.Result.TopicMetadata.Count > 0);
                        Assert.True(result2.Result.TopicMetadata.Any(x => x.TopicName == topicName), "MetadataRequest did not return expected topic.");

                        Assert.AreEqual(result3.Result.Responses.Count, 1);
                        Assert.True(result3.Result.Responses.First().TopicName == topicName, "OffsetRequest did not return expected topic.");

                        Assert.AreEqual(result4.Result.Responses.Count, 1);
                        Assert.True(result4.Result.Responses.First().TopicName == topicName, "FetchRequest did not return expected topic.");
                    }
                );
            }
        }
    }
}