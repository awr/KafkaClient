using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using Xunit;

namespace KafkaClient.Tests.Integration
{
    public class ConnectionTests
    {
        [Fact]
        public async Task EnsureTwoRequestsCanCallOneAfterAnother()
        {
            await Async.Using(
                await TestConfig.IntegrationOptions.CreateConnectionAsync(),
                async connection => {
                    var result1 = await connection.SendAsync(new MetadataRequest(), CancellationToken.None);
                    var result2 = await connection.SendAsync(new MetadataRequest(), CancellationToken.None);
                    Assert.Equal(result1.Errors.Count(code => code != ErrorCode.NONE), 0);
                    Assert.Equal(result2.Errors.Count(code => code != ErrorCode.NONE), 0);
                });
        }

        [Fact]
        public async Task EnsureAsyncRequestResponsesCorrelate()
        {
            await Async.Using(
                await TestConfig.IntegrationOptions.CreateConnectionAsync(),
                async connection => {
                    var result1 = connection.SendAsync(new MetadataRequest(), CancellationToken.None);
                    var result2 = connection.SendAsync(new MetadataRequest(), CancellationToken.None);
                    var result3 = connection.SendAsync(new MetadataRequest(), CancellationToken.None);

                    await Task.WhenAll(result1, result2, result3);

                    Assert.Equal(result1.Result.Errors.Count(code => code != ErrorCode.NONE), 0);
                    Assert.Equal(result2.Result.Errors.Count(code => code != ErrorCode.NONE), 0);
                    Assert.Equal(result3.Result.Errors.Count(code => code != ErrorCode.NONE), 0);
                });
        }

        [Theory]
        [InlineData(1, 10)]
        [InlineData(1, 50)]
        [InlineData(5, 10)]
        [InlineData(5, 10)]
        [InlineData(5, 200)]
        public async Task EnsureMultipleAsyncRequestsCanReadResponses(int senders, int totalRequests)
        {
            var requestsSoFar = 0;
            var requestTasks = new ConcurrentBag<Task<MetadataResponse>>();
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var singleResult = await router.Connections.First().SendAsync(new MetadataRequest(TestConfig.TopicName()), CancellationToken.None);
                    Assert.True(singleResult.topic_metadata.Count > 0);
                    Assert.True(singleResult.topic_metadata.First().partition_metadata.Count > 0);

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
                    Assert.Equal(results.Count, totalRequests);
                });
            }
        }

        [Fact]
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

                        Assert.Equal(result1.Result.responses.Count, 1);
                        Assert.True(result1.Result.responses.First().topic == topicName, "ProduceRequest did not return expected topic.");

                        Assert.True(result2.Result.topic_metadata.Count > 0);
                        Assert.True(result2.Result.topic_metadata.Any(x => x.topic == topicName), "MetadataRequest did not return expected topic.");

                        Assert.Equal(result3.Result.responses.Count, 1);
                        Assert.True(result3.Result.responses.First().topic == topicName, "OffsetRequest did not return expected topic.");

                        Assert.Equal(result4.Result.responses.Count, 1);
                        Assert.True(result4.Result.responses.First().topic == topicName, "FetchRequest did not return expected topic.");
                    }
                );
            }
        }
    }
}