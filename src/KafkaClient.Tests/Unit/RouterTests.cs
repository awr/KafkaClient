using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;
using Xunit;

#pragma warning disable 1998

namespace KafkaClient.Tests.Unit
{
    public class RouterTests
    {
        #region SendAsync

        [InlineData(ErrorCode.NOT_LEADER_FOR_PARTITION)]
        [InlineData(ErrorCode.LEADER_NOT_AVAILABLE)]
        [InlineData(ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE)]
        [InlineData(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION)]
        [Fact]
        public async Task ShouldTryToRefreshMataDataIfCanRecoverByRefreshMetadata(ErrorCode code)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = new TimeSpan(10);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageError(code, cacheExpiration));
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());

            await router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None);

            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
            Assert.Equal(scenario.Connection1[ApiKey.Fetch], 2);
        }

        [Theory]
        [InlineData(typeof(ConnectionException))]
        [InlineData(typeof(FetchOutOfRangeException))]
        [InlineData(typeof(RoutingException))]
        public async Task ShouldTryToRefreshMataDataIfOnExceptions(Type exceptionType)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageException(exceptionType, cacheExpiration));
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());

            await router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None);

            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
            Assert.Equal(scenario.Connection1[ApiKey.Fetch], 2);
        }

        [InlineData(typeof(Exception))]
        [InlineData(typeof(RequestException))]
        public async Task SendProtocolRequestShouldThrowException(Type exceptionType)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageException(exceptionType, cacheExpiration));
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            Assert.ThrowsAsync(exceptionType, async () => await router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None));
        }

        [Theory]
        [InlineData(ErrorCode.INVALID_FETCH_SIZE)]
        [InlineData(ErrorCode.MESSAGE_TOO_LARGE)]
        [InlineData(ErrorCode.OFFSET_METADATA_TOO_LARGE)]
        [InlineData(ErrorCode.OFFSET_OUT_OF_RANGE)]
        [InlineData(ErrorCode.UNKNOWN)]
        [InlineData(ErrorCode.STALE_CONTROLLER_EPOCH)]
        [InlineData(ErrorCode.REPLICA_NOT_AVAILABLE)]
        public async Task SendProtocolRequestShouldNotTryToRefreshMataDataIfCanNotRecoverByRefreshMetadata(
            ErrorCode code)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, FailedInFirstMessageError(code, cacheExpiration));
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            Assert.ThrowsAsync<RequestException>(async () => await router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None));
        }

        [Fact]
        public async Task ShouldUpdateMetadataOnce()
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = scenario.CreateRouter(cacheExpiration);

            scenario.Connection1.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            int numberOfCall = 1000;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None);
            }
            await Task.Delay(cacheExpiration);
            await Task.Delay(1);
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(new FetchRequest(), RoutingScenario.TestTopic, 0, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.Equal(scenario.Connection1[ApiKey.Fetch], numberOfCall);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1);
        }

        [Fact]
        public async Task ShouldRecoverUpdateMetadataForNewTopic()
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = scenario.CreateRouter(cacheExpiration);

            var fetchRequest = new FetchRequest();

            scenario.Connection1.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            int numberOfCall = 100;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, RoutingScenario.TestTopic, 0, CancellationToken.None);
            }

            scenario.Connection1.Add(ApiKey.Metadata, async _ => {
                var response = await RoutingScenario.DefaultMetadataResponse();
                return new MetadataResponse(response.brokers, response.topic_metadata.Select(t => new MetadataResponse.Topic("test2", t.topic_error_code, t.partition_metadata)));
            });

            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(fetchRequest, "test2", 0, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.Equal(scenario.Connection1[ApiKey.Fetch], numberOfCall);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
        }

        [Fact]
        public async Task ShouldRecoverFromFailureByUpdateMetadataOnce() //Do not debug this test !!
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(1000);
            var router = scenario.CreateRouter(cacheExpiration);

            int partitionId = 0;
            var fetchRequest = new FetchRequest();

            int numberOfCall = 100;
            long numberOfErrorSend = 0;
            TaskCompletionSource<int> x = new TaskCompletionSource<int>();
            Func<IRequestContext, Task<IResponse>> ShouldReturnNotLeaderForPartitionAndThenNoError = async _ =>
            {
                var log = TestConfig.Log;
                log.Debug(() => LogEvent.Create("FetchResponse Start "));
                if (!x.Task.IsCompleted)
                {
                    if (Interlocked.Increment(ref numberOfErrorSend) == numberOfCall)
                    {
                        await Task.Delay(cacheExpiration);
                        await Task.Delay(1);
                        x.TrySetResult(1);
                        log.Debug(() => LogEvent.Create("all is complete "));
                    }

                    await x.Task;
                    log.Debug(() => LogEvent.Create("SocketException "));
                    throw new ConnectionException(scenario.Connection1.Endpoint);
                }
                log.Debug(() => LogEvent.Create("Completed "));

                return new FetchResponse();
            };

            scenario.Connection1.Add(ApiKey.Fetch, ShouldReturnNotLeaderForPartitionAndThenNoError);
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());

            Task[] tasks = new Task[numberOfCall];

            for (int i = 0; i < numberOfCall; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, RoutingScenario.TestTopic, partitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.True(numberOfErrorSend > 1, "numberOfErrorSend");
            Assert.Equal(scenario.Connection1[ApiKey.Fetch], numberOfCall + numberOfErrorSend); // RequestCallCount(ApiKey.Fetch)
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2); // RequestCallCount(ApiKey.Metadata)
        }

        [Fact]
        public async Task ShouldRecoverFromConnectionExceptionByUpdateMetadataOnceFullScenario() //Do not debug this test !!
        {
            await ShouldRecoverByUpdateMetadataOnceFullScenario(
                FailedInFirstMessageException(typeof(ConnectionException), TimeSpan.Zero));
        }

        [Fact]
        public async Task ShouldRecoverFromFetchErrorByUpdateMetadataOnceFullScenario1()
        {
            await ShouldRecoverByUpdateMetadataOnceFullScenario(
                FailedInFirstMessageError(ErrorCode.LEADER_NOT_AVAILABLE, TimeSpan.Zero));
        }

        /// <summary>
        /// Do not debug this test !!
        /// </summary>
        private async Task ShouldRecoverByUpdateMetadataOnceFullScenario(Func<IRequestContext, Task<IResponse>> fetchResponse)
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.Zero;
            var router = scenario.CreateRouter(cacheExpiration);
            int partitionId = 0;
            var fetchRequest = new FetchRequest();

            CreateSuccessfulSendMock(scenario);

            //Send Successful Message
            await router.SendAsync(fetchRequest, RoutingScenario.TestTopic, partitionId, CancellationToken.None);

            Assert.Equal(scenario.Connection1[ApiKey.Fetch], 1); // "RequestCallCount(ApiKey.Fetch)"
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1); // "RequestCallCount(ApiKey.Metadata)"
            Assert.Equal(scenario.Connection2[ApiKey.Metadata], 0); // "RequestCallCount(ApiKey.Metadata)"

            scenario.Connection1.Add(ApiKey.Fetch, fetchResponse);
            //triger to update metadata
            scenario.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.MetaResponseWithException());
            scenario.Connection2.Add(ApiKey.Metadata, async _ => await RoutingScenario.MetadataResponseWithSingleBroker());

            //Reset variables
            scenario.Connection1[ApiKey.Fetch] = 0;
            scenario.Connection2[ApiKey.Fetch] = 0;
            scenario.Connection1[ApiKey.Metadata] = 0;
            scenario.Connection2[ApiKey.Metadata] = 0;

            //Send Successful Message that was recover from exception
            await router.SendAsync(fetchRequest, RoutingScenario.TestTopic, partitionId, CancellationToken.None);

            Assert.Equal(scenario.Connection1[ApiKey.Fetch], 1); // "RequestCallCount(ApiKey.Fetch)"
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1); // "RequestCallCount(ApiKey.Metadata)"

            Assert.Equal(scenario.Connection2[ApiKey.Fetch], 1); // "RequestCallCount(ApiKey.Fetch)"
            Assert.Equal(scenario.Connection2[ApiKey.Metadata], 1); // "RequestCallCount(ApiKey.Metadata)"
        }


        private static Func<IRequestContext, Task<IResponse>> FailedInFirstMessageError(ErrorCode errorCode, TimeSpan delay)
        {
            return async context => {
                if (context.CorrelationId == 1)
                {
                    await Task.Delay(delay);
                    await Task.Delay(1);
                    return new FetchResponse(new[] { new FetchResponse.Topic("foo", 1, 0, errorCode) });
                }
                return new FetchResponse();
            };
        }

        private Func<IRequestContext, Task<IResponse>> FailedInFirstMessageException(Type exceptionType, TimeSpan delay)
        {
            return async context =>
            {
                if (context.CorrelationId == 1)
                {
                    await Task.Delay(delay.Add(TimeSpan.FromMilliseconds(1)));
                    if (exceptionType == typeof(ConnectionException)) {
                        throw new ConnectionException("error test");
                    }
                    if (exceptionType == typeof (RequestException)) {
                        throw new RequestException(ApiKey.CreateTopics, ErrorCode.BROKER_NOT_AVAILABLE, TestConfig.ServerEndpoint());
                    }
                    if (exceptionType == typeof (FetchOutOfRangeException)) {
                        throw new FetchOutOfRangeException(new FetchRequest.Topic("name", 0, 0L), ErrorCode.BROKER_NOT_AVAILABLE, TestConfig.ServerEndpoint());
                    }
                    var args = new object[] { "error Test" };
                    throw (Exception)Activator.CreateInstance(exceptionType, args);
                }
                return new FetchResponse();
            };
        }

        private void CreateSuccessfulSendMock(RoutingScenario router)
        {
            router.Connection1.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            router.Connection1.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
            router.Connection2.Add(ApiKey.Fetch, ShouldReturnValidMessage);
            router.Connection2.Add(ApiKey.Metadata, async _ => await RoutingScenario.DefaultMetadataResponse());
        }

        private Task<IResponse> ShouldReturnValidMessage(IRequestContext context)
        {
            return Task.FromResult((IResponse)new FetchResponse());
        }

        #endregion

        #region Construction

        [Fact]
        public void CanConstruct()
        {
            var connections = CreateConnections(1);
            var factory = CreateFactory(connections);

            var result = new Router(new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), factory);

            Assert.NotNull(result);
        }

        [Fact]
        public void ConstructorThrowsException()
        {
            Assert.ThrowsAsync<ConnectionException>(() => Router.CreateAsync(new Uri("tcp://noaddress:1")));
        }

        [Fact]
        public async Task ConstructorShouldIgnoreUnresolvableUriWhenAtLeastOneIsGood()
        {
            var result = await Router.CreateAsync(new [] { new Uri("tcp://noaddress:1"), new Uri("tcp://localhost:1") });
        }

        #endregion

        #region Connection

        private IList<IConnection> CreateConnections(int count)
        {
            var connections = new List<IConnection>();
            for (var index = 0; index < count; index++) {
                var connection = Substitute.For<IConnection>();
                connection.Endpoint.Returns(new Endpoint(new IPEndPoint(IPAddress.Loopback, index), "tcp://127.0.0.1"));
                connections.Add(connection);
            }
            return connections;
        }

        private IConnectionFactory CreateFactory(IEnumerable<IConnection> connections)
        {
            var factory = Substitute.For<IConnectionFactory>();
            factory
                .Create(Arg.Any<Endpoint>(), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>())
                .Returns(_ => connections.SingleOrDefault(connection => connection.Endpoint == _.Arg<Endpoint>()));
            return factory;
        }

        [Fact]
        public async Task UsesFactoryToAddNewServers()
        {
            // Arrange
            var connections = CreateConnections(2);
            foreach (var connection in connections) {
                connection
                    .SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                    .Returns(_ => RoutingScenario.DefaultMetadataResponse());
            }
            var factory = CreateFactory(connections);
            var router = new Router(new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), factory);

            // Act
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            var topics = router.GetTopicMetadata(testTopic);

            // Assert
            factory.Received()
                   .Create(Arg.Is<Endpoint>(e => e.Ip.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>());
        }

        [Fact]
        public async Task UsesFactoryToAddNewServersFromGroups()
        {
            // Arrange
            var connections = CreateConnections(2);
            foreach (var connection in connections) {
                connection
                    .SendAsync(Arg.Any<GroupCoordinatorRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                    .Returns(_ => RoutingScenario.DefaultGroupCoordinatorResponse(1));
            }
            var factory = CreateFactory(connections);
            var router = new Router(new Endpoint(new IPEndPoint(IPAddress.Loopback, 1)), factory);

            // Act
            var testTopic = RoutingScenario.TestTopic;
            await router.GetGroupConnectionAsync(testTopic, CancellationToken.None);
            var broker = router.GetGroupConnection(testTopic);

            // Assert
            factory.Received()
                   .Create(Arg.Is<Endpoint>(e => e.Ip.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>());
        }

        #endregion

        #region Group

        [Fact]
        public async Task GetGroupShouldThrowWhenServerCollectionIsEmpty()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            Assert.Throws<RoutingException>(() => router.GetGroupConnection("unknown"));
        }

        [Fact]
        public async Task ShouldCycleThroughEachServerUntilOneIsFoundForGroup()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.GroupCoordinator, _ => { throw new Exception("some error"); });
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetGroupConnectionAsync(testTopic, CancellationToken.None);
            var result = router.GetGroupConnection(testTopic);
            Assert.NotNull(result);
            Assert.Equal(scenario.Connection1[ApiKey.GroupCoordinator], 1);
            Assert.Equal(scenario.Connection2[ApiKey.GroupCoordinator], 1);
        }

        [Fact]
        public async Task ShouldThrowIfCycleCouldNotConnectToAnyServerForGroup()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.GroupCoordinator, _ => { throw new Exception("some error"); });
            scenario.Connection2.Add(ApiKey.GroupCoordinator, _ => { throw new Exception("some error"); });
            var router = scenario.CreateRouter();

            Assert.ThrowsAsync<RoutingException>(async () => await router.GetGroupConnectionAsync(RoutingScenario.TestTopic, CancellationToken.None));

            Assert.Equal(scenario.Connection1[ApiKey.GroupCoordinator], 1);
            Assert.Equal(scenario.Connection2[ApiKey.GroupCoordinator], 1);
        }

        [Fact]
        public async Task ShouldReturnGroupFromCache()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetGroupConnectionAsync(testTopic, CancellationToken.None);
            var result1 = router.GetGroupConnection(testTopic);
            var result2 = router.GetGroupConnection(testTopic);

            Assert.Equal(scenario.Connection1[ApiKey.GroupCoordinator], 1);
            Assert.Equal(result1.GroupId, testTopic);
            Assert.Equal(result2.GroupId, testTopic);
        }

        [Fact]
        public async Task RefreshGroupMetadataShouldIgnoreCacheAndAlwaysCauseRequestAfterExpirationDate()
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = scenario.CreateRouter(cacheExpiration);
            var testTopic = RoutingScenario.TestTopic;
            await router.RefreshGroupConnectionAsync(testTopic, true, CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.GroupCoordinator], 1);
            await Task.Delay(cacheExpiration.Add(TimeSpan.FromMilliseconds(1))); // After cache is expired
            await router.RefreshGroupConnectionAsync(testTopic, true, CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.GroupCoordinator], 2);
        }

        [Fact]
        public async Task SimultaneouslyRefreshGroupMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();

            var testTopic = RoutingScenario.TestTopic;
            await Task.WhenAll(
                router.RefreshGroupConnectionAsync(testTopic, true, CancellationToken.None),
                router.RefreshGroupConnectionAsync(testTopic, true, CancellationToken.None));
            Assert.Equal(scenario.Connection1[ApiKey.GroupCoordinator], 2);
        }

        [Fact]
        public async Task SimultaneouslyGetGroupMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter(TimeSpan.FromMinutes(1)); // long timeout to avoid race condition on lock lasting longer than cache timeout

            var testTopic = RoutingScenario.TestTopic;
            await Task.WhenAll(
                router.GetGroupConnectionAsync(testTopic, CancellationToken.None), 
                router.GetGroupConnectionAsync(testTopic, CancellationToken.None));
            Assert.Equal(scenario.Connection1[ApiKey.GroupCoordinator], 1);
        }

        #endregion

        #region Topic Metadata

        [Theory]
        [InlineData(ErrorCode.LEADER_NOT_AVAILABLE)]
        [InlineData(ErrorCode.GROUP_LOAD_IN_PROGRESS)]
        [InlineData(ErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE)]
        public async Task ShouldRetryWhenReceiveAnRetryErrorCode(ErrorCode errorCode)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>())
                .Returns(x => CreateMetadataResponse(errorCode), x => CreateMetadataResponse(errorCode));

            var router = GetRouter(conn);
            var log = new MemoryLog();
            router.Log.ReturnsForAnyArgs(log);
            var response = await router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
                //_log.OnLogged(LogLevel.Warn, It.Is<LogEvent>(e => e.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
                //_log.OnLogged(LogLevel.Warn, It.Is<LogEvent>(e => e.Message.StartsWith("Failed metadata request on attempt 1: Will retry in")));
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
            });

            Assert.True(log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 0: Will retry in 00:00:00")));
            Assert.True(log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 1: Will retry in 00:00:00")));
            Assert.False(log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 2: Will retry in 00:00:00")));
            Assert.Equal(log.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt")), 3);
        }

        private static IRouter GetRouter(IConnection conn)
        {
            var router = Substitute.For<IRouter>();
            router.Configuration.ReturnsForAnyArgs(TestConfig.Options.RouterConfiguration);
            router.Connections.ReturnsForAnyArgs(new List<IConnection> { conn });
            return router;
        }

        [Fact]
        public async Task ShouldRetryWhenReceiveServerIdNegativeOne()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(x => CreateMetadataResponse(-1, "123", 1), x => CreateMetadataResponse(ErrorCode.NONE));

            var router = GetRouter(conn);
            var log = new MemoryLog();
            router.Log.ReturnsForAnyArgs(log);
            var response = await router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
                //_log.OnLogged.Invoke(LogLevel.Warn, It.Is<LogEvent>(e => e.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
            });

            Assert.True(log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
            Assert.Equal(log.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt")), 1);
        }

        [Fact]
        public void ShouldReturnWhenNoErrorReceived()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorCode.NONE));

            var router = GetRouter(conn);
            var source = new CancellationTokenSource();
            var response = router.GetMetadataAsync(new MetadataRequest("Test"), source.Token);
            source.Cancel();

            conn.Received(1).SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
        }

        [Fact]
        public void ShouldReturnWhenNoErrorReceivedAndTopicsNotSpecified()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorCode.NONE));

            var router = GetRouter(conn);
            var source = new CancellationTokenSource();
            var response = router.GetMetadataAsync(new MetadataRequest(), source.Token);
            source.Cancel();

            conn.Received(1).SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
        }

        [Theory]
        [InlineData(ErrorCode.UNKNOWN)]
        [InlineData(ErrorCode.INVALID_TOPIC_EXCEPTION)]
        [InlineData(ErrorCode.INVALID_REQUIRED_ACKS)]
        public async Task ShouldThrowExceptionWhenNotARetriableErrorCode(ErrorCode errorCode)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>()).Returns(x => CreateMetadataResponse(errorCode));

            var router = GetRouter(conn);
            Assert.ThrowsAsync<RequestException>(() => router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void ShouldThrowExceptionWhenHostIsMissing(string host)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>()).Returns(x => CreateMetadataResponse(1, host, 1));

            var router = GetRouter(conn);
            Assert.ThrowsAsync<ConnectionException>(() => router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void ShouldThrowExceptionWhenPortIsMissing(int port)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>()).Returns(x => CreateMetadataResponse(1, "123", port));

            var router = GetRouter(conn);
            Assert.ThrowsAsync<ConnectionException>(() => router.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None));
        }

#pragma warning disable 1998
        private Task<MetadataResponse> CreateMetadataResponse(int brokerId, string host, int port)
        {
            var tcs = new TaskCompletionSource<MetadataResponse>();
            tcs.SetResult(new MetadataResponse(new[] { new KafkaClient.Protocol.Server(brokerId, host, port) }, new MetadataResponse.Topic[] { }));
            return tcs.Task;
        }

        private async Task<MetadataResponse> CreateMetadataResponse(ErrorCode errorCode)
        {
            return new MetadataResponse(new KafkaClient.Protocol.Server[] { }, new[] { new MetadataResponse.Topic("Test", errorCode, new MetadataResponse.Partition[] { }) });
        }
#pragma warning restore 1998

        [Fact]
        public async Task ShouldCycleThroughEachServerUntilOneIsFound()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.Metadata, _ => { throw new Exception("some error"); });
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            var result = router.GetTopicMetadata(testTopic);
            Assert.NotNull(result);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1);
            Assert.Equal(scenario.Connection2[ApiKey.Metadata], 1);
        }

        [Fact]
        public async Task ShouldThrowIfCycleCouldNotConnectToAnyServer()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.Metadata, _ => { throw new ConnectionException("some error"); });
            scenario.Connection2.Add(ApiKey.Metadata, _ => { throw new ConnectionException("some error"); });
            var router = scenario.CreateRouter();

            await AssertAsync.Throws<ConnectionException>(() => router.GetTopicMetadataAsync(RoutingScenario.TestTopic, CancellationToken.None));

            // 3 attempts total, round robin so 2 to connection1, 1 to connection2
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
            Assert.Equal(scenario.Connection2[ApiKey.Metadata], 1);
        }

        [Fact]
        public async Task GetTopicShouldReturnTopic()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            await router.GetTopicMetadataAsync(RoutingScenario.TestTopic, CancellationToken.None);

            var result = router.GetTopicMetadata(RoutingScenario.TestTopic);
            Assert.Equal(result.topic, RoutingScenario.TestTopic);
        }

        [Fact]
        public void EmptyTopicMetadataShouldThrowException()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();

            Assert.Throws<RoutingException>(() => router.GetTopicMetadata("MissingTopic"));
        }

        [Fact]
        public async Task ShouldReturnTopicFromCache()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            var result1 = router.GetTopicMetadata(testTopic);
            var result2 = router.GetTopicMetadata(testTopic);

            Assert.Equal(1, router.GetTopicMetadata().Count);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1);
            Assert.Equal(result1.topic, testTopic);
            Assert.Equal(result2.topic, testTopic);
        }

        [Fact]
        public async Task ShouldThrowNoLeaderElectedForPartition()
        {
            var scenario = new RoutingScenario {
                MetadataResponse = RoutingScenario.MetadataResponseWithNotEndToElectLeader
            };

            var router = scenario.CreateRouter();
            await AssertAsync.Throws<RoutingException>(() => router.GetTopicMetadataAsync(RoutingScenario.TestTopic, CancellationToken.None));
            Assert.Equal(0, router.GetTopicMetadata().Count);
        }

        [Fact]
        public async Task ShouldReturnAllTopicsFromCache()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            var result1 = router.GetTopicMetadata();
            var result2 = router.GetTopicMetadata();

            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1);
            Assert.Equal(result1.Count, 1);
            var testTopic = RoutingScenario.TestTopic;
            Assert.Equal(result1[0].topic, testTopic);
            Assert.Equal(result2.Count, 1);
            Assert.Equal(result2[0].topic, testTopic);
        }

        [Fact]
        public async Task RefreshTopicMetadataShouldIgnoreCacheAndAlwaysCauseMetadataRequestAfterExpirationDate()
        {
            var scenario = new RoutingScenario();
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = scenario.CreateRouter(cacheExpiration);
            var testTopic = RoutingScenario.TestTopic;
            await router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1);
            await Task.Delay(cacheExpiration.Add(TimeSpan.FromMilliseconds(1))); // After cache is expired
            await router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
        }

        [Fact]
        public async Task RefreshTopicsMetadataShouldIgnoreCacheAndAlwaysCauseMetadataRequestAfterExpirationDate()
        {
            var scenario = new RoutingScenario { MetadataResponse = RoutingScenario.MetadataResponseWithTwoTopics };
            var cacheExpiration = TimeSpan.FromMilliseconds(100);
            var router = scenario.CreateRouter(cacheExpiration);
            var testTopics = new [] { RoutingScenario.TestTopic, RoutingScenario.TestTopic2 };
            await router.RefreshTopicMetadataAsync(testTopics, true, CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1);
            await Task.Delay(cacheExpiration.Add(TimeSpan.FromMilliseconds(1))); // After cache is expired
            await router.RefreshTopicMetadataAsync(testTopics, true, CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
        }

        [Fact]
        public async Task RefreshAllTopicMetadataShouldAlwaysDoRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1);
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
        }

        [Fact]
        public async Task SelectShouldChange()
        {
            var scenario = new RoutingScenario();

            var cacheExpiry = TimeSpan.FromMilliseconds(1);
            var router = scenario.CreateRouter(cacheExpiry);

            scenario.MetadataResponse = RoutingScenario.DefaultMetadataResponse;
            var testTopic = RoutingScenario.TestTopic;
            await router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None);

            var router1 = router.GetTopicConnection(testTopic, 0);

            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1);
            await Task.Delay(cacheExpiry.Add(TimeSpan.FromMilliseconds(1))); // After cache is expired
            scenario.MetadataResponse = RoutingScenario.MetadataResponseWithSingleBroker;
            await router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None);
            var router2 = router.GetTopicConnection(testTopic, 0);

            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
            Assert.Equal(router1.Connection.Endpoint, scenario.Connection1.Endpoint);
            Assert.Equal(router2.Connection.Endpoint, scenario.Connection2.Endpoint);
            Assert.NotEqual(router1.Connection.Endpoint, router2.Connection.Endpoint);
        }

        [Fact]
        public async Task SimultaneouslyRefreshTopicMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();

            var testTopic = RoutingScenario.TestTopic;
            await Task.WhenAll(
                router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None), 
                router.RefreshTopicMetadataAsync(testTopic, true, CancellationToken.None)
                ); //do not debug
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
        }

        [Fact]
        public async Task SimultaneouslyRefreshTopicsMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var scenario = new RoutingScenario { MetadataResponse = RoutingScenario.MetadataResponseWithTwoTopics };
            var router = scenario.CreateRouter();

            var testTopics = new [] { RoutingScenario.TestTopic, RoutingScenario.TestTopic2 };
            await Task.WhenAll(
                router.RefreshTopicMetadataAsync(testTopics, true, CancellationToken.None), 
                router.RefreshTopicMetadataAsync(testTopics, true, CancellationToken.None)
                ); //do not debug
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 2);
        }

        [Fact]
        public async Task SimultaneouslyGetTopicMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter(TimeSpan.FromMinutes(1)); // long timeout to avoid race condition on lock lasting longer than cache timeout

            var testTopic = RoutingScenario.TestTopic;
            await Task.WhenAll(
                router.GetTopicMetadataAsync(testTopic, CancellationToken.None), 
                router.GetTopicMetadataAsync(testTopic, CancellationToken.None)
                );
            Assert.Equal(scenario.Connection1[ApiKey.Metadata], 1);
        }

        #endregion

        #region SelectAsync

        [Fact]
        public async Task SelectExactPartitionShouldReturnRequestedPartition()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            var p0 = router.GetTopicConnection(testTopic, 0);
            var p1 = router.GetTopicConnection(testTopic, 1);

            Assert.Equal(p0.PartitionId, 0);
            Assert.Equal(p1.PartitionId, 1);
        }

        [Fact]
        public async Task SelectExactPartitionShouldThrowWhenPartitionDoesNotExist()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            Assert.Throws<RoutingException>(() => router.GetTopicConnection(testTopic, 3));
        }

        [Fact]
        public async Task SelectExactPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = await RoutingScenario.DefaultMetadataResponse();
            metadataResponse.topic_metadata.Clear();

            var scenario = new RoutingScenario();
#pragma warning disable 1998
            scenario.Connection1.Add(ApiKey.Metadata, async _ => metadataResponse);
#pragma warning restore 1998

            Assert.Throws<RoutingException>(() => scenario.CreateRouter().GetTopicConnection(RoutingScenario.TestTopic, 1));
        }

        [Fact]
        public async Task SelectExactPartitionShouldThrowWhenServerCollectionIsEmpty()
        {
            var metadataResponse = await RoutingScenario.DefaultMetadataResponse();
            metadataResponse = new MetadataResponse(topics: metadataResponse.topic_metadata);

            var scenario = new RoutingScenario();
#pragma warning disable 1998
            scenario.Connection1.Add(ApiKey.Metadata, async _ => metadataResponse);
#pragma warning restore 1998
            var router = scenario.CreateRouter();
            var testTopic = RoutingScenario.TestTopic;
            await router.GetTopicMetadataAsync(testTopic, CancellationToken.None);
            Assert.Throws<RoutingException>(() => router.GetTopicConnection(testTopic, 1));
        }

        #endregion

        #region GetTopicOffset

        [Fact]
        public async Task GetTopicOffsetsShouldQueryEachServer()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();

            var results = await router.GetOffsetsAsync(RoutingScenario.TestTopic, 2, -1, CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.Offsets], 1);
            Assert.Equal(scenario.Connection2[ApiKey.Offsets], 1);
            Assert.Equal(results.Count, 2);
        }

        [Fact]
        public async Task GetGroupOffsetsShouldQueryEachServer()
        {
            var scenario = new RoutingScenario();
            var router = scenario.CreateRouter();

            var results = await router.GetOffsetsAsync(RoutingScenario.TestTopic, RoutingScenario.TestGroup, CancellationToken.None);
            Assert.Equal(scenario.Connection1[ApiKey.OffsetFetch], 1);
            Assert.Equal(scenario.Connection2[ApiKey.OffsetFetch], 1);
            Assert.Equal(results.Count, 2);
        }

        [Fact]
        public async Task GetTopicOffsetsShouldThrowAnyException()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.Offsets, _ => { throw new BufferUnderRunException("test 99"); });
            var router = scenario.CreateRouter();

            await AssertAsync.Throws<BufferUnderRunException>(
                () => router.GetOffsetsAsync(RoutingScenario.TestTopic, 2, -1, CancellationToken.None),
                ex => ex.Message.Contains("test 99"));
        }

        [Fact]
        public async Task GetGroupOffsetsShouldThrowAnyException()
        {
            var scenario = new RoutingScenario();
            scenario.Connection1.Add(ApiKey.OffsetFetch, _ => { throw new BufferUnderRunException("test 99"); });
            var router = scenario.CreateRouter();

            await AssertAsync.Throws<BufferUnderRunException>(
                () => router.GetOffsetsAsync(RoutingScenario.TestGroup, RoutingScenario.TestTopic, CancellationToken.None),
                ex => ex.Message.Contains("test 99"));
        }

        #endregion
    }
}