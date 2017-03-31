using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;
using Xunit;

namespace KafkaClient.Tests.Unit
{
    public class ConsumerTests
    {
//        [Fact]
//        public async Task CancellationShouldInterruptConsumption()
//        {
//            var scenario = new RoutingScenario();
//#pragma warning disable 1998
//            scenario.Connection1.Add(ApiKey.Fetch, async context => new FetchResponse(new FetchResponse.Topic[] { }));
//#pragma warning restore 1998

//            var router = scenario.CreateRouter();
//            var consumer = new Consumer(router);
//            var tokenSrc = new CancellationTokenSource();

//            var consumeTask = consumer.FetchBatchAsync("TestTopic", 0, 0, tokenSrc.Token, 2048);

//            //wait until the fake broker is running and requesting fetches
//            var wait = await TaskTest.WaitFor(() => scenario.Connection1[ApiKey.Fetch] > 10);

//            tokenSrc.Cancel();

//            try
//            {
//                await consumeTask;
//                Assert.True(false, "Should throw OperationFailedException");
//            }
//            catch (AggregateException ex) when (ex.InnerException is OperationCanceledException)
//            {
//            }
//        }

        [Fact]
        public async Task EnsureConsumerDisposesRouter()
        {
            var router = Substitute.For<IRouter>();

            var consumer = new Consumer("test", 0, router, leaveRouterOpen: false);
            await consumer.DisposeAsync();
#pragma warning disable 4014
            router.Received(1).DisposeAsync();
#pragma warning restore 4014
        }

        [Fact]
        public async Task EnsureConsumerDoesNotDisposeRouter()
        {
            var router = Substitute.For<IRouter>();
            var consumer = new Consumer("test", 0, router);
            await consumer.DisposeAsync();
#pragma warning disable 4014
            router.DidNotReceive().DisposeAsync();
#pragma warning restore 4014
            router.DidNotReceive().Dispose();
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("unknown")]
        public async Task ConsumerThowsArgumentExceptionWhenMemberMetadataIsNotKnownByConsumer(string protocolType)
        {
            var router = Substitute.For<IRouter>();

            await AssertAsync.Throws<ArgumentOutOfRangeException>(
                () => router.CreateGroupConsumerAsync("group", protocolType, new ByteTypeMetadata("mine", new ArraySegment<byte>()), new ConsumerConfiguration(), ConnectionConfiguration.Defaults.Encoders(), CancellationToken.None),
                ex => ex.Message.StartsWith($"ProtocolType {protocolType} is unknown"));
        }

        [Fact]
        public async Task ConsumerDoesNotThowArgumentExceptionWhenMemberMetadataIsKnownByConsumer()
        {
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupConnectionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupConnection(_.Arg<string>(), 0, conn)));
            router.Configuration.Returns(new RouterConfiguration(refreshRetry: new Retry(1, TimeSpan.FromSeconds(2))));

            var configuration = new ConsumerConfiguration(coordinationRetry: Retry.AtMost(2));
            var encoders = ConnectionConfiguration.Defaults.Encoders();
            await AssertAsync.Throws<RequestException>(
                () => router.CreateGroupConsumerAsync("group", ConsumerEncoder.Protocol, new ByteTypeMetadata("mine", new ArraySegment<byte>()), configuration, encoders, CancellationToken.None));
        }

        [Fact]
        public async Task ConsumerSyncsGroupAfterJoining()
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupConnectionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupConnection(_.Arg<string>(), 0, conn)));            
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));

            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromSeconds(30), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.NONE, 1, protocol.protocol_name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new GroupConsumer(router, request.group_id, request.protocol_type, response)) {
                await Task.Delay(300);
            }

#pragma warning disable 4014
            router.Received().SyncGroupAsync(
                Arg.Is((SyncGroupRequest s) => s.group_id == request.group_id && s.member_id == memberId),
                Arg.Any<IRequestContext>(),
                Arg.Any<IRetry>(), 
                Arg.Any<CancellationToken>());
            conn.DidNotReceive().SendAsync(
                Arg.Is((HeartbeatRequest s) => s.group_id == request.group_id && s.member_id == memberId), 
                Arg.Any<CancellationToken>(),
                Arg.Any<IRequestContext>());
#pragma warning restore 4014
        }

        [Fact]
        public async Task ConsumerLeaderSyncsGroupWithAssignment()
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupConnectionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupConnection(_.Arg<string>(), 0, conn)));            
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));

            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromSeconds(30), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.NONE, 1, protocol.protocol_name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new GroupConsumer(router, request.group_id, request.protocol_type, response)) {
                await Task.Delay(300);
            }

#pragma warning disable 4014
            router.Received().SyncGroupAsync(
                Arg.Is((SyncGroupRequest s) => s.group_id == request.group_id && s.member_id == memberId && s.group_assignments.Count > 0),
                Arg.Any<IRequestContext>(),
                Arg.Any<IRetry>(), 
                Arg.Any<CancellationToken>());
#pragma warning restore 4014
        }

        [Fact]
        public async Task ConsumerFollowerSyncsGroupWithoutAssignment()
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupConnectionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupConnection(_.Arg<string>(), 0, conn)));            
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));

            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromSeconds(30), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.NONE, 1, protocol.protocol_name, "other" + memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new GroupConsumer(router, request.group_id, request.protocol_type, response)) {
                await Task.Delay(300);
            }

#pragma warning disable 4014
            router.Received().SyncGroupAsync(
                Arg.Is((SyncGroupRequest s) => s.group_id == request.group_id && s.member_id == memberId && s.group_assignments.Count == 0),
                Arg.Any<IRequestContext>(),
                Arg.Any<IRetry>(), 
                Arg.Any<CancellationToken>());
#pragma warning restore 4014
        }

        [Theory]
        [InlineData(0, 100, 0)]
        [InlineData(9, 100, 1000)]
        public async Task ConsumerHeartbeatsAtDesiredIntervals(int expectedHeartbeats, int heartbeatMilliseconds, int totalMilliseconds)
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));
            conn.SendAsync(Arg.Any<HeartbeatRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(_ => Task.FromResult(new HeartbeatResponse(ErrorCode.NONE)));

            var config = new ConsumerConfiguration(heartbeatTimeout: TimeSpan.FromMilliseconds(heartbeatMilliseconds * 2));
            var request = new JoinGroupRequest(TestConfig.GroupId(), config.GroupHeartbeat, "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.NONE, 1, protocol.protocol_name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new GroupConsumer(router, request.group_id, request.protocol_type, response, config)) {
                await Task.Delay(totalMilliseconds);
            }

            Assert.InRange(conn.ReceivedCalls()
                            .Count(c => {
                                if (c.GetMethodInfo().Name != nameof(Connection.SendAsync)) return false;
                                var s = c.GetArguments()[0] as HeartbeatRequest;
                                if (s == null) return false;
                                return s.group_id == request.group_id && s.member_id == memberId && s.generation_id == response.generation_id;
                            }), expectedHeartbeats - 1, expectedHeartbeats + 1);
        }

        [Theory]
        [InlineData(100, 700)]
        [InlineData(150, 700)]
        [InlineData(250, 700)]
        public async Task ConsumerHeartbeatsWithinTimeLimit(int heartbeatMilliseconds, int totalMilliseconds)
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetGroupConnectionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new GroupConnection(_.Arg<string>(), 0, conn)));

            var lastHeartbeat = DateTimeOffset.UtcNow;
            var heartbeatIntervals = ImmutableArray<TimeSpan>.Empty;
            conn.SendAsync(Arg.Any<HeartbeatRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(_ => {
                            heartbeatIntervals = heartbeatIntervals.Add(DateTimeOffset.UtcNow - lastHeartbeat);
                            lastHeartbeat = DateTimeOffset.UtcNow;
                            return Task.FromResult(new HeartbeatResponse(ErrorCode.NONE));
                        });
            var request = new JoinGroupRequest(TestConfig.GroupId(), TimeSpan.FromMilliseconds(heartbeatMilliseconds), "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.NONE, 1, protocol.protocol_name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });
            lastHeartbeat = DateTimeOffset.UtcNow;

            using (new GroupConsumer(router, request.group_id, request.protocol_type, response)) {
                await Task.Delay(totalMilliseconds);
            }

            foreach (var interval in heartbeatIntervals) {
                Assert.True((int)interval.TotalMilliseconds <= heartbeatMilliseconds);
            }
        }

        [Theory]
        [InlineData(100)]
        [InlineData(150)]
        [InlineData(250)]
        public async Task ConsumerHeartbeatsUntilDisposed(int heartbeatMilliseconds)
        {
            var protocol = new JoinGroupRequest.GroupProtocol(new ConsumerProtocolMetadata("mine"));
            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment(new [] { new TopicPartition("name", 0) }))));
            conn.SendAsync(Arg.Any<HeartbeatRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(_ => Task.FromResult(new HeartbeatResponse(ErrorCode.NETWORK_EXCEPTION)));

            var heartbeat = TimeSpan.FromMilliseconds(heartbeatMilliseconds);
            var config = new ConsumerConfiguration(heartbeatTimeout: heartbeat, coordinationRetry: Retry.Until(heartbeat, maximumDelay: TimeSpan.FromMilliseconds(50)));
            var request = new JoinGroupRequest(TestConfig.GroupId(), config.GroupHeartbeat, "", ConsumerEncoder.Protocol, new [] { protocol });
            var memberId = Guid.NewGuid().ToString("N");
            var response = new JoinGroupResponse(ErrorCode.NONE, 1, protocol.protocol_name, memberId, memberId, new []{ new JoinGroupResponse.Member(memberId, new ConsumerProtocolMetadata("mine")) });

            using (new GroupConsumer(router, request.group_id, request.protocol_type, response, config)) {
                await Task.Delay(heartbeatMilliseconds * 3);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                conn.DidNotReceive().SendAsync(
                    Arg.Is((LeaveGroupRequest s) => s.group_id == request.group_id && s.member_id == memberId),
                    Arg.Any<CancellationToken>(),
                    Arg.Any<IRequestContext>());
            }
            conn.Received().SendAsync(
                Arg.Is((LeaveGroupRequest s) => s.group_id == request.group_id && s.member_id == memberId),
                Arg.Any<CancellationToken>(),
                Arg.Any<IRequestContext>());
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            Assert.True(conn.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(Connection.SendAsync) && (c.GetArguments()[0] as HeartbeatRequest) != null) >= 2);
        }

        // design unit TESTS to write:
        // (async) locking is done correctly in the member
        // dealing correctly with losing ownership
        // multiple partition assignment test
        // initial group describe dictates what call happens next (based on server state)
        // add router tests for group metadata caching
    }
}