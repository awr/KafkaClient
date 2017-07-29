using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("CI")]
    public class AssignmentTests
    {
        [Test]
        public async Task AssignmentThrowsExceptionWhenStrategyNotFound()
        {
            var metadata = new ConsumerProtocolMetadata("mine", "unknown");

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorCode.NONE, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().MemberId, _.Arg<JoinGroupRequest>().MemberId, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().MemberId, metadata) })));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            try {
                using (var m = await router.CreateGroupConsumerAsync("group", metadata, CancellationToken.None)) {
                    var member = (GroupConsumer) m;
                    await member.SyncGroupAsync(CancellationToken.None);
                }
                Assert.True(false, "Should have thrown exception");
            } catch (ArgumentOutOfRangeException ex) when (ex.Message.StartsWith($"Unknown strategy {metadata.AssignmentStrategy} for ProtocolType {ConsumerEncoder.Protocol}")) {
                // not configured here
            }
        }

        [TestCase("type1")]
        [TestCase("type2")]
        public async Task AssignmentFoundWhenStrategyExists(string strategy)
        {
            var metadata = new ConsumerProtocolMetadata("mine", strategy);

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment())));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorCode.NONE, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().MemberId, _.Arg<JoinGroupRequest>().MemberId, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().MemberId, metadata) })));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            var assignor = Substitute.For<IMembershipAssignor>();
            assignor.AssignmentStrategy.ReturnsForAnyArgs(_ => strategy);
            var encoders = ConnectionConfiguration.Defaults.Encoders(new ConsumerEncoder(new SimpleAssignor(), assignor));

            using (var m = await router.CreateGroupConsumerAsync("group", metadata, ConsumerConfiguration.Default, encoders, CancellationToken.None)) {
                var member = (GroupConsumer) m;
                await member.SyncGroupAsync(CancellationToken.None);
            }
        }

        [TestCase("type1")]
        [TestCase("type2")]
        public async Task AssignorFoundWhenStrategyExists(string strategy)
        {
            var metadata = new ConsumerProtocolMetadata("mine", strategy);

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment())));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorCode.NONE, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().MemberId, _.Arg<JoinGroupRequest>().MemberId, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().MemberId, metadata) })));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            var assignor = Substitute.For<IMembershipAssignor>();
            assignor.AssignmentStrategy.ReturnsForAnyArgs(_ => strategy);
            var encoders = ConnectionConfiguration.Defaults.Encoders(new ConsumerEncoder(new SimpleAssignor(), assignor));
            using (var m = await router.CreateGroupConsumerAsync("group", metadata, ConsumerConfiguration.Default, encoders, CancellationToken.None)) {
                var member = (GroupConsumer) m;
                await member.SyncGroupAsync(CancellationToken.None);
            }
        }

        [Test]
        public async Task AssigmentSucceedsWhenStrategyExists()
        {
            var metadata = new ConsumerProtocolMetadata("mine");

            var router = Substitute.For<IRouter>();
            var conn = Substitute.For<IConnection>();
            router.GetConnectionAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(conn));
            router.SyncGroupAsync(Arg.Any<SyncGroupRequest>(), Arg.Any<IRequestContext>(), Arg.Any<IRetry>(), Arg.Any<CancellationToken>())
                  .Returns(_ => Task.FromResult(new SyncGroupResponse(ErrorCode.NONE, new ConsumerMemberAssignment())));
            conn.SendAsync(Arg.Any<JoinGroupRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new JoinGroupResponse(ErrorCode.NONE, 1, metadata.AssignmentStrategy, _.Arg<JoinGroupRequest>().MemberId, _.Arg<JoinGroupRequest>().MemberId, new []{ new JoinGroupResponse.Member(_.Arg<JoinGroupRequest>().MemberId, metadata) })));
            conn.SendAsync(Arg.Any<DescribeGroupsRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                  .Returns(_ => Task.FromResult(new DescribeGroupsResponse(null)));

            using (var m = await router.CreateGroupConsumerAsync("group", metadata, CancellationToken.None)) {
                var member = (GroupConsumer) m;
                await member.SyncGroupAsync(CancellationToken.None);
            }
        }

        [Test]
        public void InterfacesAreFormattedWithinProtocol()
        {
            var request = new SyncGroupRequest("group", 5, "member", new[] { new SyncGroupRequest.GroupAssignment("member", new ConsumerMemberAssignment(new[] { new TopicPartition("topic-foo", 0), new TopicPartition("topic", 1) })) });
            var formatted = request.ToString();
            Assert.True(formatted.Contains("topic:topic-foo"));
            Assert.True(formatted.Contains("partition_id:1"));
        }

        // design unit TESTS to write:
        // assignment priority is given to first assignor if multiple available
        // non-leader calls to get assignment data
        // leader does not call to get assignment data
        // sticky assignment ensures:
        // - existing assignments are assigned as before
        // - new assignments are assigned
        // - no member is unassigned (meaning it's possible that existing assignments are moved)
        // reassignment disposes open batches
        // reassignment enables new batches
        // reassignment before batches used does nothing
        // synching without changing assignment does not dispose
        // synching without changing assignment results in no new batches
    }
}