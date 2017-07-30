using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [Category("Integration")]
    public class RouterTests
    {
        [Test]
        public async Task OffsetFetchRequestOfNonExistingGroupShouldReturnNoError()
        {
            //From documentation: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+ProtocolTests#AGuideToTheKafkaProtocol-OffsetFetchRequest
            //Note that if there is no offset associated with a topic-partition under that consumer group the broker does not set an error code
            //(since it is not really an error), but returns empty metadata and sets the offset field to -1.
            const int partitionId = 0;
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var request = new OffsetFetchRequest(Guid.NewGuid().ToString(), new TopicPartition(topicName, partitionId));
                    await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    var conn = router.GetTopicConnection(topicName, partitionId);

                    var response = await conn.Connection.SendAsync(request, CancellationToken.None);
                    var topic = response.Responses.FirstOrDefault();

                    Assert.NotNull(topic);
                    Assert.AreEqual(topic.Error, ErrorCode.NONE);
                    Assert.AreEqual(topic.Offset, -1);
                });
            }
        }

        [Test]
        public async Task OffsetCommitShouldStoreAndReturnSuccess()
        {
            const int partitionId = 0;
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    var conn = router.GetTopicConnection(topicName, partitionId);

                    // ensure the group exists
                    var groupId = TestConfig.GroupId();
                    var group = new GroupCoordinatorRequest(groupId);
                    var groupResponse = await conn.Connection.SendAsync(group, CancellationToken.None);
                    Assert.NotNull(groupResponse);
                    Assert.AreEqual(groupResponse.error_code, ErrorCode.NONE);

                    var commit = new OffsetCommitRequest(group.group_id, new []{ new OffsetCommitRequest.Topic(topicName, partitionId, 10, null) });
                    var response = await conn.Connection.SendAsync(commit, CancellationToken.None);
                    var topic = response.Responses.FirstOrDefault();

                    Assert.NotNull(topic);
                    Assert.AreEqual(topic.Error, ErrorCode.NONE);
                });
            }
        }

        [Test]
        public async Task OffsetCommitShouldStoreOffsetValue()
        {
            const int partitionId = 0;
            const long offset = 99;

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    var conn = router.GetTopicConnection(topicName, partitionId);

                    // ensure the group exists
                    var groupId = TestConfig.GroupId();
                    var group = new GroupCoordinatorRequest(groupId);
                    var groupResponse = await conn.Connection.SendAsync(group, CancellationToken.None);
                    Assert.NotNull(groupResponse);
                    Assert.AreEqual(groupResponse.error_code, ErrorCode.NONE);

                    var commit = new OffsetCommitRequest(group.group_id, new []{ new OffsetCommitRequest.Topic(topicName, partitionId, offset, null) });
                    var commitResponse = await conn.Connection.SendAsync(commit, CancellationToken.None);
                    var commitTopic = commitResponse.Responses.SingleOrDefault();

                    Assert.NotNull(commitTopic);
                    Assert.AreEqual(commitTopic.Error, ErrorCode.NONE);

                    var fetch = new OffsetFetchRequest(groupId, new TopicPartition(topicName, partitionId));
                    var fetchResponse = await conn.Connection.SendAsync(fetch, CancellationToken.None);
                    var fetchTopic = fetchResponse.Responses.SingleOrDefault();

                    Assert.NotNull(fetchTopic);
                    Assert.AreEqual(fetchTopic.Error, ErrorCode.NONE);
                    Assert.AreEqual(fetchTopic.Offset, offset);
                });
            }
        }

        [Test]
        public async Task OffsetCommitShouldStoreMetadata()
        {
            const int partitionId = 0;
            const long offset = 101;
            const string metadata = "metadata";

            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var conn = await router.GetTopicConnectionAsync(topicName, partitionId, CancellationToken.None);

                    // ensure the group exists
                    var groupId = TestConfig.GroupId();
                    var group = new GroupCoordinatorRequest(groupId);
                    var groupResponse = await conn.Connection.SendAsync(group, CancellationToken.None);
                    Assert.NotNull(groupResponse);
                    Assert.AreEqual(groupResponse.error_code, ErrorCode.NONE);

                    var commit = new OffsetCommitRequest(group.group_id, new []{ new OffsetCommitRequest.Topic(topicName, partitionId, offset, metadata) });
                    var commitResponse = await conn.Connection.SendAsync(commit, CancellationToken.None);
                    var commitTopic = commitResponse.Responses.SingleOrDefault();

                    Assert.NotNull(commitTopic);
                    Assert.AreEqual(commitTopic.Error, ErrorCode.NONE);

                    var fetch = new OffsetFetchRequest(groupId, commitTopic);
                    var fetchResponse = await conn.Connection.SendAsync(fetch, CancellationToken.None);
                    var fetchTopic = fetchResponse.Responses.SingleOrDefault();

                    Assert.NotNull(fetchTopic);
                    Assert.AreEqual(fetchTopic.Error, ErrorCode.NONE);
                    Assert.AreEqual(fetchTopic.Offset, offset);
                    Assert.AreEqual(fetchTopic.Metadata, metadata);
                });
            }
        }

        [Test]
        public async Task ConsumerMetadataRequestShouldReturnWithoutError()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var conn = await router.GetTopicConnectionAsync(topicName, 0, CancellationToken.None);

                    var groupId = TestConfig.GroupId();
                    var request = new GroupCoordinatorRequest(groupId);

                    var response = await conn.Connection.SendAsync(request, CancellationToken.None);

                    Assert.NotNull(response);
                    Assert.AreEqual(response.error_code, ErrorCode.NONE);
                });
            }
        }

        [Test]
        public async Task CanCreateAndDeleteTopics()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var response = await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    Assert.AreEqual(response.TopicError, ErrorCode.NONE);
                });
            }
        }
    }
}