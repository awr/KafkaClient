using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using Xunit;

namespace KafkaClient.Tests.Integration
{
    public class RouterTests
    {
        [Fact]
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
                    var topic = response.responses.FirstOrDefault();

                    Assert.That(topic, Is.Not.Null);
                    Assert.That(topic.error_code, Is.EqualTo(ErrorCode.NONE));
                    Assert.That(topic.offset, Is.EqualTo(-1));
                });
            }
        }

        [Fact]
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
                    Assert.That(groupResponse, Is.Not.Null);
                    Assert.That(groupResponse.error_code, Is.EqualTo(ErrorCode.NONE));

                    var commit = new OffsetCommitRequest(group.group_id, new []{ new OffsetCommitRequest.Topic(topicName, partitionId, 10, null) });
                    var response = await conn.Connection.SendAsync(commit, CancellationToken.None);
                    var topic = response.responses.FirstOrDefault();

                    Assert.That(topic, Is.Not.Null);
                    Assert.That(topic.error_code, Is.EqualTo(ErrorCode.NONE));
                });
            }
        }

        [Fact]
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
                    Assert.That(groupResponse, Is.Not.Null);
                    Assert.That(groupResponse.error_code, Is.EqualTo(ErrorCode.NONE));

                    var commit = new OffsetCommitRequest(group.group_id, new []{ new OffsetCommitRequest.Topic(topicName, partitionId, offset, null) });
                    var commitResponse = await conn.Connection.SendAsync(commit, CancellationToken.None);
                    var commitTopic = commitResponse.responses.SingleOrDefault();

                    Assert.That(commitTopic, Is.Not.Null);
                    Assert.That(commitTopic.error_code, Is.EqualTo(ErrorCode.NONE));

                    var fetch = new OffsetFetchRequest(groupId, new TopicPartition(topicName, partitionId));
                    var fetchResponse = await conn.Connection.SendAsync(fetch, CancellationToken.None);
                    var fetchTopic = fetchResponse.responses.SingleOrDefault();

                    Assert.That(fetchTopic, Is.Not.Null);
                    Assert.That(fetchTopic.error_code, Is.EqualTo(ErrorCode.NONE));
                    Assert.That(fetchTopic.offset, Is.EqualTo(offset));
                });
            }
        }

        [Fact]
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
                    Assert.That(groupResponse, Is.Not.Null);
                    Assert.That(groupResponse.error_code, Is.EqualTo(ErrorCode.NONE));

                    var commit = new OffsetCommitRequest(group.group_id, new []{ new OffsetCommitRequest.Topic(topicName, partitionId, offset, metadata) });
                    var commitResponse = await conn.Connection.SendAsync(commit, CancellationToken.None);
                    var commitTopic = commitResponse.responses.SingleOrDefault();

                    Assert.That(commitTopic, Is.Not.Null);
                    Assert.That(commitTopic.error_code, Is.EqualTo(ErrorCode.NONE));

                    var fetch = new OffsetFetchRequest(groupId, commitTopic);
                    var fetchResponse = await conn.Connection.SendAsync(fetch, CancellationToken.None);
                    var fetchTopic = fetchResponse.responses.SingleOrDefault();

                    Assert.That(fetchTopic, Is.Not.Null);
                    Assert.That(fetchTopic.error_code, Is.EqualTo(ErrorCode.NONE));
                    Assert.That(fetchTopic.offset, Is.EqualTo(offset));
                    Assert.That(fetchTopic.metadata, Is.EqualTo(metadata));
                });
            }
        }

        [Fact]
        public async Task ConsumerMetadataRequestShouldReturnWithoutError()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var conn = await router.GetTopicConnectionAsync(topicName, 0, CancellationToken.None);

                    var groupId = TestConfig.GroupId();
                    var request = new GroupCoordinatorRequest(groupId);

                    var response = await conn.Connection.SendAsync(request, CancellationToken.None);

                    Assert.That(response, Is.Not.Null);
                    Assert.That(response.error_code, Is.EqualTo(ErrorCode.NONE));
                });
            }
        }

        [Fact]
        public async Task CanCreateAndDeleteTopics()
        {
            using (var router = await TestConfig.IntegrationOptions.CreateRouterAsync()) {
                await router.TemporaryTopicAsync(async topicName => {
                    var response = await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    Assert.That(response.topic_error_code, Is.EqualTo(ErrorCode.NONE));
                });
            }
        }
    }
}